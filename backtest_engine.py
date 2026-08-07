from __future__ import annotations

import argparse
import json
import math
import sqlite3
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd


OptionSide = Literal["CE", "PE"]
Bias = Literal["BULLISH", "BEARISH", "NEUTRAL"]
ExitReason = Literal["TARGET", "SL", "EOD", "DATA_CHANGE", "TIME_STOP", "OTHER"]
ContextMode = Literal["OFF", "VETO", "STRICT"]
PCRBasis = Literal["change_oi", "oi"]


@dataclass(frozen=True)
class BacktestConfig:
    """
    Backtest configuration for the strategy described in the supplied video.

    Source-derived strategy rules implemented here:
      * allow intraday data to form before trading;
      * use Overall PCR around ATM +/- 4 strikes AND ATM PCR;
      * bullish only when both PCRs are above 1; bearish only when both are below 1;
      * prefer ATM option for the simplified implementation;
      * enter only when NIFTY Futures comes back near its own VWAP and confirms a bounce/rejection;
      * place the initial option stop slightly beyond the option's day low;
      * support time-stop and PCR/data-change exits;
      * partial profit booking at 1R / 2R / 3R.

    Engineering choices (configurable, not claimed as exact video rules):
      * VWAP tolerance;
      * 2-point buffer below option day low;
      * exact time-stop = 150 minutes;
      * how strict optional market-context confirmation should be.
    """

    symbol: str = "NIFTY"
    entry_after: str = "10:15"  # video: give the market at least one hour after 09:15
    square_off: str = "15:20"

    # PCR rules
    pcr_basis: PCRBasis = "change_oi"
    pcr_window_each_side: int = 4
    pcr_upper: float = 1.0
    pcr_lower: float = 1.0
    pcr_strong_bullish: float = 1.50
    pcr_strong_bearish: float = 0.75
    require_strong_pcr: bool = False

    # Entry rules
    vwap_tolerance_pct: float = 0.001  # 0.10%; engineering parameter
    require_vwap_bounce: bool = True
    allow_spot_vwap_fallback: bool = False

    # Risk / sizing
    lot_size: int = 75
    default_lots: int = 1
    max_lots: int = 10
    risk_per_trade_rupees: float = 0.0  # 0 => use default_lots; >0 => risk-based sizing
    skip_if_one_lot_exceeds_risk: bool = True
    sl_buffer_points: float = 2.0

    # Partial exits from the video
    t1_r_multiple: float = 1.0
    t2_r_multiple: float = 2.0
    t3_r_multiple: float = 3.0
    t1_fraction: float = 0.50
    t2_fraction: float = 0.25
    trail_after_t1_to_entry: bool = True
    trail_after_t2_to_t1: bool = True

    # Exit rules
    time_stop_minutes: int = 150  # video says ~2 to 2.5 hours if no movement
    exit_on_bias_flip: bool = True

    # Optional broader market confirmation from the video
    context_mode: ContextMode = "OFF"
    required_context_signals: tuple[str, ...] = (
        "top10",
        "nifty_breadth",
        "market_breadth",
        "sector",
        "option_volume",
    )

    # Existing execution/backtest controls retained
    max_trades_per_day: int = 1
    allow_reentry: bool = False
    slippage_points: float = 0.0
    brokerage_per_trade: float = 0.0
    dte_mode: Literal["calendar"] = "calendar"

    # Retained only for analysis/reporting of entry delta, not for SL/target anymore.
    delta_buckets: dict[float, tuple[float, float]] = field(
        default_factory=lambda: {
            0.3: (30.0, 20.0),
            0.4: (40.0, 25.0),
            0.5: (50.0, 30.0),
        }
    )

    def __post_init__(self) -> None:
        normalized = {
            float(key): (float(value[0]), float(value[1]))
            for key, value in self.delta_buckets.items()
        }
        object.__setattr__(self, "delta_buckets", normalized)
        object.__setattr__(self, "pcr_basis", str(self.pcr_basis).lower())
        object.__setattr__(self, "context_mode", str(self.context_mode).upper())
        object.__setattr__(self, "required_context_signals", tuple(self.required_context_signals))

        if self.pcr_basis not in {"change_oi", "oi"}:
            raise ValueError("pcr_basis must be 'change_oi' or 'oi'.")
        if self.context_mode not in {"OFF", "VETO", "STRICT"}:
            raise ValueError("context_mode must be OFF, VETO, or STRICT.")
        if self.pcr_window_each_side < 0:
            raise ValueError("pcr_window_each_side must be >= 0.")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be > 0.")
        if self.default_lots <= 0 or self.max_lots <= 0:
            raise ValueError("default_lots and max_lots must be > 0.")
        if self.max_lots < self.default_lots:
            raise ValueError("max_lots must be >= default_lots.")
        if self.sl_buffer_points < 0:
            raise ValueError("sl_buffer_points must be >= 0.")
        if self.vwap_tolerance_pct < 0:
            raise ValueError("vwap_tolerance_pct must be >= 0.")
        if not (0 <= self.t1_fraction <= 1 and 0 <= self.t2_fraction <= 1):
            raise ValueError("Partial exit fractions must be between 0 and 1.")
        if self.t1_fraction + self.t2_fraction > 1:
            raise ValueError("t1_fraction + t2_fraction cannot exceed 1.")


@dataclass(frozen=True)
class MarketSignal:
    timestamp: datetime
    expiry: date
    atm_strike: float
    raw_bias: Bias
    bias: Bias
    strength: str
    overall_pcr: float | None
    atm_pcr: float | None
    total_call_basis: float
    total_put_basis: float
    atm_call_basis: float
    atm_put_basis: float
    n_strikes_used: int
    context_confirmations: int = 0
    context_oppositions: int = 0
    context_available: int = 0


@dataclass
class Position:
    trade_id: str
    date: date
    symbol: str
    expiry: date
    dte: int
    entry_time: datetime
    strike: float
    option_type: OptionSide
    entry_premium: float
    delta_at_entry: float
    delta_bucket: float

    # Strategy state
    option_day_low_at_entry: float
    initial_stop_price: float
    active_stop_price: float
    risk_points: float
    t1_price: float
    t2_price: float
    t3_price: float
    initial_quantity: int
    remaining_quantity: int
    t1_quantity: int
    t2_quantity: int
    t3_quantity: int
    t1_done: bool = False
    t2_done: bool = False

    # Signal context at entry
    pcr_ratio_at_entry: float | None = None  # backwards-compatible alias for overall PCR
    pcr_diff_at_entry: float = 0.0
    overall_pcr_at_entry: float | None = None
    atm_pcr_at_entry: float | None = None
    raw_bias_at_entry: str = "NEUTRAL"
    signal_strength_at_entry: str = "NEUTRAL"
    n_strikes_used: int = 0
    context_confirmations_at_entry: int = 0
    context_oppositions_at_entry: int = 0

    realized_pnl_points_qty: float = 0.0
    exit_legs: list[dict] = field(default_factory=list)


# -----------------------------
# Generic loading / normalization
# -----------------------------

def parse_hhmm(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def normalize_column_name(name: object) -> str:
    return "".join(char for char in str(name).strip().lower() if char.isalnum())


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = {
        "timestamp": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
        "time": "timestamp",
        "tradingsymbol": "tradingsymbol",
        "symbol": "symbol",
        "underlying": "symbol",
        "expiry": "expiry",
        "expirydate": "expiry",
        "strike": "strike",
        "strikeprice": "strike",
        "optiontype": "option_type",
        "type": "option_type",
        "instrumenttype": "option_type",
        "ltp": "ltp",
        "last": "ltp",
        "lastprice": "ltp",
        "close": "close",
        "premium": "ltp",
        "oi": "oi",
        "openinterest": "oi",
        "changeinoi": "change_oi",
        "changeoi": "change_oi",
        "oichange": "change_oi",
        "chginoi": "change_oi",
        "delta": "delta",
        "iv": "iv",
        "spot": "spot",
        "underlyingprice": "spot",
        "underlyingltp": "spot",
        "vwap": "vwap",
        "volume": "volume",
        "open": "open",
        "high": "high",
        "low": "low",
        # Futures aliases
        "future": "future",
        "futures": "future",
        "futureprice": "future",
        "futuresprice": "future",
        "futureltp": "future",
        "futuresltp": "future",
        # Optional market context
        "top10advancing": "top10_advancing",
        "top10declining": "top10_declining",
        "top10bias": "top10_bias",
        "niftyadvancing": "nifty_advancing",
        "niftydeclining": "nifty_declining",
        "niftybreadthbias": "nifty_breadth_bias",
        "marketadvancing": "market_advancing",
        "marketdeclining": "market_declining",
        "marketbreadthbias": "market_breadth_bias",
        "sectoradvancing": "sector_advancing",
        "sectordeclining": "sector_declining",
        "sectorbias": "sector_bias",
        "optionvolumebias": "option_volume_bias",
    }
    renamed = {}
    for column in frame.columns:
        renamed[column] = aliases.get(normalize_column_name(column), str(column).strip().lower())
    return frame.rename(columns=renamed)


def read_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".json":
        return pd.read_json(path)
    return pd.read_csv(path)


def load_option_chain(path: str | Path, config: BacktestConfig) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    required = ["timestamp", "expiry", "strike", "option_type", "ltp", config.pcr_basis]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError(f"Option-chain file missing columns: {', '.join(missing)}")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    frame["expiry"] = pd.to_datetime(frame["expiry"]).dt.date
    frame["trading_date"] = frame["timestamp"].dt.date
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    frame["ltp"] = pd.to_numeric(frame["ltp"], errors="coerce")
    frame["option_type"] = frame["option_type"].astype(str).str.upper().str.strip()

    for name in ("change_oi", "oi", "delta", "iv", "volume", "open", "high", "low", "close"):
        if name in frame.columns:
            frame[name] = pd.to_numeric(frame[name], errors="coerce")
    if "change_oi" in frame.columns:
        frame["change_oi"] = frame["change_oi"].fillna(0)
    if "delta" in frame.columns:
        frame["delta"] = frame["delta"].abs()

    return frame.dropna(subset=["timestamp", "expiry", "strike", "ltp"])


def _calculate_vwap_or_raise(frame: pd.DataFrame, price_col: str) -> pd.Series:
    """Calculate true running VWAP only when OHLCV is available."""
    if {"high", "low", "close", "volume"} <= set(frame.columns):
        typical = (
            pd.to_numeric(frame["high"], errors="coerce")
            + pd.to_numeric(frame["low"], errors="coerce")
            + pd.to_numeric(frame["close"], errors="coerce")
        ) / 3
        volume = pd.to_numeric(frame["volume"], errors="coerce").fillna(0)
        day = pd.to_datetime(frame["timestamp"]).dt.date
        pv = typical * volume
        denominator = volume.groupby(day).cumsum().replace(0, np.nan)
        return (pv.groupby(day).cumsum() / denominator).fillna(pd.to_numeric(frame[price_col], errors="coerce"))
    raise ValueError(
        "VWAP is missing. Provide a vwap column or OHLCV columns (high, low, close, volume). "
        "The new strategy must not replace VWAP with a simple average."
    )


def load_spot(path: str | Path) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    if "timestamp" not in frame.columns:
        raise ValueError("Spot file missing timestamp column.")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    if "spot" not in frame.columns:
        if "close" in frame.columns:
            frame["spot"] = pd.to_numeric(frame["close"], errors="coerce")
        elif "ltp" in frame.columns:
            frame["spot"] = pd.to_numeric(frame["ltp"], errors="coerce")
        else:
            raise ValueError("Spot file must contain spot, close, or ltp column.")
    frame["spot"] = pd.to_numeric(frame["spot"], errors="coerce")

    # Preserve an existing spot VWAP only for an explicitly enabled legacy fallback.
    if "vwap" in frame.columns:
        frame["spot_vwap"] = pd.to_numeric(frame["vwap"], errors="coerce")
    elif {"high", "low", "close", "volume"} <= set(frame.columns):
        frame["spot_vwap"] = _calculate_vwap_or_raise(frame, "spot")

    keep = [column for column in ["timestamp", "spot", "spot_vwap"] if column in frame.columns]
    return frame[keep].dropna(subset=["timestamp", "spot"]).sort_values("timestamp")


def load_futures(path: str | Path) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    if "timestamp" not in frame.columns:
        raise ValueError("Futures file missing timestamp column.")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)

    if "future" in frame.columns:
        frame["futures_price"] = pd.to_numeric(frame["future"], errors="coerce")
    elif "close" in frame.columns:
        frame["futures_price"] = pd.to_numeric(frame["close"], errors="coerce")
    elif "ltp" in frame.columns:
        frame["futures_price"] = pd.to_numeric(frame["ltp"], errors="coerce")
    elif "spot" in frame.columns:
        frame["futures_price"] = pd.to_numeric(frame["spot"], errors="coerce")
    else:
        raise ValueError("Futures file must contain future/futures_price, close, ltp, or spot column.")

    for name in ("open", "high", "low", "close", "volume"):
        if name in frame.columns:
            frame[name] = pd.to_numeric(frame[name], errors="coerce")

    if "vwap" in frame.columns:
        frame["futures_vwap"] = pd.to_numeric(frame["vwap"], errors="coerce")
    else:
        frame["futures_vwap"] = _calculate_vwap_or_raise(frame, "futures_price")

    rename = {
        "open": "futures_open",
        "high": "futures_high",
        "low": "futures_low",
        "close": "futures_close",
        "volume": "futures_volume",
    }
    frame = frame.rename(columns=rename)
    if "futures_close" not in frame.columns:
        frame["futures_close"] = frame["futures_price"]
    if "futures_high" not in frame.columns:
        frame["futures_high"] = frame["futures_price"]
    if "futures_low" not in frame.columns:
        frame["futures_low"] = frame["futures_price"]

    keep = [
        "timestamp",
        "futures_price",
        "futures_vwap",
        "futures_open",
        "futures_high",
        "futures_low",
        "futures_close",
        "futures_volume",
    ]
    keep = [column for column in keep if column in frame.columns]
    return frame[keep].dropna(subset=["timestamp", "futures_price", "futures_vwap"]).sort_values("timestamp")


def load_market_context(path: str | Path) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    if "timestamp" not in frame.columns:
        raise ValueError("Market-context file missing timestamp column.")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    numeric = [
        "top10_advancing",
        "top10_declining",
        "nifty_advancing",
        "nifty_declining",
        "market_advancing",
        "market_declining",
        "sector_advancing",
        "sector_declining",
    ]
    for column in numeric:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values("timestamp")


def merge_market_data(
    option_chain: pd.DataFrame,
    spot: pd.DataFrame,
    futures: pd.DataFrame | None,
    market_context: pd.DataFrame | None,
    config: BacktestConfig,
) -> pd.DataFrame:
    chain = option_chain.sort_values("timestamp")
    data = pd.merge_asof(chain, spot.sort_values("timestamp"), on="timestamp", direction="backward")

    if futures is not None:
        data = pd.merge_asof(data.sort_values("timestamp"), futures, on="timestamp", direction="backward")
    else:
        if not config.allow_spot_vwap_fallback:
            raise ValueError(
                "The new strategy requires NIFTY Futures VWAP. Pass --futures. "
                "Use --allow-spot-vwap-fallback only for legacy comparison, not strategy-faithful results."
            )
        if "spot_vwap" not in data.columns:
            raise ValueError("Spot VWAP fallback requested, but the spot file has no usable VWAP/OHLCV data.")
        data["futures_price"] = data["spot"]
        data["futures_vwap"] = data["spot_vwap"]
        data["futures_close"] = data["spot"]
        data["futures_high"] = data["spot"]
        data["futures_low"] = data["spot"]

    if market_context is not None:
        data = pd.merge_asof(
            data.sort_values("timestamp"),
            market_context.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )
    elif config.context_mode == "STRICT":
        raise ValueError("context_mode=STRICT requires --market-context.")

    return data.sort_values("timestamp")


# -----------------------------
# Strategy calculations
# -----------------------------

def days_to_expiry(trading_day: date, expiry: date, config: BacktestConfig) -> int:
    if config.dte_mode != "calendar":
        raise ValueError(f"Unsupported dte_mode: {config.dte_mode}")
    return max(0, (expiry - trading_day).days)


def selected_strikes(strikes: list[float], spot: float, n_each_side: int) -> list[float]:
    """Return ATM plus a fixed number of strikes above/below it."""
    if not strikes or math.isnan(spot):
        return []
    strikes = sorted(set(float(item) for item in strikes))
    atm_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - spot))
    start = max(0, atm_index - n_each_side)
    end = min(len(strikes), atm_index + n_each_side + 1)
    return strikes[start:end]


def nearest_atm_strike(strikes: list[float], spot: float) -> float | None:
    if not strikes or math.isnan(spot):
        return None
    return min(strikes, key=lambda strike: abs(float(strike) - spot))


def safe_pcr(put_value: float, call_value: float) -> float | None:
    # A PCR based on negative/zero aggregate inputs is not meaningful enough to trade automatically.
    if not np.isfinite(call_value) or not np.isfinite(put_value):
        return None
    if call_value <= 0 or put_value < 0:
        return None
    return float(put_value / call_value)


def _value_at(snapshot: pd.DataFrame, column: str) -> object | None:
    if column not in snapshot.columns:
        return None
    values = snapshot[column].dropna()
    if values.empty:
        return None
    return values.iloc[-1]


def _normalize_bias_value(value: object | None) -> Bias:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "NEUTRAL"
    text = str(value).strip().upper()
    if text in {"BULLISH", "BULL", "POSITIVE", "UP", "CALL", "CE", "1", "+1"}:
        return "BULLISH"
    if text in {"BEARISH", "BEAR", "NEGATIVE", "DOWN", "PUT", "PE", "-1"}:
        return "BEARISH"
    return "NEUTRAL"


def _count_bias(advancing: object | None, declining: object | None) -> Bias:
    if advancing is None or declining is None:
        return "NEUTRAL"
    try:
        a = float(advancing)
        d = float(declining)
    except (TypeError, ValueError):
        return "NEUTRAL"
    if not np.isfinite(a) or not np.isfinite(d):
        return "NEUTRAL"
    if a > d:
        return "BULLISH"
    if a < d:
        return "BEARISH"
    return "NEUTRAL"


def context_signals(snapshot: pd.DataFrame) -> dict[str, Bias]:
    """
    Optional broader confirmations described in the video.

    For option-volume confirmation the input should provide option_volume_bias directly,
    because the video does not define a single reproducible formula from raw call/put volume.
    """
    signals: dict[str, Bias] = {}

    direct = _normalize_bias_value(_value_at(snapshot, "top10_bias"))
    if direct == "NEUTRAL":
        direct = _count_bias(_value_at(snapshot, "top10_advancing"), _value_at(snapshot, "top10_declining"))
    signals["top10"] = direct

    direct = _normalize_bias_value(_value_at(snapshot, "nifty_breadth_bias"))
    if direct == "NEUTRAL":
        direct = _count_bias(_value_at(snapshot, "nifty_advancing"), _value_at(snapshot, "nifty_declining"))
    signals["nifty_breadth"] = direct

    direct = _normalize_bias_value(_value_at(snapshot, "market_breadth_bias"))
    if direct == "NEUTRAL":
        direct = _count_bias(_value_at(snapshot, "market_advancing"), _value_at(snapshot, "market_declining"))
    signals["market_breadth"] = direct

    direct = _normalize_bias_value(_value_at(snapshot, "sector_bias"))
    if direct == "NEUTRAL":
        direct = _count_bias(_value_at(snapshot, "sector_advancing"), _value_at(snapshot, "sector_declining"))
    signals["sector"] = direct

    signals["option_volume"] = _normalize_bias_value(_value_at(snapshot, "option_volume_bias"))
    return signals


def apply_context_confirmation(
    raw_bias: Bias,
    snapshot: pd.DataFrame,
    config: BacktestConfig,
) -> tuple[Bias, int, int, int]:
    if raw_bias == "NEUTRAL" or config.context_mode == "OFF":
        return raw_bias, 0, 0, 0

    signals = context_signals(snapshot)
    opposite: Bias = "BEARISH" if raw_bias == "BULLISH" else "BULLISH"
    available = {name: value for name, value in signals.items() if value != "NEUTRAL"}
    confirmations = sum(1 for value in available.values() if value == raw_bias)
    oppositions = sum(1 for value in available.values() if value == opposite)

    if config.context_mode == "VETO":
        if oppositions > 0:
            return "NEUTRAL", confirmations, oppositions, len(available)
        return raw_bias, confirmations, oppositions, len(available)

    # STRICT: every configured context signal must be present and agree with PCR direction.
    for name in config.required_context_signals:
        if signals.get(name, "NEUTRAL") != raw_bias:
            return "NEUTRAL", confirmations, oppositions, len(available)
    return raw_bias, confirmations, oppositions, len(available)


def market_signal(snapshot: pd.DataFrame, config: BacktestConfig) -> MarketSignal | None:
    if snapshot.empty:
        return None
    timestamp = pd.Timestamp(snapshot["timestamp"].iloc[0])
    spot_values = snapshot["spot"].dropna()
    if spot_values.empty:
        return None
    spot_value = float(spot_values.iloc[-1])

    expiries = sorted(snapshot["expiry"].dropna().unique())
    if not expiries:
        return None
    expiry = expiries[0]

    # Important correction: PCR is calculated only on the nearest expiry, not mixed across expiries.
    expiry_snapshot = snapshot[snapshot["expiry"] == expiry].copy()
    strikes = sorted(expiry_snapshot["strike"].dropna().unique())
    atm = nearest_atm_strike(strikes, spot_value)
    if atm is None:
        return None

    window = selected_strikes(strikes, spot_value, config.pcr_window_each_side)
    universe = expiry_snapshot[expiry_snapshot["strike"].isin(window)]
    basis = config.pcr_basis
    if basis not in universe.columns:
        return None

    call_total = float(universe.loc[universe["option_type"] == "CE", basis].fillna(0).sum())
    put_total = float(universe.loc[universe["option_type"] == "PE", basis].fillna(0).sum())
    overall_pcr = safe_pcr(put_total, call_total)

    atm_rows = expiry_snapshot[expiry_snapshot["strike"] == atm]
    atm_call = float(atm_rows.loc[atm_rows["option_type"] == "CE", basis].fillna(0).sum())
    atm_put = float(atm_rows.loc[atm_rows["option_type"] == "PE", basis].fillna(0).sum())
    atm_pcr = safe_pcr(atm_put, atm_call)

    raw_bias: Bias = "NEUTRAL"
    if overall_pcr is not None and atm_pcr is not None:
        if overall_pcr > config.pcr_upper and atm_pcr > config.pcr_upper:
            raw_bias = "BULLISH"
        elif overall_pcr < config.pcr_lower and atm_pcr < config.pcr_lower:
            raw_bias = "BEARISH"

    if raw_bias == "BULLISH" and overall_pcr is not None and overall_pcr >= config.pcr_strong_bullish:
        strength = "STRONG_BULLISH"
    elif raw_bias == "BEARISH" and overall_pcr is not None and overall_pcr <= config.pcr_strong_bearish:
        strength = "STRONG_BEARISH"
    elif raw_bias == "BULLISH":
        strength = "BULLISH"
    elif raw_bias == "BEARISH":
        strength = "BEARISH"
    else:
        strength = "NEUTRAL"

    final_bias, confirmations, oppositions, available = apply_context_confirmation(raw_bias, snapshot, config)
    if final_bias == "NEUTRAL" and raw_bias != "NEUTRAL" and config.context_mode != "OFF":
        strength = "CONTEXT_REJECTED"

    return MarketSignal(
        timestamp=timestamp.to_pydatetime(),
        expiry=expiry,
        atm_strike=float(atm),
        raw_bias=raw_bias,
        bias=final_bias,
        strength=strength,
        overall_pcr=overall_pcr,
        atm_pcr=atm_pcr,
        total_call_basis=call_total,
        total_put_basis=put_total,
        atm_call_basis=atm_call,
        atm_put_basis=atm_put,
        n_strikes_used=len(window),
        context_confirmations=confirmations,
        context_oppositions=oppositions,
        context_available=available,
    )


def futures_vwap_entry_ok(snapshot: pd.DataFrame, bias: Bias, config: BacktestConfig) -> bool:
    if bias not in {"BULLISH", "BEARISH"}:
        return False

    required = ["futures_vwap", "futures_close", "futures_high", "futures_low"]
    values: dict[str, float] = {}
    for column in required:
        raw = _value_at(snapshot, column)
        if raw is None:
            return False
        values[column] = float(raw)
        if not np.isfinite(values[column]):
            return False

    vwap = values["futures_vwap"]
    close = values["futures_close"]
    high = values["futures_high"]
    low = values["futures_low"]
    tolerance = config.vwap_tolerance_pct
    lower_band = vwap * (1 - tolerance)
    upper_band = vwap * (1 + tolerance)

    # Candle/range must interact with the VWAP band.
    touched = high >= lower_band and low <= upper_band
    if not touched:
        return False
    if not config.require_vwap_bounce:
        return True

    # Video: bullish setup => wait for bounce from futures VWAP; bearish => rejection from futures VWAP.
    if bias == "BULLISH":
        return close > vwap
    return close < vwap


def nearest_delta_bucket(delta: float, config: BacktestConfig) -> tuple[float, float, float]:
    buckets = sorted(config.delta_buckets)
    bucket = min(buckets, key=lambda item: abs(item - delta))
    old_target, old_sl = config.delta_buckets[bucket]
    return bucket, old_target, old_sl


def fallback_delta(option_type: OptionSide, strike: float, spot: float) -> float:
    if not spot or math.isnan(spot):
        return 0.5
    moneyness = abs(strike - spot) / spot
    if moneyness <= 0.003:
        return 0.5
    if moneyness <= 0.008:
        return 0.4
    return 0.3


def pick_entry_contract(snapshot: pd.DataFrame, option_type: OptionSide, atm_strike: float, expiry: date) -> pd.Series | None:
    """Pick the ATM contract. Do not compare a strike price with VWAP (different quantities)."""
    side = snapshot[
        (snapshot["option_type"] == option_type)
        & (snapshot["expiry"] == expiry)
    ].copy()
    if side.empty:
        return None
    side["atm_distance"] = (side["strike"] - atm_strike).abs()
    return side.sort_values(["atm_distance", "strike"]).iloc[0]


def _contract_key(expiry: date, strike: float, option_type: str) -> tuple[date, float, str]:
    return expiry, float(strike), str(option_type)


def update_option_day_lows(snapshot: pd.DataFrame, tracker: dict[tuple[date, float, str], float]) -> None:
    for _, row in snapshot.iterrows():
        if pd.isna(row.get("strike")) or pd.isna(row.get("expiry")):
            continue
        observed = row.get("low")
        if observed is None or pd.isna(observed):
            observed = row.get("ltp")
        if observed is None or pd.isna(observed):
            continue
        key = _contract_key(row["expiry"], float(row["strike"]), str(row["option_type"]))
        value = float(observed)
        tracker[key] = min(tracker.get(key, value), value)


def position_quantity(risk_points: float, config: BacktestConfig) -> int:
    if risk_points <= 0:
        return 0

    if config.risk_per_trade_rupees > 0:
        one_lot_risk = risk_points * config.lot_size
        if one_lot_risk <= 0:
            return 0
        lots = int(config.risk_per_trade_rupees // one_lot_risk)
        if lots < 1:
            if config.skip_if_one_lot_exceeds_risk:
                return 0
            lots = 1
        lots = min(lots, config.max_lots)
    else:
        lots = min(config.default_lots, config.max_lots)
    return int(lots * config.lot_size)


def split_quantities(total_quantity: int, config: BacktestConfig) -> tuple[int, int, int]:
    if total_quantity <= 0:
        return 0, 0, 0
    q1 = int(round(total_quantity * config.t1_fraction))
    q2 = int(round(total_quantity * config.t2_fraction))
    q1 = min(q1, total_quantity)
    q2 = min(q2, total_quantity - q1)
    q3 = total_quantity - q1 - q2
    return q1, q2, q3


def build_position(
    trading_day: date,
    symbol: str,
    row: pd.Series,
    signal: MarketSignal,
    option_day_low: float,
    config: BacktestConfig,
) -> Position | None:
    delta = row.get("delta")
    spot = float(row.get("spot", float("nan")))
    if pd.isna(delta) if delta is not None else True:
        delta = fallback_delta(str(row["option_type"]), float(row["strike"]), spot)
    delta = abs(float(delta))
    bucket, _, _ = nearest_delta_bucket(delta, config)

    entry = float(row["ltp"]) + config.slippage_points
    initial_stop = max(0.05, float(option_day_low) - config.sl_buffer_points)
    risk_points = entry - initial_stop
    if risk_points <= 0:
        return None

    quantity = position_quantity(risk_points, config)
    if quantity <= 0:
        return None
    q1, q2, q3 = split_quantities(quantity, config)

    pcr_diff = signal.total_call_basis - signal.total_put_basis
    dte = days_to_expiry(trading_day, signal.expiry, config)

    return Position(
        trade_id=str(uuid.uuid4()),
        date=trading_day,
        symbol=symbol,
        expiry=signal.expiry,
        dte=dte,
        entry_time=pd.Timestamp(row["timestamp"]).to_pydatetime(),
        strike=float(row["strike"]),
        option_type=str(row["option_type"]),
        entry_premium=entry,
        delta_at_entry=delta,
        delta_bucket=bucket,
        option_day_low_at_entry=float(option_day_low),
        initial_stop_price=initial_stop,
        active_stop_price=initial_stop,
        risk_points=risk_points,
        t1_price=entry + config.t1_r_multiple * risk_points,
        t2_price=entry + config.t2_r_multiple * risk_points,
        t3_price=entry + config.t3_r_multiple * risk_points,
        initial_quantity=quantity,
        remaining_quantity=quantity,
        t1_quantity=q1,
        t2_quantity=q2,
        t3_quantity=q3,
        pcr_ratio_at_entry=signal.overall_pcr,
        pcr_diff_at_entry=pcr_diff,
        overall_pcr_at_entry=signal.overall_pcr,
        atm_pcr_at_entry=signal.atm_pcr,
        raw_bias_at_entry=signal.raw_bias,
        signal_strength_at_entry=signal.strength,
        n_strikes_used=signal.n_strikes_used,
        context_confirmations_at_entry=signal.context_confirmations,
        context_oppositions_at_entry=signal.context_oppositions,
    )


# -----------------------------
# Position management
# -----------------------------

def _record_exit_leg(
    position: Position,
    timestamp: pd.Timestamp,
    quantity: int,
    fill_price: float,
    reason: str,
) -> None:
    quantity = min(int(quantity), position.remaining_quantity)
    if quantity <= 0:
        return
    pnl_points = float(fill_price) - position.entry_premium
    position.realized_pnl_points_qty += pnl_points * quantity
    position.remaining_quantity -= quantity
    position.exit_legs.append(
        {
            "timestamp": timestamp.to_pydatetime().isoformat(sep=" "),
            "reason": reason,
            "quantity": quantity,
            "fill_price": float(fill_price),
            "pnl_points": pnl_points,
        }
    )


def _finalize_position(
    position: Position,
    timestamp: pd.Timestamp,
    reason: ExitReason,
    config: BacktestConfig,
) -> dict:
    exited_quantity = sum(int(leg["quantity"]) for leg in position.exit_legs)
    if exited_quantity <= 0:
        weighted_exit = position.entry_premium
    else:
        weighted_exit = sum(float(leg["fill_price"]) * int(leg["quantity"]) for leg in position.exit_legs) / exited_quantity

    pnl_amount = position.realized_pnl_points_qty - config.brokerage_per_trade
    avg_pnl_points = position.realized_pnl_points_qty / position.initial_quantity if position.initial_quantity else 0.0

    return {
        "trade_id": position.trade_id,
        "date": position.date.isoformat(),
        "symbol": position.symbol,
        "expiry": position.expiry.isoformat(),
        "dte": position.dte,
        "entry_time": position.entry_time.isoformat(sep=" "),
        "exit_time": timestamp.to_pydatetime().isoformat(sep=" "),
        "strike": position.strike,
        "option_type": position.option_type,
        "entry_premium": position.entry_premium,
        "exit_premium": weighted_exit,
        "delta_at_entry": position.delta_at_entry,
        "delta_bucket": position.delta_bucket,
        # backwards-compatible columns; now reflect R-based structure, not delta-bucket SL/target
        "target_pts": position.t2_price - position.entry_premium,
        "sl_pts": position.entry_premium - position.initial_stop_price,
        "option_day_low_at_entry": position.option_day_low_at_entry,
        "initial_stop_price": position.initial_stop_price,
        "final_stop_price": position.active_stop_price,
        "risk_points": position.risk_points,
        "t1_price": position.t1_price,
        "t2_price": position.t2_price,
        "t3_price": position.t3_price,
        "initial_quantity": position.initial_quantity,
        "exit_reason": reason,
        "pnl_points": avg_pnl_points,
        "pnl_amount": pnl_amount,
        "pcr_ratio_at_entry": position.pcr_ratio_at_entry,
        "pcr_diff_at_entry": position.pcr_diff_at_entry,
        "overall_pcr_at_entry": position.overall_pcr_at_entry,
        "atm_pcr_at_entry": position.atm_pcr_at_entry,
        "raw_bias_at_entry": position.raw_bias_at_entry,
        "signal_strength_at_entry": position.signal_strength_at_entry,
        "n_strikes_used": position.n_strikes_used,
        "context_confirmations_at_entry": position.context_confirmations_at_entry,
        "context_oppositions_at_entry": position.context_oppositions_at_entry,
        "holding_minutes": (timestamp.to_pydatetime() - position.entry_time).total_seconds() / 60,
        "exit_legs_json": json.dumps(position.exit_legs),
    }


class OptionsBacktester:
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.entry_after = parse_hhmm(config.entry_after)
        self.square_off = parse_hhmm(config.square_off)

    def run(
        self,
        option_chain: pd.DataFrame,
        spot: pd.DataFrame,
        futures: pd.DataFrame | None = None,
        market_context: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, dict]:
        data = merge_market_data(option_chain, spot, futures, market_context, self.config)
        trades: list[dict] = []
        for trading_day, day_data in data.groupby("trading_date", sort=True):
            trades.extend(self._run_day(trading_day, day_data))
        trades_frame = pd.DataFrame(trades)
        return trades_frame, performance_metrics(trades_frame)

    def _run_day(self, trading_day: date, day_data: pd.DataFrame) -> list[dict]:
        trades: list[dict] = []
        open_position: Position | None = None
        entries_taken = 0
        day_lows: dict[tuple[date, float, str], float] = {}

        for timestamp, snapshot in day_data.groupby("timestamp", sort=True):
            timestamp = pd.Timestamp(timestamp)
            current_time = timestamp.time()
            if snapshot.empty:
                continue

            update_option_day_lows(snapshot, day_lows)

            if open_position is not None:
                exit_trade = self._maybe_exit(open_position, snapshot, timestamp)
                if exit_trade is not None:
                    trades.append(exit_trade)
                    open_position = None
                if current_time >= self.square_off:
                    continue

            if current_time < self.entry_after or current_time >= self.square_off:
                continue
            if open_position is not None:
                continue
            if entries_taken >= self.config.max_trades_per_day:
                continue
            if trades and not self.config.allow_reentry:
                continue

            entry = self._maybe_enter(trading_day, snapshot, day_lows)
            if entry is not None:
                open_position = entry
                entries_taken += 1

        if open_position is not None:
            last_timestamp = pd.Timestamp(day_data["timestamp"].max())
            last_snapshot = day_data[day_data["timestamp"] == last_timestamp]
            current = self._current_contract_premium(open_position, last_snapshot)
            if current is not None and open_position.remaining_quantity > 0:
                fill = float(current) - self.config.slippage_points
                _record_exit_leg(open_position, last_timestamp, open_position.remaining_quantity, fill, "EOD")
                trades.append(_finalize_position(open_position, last_timestamp, "EOD", self.config))
        return trades

    def _maybe_enter(
        self,
        trading_day: date,
        snapshot: pd.DataFrame,
        day_lows: dict[tuple[date, float, str], float],
    ) -> Position | None:
        signal = market_signal(snapshot, self.config)
        if signal is None or signal.bias == "NEUTRAL":
            return None

        if self.config.require_strong_pcr:
            expected = "STRONG_BULLISH" if signal.bias == "BULLISH" else "STRONG_BEARISH"
            if signal.strength != expected:
                return None

        if not futures_vwap_entry_ok(snapshot, signal.bias, self.config):
            return None

        option_type: OptionSide = "CE" if signal.bias == "BULLISH" else "PE"
        row = pick_entry_contract(snapshot, option_type, signal.atm_strike, signal.expiry)
        if row is None:
            return None

        key = _contract_key(signal.expiry, float(row["strike"]), option_type)
        option_day_low = day_lows.get(key)
        if option_day_low is None:
            option_day_low = float(row["ltp"])

        return build_position(
            trading_day=trading_day,
            symbol=self.config.symbol,
            row=row,
            signal=signal,
            option_day_low=option_day_low,
            config=self.config,
        )

    def _maybe_exit(self, position: Position, snapshot: pd.DataFrame, timestamp: pd.Timestamp) -> dict | None:
        current = self._current_contract_premium(position, snapshot)
        if current is None:
            return None
        current = float(current)

        if timestamp.time() >= self.square_off:
            fill = current - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.remaining_quantity, fill, "EOD")
            return _finalize_position(position, timestamp, "EOD", self.config)

        # 1) Hard / trailing stop first.
        if current <= position.active_stop_price:
            fill = current - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.remaining_quantity, fill, "SL")
            return _finalize_position(position, timestamp, "SL", self.config)

        # 2) Partial profit booking: 50% at 1R, 25% at 2R, final 25% at 3R.
        if not position.t1_done and position.t1_quantity > 0 and current >= position.t1_price:
            fill = position.t1_price - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.t1_quantity, fill, "T1_1R")
            position.t1_done = True
            if self.config.trail_after_t1_to_entry:
                position.active_stop_price = max(position.active_stop_price, position.entry_premium)

        if not position.t2_done and position.t2_quantity > 0 and current >= position.t2_price:
            fill = position.t2_price - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.t2_quantity, fill, "T2_2R")
            position.t2_done = True
            if self.config.trail_after_t2_to_t1:
                position.active_stop_price = max(position.active_stop_price, position.t1_price)

        if position.remaining_quantity > 0 and current >= position.t3_price:
            fill = position.t3_price - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.remaining_quantity, fill, "T3_3R")
            return _finalize_position(position, timestamp, "TARGET", self.config)

        # 3) Data-change exit: close if PCR/context flips to the opposite direction.
        if self.config.exit_on_bias_flip:
            signal = market_signal(snapshot, self.config)
            if signal is not None:
                opposite = (
                    position.option_type == "CE" and signal.bias == "BEARISH"
                ) or (
                    position.option_type == "PE" and signal.bias == "BULLISH"
                )
                if opposite:
                    fill = current - self.config.slippage_points
                    _record_exit_leg(position, timestamp, position.remaining_quantity, fill, "DATA_CHANGE")
                    return _finalize_position(position, timestamp, "DATA_CHANGE", self.config)

        # 4) Time stop: video says exit after ~2-2.5 hours if there is no movement.
        # Engineering definition of "no movement": first 1R target has not been reached.
        holding_minutes = (timestamp.to_pydatetime() - position.entry_time).total_seconds() / 60
        if holding_minutes >= self.config.time_stop_minutes and not position.t1_done:
            fill = current - self.config.slippage_points
            _record_exit_leg(position, timestamp, position.remaining_quantity, fill, "TIME_STOP")
            return _finalize_position(position, timestamp, "TIME_STOP", self.config)

        return None

    @staticmethod
    def _current_contract_premium(position: Position, snapshot: pd.DataFrame) -> float | None:
        rows = snapshot[
            (snapshot["strike"] == position.strike)
            & (snapshot["option_type"] == position.option_type)
            & (snapshot["expiry"] == position.expiry)
        ]
        if rows.empty:
            return None
        return float(rows.iloc[0]["ltp"])


# -----------------------------
# Metrics / persistence
# -----------------------------

def performance_metrics(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl_per_trade": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "largest_win": 0.0,
            "largest_loss": 0.0,
            "avg_holding_minutes": 0.0,
            "by_delta_bucket": {},
            "by_day_of_week": {},
            "by_dte": {},
            "by_signal_strength": {},
            "by_exit_reason": {},
        }

    trades = trades.copy()
    pnl = pd.to_numeric(trades["pnl_amount"], errors="coerce").fillna(0)
    equity = pnl.cumsum()
    drawdown = equity - equity.cummax()
    trades["entry_dt"] = pd.to_datetime(trades["entry_time"])
    trades["day_of_week"] = trades["entry_dt"].dt.day_name()

    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    return {
        "total_trades": int(len(trades)),
        "win_rate": float((pnl > 0).mean()),
        "total_pnl": float(pnl.sum()),
        "avg_pnl_per_trade": float(pnl.mean()),
        "profit_factor": profit_factor,
        "max_drawdown": float(drawdown.min()),
        "largest_win": float(pnl.max()),
        "largest_loss": float(pnl.min()),
        "avg_holding_minutes": float(pd.to_numeric(trades["holding_minutes"], errors="coerce").mean()),
        "by_delta_bucket": grouped_pnl(trades, "delta_bucket"),
        "by_day_of_week": grouped_pnl(trades, "day_of_week"),
        "by_dte": grouped_pnl(trades, "dte"),
        "by_signal_strength": grouped_pnl(trades, "signal_strength_at_entry"),
        "by_exit_reason": grouped_pnl(trades, "exit_reason"),
    }


def grouped_pnl(trades: pd.DataFrame, column: str) -> dict:
    if column not in trades.columns:
        return {}
    result = {}
    for key, group in trades.groupby(column, dropna=False):
        pnl = pd.to_numeric(group["pnl_amount"], errors="coerce").fillna(0)
        result[str(key)] = {
            "trades": int(len(group)),
            "win_rate": float((pnl > 0).mean()),
            "total_pnl": float(pnl.sum()),
            "avg_pnl": float(pnl.mean()),
        }
    return result


def _sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series.dtype):
        return "REAL"
    return "TEXT"


def _ensure_sqlite_columns(conn: sqlite3.Connection, table: str, frame: pd.DataFrame) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for column in frame.columns:
        if column in existing:
            continue
        sql_type = _sqlite_type(frame[column])
        conn.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {sql_type}')


def write_sqlite(db_path: str | Path, trades: pd.DataFrame, metrics: dict, config: BacktestConfig) -> str:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                config_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO backtest_runs VALUES (?, ?, ?, ?)",
            (
                run_id,
                datetime.now().isoformat(sep=" "),
                json.dumps(config_to_json(config)),
                json.dumps(metrics),
            ),
        )

        if not trades.empty:
            rows = trades.copy()
            rows.insert(0, "run_id", run_id)

            # Preserve the existing mock_trades table and migrate it in place by adding new columns.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mock_trades (
                    run_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    date TEXT,
                    entry_time TEXT,
                    exit_time TEXT,
                    symbol TEXT,
                    expiry TEXT,
                    strike REAL,
                    option_type TEXT,
                    entry_premium REAL,
                    exit_premium REAL,
                    delta_at_entry REAL,
                    delta_bucket REAL,
                    target_pts REAL,
                    sl_pts REAL,
                    exit_reason TEXT,
                    pnl_points REAL,
                    pnl_amount REAL,
                    pcr_ratio_at_entry REAL,
                    pcr_diff_at_entry REAL,
                    n_strikes_used INTEGER,
                    holding_minutes REAL,
                    PRIMARY KEY (run_id, trade_id)
                )
                """
            )
            _ensure_sqlite_columns(conn, "mock_trades", rows)
            rows.to_sql("mock_trades", conn, if_exists="append", index=False)
    return run_id


def config_to_json(config: BacktestConfig) -> dict:
    payload = asdict(config)
    payload["delta_buckets"] = {str(key): value for key, value in config.delta_buckets.items()}
    payload["required_context_signals"] = list(config.required_context_signals)
    return payload


def write_outputs(out_dir: str | Path, trades: pd.DataFrame, metrics: dict, config: BacktestConfig) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_path / "mock_trades.csv", index=False)
    (out_path / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (out_path / "config.json").write_text(json.dumps(config_to_json(config), indent=2), encoding="utf-8")


def load_config(path: str | Path | None, args: argparse.Namespace) -> BacktestConfig:
    payload = {}
    if path:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))

    keys = (
        "symbol",
        "entry_after",
        "square_off",
        "pcr_basis",
        "pcr_window_each_side",
        "pcr_upper",
        "pcr_lower",
        "pcr_strong_bullish",
        "pcr_strong_bearish",
        "require_strong_pcr",
        "vwap_tolerance_pct",
        "allow_spot_vwap_fallback",
        "sl_buffer_points",
        "lot_size",
        "default_lots",
        "max_lots",
        "risk_per_trade_rupees",
        "time_stop_minutes",
        "context_mode",
        "max_trades_per_day",
        "allow_reentry",
        "slippage_points",
        "brokerage_per_trade",
    )
    for key in keys:
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    return BacktestConfig(**payload)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backtest the NIFTY intraday PCR + NIFTY Futures VWAP option-buying strategy."
    )
    parser.add_argument("--option-chain", required=True, help="CSV/JSON option-chain snapshots.")
    parser.add_argument("--spot", required=True, help="CSV/JSON NIFTY spot data used for ATM selection.")
    parser.add_argument("--futures", help="CSV/JSON NIFTY Futures OHLCV/VWAP data. Required for strategy-faithful runs.")
    parser.add_argument(
        "--market-context",
        help="Optional CSV/JSON top-stock/breadth/sector/option-volume confirmations from the video.",
    )
    parser.add_argument("--config", help="Optional JSON config file.")
    parser.add_argument("--out", default="data/backtests/latest", help="Output directory for CSV/JSON reports.")
    parser.add_argument("--sqlite", default="data/backtests/backtests.sqlite", help="SQLite database path.")

    parser.add_argument("--symbol")
    parser.add_argument("--entry-after")
    parser.add_argument("--square-off")
    parser.add_argument("--pcr-basis", choices=["change_oi", "oi"])
    parser.add_argument("--pcr-window-each-side", type=int)
    parser.add_argument("--pcr-upper", type=float)
    parser.add_argument("--pcr-lower", type=float)
    parser.add_argument("--pcr-strong-bullish", type=float)
    parser.add_argument("--pcr-strong-bearish", type=float)
    parser.add_argument("--require-strong-pcr", action="store_true", default=None)
    parser.add_argument("--vwap-tolerance-pct", type=float)
    parser.add_argument("--allow-spot-vwap-fallback", action="store_true", default=None)
    parser.add_argument("--sl-buffer-points", type=float)
    parser.add_argument("--lot-size", type=int)
    parser.add_argument("--default-lots", type=int)
    parser.add_argument("--max-lots", type=int)
    parser.add_argument("--risk-per-trade-rupees", type=float)
    parser.add_argument("--time-stop-minutes", type=int)
    parser.add_argument("--context-mode", choices=["OFF", "VETO", "STRICT"])
    parser.add_argument("--max-trades-per-day", type=int)
    parser.add_argument("--allow-reentry", action="store_true", default=None)
    parser.add_argument("--slippage-points", type=float)
    parser.add_argument("--brokerage-per-trade", type=float)
    args = parser.parse_args()

    config = load_config(args.config, args)
    option_chain = load_option_chain(args.option_chain, config)
    spot = load_spot(args.spot)
    futures = load_futures(args.futures) if args.futures else None
    market_context = load_market_context(args.market_context) if args.market_context else None

    trades, metrics = OptionsBacktester(config).run(option_chain, spot, futures, market_context)
    write_outputs(args.out, trades, metrics, config)
    run_id = write_sqlite(args.sqlite, trades, metrics, config)
    print(
        json.dumps(
            {
                "run_id": run_id,
                "metrics": metrics,
                "output_dir": args.out,
                "sqlite": args.sqlite,
                "strategy": "PCR_OVERALL_AND_ATM__NIFTY_FUTURES_VWAP__ATM_OPTION__DAY_LOW_SL__PARTIAL_R_EXITS",
                "futures_vwap_source": "futures" if futures is not None else "LEGACY_SPOT_FALLBACK",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
