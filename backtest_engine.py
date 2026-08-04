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
ExitReason = Literal["TARGET", "SL", "EOD", "OTHER"]


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str = "NIFTY"
    entry_after: str = "11:30"
    square_off: str = "15:20"
    pcr_upper: float = 1.10
    pcr_lower: float = 0.90
    lot_size: int = 75
    max_trades_per_day: int = 1
    allow_reentry: bool = False
    slippage_points: float = 0.0
    brokerage_per_trade: float = 0.0
    dte_mode: Literal["calendar"] = "calendar"
    delta_buckets: dict[float, tuple[float, float]] = field(
        default_factory=lambda: {
            0.3: (30.0, 20.0),
            0.4: (40.0, 25.0),
            0.5: (50.0, 30.0),
        }
    )

    def __post_init__(self) -> None:
        normalized = {float(key): (float(value[0]), float(value[1])) for key, value in self.delta_buckets.items()}
        object.__setattr__(self, "delta_buckets", normalized)


@dataclass
class Position:
    trade_id: str
    date: date
    symbol: str
    expiry: date
    entry_time: datetime
    strike: float
    option_type: OptionSide
    entry_premium: float
    delta_at_entry: float
    delta_bucket: float
    target_pts: float
    sl_pts: float
    pcr_ratio_at_entry: float | None
    pcr_diff_at_entry: float
    n_strikes_used: int


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


def load_option_chain(path: str | Path) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    required = ["timestamp", "expiry", "strike", "option_type", "ltp", "change_oi"]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise ValueError(f"Option-chain file missing columns: {', '.join(missing)}")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    frame["expiry"] = pd.to_datetime(frame["expiry"]).dt.date
    frame["trading_date"] = frame["timestamp"].dt.date
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    frame["ltp"] = pd.to_numeric(frame["ltp"], errors="coerce")
    frame["change_oi"] = pd.to_numeric(frame["change_oi"], errors="coerce").fillna(0)
    frame["option_type"] = frame["option_type"].astype(str).str.upper().str.strip()
    if "delta" in frame.columns:
        frame["delta"] = pd.to_numeric(frame["delta"], errors="coerce").abs()
    if "oi" in frame.columns:
        frame["oi"] = pd.to_numeric(frame["oi"], errors="coerce")
    return frame.dropna(subset=["timestamp", "expiry", "strike", "ltp"])


def load_spot(path: str | Path) -> pd.DataFrame:
    frame = normalize_columns(read_table(path))
    if "timestamp" not in frame.columns:
        raise ValueError("Spot file missing timestamp column.")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(None)
    if "spot" not in frame.columns:
        if "close" in frame.columns:
            frame["spot"] = pd.to_numeric(frame["close"], errors="coerce")
        else:
            raise ValueError("Spot file must contain spot or close column.")
    frame["spot"] = pd.to_numeric(frame["spot"], errors="coerce")
    if "vwap" in frame.columns:
        frame["vwap"] = pd.to_numeric(frame["vwap"], errors="coerce")
    else:
        frame["vwap"] = compute_running_vwap(frame)
    return frame.dropna(subset=["timestamp", "spot"]).sort_values("timestamp")


def compute_running_vwap(frame: pd.DataFrame) -> pd.Series:
    if {"high", "low", "close", "volume"} <= set(frame.columns):
        typical = (
            pd.to_numeric(frame["high"], errors="coerce")
            + pd.to_numeric(frame["low"], errors="coerce")
            + pd.to_numeric(frame["close"], errors="coerce")
        ) / 3
        volume = pd.to_numeric(frame["volume"], errors="coerce").fillna(0)
        day = pd.to_datetime(frame["timestamp"]).dt.date
        pv = typical * volume
        vwap = pv.groupby(day).cumsum() / volume.groupby(day).cumsum().replace(0, np.nan)
        return vwap.fillna(pd.to_numeric(frame["spot"], errors="coerce"))
    return pd.to_numeric(frame["spot"], errors="coerce").groupby(pd.to_datetime(frame["timestamp"]).dt.date).expanding().mean().reset_index(level=0, drop=True)


def merge_spot(option_chain: pd.DataFrame, spot: pd.DataFrame) -> pd.DataFrame:
    chain = option_chain.sort_values("timestamp")
    spot_data = spot[["timestamp", "spot", "vwap"]].sort_values("timestamp")
    return pd.merge_asof(chain, spot_data, on="timestamp", direction="backward")


def days_to_expiry(trading_day: date, expiry: date, config: BacktestConfig) -> int:
    if config.dte_mode != "calendar":
        raise ValueError(f"Unsupported dte_mode: {config.dte_mode}")
    return max(0, (expiry - trading_day).days)


def selected_strikes(strikes: list[float], spot: float, n: int) -> list[float]:
    if not strikes:
        return []
    atm_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - spot))
    start = max(0, atm_index - n)
    end = min(len(strikes), atm_index + n + 1)
    return strikes[start:end]


def pcr_bias(total_call_change_oi: float, total_put_change_oi: float, config: BacktestConfig) -> tuple[str, float | None, float]:
    diff = total_call_change_oi - total_put_change_oi
    if total_call_change_oi == 0:
        ratio = None
    else:
        ratio = total_put_change_oi / total_call_change_oi
    if ratio is not None and ratio >= config.pcr_upper:
        return "BULLISH", ratio, diff
    if ratio is not None and ratio <= config.pcr_lower:
        return "BEARISH", ratio, diff
    if ratio is None:
        if diff < 0:
            return "BULLISH", ratio, diff
        if diff > 0:
            return "BEARISH", ratio, diff
    return "NEUTRAL", ratio, diff


def nearest_delta_bucket(delta: float, config: BacktestConfig) -> tuple[float, float, float]:
    buckets = sorted(config.delta_buckets)
    bucket = min(buckets, key=lambda item: abs(item - delta))
    target, sl = config.delta_buckets[bucket]
    return bucket, target, sl


def fallback_delta(option_type: OptionSide, strike: float, spot: float) -> float:
    if not spot or math.isnan(spot):
        return 0.5
    moneyness = abs(strike - spot) / spot
    if moneyness <= 0.003:
        return 0.5
    if moneyness <= 0.008:
        return 0.4
    return 0.3


def pick_entry_contract(snapshot: pd.DataFrame, option_type: OptionSide, vwap: float) -> pd.Series | None:
    side = snapshot[snapshot["option_type"] == option_type].copy()
    if side.empty:
        return None
    side["vwap_distance"] = (side["strike"] - vwap).abs()
    return side.sort_values(["vwap_distance", "strike"]).iloc[0]


def build_position(
    trading_day: date,
    symbol: str,
    expiry: date,
    row: pd.Series,
    option_type: OptionSide,
    config: BacktestConfig,
    pcr_ratio: float | None,
    pcr_diff: float,
    n_strikes: int,
) -> Position:
    delta = row.get("delta")
    if pd.isna(delta) if delta is not None else True:
        delta = fallback_delta(option_type, float(row["strike"]), float(row["spot"]))
    delta = abs(float(delta))
    bucket, target, sl = nearest_delta_bucket(delta, config)
    entry = float(row["ltp"]) + config.slippage_points
    return Position(
        trade_id=str(uuid.uuid4()),
        date=trading_day,
        symbol=symbol,
        expiry=expiry,
        entry_time=row["timestamp"].to_pydatetime(),
        strike=float(row["strike"]),
        option_type=option_type,
        entry_premium=entry,
        delta_at_entry=delta,
        delta_bucket=bucket,
        target_pts=target,
        sl_pts=sl,
        pcr_ratio_at_entry=pcr_ratio,
        pcr_diff_at_entry=pcr_diff,
        n_strikes_used=n_strikes,
    )


def close_position(position: Position, timestamp: pd.Timestamp, premium: float, reason: ExitReason, config: BacktestConfig) -> dict:
    exit_premium = float(premium) - config.slippage_points
    pnl_points = exit_premium - position.entry_premium
    pnl_amount = pnl_points * config.lot_size - config.brokerage_per_trade
    return {
        **asdict(position),
        "date": position.date.isoformat(),
        "expiry": position.expiry.isoformat(),
        "entry_time": position.entry_time.isoformat(sep=" "),
        "exit_time": timestamp.to_pydatetime().isoformat(sep=" "),
        "exit_premium": exit_premium,
        "exit_reason": reason,
        "pnl_points": pnl_points,
        "pnl_amount": pnl_amount,
        "holding_minutes": (timestamp.to_pydatetime() - position.entry_time).total_seconds() / 60,
    }


class OptionsBacktester:
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.entry_after = parse_hhmm(config.entry_after)
        self.square_off = parse_hhmm(config.square_off)

    def run(self, option_chain: pd.DataFrame, spot: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        data = merge_spot(option_chain, spot)
        trades: list[dict] = []
        for trading_day, day_data in data.groupby("trading_date", sort=True):
            day_trades = self._run_day(trading_day, day_data)
            trades.extend(day_trades)
        trades_frame = pd.DataFrame(trades)
        return trades_frame, performance_metrics(trades_frame)

    def _run_day(self, trading_day: date, day_data: pd.DataFrame) -> list[dict]:
        trades: list[dict] = []
        open_position: Position | None = None
        entries_taken = 0
        for timestamp, snapshot in day_data.groupby("timestamp", sort=True):
            current_time = timestamp.time()
            if snapshot.empty:
                continue

            if open_position is not None:
                exit_trade = self._maybe_exit(open_position, snapshot, timestamp)
                if exit_trade:
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

            entry = self._maybe_enter(trading_day, snapshot)
            if entry is not None:
                open_position = entry
                entries_taken += 1

        if open_position is not None:
            last_timestamp = day_data["timestamp"].max()
            last_snapshot = day_data[day_data["timestamp"] == last_timestamp]
            exit_trade = self._close_at_snapshot(open_position, last_snapshot, last_timestamp, "EOD")
            if exit_trade:
                trades.append(exit_trade)
        return trades

    def _maybe_enter(self, trading_day: date, snapshot: pd.DataFrame) -> Position | None:
        spot_value = float(snapshot["spot"].dropna().iloc[-1]) if snapshot["spot"].notna().any() else float("nan")
        vwap_value = float(snapshot["vwap"].dropna().iloc[-1]) if snapshot["vwap"].notna().any() else spot_value
        expiry = min(snapshot["expiry"].dropna().unique())
        n = days_to_expiry(trading_day, expiry, self.config)
        strikes = sorted(snapshot["strike"].dropna().unique())
        selected = selected_strikes(strikes, spot_value, n)
        if not selected:
            return None
        universe = snapshot[snapshot["strike"].isin(selected)]
        call_change = float(universe[universe["option_type"] == "CE"]["change_oi"].sum())
        put_change = float(universe[universe["option_type"] == "PE"]["change_oi"].sum())
        bias, ratio, diff = pcr_bias(call_change, put_change, self.config)
        if bias == "NEUTRAL":
            return None
        option_type: OptionSide = "CE" if bias == "BULLISH" else "PE"
        row = pick_entry_contract(universe, option_type, vwap_value)
        if row is None:
            return None
        return build_position(
            trading_day,
            self.config.symbol,
            expiry,
            row,
            option_type,
            self.config,
            ratio,
            diff,
            n,
        )

    def _maybe_exit(self, position: Position, snapshot: pd.DataFrame, timestamp: pd.Timestamp) -> dict | None:
        if timestamp.time() >= self.square_off:
            return self._close_at_snapshot(position, snapshot, timestamp, "EOD")
        current = self._current_contract_premium(position, snapshot)
        if current is None:
            return None
        if current >= position.entry_premium + position.target_pts:
            return close_position(position, timestamp, current, "TARGET", self.config)
        if current <= position.entry_premium - position.sl_pts:
            return close_position(position, timestamp, current, "SL", self.config)
        return None

    def _close_at_snapshot(self, position: Position, snapshot: pd.DataFrame, timestamp: pd.Timestamp, reason: ExitReason) -> dict | None:
        current = self._current_contract_premium(position, snapshot)
        if current is None:
            return None
        return close_position(position, timestamp, current, reason, self.config)

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


def performance_metrics(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl_per_trade": 0.0,
            "max_drawdown": 0.0,
            "largest_win": 0.0,
            "largest_loss": 0.0,
            "avg_holding_minutes": 0.0,
            "by_delta_bucket": {},
            "by_day_of_week": {},
            "by_dte": {},
        }
    pnl = pd.to_numeric(trades["pnl_amount"], errors="coerce").fillna(0)
    equity = pnl.cumsum()
    drawdown = equity - equity.cummax()
    trades = trades.copy()
    trades["entry_dt"] = pd.to_datetime(trades["entry_time"])
    trades["day_of_week"] = trades["entry_dt"].dt.day_name()
    metrics = {
        "total_trades": int(len(trades)),
        "win_rate": float((pnl > 0).mean()),
        "total_pnl": float(pnl.sum()),
        "avg_pnl_per_trade": float(pnl.mean()),
        "max_drawdown": float(drawdown.min()),
        "largest_win": float(pnl.max()),
        "largest_loss": float(pnl.min()),
        "avg_holding_minutes": float(pd.to_numeric(trades["holding_minutes"], errors="coerce").mean()),
        "by_delta_bucket": grouped_pnl(trades, "delta_bucket"),
        "by_day_of_week": grouped_pnl(trades, "day_of_week"),
        "by_dte": grouped_pnl(trades, "n_strikes_used"),
    }
    return metrics


def grouped_pnl(trades: pd.DataFrame, column: str) -> dict:
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


def write_sqlite(db_path: str | Path, trades: pd.DataFrame, metrics: dict, config: BacktestConfig) -> str:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path = str(db_path)
    run_id = str(uuid.uuid4())
    with sqlite3.connect(db_path) as conn:
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
        conn.execute(
            "INSERT INTO backtest_runs VALUES (?, ?, ?, ?)",
            (run_id, datetime.now().isoformat(sep=" "), json.dumps(config_to_json(config)), json.dumps(metrics)),
        )
        if not trades.empty:
            rows = trades.copy()
            rows.insert(0, "run_id", run_id)
            rows.to_sql("mock_trades", conn, if_exists="append", index=False)
    return run_id


def config_to_json(config: BacktestConfig) -> dict:
    payload = asdict(config)
    payload["delta_buckets"] = {str(key): value for key, value in config.delta_buckets.items()}
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
    for key in ("symbol", "entry_after", "square_off", "pcr_upper", "pcr_lower", "lot_size", "max_trades_per_day", "allow_reentry", "slippage_points", "brokerage_per_trade"):
        value = getattr(args, key, None)
        if value is not None:
            payload[key] = value
    return BacktestConfig(**payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest a PCR + VWAP + Delta NSE options mock strategy.")
    parser.add_argument("--option-chain", required=True, help="CSV/JSON option-chain snapshots.")
    parser.add_argument("--spot", required=True, help="CSV/JSON underlying spot/VWAP data.")
    parser.add_argument("--config", help="Optional JSON config file.")
    parser.add_argument("--out", default="data/backtests/latest", help="Output directory for CSV/JSON reports.")
    parser.add_argument("--sqlite", default="data/backtests/backtests.sqlite", help="SQLite database path.")
    parser.add_argument("--symbol")
    parser.add_argument("--entry-after")
    parser.add_argument("--square-off")
    parser.add_argument("--pcr-upper", type=float)
    parser.add_argument("--pcr-lower", type=float)
    parser.add_argument("--lot-size", type=int)
    parser.add_argument("--max-trades-per-day", type=int)
    parser.add_argument("--allow-reentry", action="store_true", default=None)
    parser.add_argument("--slippage-points", type=float)
    parser.add_argument("--brokerage-per-trade", type=float)
    args = parser.parse_args()

    config = load_config(args.config, args)
    option_chain = load_option_chain(args.option_chain)
    spot = load_spot(args.spot)
    trades, metrics = OptionsBacktester(config).run(option_chain, spot)
    write_outputs(args.out, trades, metrics, config)
    run_id = write_sqlite(args.sqlite, trades, metrics, config)
    print(json.dumps({"run_id": run_id, "metrics": metrics, "output_dir": args.out, "sqlite": args.sqlite}, indent=2))


if __name__ == "__main__":
    main()
