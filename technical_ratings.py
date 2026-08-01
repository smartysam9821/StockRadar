from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd


Signal = Literal[-1, 0, 1]


@dataclass(frozen=True)
class RatingResult:
    symbol: str
    price: float
    overall_score: float
    ma_score: float
    oscillator_score: float
    overall_label: str
    ma_label: str
    oscillator_label: str
    moving_averages: dict[str, Signal]
    oscillators: dict[str, Signal]
    indicator_values: dict[str, float | None]


MA_LENGTHS = (10, 20, 30, 50, 100, 200)


def technical_ratings(df: pd.DataFrame, symbol: str = "") -> RatingResult:
    """Calculate TradingView-style Technical Ratings from OHLCV bars.

    The input must contain Open, High, Low, Close, and Volume columns. Each
    component contributes -1, 0, or +1, then the MA and oscillator groups are
    averaged separately. Overall is the mean of the two group scores.
    """
    data = _normalize_ohlcv(df)
    if len(data) < 220:
        raise ValueError(
            "Need at least 220 OHLCV bars for TradingView-style 200-period indicators. "
            f"Only {len(data)} bars found."
        )

    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    volume = data["Volume"]
    price = float(close.iloc[-1])

    values: dict[str, float | None] = {}
    ma_signals: dict[str, Signal] = {}

    for length in MA_LENGTHS:
        sma = close.rolling(length).mean()
        ema = close.ewm(span=length, adjust=False, min_periods=length).mean()
        ma_signals[f"SMA{length}"] = _ma_signal(_last(sma), price)
        ma_signals[f"EMA{length}"] = _ma_signal(_last(ema), price)
        values[f"SMA{length}"] = _clean_float(_last(sma))
        values[f"EMA{length}"] = _clean_float(_last(ema))

    hma9 = _hma(close, 9)
    vwma20 = (close * volume).rolling(20).sum() / volume.rolling(20).sum()
    ma_signals["HMA9"] = _ma_signal(_last(hma9), price)
    ma_signals["VWMA20"] = _ma_signal(_last(vwma20), price)
    values["HMA9"] = _clean_float(_last(hma9))
    values["VWMA20"] = _clean_float(_last(vwma20))

    conversion, base, span_a, span_b = _ichimoku(high, low)
    ma_signals["Ichimoku"] = _ichimoku_signal(
        _last(conversion), _last(base), _last(span_a), _last(span_b), price
    )
    values["IchimokuConversion"] = _clean_float(_last(conversion))
    values["IchimokuBase"] = _clean_float(_last(base))
    values["IchimokuSpanA"] = _clean_float(_last(span_a))
    values["IchimokuSpanB"] = _clean_float(_last(span_b))

    osc_signals: dict[str, Signal] = {}

    rsi14 = _rsi(close, 14)
    osc_signals["RSI14"] = _range_reversal_signal(_last(rsi14), _prev(rsi14), 30, 70)
    values["RSI14"] = _clean_float(_last(rsi14))

    stoch_k, stoch_d = _stochastic(high, low, close, 14, 3, 3)
    osc_signals["Stochastic"] = _stochastic_signal(_last(stoch_k), _last(stoch_d))
    values["StochK"] = _clean_float(_last(stoch_k))
    values["StochD"] = _clean_float(_last(stoch_d))

    cci20 = _cci(high, low, close, 20)
    osc_signals["CCI20"] = _cci_signal(_last(cci20), _prev(cci20))
    values["CCI20"] = _clean_float(_last(cci20))

    adx, plus_di, minus_di = _adx(high, low, close, 14)
    osc_signals["ADX14"] = _adx_signal(_last(adx), _prev(adx), _last(plus_di), _last(minus_di))
    values["ADX14"] = _clean_float(_last(adx))
    values["PlusDI14"] = _clean_float(_last(plus_di))
    values["MinusDI14"] = _clean_float(_last(minus_di))

    ao = ((high + low) / 2).rolling(5).mean() - ((high + low) / 2).rolling(34).mean()
    osc_signals["AO"] = _ao_signal(ao)
    values["AO"] = _clean_float(_last(ao))

    momentum10 = close - close.shift(10)
    osc_signals["Momentum10"] = _momentum_signal(_last(momentum10), _prev(momentum10))
    values["Momentum10"] = _clean_float(_last(momentum10))

    macd, macd_signal = _macd(close, 12, 26, 9)
    osc_signals["MACD"] = _compare_signal(_last(macd), _last(macd_signal))
    values["MACD"] = _clean_float(_last(macd))
    values["MACDSignal"] = _clean_float(_last(macd_signal))

    stoch_rsi_k, stoch_rsi_d = _stoch_rsi(close, 14, 3, 3)
    ema13 = close.ewm(span=13, adjust=False, min_periods=13).mean()
    trend = _trend(close, ema13)
    osc_signals["StochRSI"] = _stoch_rsi_signal(
        _last(stoch_rsi_k), _last(stoch_rsi_d), trend
    )
    values["StochRSIK"] = _clean_float(_last(stoch_rsi_k))
    values["StochRSID"] = _clean_float(_last(stoch_rsi_d))

    williams_r = _williams_r(high, low, close, 14)
    osc_signals["WilliamsR"] = _williams_signal(_last(williams_r), _prev(williams_r))
    values["WilliamsR"] = _clean_float(_last(williams_r))

    bull_power = high - ema13
    bear_power = low - ema13
    osc_signals["BullBearPower"] = _bull_bear_power_signal(
        _last(bull_power), _prev(bull_power), _last(bear_power), _prev(bear_power), trend
    )
    values["BullPower13"] = _clean_float(_last(bull_power))
    values["BearPower13"] = _clean_float(_last(bear_power))

    ultimate = _ultimate_oscillator(high, low, close, 7, 14, 28)
    osc_signals["UltimateOscillator"] = _ultimate_signal(_last(ultimate))
    values["UltimateOscillator"] = _clean_float(_last(ultimate))

    ma_score = _mean_signal(ma_signals)
    oscillator_score = _mean_signal(osc_signals)
    overall_score = _mean_signal({**ma_signals, **osc_signals})

    return RatingResult(
        symbol=symbol,
        price=price,
        overall_score=overall_score,
        ma_score=ma_score,
        oscillator_score=oscillator_score,
        overall_label=rating_label(overall_score),
        ma_label=rating_label(ma_score),
        oscillator_label=rating_label(oscillator_score),
        moving_averages=ma_signals,
        oscillators=osc_signals,
        indicator_values=values,
    )


def rating_label(score: float) -> str:
    if score < -0.5:
        return "Strong Sell"
    if score < -0.1:
        return "Sell"
    if score <= 0.1:
        return "Neutral"
    if score <= 0.5:
        return "Buy"
    return "Strong Buy"


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    rename = {col: str(col).strip().title() for col in df.columns}
    data = df.rename(columns=rename).copy()
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [col for col in required if col not in data.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    data = data[required].apply(pd.to_numeric, errors="coerce").dropna()
    return data.reset_index(drop=True)


def _ma_signal(value: float, price: float) -> Signal:
    if np.isnan(value):
        return 0
    if value < price:
        return 1
    if value > price:
        return -1
    return 0


def _compare_signal(left: float, right: float) -> Signal:
    if np.isnan(left) or np.isnan(right):
        return 0
    return 1 if left > right else -1 if left < right else 0


def _range_reversal_signal(value: float, previous: float, lower: float, upper: float) -> Signal:
    if np.isnan(value) or np.isnan(previous):
        return 0
    if value < lower and value > previous:
        return 1
    if value > upper and value < previous:
        return -1
    return 0


def _stochastic_signal(k: float, d: float) -> Signal:
    if np.isnan(k) or np.isnan(d):
        return 0
    if k < 20 and d < 20 and k > d:
        return 1
    if k > 80 and d > 80 and k < d:
        return -1
    return 0


def _cci_signal(value: float, previous: float) -> Signal:
    if np.isnan(value) or np.isnan(previous):
        return 0
    if value < -100 and value > previous:
        return 1
    if value > 100 and value < previous:
        return -1
    return 0


def _adx_signal(adx: float, prev_adx: float, plus_di: float, minus_di: float) -> Signal:
    if any(np.isnan(item) for item in (adx, prev_adx, plus_di, minus_di)):
        return 0
    if plus_di > minus_di and adx > 20 and adx > prev_adx:
        return 1
    if plus_di < minus_di and adx > 20 and adx < prev_adx:
        return -1
    return 0


def _ao_signal(ao: pd.Series) -> Signal:
    now, prev, prev2 = _last(ao), _nth_from_end(ao, 2), _nth_from_end(ao, 3)
    if any(np.isnan(item) for item in (now, prev, prev2)):
        return 0
    if (prev <= 0 < now) or (now > 0 and prev > 0 and now > prev and prev < prev2):
        return 1
    if (prev >= 0 > now) or (now < 0 and prev < 0 and now < prev and prev > prev2):
        return -1
    return 0


def _momentum_signal(value: float, previous: float) -> Signal:
    return _compare_signal(value, previous)


def _stoch_rsi_signal(k: float, d: float, trend: int) -> Signal:
    if np.isnan(k) or np.isnan(d):
        return 0
    if trend < 0 and k < 20 and d < 20 and k > d:
        return 1
    if trend > 0 and k > 80 and d > 80 and k < d:
        return -1
    return 0


def _williams_signal(value: float, previous: float) -> Signal:
    if np.isnan(value) or np.isnan(previous):
        return 0
    if value < -80 and value > previous:
        return 1
    if value > -20 and value < previous:
        return -1
    return 0


def _bull_bear_power_signal(
    bull: float, prev_bull: float, bear: float, prev_bear: float, trend: int
) -> Signal:
    if any(np.isnan(item) for item in (bull, prev_bull, bear, prev_bear)):
        return 0
    if trend > 0 and bear < 0 and bear > prev_bear:
        return 1
    if trend < 0 and bull > 0 and bull < prev_bull:
        return -1
    return 0


def _ultimate_signal(value: float) -> Signal:
    if np.isnan(value):
        return 0
    if value > 70:
        return 1
    if value < 30:
        return -1
    return 0


def _ichimoku_signal(
    conversion: float, base: float, span_a: float, span_b: float, price: float
) -> Signal:
    if any(np.isnan(item) for item in (conversion, base, span_a, span_b)):
        return 0
    if span_a > span_b and base > span_a and conversion > base and price > conversion:
        return 1
    if span_a < span_b and base < span_a and conversion < base and price < conversion:
        return -1
    return 0


def _mean_signal(signals: dict[str, Signal]) -> float:
    return float(np.mean(list(signals.values()))) if signals else 0.0


def _wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    return series.rolling(length).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)


def _hma(series: pd.Series, length: int) -> pd.Series:
    half = max(1, length // 2)
    sqrt_len = max(1, int(np.sqrt(length)))
    return _wma(2 * _wma(series, half) - _wma(series, length), sqrt_len)


def _ichimoku(high: pd.Series, low: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    conversion = (high.rolling(9).max() + low.rolling(9).min()) / 2
    base = (high.rolling(26).max() + low.rolling(26).min()) / 2
    span_a = (conversion + base) / 2
    span_b = (high.rolling(52).max() + low.rolling(52).min()) / 2
    return conversion, base, span_a, span_b


def _rsi(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series, length: int, smooth_k: int, smooth_d: int
) -> tuple[pd.Series, pd.Series]:
    lowest = low.rolling(length).min()
    highest = high.rolling(length).max()
    raw_k = 100 * (close - lowest) / (highest - lowest)
    k = raw_k.rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k, d


def _cci(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    typical = (high + low + close) / 3
    sma = typical.rolling(length).mean()
    mean_dev = typical.rolling(length).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    return (typical - sma) / (0.015 * mean_dev)


def _adx(
    high: pd.Series, low: pd.Series, close: pd.Series, length: int
) -> tuple[pd.Series, pd.Series, pd.Series]:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=high.index
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=high.index
    )
    tr = pd.concat(
        [(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / length, adjust=False, min_periods=length).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / length, adjust=False, min_periods=length).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    return adx, plus_di, minus_di


def _macd(close: pd.Series, fast: int, slow: int, signal: int) -> tuple[pd.Series, pd.Series]:
    fast_ema = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
    slow_ema = close.ewm(span=slow, adjust=False, min_periods=slow).mean()
    macd = fast_ema - slow_ema
    macd_signal = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    return macd, macd_signal


def _stoch_rsi(close: pd.Series, rsi_len: int, smooth_k: int, smooth_d: int) -> tuple[pd.Series, pd.Series]:
    rsi = _rsi(close, rsi_len)
    lowest = rsi.rolling(rsi_len).min()
    highest = rsi.rolling(rsi_len).max()
    raw = 100 * (rsi - lowest) / (highest - lowest)
    k = raw.rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k, d


def _williams_r(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    highest = high.rolling(length).max()
    lowest = low.rolling(length).min()
    return -100 * (highest - close) / (highest - lowest)


def _ultimate_oscillator(
    high: pd.Series, low: pd.Series, close: pd.Series, short: int, medium: int, long: int
) -> pd.Series:
    prev_close = close.shift()
    buying_pressure = close - pd.concat([low, prev_close], axis=1).min(axis=1)
    true_range = pd.concat([high, prev_close], axis=1).max(axis=1) - pd.concat(
        [low, prev_close], axis=1
    ).min(axis=1)
    avg_short = buying_pressure.rolling(short).sum() / true_range.rolling(short).sum()
    avg_medium = buying_pressure.rolling(medium).sum() / true_range.rolling(medium).sum()
    avg_long = buying_pressure.rolling(long).sum() / true_range.rolling(long).sum()
    return 100 * ((4 * avg_short) + (2 * avg_medium) + avg_long) / 7


def _trend(close: pd.Series, ema: pd.Series) -> int:
    current_close = _last(close)
    current_ema = _last(ema)
    if np.isnan(current_ema):
        return 0
    return 1 if current_close > current_ema else -1 if current_close < current_ema else 0


def _last(series: pd.Series) -> float:
    return _nth_from_end(series, 1)


def _prev(series: pd.Series) -> float:
    return _nth_from_end(series, 2)


def _nth_from_end(series: pd.Series, n: int) -> float:
    clean = series.dropna()
    if len(clean) < n:
        return float("nan")
    return float(clean.iloc[-n])


def _clean_float(value: float) -> float | None:
    return None if np.isnan(value) or np.isinf(value) else float(value)
