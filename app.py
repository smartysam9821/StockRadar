from __future__ import annotations

import argparse
import base64
import difflib
import hmac
import html
import io
import json
import logging
import os
import re
import secrets
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import http.cookiejar
from http import cookies
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect
try:
    import psycopg
except ImportError:  # Optional PostgreSQL event sink.
    psycopg = None
try:
    from tradingview_ta import Interval as TVInterval
    from tradingview_ta import TA_Handler
except ImportError:  # Optional production confirmation layer.
    TVInterval = None
    TA_Handler = None

from technical_ratings import RatingResult, technical_ratings


DEFAULT_SYMBOL = "ASIANPAINT.NS"
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


def load_env_file(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name:
            os.environ[name] = value


load_env_file()

DATA_DIR = Path(os.environ.get("STOCKRADAR_DATA_DIR", BASE_DIR / "data")).resolve()
KITE_INSTRUMENT_CACHE = DATA_DIR / "kite_instruments.csv"
KITE_TOKEN_FILE = DATA_DIR / "kite_access_token.json"
KITE_ACCESS_TOKEN_MEMORY = ""
APP_SESSION_COOKIE = "stock_app_session"
ALLOWED_INTERVALS = {"1m", "5m", "15m", "30m"}
ALLOWED_RANGES = {"1d", "5d", "7d", "30d", "60d", "90d", "6mo", "1y", "2y", "5y", "10y", "max"}
SYMBOL_RE = re.compile(r"^(?:[A-Z]{2,5}:)?[A-Z0-9&.\- ]{1,40}(?:\.NS)?$")
INDEX_OPTION_UNDERLYING_ALIASES = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY FIN SERVICE": "FINNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
}
DEFAULT_OI_BASELINE_UNDERLYINGS = {
    "NIFTY",
    "ADANIENT",
    "ADANIPORTS",
    "APOLLOHOSP",
    "ASIANPAINT",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJFINANCE",
    "BAJAJFINSV",
    "BEL",
    "BHARTIARTL",
    "CIPLA",
    "COALINDIA",
    "DRREDDY",
    "EICHERMOT",
    "ETERNAL",
    "GRASIM",
    "HCLTECH",
    "HDFCBANK",
    "HDFCLIFE",
    "HEROMOTOCO",
    "HINDALCO",
    "HINDUNILVR",
    "ICICIBANK",
    "INDUSINDBK",
    "INFY",
    "ITC",
    "JIOFIN",
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "M&M",
    "MARUTI",
    "NESTLEIND",
    "NTPC",
    "ONGC",
    "POWERGRID",
    "RELIANCE",
    "SBILIFE",
    "SBIN",
    "SHRIRAMFIN",
    "SUNPHARMA",
    "TATACONSUM",
    "TATAMOTORS",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "TRENT",
    "ULTRACEMCO",
    "WIPRO",
}
TRADINGVIEW_SYMBOL_ALIASES = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY FIN SERVICE": "CNXFINANCE",
}
TOKEN_LOCK = threading.RLock()
INSTRUMENT_LOCK = threading.RLock()
INSTRUMENT_CACHE_FRAME: pd.DataFrame | None = None
REQUEST_CONTEXT = threading.local()
DB_LOCK = threading.RLock()
DB_READY = False
OI_BASELINE_THREAD_STARTED = False
TV_CONFIRMATION_LOCK = threading.RLock()
TV_CONFIRMATION_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
TV_INTERVALS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
}


LOG_FILE_PATH: Path | None = None


def configure_logging() -> logging.Logger:
    global LOG_FILE_PATH
    level_name = os.environ.get("STOCKRADAR_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_to_file = os.environ.get("STOCKRADAR_LOG_TO_FILE", "true").lower() in {"1", "true", "yes"}
    if log_to_file:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        log_file = Path(os.environ.get("STOCKRADAR_LOG_FILE", DATA_DIR / "stockradar.log")).resolve()
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
        LOG_FILE_PATH = log_file
    logging.basicConfig(level=level, format="%(message)s", handlers=handlers, force=True)
    return logging.getLogger("stockradar")


LOGGER = configure_logging()


def current_request_id() -> str:
    return str(getattr(REQUEST_CONTEXT, "request_id", "") or "")


def log_event(event: str, level: str = "info", **fields: object) -> None:
    payload = {
        "ts": datetime.now().isoformat(timespec="milliseconds"),
        "event": event,
        "request_id": current_request_id(),
        **fields,
    }
    log_level = getattr(logging, level.upper(), logging.INFO)
    LOGGER.log(log_level, json.dumps(payload, default=str, separators=(",", ":")))


def sanitized_query(query: str) -> dict:
    sensitive = {"request_token", "access_token", "password", "api_key", "api_secret"}
    clean: dict[str, object] = {}
    for key, values in urllib.parse.parse_qs(query, keep_blank_values=True).items():
        if key.lower() in sensitive:
            clean[key] = "<redacted>"
        else:
            clean[key] = values[0] if len(values) == 1 else values
    return clean


def disable_kite_proxy_env() -> None:
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.pop(name, None)
    os.environ["NO_PROXY"] = "api.kite.trade,kite.zerodha.com,127.0.0.1,localhost"


def app_username() -> str:
    return os.environ.get("APP_USERNAME", "admin").strip() or "admin"


def app_password() -> str:
    return os.environ.get("APP_PASSWORD", "").strip()


def app_session_secret() -> str:
    secret = os.environ.get("APP_SESSION_SECRET", "").strip()
    return secret or app_password() or "dev-only-change-me"


def auth_configured() -> bool:
    return bool(app_password())


def public_host(host: str) -> bool:
    return host not in {"127.0.0.1", "localhost", "::1"}


def validate_runtime_config(host: str) -> None:
    if public_host(host) and not auth_configured():
        raise RuntimeError("APP_PASSWORD must be set before binding the app to a public interface.")
    if public_host(host) and app_session_secret() == "dev-only-change-me":
        raise RuntimeError("APP_SESSION_SECRET must be set before binding the app to a public interface.")


def validate_interval(interval: str) -> str:
    clean = interval.strip()
    if clean not in ALLOWED_INTERVALS:
        raise ValueError(f"Unsupported interval: {interval}")
    return clean


def validate_range(range_: str) -> str:
    clean = range_.strip()
    if clean not in ALLOWED_RANGES:
        raise ValueError(f"Unsupported range: {range_}")
    return clean


def validate_symbol(symbol: str) -> str:
    clean = symbol.strip().upper() or DEFAULT_SYMBOL
    if not SYMBOL_RE.match(clean):
        raise ValueError(f"Invalid symbol: {symbol}")
    return clean


def parse_int(value: str, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def http_csv_enabled() -> bool:
    return os.environ.get("APP_ALLOW_HTTP_CSV", "").lower() in {"1", "true", "yes"}


def tradingview_confirmation_enabled() -> bool:
    return os.environ.get("TRADINGVIEW_CONFIRMATION_ENABLED", "true").lower() in {"1", "true", "yes"}


def tradingview_cache_ttl_seconds() -> int:
    return parse_int(
        os.environ.get("TRADINGVIEW_CONFIRMATION_TTL_SECONDS", "300"),
        default=300,
        minimum=60,
        maximum=3600,
    )


def oi_baseline_enabled() -> bool:
    return os.environ.get("OI_BASELINE_ENABLED", "true").lower() in {"1", "true", "yes"}


def oi_baseline_capture_time() -> tuple[int, int]:
    value = os.environ.get("OI_BASELINE_CAPTURE_TIME", "15:35").strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", value)
    if not match:
        return 15, 35
    return int(match.group(1)), int(match.group(2))


def oi_baseline_poll_seconds() -> int:
    return parse_int(os.environ.get("OI_BASELINE_POLL_SECONDS", "60"), 60, 30, 3600)


def nse_bhavcopy_lookback_days() -> int:
    return parse_int(os.environ.get("NSE_BHAVCOPY_LOOKBACK_DAYS", "10"), 10, 1, 30)


def nse_bhavcopy_base_url() -> str:
    return os.environ.get("NSE_BHAVCOPY_BASE_URL", "https://nsearchives.nseindia.com/content/fo").rstrip("/")


def oi_baseline_underlyings() -> set[str]:
    configured = os.environ.get("OI_BASELINE_UNDERLYINGS", "").strip()
    if not configured:
        return set(DEFAULT_OI_BASELINE_UNDERLYINGS)
    values = {
        option_underlying_symbol(item.strip().upper().removesuffix(".NS"))
        for item in configured.split(",")
        if item.strip()
    }
    return values or set(DEFAULT_OI_BASELINE_UNDERLYINGS)


def safe_error_html(title: str, message: object) -> str:
    return f"<h1>{html.escape(title)}</h1><p>{html.escape(str(message))}</p>"


def make_session_cookie(username: str, max_age: int = 12 * 60 * 60) -> str:
    payload = {
        "u": username,
        "iat": int(time.time()),
        "ttl": max_age,
        "nonce": secrets.token_hex(12),
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()
    sig = hmac.new(app_session_secret().encode(), encoded.encode(), "sha256").hexdigest()
    return f"{encoded}.{sig}"


def verify_session_cookie(value: str) -> bool:
    if not value or "." not in value:
        return False
    encoded, sig = value.rsplit(".", 1)
    expected = hmac.new(app_session_secret().encode(), encoded.encode(), "sha256").hexdigest()
    if not hmac.compare_digest(sig, expected):
        return False
    try:
        payload = json.loads(base64.urlsafe_b64decode(encoded.encode()))
    except (ValueError, json.JSONDecodeError):
        return False
    age = int(time.time()) - int(payload.get("iat", 0))
    ttl = int(payload.get("ttl", 12 * 60 * 60))
    return payload.get("u") == app_username() and 0 <= age <= ttl


def fetch_kite_ohlcv(symbol: str, interval: str = "30m", range_: str = "2y") -> pd.DataFrame:
    started = time.perf_counter()
    symbol = validate_symbol(symbol)
    interval = validate_interval(interval)
    range_ = validate_range(range_)
    log_event("kite.ohlcv.start", symbol=symbol, interval=interval, range=range_)
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    access_token = current_kite_access_token()
    if not api_key:
        raise ValueError("Missing KITE_API_KEY env var.")
    if not os.environ.get("KITE_API_SECRET", "").strip() and not access_token:
        raise ValueError("Missing KITE_API_SECRET env var.")
    if not access_token:
        raise ValueError(
            "Kite session not connected. Open /kite/login and complete Kite login once."
        )

    exchange, tradingsymbol = normalize_kite_symbol(symbol)
    instrument_token = get_kite_instrument_token(exchange, tradingsymbol)
    source_interval = kite_source_interval(interval)
    from_dt, to_dt = kite_date_window(interval, range_)
    try:
        chunks = fetch_kite_historical_chunks(
            api_key, access_token, instrument_token, source_interval, from_dt, to_dt
        )
        if not chunks:
            raise ValueError(f"No Kite candles returned for {exchange}:{tradingsymbol}.")

        df = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["Date"]).sort_values("Date")
        df = df.reset_index(drop=True)
        log_event(
            "kite.ohlcv.success",
            symbol=f"{exchange}:{tradingsymbol}",
            interval=interval,
            source_interval=source_interval,
            bars=len(df),
            chunks=len(chunks),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        return df
    except Exception as exc:
        log_event(
            "kite.ohlcv.error",
            "error",
            symbol=f"{exchange}:{tradingsymbol}",
            interval=interval,
            error=str(exc),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        raise


def current_kite_access_token() -> str:
    if KITE_ACCESS_TOKEN_MEMORY:
        return KITE_ACCESS_TOKEN_MEMORY
    env_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token
    return load_saved_kite_access_token()


def load_saved_kite_access_token() -> str:
    with TOKEN_LOCK:
        if not KITE_TOKEN_FILE.exists():
            return ""
        try:
            payload = json.loads(KITE_TOKEN_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return ""
    token = str(payload.get("access_token", "")).strip()
    saved_date = str(payload.get("date", "")).strip()
    if saved_date and saved_date != datetime.now().date().isoformat():
        return ""
    return token


def save_kite_access_token(access_token: str) -> None:
    payload = {
        "access_token": access_token,
        "date": datetime.now().date().isoformat(),
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    with TOKEN_LOCK:
        KITE_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=KITE_TOKEN_FILE.parent, encoding="utf-8", suffix=".tmp"
        ) as handle:
            json.dump(payload, handle, indent=2)
            temp_name = handle.name
        Path(temp_name).replace(KITE_TOKEN_FILE)


def kite_login_url() -> str:
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing KITE_API_KEY env var.")
    return f"https://kite.zerodha.com/connect/login?v=3&api_key={urllib.parse.quote(api_key)}"


def exchange_kite_request_token(request_token: str) -> dict:
    started = time.perf_counter()
    global KITE_ACCESS_TOKEN_MEMORY
    disable_kite_proxy_env()
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    api_secret = os.environ.get("KITE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise ValueError("Missing KITE_API_KEY or KITE_API_SECRET env var.")
    log_event("kite.login.exchange.start")
    kite = KiteConnect(api_key=api_key, timeout=20)
    payload = kite.generate_session(request_token, api_secret=api_secret)
    access_token = payload.get("access_token", "")
    if not access_token:
        raise ValueError("Kite token exchange succeeded but no access_token returned.")
    KITE_ACCESS_TOKEN_MEMORY = access_token
    save_kite_access_token(access_token)
    log_event("kite.login.exchange.success", duration_ms=int((time.perf_counter() - started) * 1000))
    return payload


def kite_client(access_token: str | None = None) -> KiteConnect:
    disable_kite_proxy_env()
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing KITE_API_KEY env var.")
    token = access_token if access_token is not None else current_kite_access_token()
    return KiteConnect(api_key=api_key, access_token=token or None, timeout=30)


def normalize_kite_symbol(symbol: str) -> tuple[str, str]:
    clean = validate_symbol(symbol)
    if ":" in clean:
        exchange, tradingsymbol = clean.split(":", 1)
        return exchange, tradingsymbol
    if clean.endswith(".NS"):
        return "NSE", clean[:-3]
    return "NSE", clean


def option_underlying_symbol(tradingsymbol: str) -> str:
    return INDEX_OPTION_UNDERLYING_ALIASES.get(tradingsymbol.upper(), tradingsymbol.upper())


def tradingview_symbol(tradingsymbol: str) -> str:
    return TRADINGVIEW_SYMBOL_ALIASES.get(tradingsymbol.upper(), tradingsymbol.upper())


def kite_source_interval(interval: str) -> str:
    mapping = {
        "1m": "minute",
        "5m": "5minute",
        "15m": "15minute",
        "30m": "30minute",
    }
    if interval not in mapping:
        raise ValueError(f"Kite interval not supported: {interval}")
    return mapping[interval]


def kite_date_window(interval: str, requested_range: str) -> tuple[datetime, datetime]:
    to_dt = datetime.now().replace(microsecond=0)
    minimum_days = {
        "1m": 10,
        "5m": 30,
        "15m": 75,
        "30m": 120,
    }
    days = minimum_days.get(interval, 120)
    return to_dt - timedelta(days=days), to_dt


def fetch_kite_historical_chunks(
    api_key: str,
    access_token: str,
    instrument_token: int,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[pd.DataFrame]:
    started = time.perf_counter()
    max_days = 60 if interval != "day" else 1900
    frames: list[pd.DataFrame] = []
    kite = kite_client(access_token)
    cursor = from_dt
    chunk_count = 0
    while cursor < to_dt:
        end = min(cursor + timedelta(days=max_days), to_dt)
        chunk_count += 1
        chunk_started = time.perf_counter()
        log_event(
            "kite.historical.chunk.start",
            interval=interval,
            chunk=chunk_count,
            from_date=cursor.isoformat(),
            to_date=end.isoformat(),
        )
        candles = kite.historical_data(
            instrument_token=instrument_token,
            from_date=cursor,
            to_date=end,
            interval=interval,
            continuous=False,
            oi=False,
        )
        log_event(
            "kite.historical.chunk.success",
            interval=interval,
            chunk=chunk_count,
            candles=len(candles or []),
            duration_ms=int((time.perf_counter() - chunk_started) * 1000),
        )
        if candles:
            frames.append(kite_candles_to_frame(candles))
        cursor = end + timedelta(seconds=1)
    log_event(
        "kite.historical.success",
        interval=interval,
        chunks=chunk_count,
        frames=len(frames),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return frames


def kite_candles_to_frame(candles: list[list]) -> pd.DataFrame:
    rows = []
    for candle in candles:
        if isinstance(candle, dict):
            date_value = candle["date"]
            open_value = candle["open"]
            high_value = candle["high"]
            low_value = candle["low"]
            close_value = candle["close"]
            volume_value = candle["volume"]
        else:
            date_value, open_value, high_value, low_value, close_value, volume_value = candle[:6]
        rows.append(
            {
                "Date": pd.to_datetime(date_value).tz_localize(None),
                "Open": open_value,
                "High": high_value,
                "Low": low_value,
                "Close": close_value,
                "Volume": volume_value,
            }
        )
    return pd.DataFrame(rows)


def get_kite_instrument_token(exchange: str, tradingsymbol: str) -> int:
    instruments = load_kite_instruments()
    match = instruments[
        (instruments["exchange"].astype(str).str.upper() == exchange)
        & (instruments["tradingsymbol"].astype(str).str.upper() == tradingsymbol)
    ]
    if match.empty:
        raise ValueError(f"Kite instrument not found: {exchange}:{tradingsymbol}")
    return int(match.iloc[0]["instrument_token"])


def search_symbols(query: str, limit: int = 25) -> list[dict]:
    started = time.perf_counter()
    limit = max(1, min(limit, 50))
    instruments = load_kite_instruments()
    stocks = instruments[
        (instruments["exchange"].astype(str).str.upper() == "NSE")
        & (instruments["instrument_type"].astype(str).str.upper() == "EQ")
    ].copy()
    if stocks.empty:
        return []

    option_names = set(
        instruments[
            (instruments["exchange"].astype(str).str.upper() == "NFO")
            & (instruments["instrument_type"].astype(str).str.upper().isin(["CE", "PE"]))
        ]["name"].astype(str).str.upper()
    )

    q = query.strip().upper()
    rows = []
    for _, row in stocks.iterrows():
        tradingsymbol = str(row["tradingsymbol"]).upper()
        name = str(row["name"]).upper()
        haystack = f"{tradingsymbol} {name}"
        if not q:
            score = 0.1
        elif q == tradingsymbol or q == name:
            score = 1.25
        elif q in haystack:
            score = 1.0 if tradingsymbol.startswith(q) else 0.85
        else:
            score = max(
                difflib.SequenceMatcher(None, q, tradingsymbol).ratio(),
                difflib.SequenceMatcher(None, q, name).ratio(),
            )
        if not q or score >= 0.45:
            optionable = tradingsymbol in option_names or option_underlying_symbol(tradingsymbol) in option_names
            segment = str(row.get("segment", ""))
            rows.append(
                {
                    "symbol": tradingsymbol,
                    "tradingsymbol": tradingsymbol,
                    "name": str(row["name"]),
                    "optionable": optionable,
                    "kind": "Index" if segment.upper() == "INDICES" else "Stock",
                    "score": score + (0.05 if optionable else 0),
                }
            )
    rows.sort(key=lambda item: (-item["score"], item["tradingsymbol"]))
    for item in rows:
        item.pop("score", None)
    result = rows[:limit]
    log_event(
        "symbols.search.success",
        query=query.strip().upper(),
        limit=limit,
        results=len(result),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return result


def fetch_option_chain(symbol: str, expiry: str = "", strikes_each_side: int = 8) -> dict:
    started = time.perf_counter()
    symbol = validate_symbol(symbol)
    strikes_each_side = max(1, min(strikes_each_side, 25))
    log_event("option_chain.start", symbol=symbol, expiry=expiry or "nearest", strikes_each_side=strikes_each_side)
    access_token = current_kite_access_token()
    if not access_token:
        raise ValueError("Kite session not connected. Open /kite/login and complete Kite login once.")

    _, underlying = normalize_kite_symbol(symbol)
    option_underlying = option_underlying_symbol(underlying)
    instruments = load_kite_instruments()
    options = instruments[
        (instruments["exchange"].astype(str).str.upper() == "NFO")
        & (instruments["name"].astype(str).str.upper() == option_underlying)
        & (instruments["instrument_type"].astype(str).str.upper().isin(["CE", "PE"]))
    ].copy()
    if options.empty:
        raise ValueError(f"No NFO options found for {underlying}.")

    options["expiry"] = pd.to_datetime(options["expiry"]).dt.date
    expiries = sorted(options["expiry"].dropna().unique())
    today = datetime.now().date()
    future_expiries = [item for item in expiries if item >= today]
    if not future_expiries:
        raise ValueError(f"No active option expiries found for {underlying}.")

    selected_expiry = pd.to_datetime(expiry).date() if expiry else future_expiries[0]
    if selected_expiry not in expiries:
        selected_expiry = future_expiries[0]

    chain = options[options["expiry"] == selected_expiry].copy()
    chain["strike"] = pd.to_numeric(chain["strike"], errors="coerce")
    spot = kite_spot_price(underlying)
    strikes = sorted(chain["strike"].dropna().unique())
    if not strikes:
        raise ValueError(f"No strikes found for {underlying} {selected_expiry}.")

    atm_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - spot))
    start = max(0, atm_index - strikes_each_side)
    end = min(len(strikes), atm_index + strikes_each_side + 1)
    selected_strikes = strikes[start:end]
    chain = chain[chain["strike"].isin(selected_strikes)]

    contract_symbols = [str(row.tradingsymbol) for row in chain.itertuples()]
    previous_oi = load_previous_oi_baselines(contract_symbols, datetime.now().date())
    keys = [f"NFO:{symbol}" for symbol in contract_symbols]
    quotes = kite_client().quote(*keys) if keys else {}
    by_strike: dict[float, dict] = {}
    for row in chain.itertuples():
        strike_row = by_strike.setdefault(float(row.strike), {"strike": float(row.strike)})
        side = str(row.instrument_type).upper()
        quote = quotes.get(f"NFO:{row.tradingsymbol}", {})
        strike_row[side] = option_quote_payload(row, quote, previous_oi.get(str(row.tradingsymbol)))

    rows = [by_strike[strike] for strike in sorted(by_strike)]
    payload = {
        "symbol": underlying,
        "option_underlying": option_underlying,
        "spot": spot,
        "expiry": selected_expiry.isoformat(),
        "expiries": [item.isoformat() for item in future_expiries[:12]],
        "rows": rows,
    }
    log_event(
        "option_chain.success",
        symbol=underlying,
        expiry=selected_expiry.isoformat(),
        spot=spot,
        strikes=len(rows),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return payload


def kite_spot_price(underlying: str) -> float:
    quote_key = f"NSE:{underlying}"
    ltp = kite_client().ltp(quote_key)
    return float(ltp[quote_key]["last_price"])


def option_quote_payload(row, quote: dict, previous_oi: int | None = None) -> dict:
    current_oi = quote.get("oi")
    if current_oi is not None and previous_oi is not None:
        oi_change = int(current_oi) - int(previous_oi)
    else:
        oi_change = quote.get("oi_change") or quote.get("change_in_oi") or quote.get("oi_day_change")
    return {
        "tradingsymbol": row.tradingsymbol,
        "ltp": quote.get("last_price"),
        "change": quote.get("net_change"),
        "oi": current_oi,
        "previous_oi": previous_oi,
        "oi_change": oi_change,
        "volume": quote.get("volume"),
        "bid": best_depth_price(quote, "buy"),
        "ask": best_depth_price(quote, "sell"),
    }


def best_depth_price(quote: dict, side: str) -> float | None:
    entries = quote.get("depth", {}).get(side, [])
    if not entries:
        return None
    return entries[0].get("price")


def load_kite_instruments() -> pd.DataFrame:
    global INSTRUMENT_CACHE_FRAME
    started = time.perf_counter()
    with INSTRUMENT_LOCK:
        fresh_file = KITE_INSTRUMENT_CACHE.exists() and cache_age_seconds(KITE_INSTRUMENT_CACHE) <= 12 * 60 * 60
        if INSTRUMENT_CACHE_FRAME is not None and fresh_file:
            log_event(
                "kite.instruments.cache.memory_hit",
                rows=len(INSTRUMENT_CACHE_FRAME),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            return INSTRUMENT_CACHE_FRAME.copy()
        if not fresh_file:
            log_event("kite.instruments.fetch.start", cache_path=str(KITE_INSTRUMENT_CACHE))
            KITE_INSTRUMENT_CACHE.parent.mkdir(parents=True, exist_ok=True)
            instruments = pd.DataFrame(kite_client(access_token="").instruments())
            with tempfile.NamedTemporaryFile(
                "w", delete=False, dir=KITE_INSTRUMENT_CACHE.parent, encoding="utf-8", suffix=".tmp"
            ) as handle:
                instruments.to_csv(handle, index=False)
                temp_name = handle.name
            Path(temp_name).replace(KITE_INSTRUMENT_CACHE)
            INSTRUMENT_CACHE_FRAME = instruments
            log_event(
                "kite.instruments.fetch.success",
                rows=len(instruments),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            return instruments.copy()
        INSTRUMENT_CACHE_FRAME = pd.read_csv(KITE_INSTRUMENT_CACHE)
        log_event(
            "kite.instruments.cache.file_hit",
            rows=len(INSTRUMENT_CACHE_FRAME),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        return INSTRUMENT_CACHE_FRAME.copy()


def cache_age_seconds(path: Path) -> float:
    return time.time() - path.stat().st_mtime


def database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def database_enabled() -> bool:
    return bool(database_url())


def db_connect():
    if psycopg is None:
        raise RuntimeError("Install psycopg[binary] to enable PostgreSQL event logging.")
    return psycopg.connect(database_url(), connect_timeout=10)


def ensure_events_table() -> bool:
    global DB_READY
    if not database_enabled():
        log_event("db.skip", reason="database_url_missing")
        return False
    if psycopg is None:
        log_event("db.skip", "warning", reason="psycopg_missing")
        return False
    with DB_LOCK:
        if DB_READY:
            return True
        started = time.perf_counter()
        try:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS events (
                            id BIGSERIAL PRIMARY KEY,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            request_id TEXT,
                            symbol TEXT NOT NULL,
                            interval TEXT NOT NULL,
                            signal TEXT NOT NULL CHECK (signal IN ('STRONG_BUY', 'STRONG_SELL')),
                            price DOUBLE PRECISION,
                            bars INTEGER,
                            overall_score DOUBLE PRECISION,
                            oscillator_score DOUBLE PRECISION,
                            ma_score DOUBLE PRECISION,
                            local_overall_label TEXT,
                            local_oscillator_label TEXT,
                            local_ma_label TEXT,
                            tv_summary TEXT NOT NULL,
                            tv_oscillators TEXT NOT NULL,
                            tv_moving_averages TEXT NOT NULL,
                            tv_cache TEXT,
                            tv_counts JSONB,
                            payload JSONB
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_events_symbol_interval_created
                        ON events (symbol, interval, created_at DESC)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_events_signal_created
                        ON events (signal, created_at DESC)
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS option_oi_daily (
                            id BIGSERIAL PRIMARY KEY,
                            trade_date DATE NOT NULL,
                            captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            underlying TEXT NOT NULL,
                            tradingsymbol TEXT NOT NULL,
                            expiry DATE,
                            strike DOUBLE PRECISION,
                            option_type TEXT NOT NULL CHECK (option_type IN ('CE', 'PE')),
                            oi BIGINT NOT NULL,
                            last_price DOUBLE PRECISION,
                            source TEXT NOT NULL DEFAULT 'nse.bhavcopy',
                            UNIQUE (trade_date, tradingsymbol)
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_option_oi_daily_symbol_date
                        ON option_oi_daily (tradingsymbol, trade_date DESC)
                        """
                    )
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_option_oi_daily_underlying_expiry
                        ON option_oi_daily (underlying, expiry, trade_date DESC)
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE option_oi_daily
                        ALTER COLUMN source SET DEFAULT 'nse.bhavcopy'
                        """
                    )
            DB_READY = True
            log_event("db.events_table.ready", duration_ms=int((time.perf_counter() - started) * 1000))
            return True
        except Exception as exc:
            log_event(
                "db.events_table.error",
                "error",
                error=str(exc),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            return False


def store_confirmed_event(result: RatingResult, interval: str, bars: int, tv: dict) -> bool:
    if not tv.get("applied") or not ensure_events_table():
        return False
    signal = "STRONG_BUY" if tv.get("summary") == "STRONG_BUY" else "STRONG_SELL"
    started = time.perf_counter()
    payload = {
        "local": result_to_dict(result),
        "tradingview": tv,
    }
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO events (
                        request_id,
                        symbol,
                        interval,
                        signal,
                        price,
                        bars,
                        overall_score,
                        oscillator_score,
                        ma_score,
                        local_overall_label,
                        local_oscillator_label,
                        local_ma_label,
                        tv_summary,
                        tv_oscillators,
                        tv_moving_averages,
                        tv_cache,
                        tv_counts,
                        payload
                    )
                    VALUES (
                        %(request_id)s,
                        %(symbol)s,
                        %(interval)s,
                        %(signal)s,
                        %(price)s,
                        %(bars)s,
                        %(overall_score)s,
                        %(oscillator_score)s,
                        %(ma_score)s,
                        %(local_overall_label)s,
                        %(local_oscillator_label)s,
                        %(local_ma_label)s,
                        %(tv_summary)s,
                        %(tv_oscillators)s,
                        %(tv_moving_averages)s,
                        %(tv_cache)s,
                        %(tv_counts)s::jsonb,
                        %(payload)s::jsonb
                    )
                    """,
                    {
                        "request_id": current_request_id(),
                        "symbol": result.symbol,
                        "interval": interval,
                        "signal": signal,
                        "price": result.price,
                        "bars": bars,
                        "overall_score": result.overall_score,
                        "oscillator_score": result.oscillator_score,
                        "ma_score": result.ma_score,
                        "local_overall_label": result.overall_label,
                        "local_oscillator_label": result.oscillator_label,
                        "local_ma_label": result.ma_label,
                        "tv_summary": tv.get("summary"),
                        "tv_oscillators": tv.get("oscillators"),
                        "tv_moving_averages": tv.get("moving_averages"),
                        "tv_cache": tv.get("cache"),
                        "tv_counts": json.dumps(tv.get("counts", {})),
                        "payload": json.dumps(payload),
                    },
                )
        log_event(
            "db.event.inserted",
            symbol=result.symbol,
            interval=interval,
            signal=signal,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        return True
    except Exception as exc:
        log_event(
            "db.event.error",
            "error",
            symbol=result.symbol,
            interval=interval,
            signal=signal,
            error=str(exc),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        return False


def option_oi_baseline_exists(trade_date: object) -> bool:
    if not ensure_events_table():
        return False
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM option_oi_daily WHERE trade_date = %s LIMIT 1", (trade_date,))
            return cur.fetchone() is not None


def load_previous_oi_baselines(tradingsymbols: list[str], trade_date: object) -> dict[str, int]:
    if not tradingsymbols or not database_enabled() or psycopg is None:
        return {}
    if not ensure_events_table():
        return {}
    placeholders = ",".join(["%s"] * len(tradingsymbols))
    sql = f"""
        SELECT DISTINCT ON (tradingsymbol) tradingsymbol, oi
        FROM option_oi_daily
        WHERE tradingsymbol IN ({placeholders}) AND trade_date < %s
        ORDER BY tradingsymbol, trade_date DESC
    """
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [*tradingsymbols, trade_date])
                return {str(symbol): int(oi) for symbol, oi in cur.fetchall()}
    except Exception as exc:
        log_event("db.option_oi_baseline.load_error", "error", error=str(exc), contracts=len(tradingsymbols))
        return {}


def nse_bhavcopy_filename(trade_date: object) -> str:
    date_value = pd.to_datetime(trade_date).strftime("%Y%m%d")
    return f"BhavCopy_NSE_FO_0_0_0_{date_value}_F_0000.csv.zip"


def nse_bhavcopy_url(trade_date: object) -> str:
    return f"{nse_bhavcopy_base_url()}/{nse_bhavcopy_filename(trade_date)}"


def nse_request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://www.nseindia.com/all-reports-derivatives",
    }


def download_nse_fo_bhavcopy(trade_date: object) -> pd.DataFrame:
    started = time.perf_counter()
    cache_dir = DATA_DIR / "nse_fo_bhavcopy"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / nse_bhavcopy_filename(trade_date)
    if cache_path.exists():
        data = cache_path.read_bytes()
        log_event("nse.bhavcopy.cache.hit", trade_date=str(trade_date), cache_path=str(cache_path), bytes=len(data))
    else:
        url = nse_bhavcopy_url(trade_date)
        headers = nse_request_headers()
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        try:
            warmup = urllib.request.Request("https://www.nseindia.com/all-reports-derivatives", headers=headers)
            opener.open(warmup, timeout=10).read(1024)
        except Exception:
            pass
        request = urllib.request.Request(url, headers=headers)
        log_event("nse.bhavcopy.download.start", trade_date=str(trade_date), url=url)
        with opener.open(request, timeout=30) as response:
            data = response.read()
        cache_path.write_bytes(data)
        log_event(
            "nse.bhavcopy.download.success",
            trade_date=str(trade_date),
            cache_path=str(cache_path),
            bytes=len(data),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        csv_name = next((name for name in archive.namelist() if name.lower().endswith(".csv")), archive.namelist()[0])
        with archive.open(csv_name) as handle:
            return pd.read_csv(handle)


def normalized_column_name(name: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def find_bhavcopy_column(frame: pd.DataFrame, candidates: list[str]) -> str:
    normalized = {normalized_column_name(column): column for column in frame.columns}
    for candidate in candidates:
        column = normalized.get(normalized_column_name(candidate))
        if column is not None:
            return column
    raise ValueError(f"NSE bhavcopy column not found. Tried: {', '.join(candidates)}")


def kite_option_contract_map() -> dict[tuple[str, object, float, str], object]:
    instruments = load_kite_instruments()
    allowed_underlyings = oi_baseline_underlyings()
    options = instruments[
        (instruments["exchange"].astype(str).str.upper() == "NFO")
        & (instruments["name"].astype(str).str.upper().isin(allowed_underlyings))
        & (instruments["instrument_type"].astype(str).str.upper().isin(["CE", "PE"]))
    ].copy()
    if options.empty:
        return {}
    options["expiry"] = pd.to_datetime(options["expiry"], errors="coerce").dt.date
    options["strike"] = pd.to_numeric(options["strike"], errors="coerce")
    result = {}
    for row in options.itertuples():
        if pd.isna(row.strike) or pd.isna(row.expiry):
            continue
        key = (str(row.name).upper(), row.expiry, round(float(row.strike), 4), str(row.instrument_type).upper())
        result[key] = row
    return result


def nse_bhavcopy_to_oi_rows(frame: pd.DataFrame, trade_date: object) -> list[dict]:
    allowed_underlyings = oi_baseline_underlyings()
    symbol_col = find_bhavcopy_column(frame, ["SYMBOL", "TckrSymb", "TICKER_SYMBOL"])
    expiry_col = find_bhavcopy_column(frame, ["EXPIRY_DT", "XpryDt", "EXPIRY_DATE"])
    strike_col = find_bhavcopy_column(frame, ["STRIKE_PR", "StrkPric", "STRIKE_PRICE"])
    option_type_col = find_bhavcopy_column(frame, ["OPTION_TYP", "OptnTp", "OPTION_TYPE"])
    oi_col = find_bhavcopy_column(frame, ["OPEN_INT", "OpnIntrst", "OPEN_INTEREST"])
    close_col = None
    try:
        close_col = find_bhavcopy_column(frame, ["CLOSE", "ClsPric", "LAST", "LastPric", "SttlmPric"])
    except ValueError:
        pass

    contracts = kite_option_contract_map()
    rows: list[dict] = []
    for item in frame.itertuples(index=False):
        record = dict(zip(frame.columns, item))
        underlying = option_underlying_symbol(str(record[symbol_col]).upper().strip())
        option_type = str(record[option_type_col]).upper().strip()
        if underlying not in allowed_underlyings or option_type not in {"CE", "PE"}:
            continue
        expiry = pd.to_datetime(record[expiry_col], errors="coerce")
        strike = pd.to_numeric(record[strike_col], errors="coerce")
        oi = pd.to_numeric(record[oi_col], errors="coerce")
        if pd.isna(expiry) or pd.isna(strike) or pd.isna(oi):
            continue
        key = (underlying, expiry.date(), round(float(strike), 4), option_type)
        contract = contracts.get(key)
        if contract is None:
            continue
        last_price = None
        if close_col:
            close_value = pd.to_numeric(record[close_col], errors="coerce")
            if not pd.isna(close_value):
                last_price = float(close_value)
        rows.append(
            {
                "trade_date": trade_date,
                "underlying": underlying,
                "tradingsymbol": str(contract.tradingsymbol),
                "expiry": expiry.date(),
                "strike": float(strike),
                "option_type": option_type,
                "oi": int(oi),
                "last_price": last_price,
                "source": "nse.bhavcopy",
            }
        )
    return rows


def save_option_oi_rows(rows: list[dict], trade_date: object, started: float) -> int:
    if not rows:
        log_event("option_oi_baseline.capture.empty", "warning", trade_date=str(trade_date), source="nse.bhavcopy")
        return 0

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO option_oi_daily (
                    trade_date,
                    underlying,
                    tradingsymbol,
                    expiry,
                    strike,
                    option_type,
                    oi,
                    last_price,
                    source
                )
                VALUES (
                    %(trade_date)s,
                    %(underlying)s,
                    %(tradingsymbol)s,
                    %(expiry)s,
                    %(strike)s,
                    %(option_type)s,
                    %(oi)s,
                    %(last_price)s,
                    %(source)s
                )
                ON CONFLICT (trade_date, tradingsymbol)
                DO UPDATE SET
                    captured_at = now(),
                    underlying = EXCLUDED.underlying,
                    expiry = EXCLUDED.expiry,
                    strike = EXCLUDED.strike,
                    option_type = EXCLUDED.option_type,
                    oi = EXCLUDED.oi,
                    last_price = EXCLUDED.last_price,
                    source = EXCLUDED.source
                """,
                rows,
            )
    log_event(
        "option_oi_baseline.capture.success",
        trade_date=str(trade_date),
        source="nse.bhavcopy",
        rows=len(rows),
        underlyings=len({row["underlying"] for row in rows}),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return len(rows)


def save_option_oi_daily_baseline(trade_date: object) -> int:
    if not ensure_events_table():
        return 0
    started = time.perf_counter()
    frame = download_nse_fo_bhavcopy(trade_date)
    rows = nse_bhavcopy_to_oi_rows(frame, trade_date)
    log_event(
        "option_oi_baseline.capture.start",
        trade_date=str(trade_date),
        source="nse.bhavcopy",
        bhavcopy_rows=len(frame),
        rows=len(rows),
        underlyings=len({row["underlying"] for row in rows}),
    )
    return save_option_oi_rows(rows, trade_date, started)


def save_latest_option_oi_baseline(max_trade_date: object | None = None) -> int:
    max_date = pd.to_datetime(max_trade_date or datetime.now().date()).date()
    for offset in range(nse_bhavcopy_lookback_days()):
        trade_date = max_date - timedelta(days=offset)
        if option_oi_baseline_exists(trade_date):
            log_event("option_oi_baseline.skip", trade_date=str(trade_date), reason="already_exists")
            return 0
        try:
            return save_option_oi_daily_baseline(trade_date)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                log_event("nse.bhavcopy.missing", "warning", trade_date=str(trade_date), status=exc.code)
                continue
            log_event("nse.bhavcopy.error", "error", trade_date=str(trade_date), status=exc.code, error=str(exc))
        except (urllib.error.URLError, TimeoutError, zipfile.BadZipFile, ValueError) as exc:
            log_event("nse.bhavcopy.error", "warning", trade_date=str(trade_date), error=str(exc))
    log_event("option_oi_baseline.skip", "warning", reason="no_nse_bhavcopy_found", lookback_days=nse_bhavcopy_lookback_days())
    return 0


def oi_baseline_due_now() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    hour, minute = oi_baseline_capture_time()
    return now.hour > hour or (now.hour == hour and now.minute >= minute)


def oi_baseline_scheduler_loop() -> None:
    log_event(
        "option_oi_baseline.scheduler.start",
        enabled=oi_baseline_enabled(),
        source="nse.bhavcopy",
        capture_time="%02d:%02d" % oi_baseline_capture_time(),
        poll_seconds=oi_baseline_poll_seconds(),
        underlyings=len(oi_baseline_underlyings()),
    )
    while True:
        try:
            today = datetime.now().date()
            if oi_baseline_enabled() and database_enabled() and oi_baseline_due_now():
                save_latest_option_oi_baseline(today)
        except Exception as exc:
            log_event("option_oi_baseline.scheduler.error", "error", error=str(exc))
        time.sleep(oi_baseline_poll_seconds())


def start_oi_baseline_scheduler() -> None:
    global OI_BASELINE_THREAD_STARTED
    if OI_BASELINE_THREAD_STARTED:
        return
    OI_BASELINE_THREAD_STARTED = True
    thread = threading.Thread(target=oi_baseline_scheduler_loop, name="oi-baseline-scheduler", daemon=True)
    thread.start()


def maybe_confirm_extreme_with_tradingview(result: RatingResult, interval: str) -> dict:
    started = time.perf_counter()
    local_extreme = local_extreme_side(result)
    if not local_extreme:
        log_event(
            "tradingview.confirmation.skip",
            symbol=result.symbol,
            interval=interval,
            reason="local_not_aligned",
        )
        return {"checked": False, "reason": "Local gauges are not aligned on one side."}
    if not tradingview_confirmation_enabled():
        log_event(
            "tradingview.confirmation.skip",
            symbol=result.symbol,
            interval=interval,
            reason="disabled",
        )
        return {"checked": False, "reason": "TradingView confirmation is disabled."}
    if TA_Handler is None or TVInterval is None:
        log_event(
            "tradingview.confirmation.skip",
            symbol=result.symbol,
            interval=interval,
            reason="package_missing",
        )
        return {"checked": False, "reason": "Install tradingview-ta to enable confirmation."}
    if interval not in TV_INTERVALS:
        log_event(
            "tradingview.confirmation.skip",
            symbol=result.symbol,
            interval=interval,
            reason="unsupported_interval",
        )
        return {"checked": False, "reason": f"TradingView interval is not supported: {interval}"}

    try:
        tv = fetch_tradingview_recommendation(result.symbol, interval)
    except Exception as exc:
        log_event(
            "tradingview.confirmation.error",
            "error",
            symbol=result.symbol,
            interval=interval,
            local_side=local_extreme,
            error=str(exc),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )
        return {
            "checked": True,
            "applied": False,
            "source": "TradingView",
            "error": str(exc),
            "local_side": local_extreme,
        }
    tv_side = tradingview_unanimous_extreme_side(tv)
    if tv_side and tv_side == local_extreme:
        apply_tradingview_extremes(result, tv, local_extreme)
        tv["applied"] = True
    else:
        tv["applied"] = False
    tv["checked"] = True
    tv["local_side"] = local_extreme
    log_event(
        "tradingview.confirmation.success",
        symbol=result.symbol,
        interval=interval,
        local_side=local_extreme,
        summary=tv.get("summary"),
        oscillators=tv.get("oscillators"),
        moving_averages=tv.get("moving_averages"),
        applied=tv["applied"],
        cache=tv.get("cache"),
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return tv


def local_extreme_side(result: RatingResult) -> str:
    labels = [result.oscillator_label, result.overall_label, result.ma_label]
    sides = {label_side(label) for label in labels}
    return sides.pop() if len(sides) == 1 and sides <= {"sell", "buy"} else ""


def label_side(label: str) -> str:
    clean = label.lower()
    if clean in {"sell", "strong sell"}:
        return "sell"
    if clean in {"buy", "strong buy"}:
        return "buy"
    return "neutral"


def recommendation_side(recommendation: str) -> str:
    clean = recommendation.upper()
    if clean in {"SELL", "STRONG_SELL"}:
        return "sell"
    if clean in {"BUY", "STRONG_BUY"}:
        return "buy"
    return "neutral"


def tradingview_unanimous_extreme_side(tv: dict) -> str:
    recommendations = {
        tv.get("summary"),
        tv.get("oscillators"),
        tv.get("moving_averages"),
    }
    if recommendations == {"STRONG_SELL"}:
        return "sell"
    if recommendations == {"STRONG_BUY"}:
        return "buy"
    return ""


def fetch_tradingview_recommendation(symbol: str, interval: str) -> dict:
    started = time.perf_counter()
    exchange, tradingsymbol = normalize_kite_symbol(symbol)
    tv_symbol = tradingview_symbol(tradingsymbol)
    cache_key = (f"{exchange}:{tv_symbol}", interval)
    now = time.time()
    ttl = tradingview_cache_ttl_seconds()
    with TV_CONFIRMATION_LOCK:
        cached = TV_CONFIRMATION_CACHE.get(cache_key)
        if cached and now - cached[0] < ttl:
            payload = dict(cached[1])
            payload["cache"] = "hit"
            payload["age_seconds"] = int(now - cached[0])
            log_event(
                "tradingview.fetch.cache_hit",
                symbol=cache_key[0],
                interval=interval,
                age_seconds=payload["age_seconds"],
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            return payload

    log_event("tradingview.fetch.start", symbol=cache_key[0], interval=interval)
    handler = TA_Handler(
        symbol=tv_symbol,
        screener="india",
        exchange=exchange,
        interval=tradingview_interval(interval),
        timeout=8,
    )
    analysis = handler.get_analysis()
    summary = normalize_tv_recommendation(analysis.summary.get("RECOMMENDATION", ""))
    oscillators = normalize_tv_recommendation(analysis.oscillators.get("RECOMMENDATION", ""))
    moving_averages = normalize_tv_recommendation(analysis.moving_averages.get("RECOMMENDATION", ""))
    payload = {
        "source": "TradingView",
        "symbol": f"{exchange}:{tv_symbol}",
        "kite_symbol": f"{exchange}:{tradingsymbol}",
        "interval": interval,
        "summary": summary,
        "oscillators": oscillators,
        "moving_averages": moving_averages,
        "counts": {
            "summary": tv_counts(analysis.summary),
            "oscillators": tv_counts(analysis.oscillators),
            "moving_averages": tv_counts(analysis.moving_averages),
        },
        "cache": "miss",
        "age_seconds": 0,
        "ttl_seconds": ttl,
    }
    with TV_CONFIRMATION_LOCK:
        TV_CONFIRMATION_CACHE[cache_key] = (now, payload)
    log_event(
        "tradingview.fetch.success",
        symbol=cache_key[0],
        interval=interval,
        summary=summary,
        oscillators=oscillators,
        moving_averages=moving_averages,
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    return dict(payload)


def tradingview_interval(interval: str) -> str:
    attr = {
        "1m": "INTERVAL_1_MINUTE",
        "5m": "INTERVAL_5_MINUTES",
        "15m": "INTERVAL_15_MINUTES",
        "30m": "INTERVAL_30_MINUTES",
    }[interval]
    return getattr(TVInterval, attr)


def normalize_tv_recommendation(value: object) -> str:
    return str(value or "NEUTRAL").strip().upper().replace(" ", "_")


def tv_counts(payload: dict) -> dict:
    return {
        "sell": int(payload.get("SELL", 0) or 0),
        "neutral": int(payload.get("NEUTRAL", 0) or 0),
        "buy": int(payload.get("BUY", 0) or 0),
    }


def apply_tradingview_extremes(result: RatingResult, tv: dict, side: str) -> None:
    if side == "sell" and tv.get("summary") == "STRONG_SELL":
        object.__setattr__(result, "overall_score", -1.0)
        object.__setattr__(result, "overall_label", "Strong Sell")
    elif side == "buy" and tv.get("summary") == "STRONG_BUY":
        object.__setattr__(result, "overall_score", 1.0)
        object.__setattr__(result, "overall_label", "Strong Buy")

    if side == "sell" and tv.get("oscillators") == "STRONG_SELL":
        object.__setattr__(result, "oscillator_score", -1.0)
        object.__setattr__(result, "oscillator_label", "Strong Sell")
    elif side == "buy" and tv.get("oscillators") == "STRONG_BUY":
        object.__setattr__(result, "oscillator_score", 1.0)
        object.__setattr__(result, "oscillator_label", "Strong Buy")

    if side == "sell" and tv.get("moving_averages") == "STRONG_SELL":
        object.__setattr__(result, "ma_score", -1.0)
        object.__setattr__(result, "ma_label", "Strong Sell")
    elif side == "buy" and tv.get("moving_averages") == "STRONG_BUY":
        object.__setattr__(result, "ma_score", 1.0)
        object.__setattr__(result, "ma_label", "Strong Buy")


def result_to_dict(result: RatingResult) -> dict:
    return {
        "symbol": result.symbol,
        "price": result.price,
        "overall_score": result.overall_score,
        "ma_score": result.ma_score,
        "oscillator_score": result.oscillator_score,
        "overall_label": result.overall_label,
        "ma_label": result.ma_label,
        "oscillator_label": result.oscillator_label,
        "moving_averages": result.moving_averages,
        "oscillators": result.oscillators,
        "indicator_values": result.indicator_values,
    }


def load_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


class RatingsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        self._begin_request("GET", parsed)
        try:
            self._dispatch_get(parsed)
        except Exception as exc:
            log_event("request.unhandled_error", "error", method="GET", path=parsed.path, error=str(exc))
            self._send_json({"ok": False, "error": "Internal server error"}, status=500)
        finally:
            self._finish_request("GET", parsed)

    def _dispatch_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/login":
            self._send_html(LOGIN_HTML)
            return
        if parsed.path == "/logout":
            self._logout()
            return
        if not self._is_authenticated():
            self._redirect("/login")
            return
        if parsed.path == "/":
            if "request_token=" in parsed.query:
                self._handle_kite_callback(parsed.query)
                return
            self._send_html(INDEX_HTML)
            return
        if parsed.path == "/kite/login":
            self._redirect(kite_login_url())
            return
        if parsed.path == "/kite/callback":
            self._handle_kite_callback(parsed.query)
            return
        if parsed.path == "/api/ratings":
            self._handle_ratings(parsed.query)
            return
        if parsed.path == "/api/option-chain":
            self._handle_option_chain(parsed.query)
            return
        if parsed.path == "/api/symbols":
            self._handle_symbols(parsed.query)
            return
        if parsed.path == "/api/health":
            self._handle_health()
            return
        self.send_error(404, "Not found")

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        self._begin_request("POST", parsed)
        try:
            if parsed.path != "/login":
                self.send_error(404, "Not found")
                return
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8")
            data = urllib.parse.parse_qs(body)
            username = data.get("username", [""])[0]
            password = data.get("password", [""])[0]
            remember = data.get("remember", [""])[0] == "on"
            if auth_configured() and username == app_username() and hmac.compare_digest(password, app_password()):
                log_event("auth.login.success", username=username, remember=remember)
                self._set_session(username, remember=remember)
            else:
                log_event("auth.login.failure", "warning", username=username, auth_configured=auth_configured())
                self._send_html(LOGIN_HTML.replace("<!--ERROR-->", "<p class='error'>Invalid login or APP_PASSWORD is not set.</p>"), status=401)
        except Exception as exc:
            log_event("request.unhandled_error", "error", method="POST", path=parsed.path, error=str(exc))
            self._send_html(safe_error_html("Internal server error", "Request failed."), status=500)
        finally:
            self._finish_request("POST", parsed)

    def log_message(self, format: str, *args: object) -> None:
        log_event("http.server", message=format % args)

    def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
        self._response_status = code
        log_event("response.error", "warning", status=code, message=message or "")
        super().send_error(code, message, explain)

    def _begin_request(self, method: str, parsed: urllib.parse.ParseResult) -> None:
        self.request_id = secrets.token_hex(8)
        self.request_started = time.perf_counter()
        self._response_status = 0
        REQUEST_CONTEXT.request_id = self.request_id
        log_event(
            "request.start",
            method=method,
            path=parsed.path,
            query=sanitized_query(parsed.query),
            client_ip=self.client_address[0] if self.client_address else "",
            user_agent=self.headers.get("User-Agent", ""),
        )

    def _finish_request(self, method: str, parsed: urllib.parse.ParseResult) -> None:
        duration_ms = int((time.perf_counter() - getattr(self, "request_started", time.perf_counter())) * 1000)
        log_event(
            "request.end",
            method=method,
            path=parsed.path,
            status=getattr(self, "_response_status", 0),
            duration_ms=duration_ms,
        )
        REQUEST_CONTEXT.request_id = ""

    def _is_authenticated(self) -> bool:
        jar = cookies.SimpleCookie(self.headers.get("Cookie", ""))
        morsel = jar.get(APP_SESSION_COOKIE)
        return verify_session_cookie(morsel.value if morsel else "")

    def _handle_ratings(self, query: str) -> None:
        started = time.perf_counter()
        params = urllib.parse.parse_qs(query)
        symbol = params.get("symbol", [DEFAULT_SYMBOL])[0]
        interval = params.get("interval", ["30m"])[0]
        range_ = params.get("range", ["2y"])[0]
        csv_path = params.get("csv", [""])[0].strip()
        log_event("ratings.request.start", symbol=symbol, interval=interval, range=range_, csv=bool(csv_path))
        try:
            if csv_path and not http_csv_enabled():
                raise ValueError("HTTP CSV loading is disabled. Use CLI CSV mode or set APP_ALLOW_HTTP_CSV=true.")
            df = load_csv(csv_path) if csv_path else fetch_kite_ohlcv(symbol, interval, range_)
            result = technical_ratings(df, symbol=symbol)
            confirmation = maybe_confirm_extreme_with_tradingview(result, validate_interval(interval))
            event_stored = store_confirmed_event(result, validate_interval(interval), len(df), confirmation)
            log_event(
                "ratings.request.success",
                symbol=symbol,
                interval=interval,
                bars=len(df),
                overall=result.overall_label,
                oscillators=result.oscillator_label,
                moving_averages=result.ma_label,
                confirmation_checked=confirmation.get("checked", False),
                confirmation_applied=confirmation.get("applied", False),
                event_stored=event_stored,
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            self._send_json(
                {
                    "ok": True,
                    "data": result_to_dict(result),
                    "bars": len(df),
                    "confirmation": confirmation,
                    "event_stored": event_stored,
                }
            )
        except ValueError as exc:
            log_event(
                "ratings.request.error",
                "warning",
                symbol=symbol,
                interval=interval,
                error=str(exc),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            self._send_json({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            log_event(
                "ratings.request.error",
                "error",
                symbol=symbol,
                interval=interval,
                error=str(exc),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_option_chain(self, query: str) -> None:
        started = time.perf_counter()
        params = urllib.parse.parse_qs(query)
        symbol = params.get("symbol", [DEFAULT_SYMBOL])[0]
        expiry = params.get("expiry", [""])[0].strip()
        strikes = parse_int(params.get("strikes", ["8"])[0], default=8, minimum=1, maximum=25)
        log_event("option_chain.request.start", symbol=symbol, expiry=expiry or "nearest", strikes=strikes)
        try:
            chain = fetch_option_chain(symbol, expiry=expiry, strikes_each_side=strikes)
            log_event(
                "option_chain.request.success",
                symbol=symbol,
                expiry=chain.get("expiry"),
                rows=len(chain.get("rows", [])),
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            self._send_json({"ok": True, "data": chain})
        except ValueError as exc:
            log_event("option_chain.request.error", "warning", symbol=symbol, error=str(exc), duration_ms=int((time.perf_counter() - started) * 1000))
            self._send_json({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            log_event("option_chain.request.error", "error", symbol=symbol, error=str(exc), duration_ms=int((time.perf_counter() - started) * 1000))
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_symbols(self, query: str) -> None:
        started = time.perf_counter()
        params = urllib.parse.parse_qs(query)
        q = params.get("q", [""])[0]
        limit = parse_int(params.get("limit", ["25"])[0], default=25, minimum=1, maximum=50)
        log_event("symbols.request.start", query=q, limit=limit)
        try:
            data = search_symbols(q, limit=limit)
            log_event("symbols.request.success", query=q, results=len(data), duration_ms=int((time.perf_counter() - started) * 1000))
            self._send_json({"ok": True, "data": data})
        except ValueError as exc:
            log_event("symbols.request.error", "warning", query=q, error=str(exc), duration_ms=int((time.perf_counter() - started) * 1000))
            self._send_json({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            log_event("symbols.request.error", "error", query=q, error=str(exc), duration_ms=int((time.perf_counter() - started) * 1000))
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_health(self) -> None:
        payload = {
            "ok": True,
            "data_dir": str(DATA_DIR),
            "log_file": str(LOG_FILE_PATH) if LOG_FILE_PATH else "",
            "tradingview_confirmation": tradingview_confirmation_enabled(),
            "database_enabled": database_enabled(),
            "database_ready": DB_READY,
            "oi_baseline_enabled": oi_baseline_enabled(),
            "oi_baseline_capture_time": "%02d:%02d" % oi_baseline_capture_time(),
            "oi_baseline_source": "nse.bhavcopy",
            "nse_bhavcopy_lookback_days": nse_bhavcopy_lookback_days(),
            "oi_baseline_thread_started": OI_BASELINE_THREAD_STARTED,
        }
        log_event("health.success", **payload)
        self._send_json(payload)

    def _handle_kite_callback(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        request_token = params.get("request_token", [""])[0].strip()
        if not request_token:
            log_event("kite.callback.error", "warning", error="missing_request_token")
            self._send_html(safe_error_html("Kite login failed", "No request_token received."), status=400)
            return
        try:
            log_event("kite.callback.start")
            exchange_kite_request_token(request_token)
            log_event("kite.callback.success")
            self._redirect("/")
        except Exception as exc:
            log_event("kite.callback.error", "error", error=str(exc))
            self._send_html(safe_error_html("Kite token exchange failed", exc), status=502)

    def _send_html(self, body: str, status: int = 200) -> None:
        payload = body.encode("utf-8")
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-ID", current_request_id())
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, data: dict, status: int = 200) -> None:
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        self._response_status = status
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-ID", current_request_id())
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _redirect(self, url: str) -> None:
        self._response_status = 302
        log_event("response.redirect", location=url)
        self.send_response(302)
        self.send_header("Location", url)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-ID", current_request_id())
        self.end_headers()

    def _set_session(self, username: str, remember: bool = False) -> None:
        max_age = 30 * 24 * 60 * 60 if remember else 12 * 60 * 60
        secure = "; Secure" if os.environ.get("APP_COOKIE_SECURE", "").lower() in {"1", "true", "yes"} else ""
        self._response_status = 302
        self.send_response(302)
        self.send_header("Location", "/")
        self.send_header(
            "Set-Cookie",
            f"{APP_SESSION_COOKIE}={make_session_cookie(username, max_age=max_age)}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}{secure}",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-ID", current_request_id())
        self.end_headers()

    def _logout(self) -> None:
        log_event("auth.logout")
        self._response_status = 302
        self.send_response(302)
        self.send_header("Location", "/login")
        self.send_header(
            "Set-Cookie",
            f"{APP_SESSION_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Request-ID", current_request_id())
        self.end_headers()


LOGIN_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>StockRadar Login</title>
  <style>
    :root {
      --page: #eef2f7;
      --ink: #050b1c;
      --muted: #58627a;
      --line: #d9e0ec;
      --dark: #0b1020;
      --dark-2: #111a32;
      --blue: #4072f2;
      --violet: #7547ed;
      --teal: #12c8c1;
      --red: #f04452;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 25% 8%, rgba(64, 114, 242, .12), transparent 32%),
        radial-gradient(circle at 80% 78%, rgba(18, 200, 193, .10), transparent 30%),
        var(--page);
      padding: 24px;
    }
    .shell {
      width: min(900px, 100%);
      display: grid;
      grid-template-columns: 1fr 1fr;
      min-height: 500px;
      border-radius: 22px;
      background: #fff;
      box-shadow: 0 34px 90px rgba(28, 39, 69, .18);
      overflow: hidden;
    }
    .brand {
      position: relative;
      padding: 38px 38px 30px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background:
        radial-gradient(circle at 30% 38%, rgba(18, 200, 193, .22), transparent 28%),
        linear-gradient(145deg, var(--dark), #070b17 72%);
      color: #fff;
      overflow: hidden;
    }
    .sr-logo {
      display: inline-flex;
      align-items: center;
      gap: 10px;
    }
    .sr-logo-mark {
      width: 44px;
      height: 44px;
      border-radius: 11px;
      background: linear-gradient(135deg, #3b6fef, #7c4def);
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      box-shadow: 0 16px 28px rgba(80, 104, 255, .28);
    }
    .sr-logo-mark svg {
      width: 23px;
      height: 23px;
    }
    .sr-logo-text {
      font-weight: 900;
      font-size: 24px;
      color: #f4f6fb;
      letter-spacing: 0;
    }
    .sr-logo-text span { color: #12b6ac; }
    .sr-radar-wrap {
      display: grid;
      place-items: center;
      gap: 10px;
      margin: 10px 0 8px;
    }
    .sr-radar-title {
      color: #7181ad;
      font-size: 12px;
      font-weight: 900;
      letter-spacing: 6px;
      text-transform: uppercase;
    }
    .sr-radar {
      position: relative;
      width: min(305px, 82%);
      aspect-ratio: 1;
      overflow: hidden;
      border-radius: 50%;
    }
    .sr-ring {
      position: absolute;
      border-radius: 50%;
      border: 1px solid #1e2740;
    }
    .sr-ring.r1 { inset: 0; }
    .sr-ring.r2 { inset: 16%; }
    .sr-ring.r3 { inset: 32%; }
    .sr-ring.r4 { inset: 48%; }
    .sr-cross {
      position: absolute;
      inset: 0;
    }
    .sr-cross::before,
    .sr-cross::after {
      content: "";
      position: absolute;
      background: #1e2740;
    }
    .sr-cross::before {
      left: 0;
      right: 0;
      top: 50%;
      height: 1px;
    }
    .sr-cross::after {
      top: 0;
      bottom: 0;
      left: 50%;
      width: 1px;
    }
    .sr-sweep {
      position: absolute;
      inset: 0;
      border-radius: 50%;
      background: conic-gradient(
        from 0deg,
        rgba(18, 182, 172, .55) 0deg,
        rgba(18, 182, 172, .16) 26deg,
        rgba(18, 182, 172, 0) 60deg,
        rgba(18, 182, 172, 0) 360deg
      );
      animation: sr-spin 4.5s linear infinite;
    }
    @keyframes sr-spin { to { transform: rotate(360deg); } }
    .sr-blip {
      position: absolute;
      width: 9px;
      height: 9px;
      border-radius: 50%;
      transform: translate(-50%, -50%);
    }
    .sr-blip::after {
      content: "";
      position: absolute;
      inset: -7px;
      border-radius: 50%;
      border: 2px solid currentColor;
      opacity: .78;
    }
    .sr-blip.buy {
      background: #12b6ac;
      color: #12b6ac;
    }
    .sr-blip.sell {
      background: #e5484d;
      color: #e5484d;
    }
    .sr-center {
      position: absolute;
      top: 50%;
      left: 50%;
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #f4f6fb;
      transform: translate(-50%, -50%);
    }
    .hero-copy h1 {
      max-width: 420px;
      margin: 0 0 12px;
      font-size: 26px;
      line-height: 1.28;
      letter-spacing: 0;
    }
    .hero-copy p {
      max-width: 420px;
      margin: 0;
      color: #aeb8d2;
      font-size: 16px;
      line-height: 1.55;
    }
    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 24px;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid rgba(136, 156, 211, .28);
      border-radius: 999px;
      padding: 7px 12px;
      color: #b9c3dc;
      font-weight: 800;
      font-size: 13px;
      background: rgba(11, 16, 32, .35);
    }
    .chip::before {
      content: "";
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--teal);
      box-shadow: 0 0 14px rgba(18, 200, 193, .8);
    }
    .sr-ticker {
      width: 100%;
      overflow: hidden;
      margin-top: 28px;
      border-top: 1px solid rgba(136, 156, 211, .25);
      border-bottom: 1px solid rgba(136, 156, 211, .12);
      padding: 14px 0;
    }
    .sr-ticker-track {
      display: flex;
      gap: 28px;
      width: max-content;
      animation: sr-scroll 22s linear infinite;
    }
    @keyframes sr-scroll {
      from { transform: translateX(0); }
      to { transform: translateX(-50%); }
    }
    .sr-tick {
      display: flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
      color: #93a0be;
      font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      font-weight: 800;
    }
    .sr-tick b {
      color: #f4f6fb;
      font-weight: 800;
    }
    .sr-tick .up { color: #1ba672; }
    .sr-tick .down { color: #e5484d; }
    .panel {
      padding: clamp(40px, 5vw, 54px);
      display: flex;
      flex-direction: column;
      justify-content: center;
      background: #fff;
    }
    .login-card {
      width: min(460px, 100%);
      margin: 0 auto;
    }
    .eyebrow {
      margin: 0 0 14px;
      color: #007f76;
      font-size: 13px;
      font-weight: 900;
      letter-spacing: .04em;
      text-transform: uppercase;
    }
    .panel h2 {
      margin: 0 0 10px;
      font-size: clamp(30px, 4vw, 36px);
      line-height: 1.1;
      letter-spacing: 0;
    }
    .panel .sub {
      margin: 0 0 34px;
      color: var(--muted);
      line-height: 1.55;
      font-size: 16px;
    }
    label {
      display: block;
      margin: 18px 0 8px;
      color: #071126;
      font-weight: 800;
      font-size: 14px;
    }
    .field {
      position: relative;
    }
    .field-icon {
      position: absolute;
      left: 17px;
      top: 50%;
      transform: translateY(-50%);
      color: #6a46c8;
      width: 18px;
      height: 18px;
      pointer-events: none;
    }
    .field-icon svg {
      width: 18px;
      height: 18px;
      display: block;
    }
    input {
      width: 100%;
      height: 54px;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 0 16px 0 46px;
      font: inherit;
      font-weight: 750;
      outline: none;
      color: #101828;
      background: #fbfcff;
      transition: border-color .18s, box-shadow .18s;
    }
    input:focus {
      border-color: var(--blue);
      box-shadow: 0 0 0 4px rgba(64, 114, 242, .12);
      background: #fff;
    }
    .row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-top: 18px;
      color: #465271;
      font-size: 14px;
      font-weight: 700;
    }
    .remember {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      margin: 0;
      color: #465271;
      font-size: 14px;
    }
    .remember input {
      width: 18px;
      height: 18px;
      padding: 0;
      accent-color: var(--blue);
    }
    .security-note {
      color: #2356d8;
      text-decoration: none;
    }
    button {
      width: 100%;
      height: 54px;
      margin-top: 34px;
      border: 0;
      border-radius: 10px;
      color: #fff;
      background: linear-gradient(92deg, var(--blue), var(--violet));
      font: inherit;
      font-weight: 900;
      cursor: pointer;
      box-shadow: 0 16px 28px rgba(64, 114, 242, .28);
      transition: transform .16s, box-shadow .16s;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
    }
    button svg {
      width: 18px;
      height: 18px;
      transition: transform .16s;
    }
    button:hover {
      transform: translateY(-1px);
      box-shadow: 0 20px 34px rgba(64, 114, 242, .32);
    }
    button:hover svg { transform: translateX(2px); }
    .setup, .error {
      border-radius: 10px;
      padding: 12px 14px;
      font-size: 14px;
      line-height: 1.45;
      margin: 0 0 18px;
    }
    .setup {
      color: #7a4100;
      background: #fff4e5;
      border: 1px solid #ffd9a8;
    }
    .error {
      color: #b42318;
      background: #fff0f2;
      border: 1px solid #ffc9d0;
    }
    .foot {
      margin-top: 22px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    code {
      color: #111827;
      font-weight: 800;
    }
    @media (max-width: 860px) {
      .shell { grid-template-columns: 1fr; }
      .brand { min-height: 560px; padding: 34px; }
      .panel { padding: 38px 34px; }
      .sr-radar { width: min(292px, 92%); }
    }
    @media (max-width: 520px) {
      body { padding: 14px; }
      .shell { border-radius: 18px; }
      .brand { min-height: 480px; padding: 28px 24px; }
      .panel { padding: 32px 22px; }
      .row { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="brand">
      <div class="sr-logo">
        <span class="sr-logo-mark">
          <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path d="M3 17 L9 10 L13 14 L21 5" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M15 5 H21 V11" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </span>
        <span class="sr-logo-text">Stock<span>Radar</span></span>
      </div>
      <div class="sr-radar-wrap">
        <div class="sr-radar-title">Scanning Market</div>
        <div class="sr-radar" role="img" aria-label="Animated market scanner">
          <div class="sr-ring r1"></div>
          <div class="sr-ring r2"></div>
          <div class="sr-ring r3"></div>
          <div class="sr-ring r4"></div>
          <div class="sr-cross"></div>
          <div class="sr-sweep"></div>
          <div class="sr-blip buy" style="top:28%;left:62%;"></div>
          <div class="sr-blip sell" style="top:68%;left:34%;"></div>
          <div class="sr-blip buy" style="top:44%;left:80%;"></div>
          <div class="sr-blip sell" style="top:20%;left:38%;"></div>
          <div class="sr-blip buy" style="top:78%;left:58%;"></div>
          <div class="sr-center"></div>
        </div>
      </div>
      <div class="hero-copy">
        <h1>Every signal, every strike, one dashboard.</h1>
        <p>Live oscillator and moving-average ratings, full option chains, and Kite-linked market data behind one private session.</p>
        <div class="chips">
          <span class="chip">Kite Connect</span>
          <span class="chip">NSE stocks</span>
          <span class="chip">Option chain</span>
        </div>
      </div>
      <div class="sr-ticker" aria-label="Moving stock price strip">
        <div class="sr-ticker-track">
          <div class="sr-tick">ASIANPAINT <b>2,738.00</b> <span class="down">v Sell</span></div>
          <div class="sr-tick">RELIANCE <b>2,912.40</b> <span class="up">^ Buy</span></div>
          <div class="sr-tick">TCS <b>4,105.10</b> <span class="up">^ Buy</span></div>
          <div class="sr-tick">HDFCBANK <b>1,678.55</b> <span class="down">v Sell</span></div>
          <div class="sr-tick">INFY <b>1,842.20</b> <span class="up">^ Buy</span></div>
          <div class="sr-tick">ASIANPAINT <b>2,738.00</b> <span class="down">v Sell</span></div>
          <div class="sr-tick">RELIANCE <b>2,912.40</b> <span class="up">^ Buy</span></div>
          <div class="sr-tick">TCS <b>4,105.10</b> <span class="up">^ Buy</span></div>
          <div class="sr-tick">HDFCBANK <b>1,678.55</b> <span class="down">v Sell</span></div>
          <div class="sr-tick">INFY <b>1,842.20</b> <span class="up">^ Buy</span></div>
        </div>
      </div>
    </section>
    <section class="panel">
      <div class="login-card">
        <p class="eyebrow">Private Session</p>
        <h2>Sign in to StockRadar</h2>
        <p class="sub">Enter your credentials to access your dashboard.</p>
        <!--ERROR-->
        <form method="post" action="/login">
          <label for="username">Username</label>
          <div class="field">
            <span class="field-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none">
                <path d="M20 21a8 8 0 0 0-16 0" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                <circle cx="12" cy="8" r="4" fill="currentColor"/>
              </svg>
            </span>
            <input id="username" name="username" value=""" + html.escape(app_username()) + r""" autocomplete="username" required>
          </div>
          <label for="password">Password</label>
          <div class="field">
            <span class="field-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24" fill="none">
                <rect x="5" y="10" width="14" height="10" rx="2" fill="currentColor" opacity=".18"/>
                <path d="M8 10V7a4 4 0 0 1 8 0v3" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                <path d="M12 14v3" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
              </svg>
            </span>
            <input id="password" name="password" type="password" autocomplete="current-password" required>
          </div>
          <div class="row">
            <label class="remember"><input type="checkbox" name="remember">Remember this device</label>
            <span class="security-note">Secure session</span>
          </div>
          <button type="submit">
            <span>Enter dashboard</span>
            <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
              <path d="M5 12h14M13 6l6 6-6 6" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
        </form>
        """ + ("" if auth_configured() else "<p class='setup'>APP_PASSWORD is not set. Login is disabled until you configure it.</p>") + r"""
      </div>
    </section>
  </main>
</body>
</html>
"""


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Technical Ratings Gauge</title>
  <style>
    :root {
      --bg: #ffffff;
      --text: #0b0f19;
      --muted: #6b7280;
      --soft: #f6f8fb;
      --line: #e7ebf1;
      --tab: #eef1f5;
      --blue: #1f5bff;
      --indigo: #5a54e8;
      --purple: #8e47bc;
      --pink: #c93b75;
      --red: #f23645;
      --track: #edf0f3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    main {
      width: min(1880px, calc(100vw - 28px));
      margin: 0 auto;
      padding: 24px 0 42px;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 32px;
    }
    .topbar-left {
      display: flex;
      align-items: center;
      gap: 18px;
      min-width: 0;
      flex: 1 1 auto;
    }
    .app-logo {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      flex: 0 0 auto;
      text-decoration: none;
    }
    .app-logo-mark {
      width: 34px;
      height: 34px;
      display: grid;
      place-items: center;
      border-radius: 9px;
      background: linear-gradient(135deg, #3b6fef, #7c4def);
      box-shadow: 0 10px 20px rgba(59, 111, 239, .22);
    }
    .app-logo-mark svg {
      width: 18px;
      height: 18px;
    }
    .app-logo-text {
      color: #101828;
      font-size: 19px;
      font-weight: 900;
      letter-spacing: 0;
      white-space: nowrap;
    }
    .app-logo-text span { color: #12b6ac; }
    .timeframes {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: nowrap;
    }
    .tf {
      height: 44px;
      border: 0;
      border-radius: 8px;
      background: transparent;
      color: #05070c;
      padding: 0 14px;
      font-size: 16px;
      font-weight: 700;
      cursor: pointer;
      white-space: nowrap;
    }
    .tf.active {
      background: var(--tab);
      box-shadow: inset 0 0 0 1px #e5e9ef;
    }
    .symbol-form {
      display: flex;
      gap: 10px;
      align-items: center;
      flex: 0 0 auto;
      flex-wrap: nowrap;
    }
    input, .refresh, .kite-login {
      height: 40px;
      border-radius: 6px;
      border: 1px solid #d9dde5;
      background: #fff;
      color: var(--text);
      font: inherit;
    }
    .search-combobox {
      position: relative;
      width: clamp(260px, 24vw, 450px);
      min-width: 220px;
    }
    .search-shell {
      position: relative;
      display: flex;
      align-items: center;
      height: 44px;
      border: 1px solid #d7deea;
      border-radius: 10px;
      background: #fff;
      box-shadow: 0 8px 24px rgba(15, 23, 42, .06);
      transition: border-color .18s, box-shadow .18s;
    }
    .search-shell:focus-within {
      border-color: #8fb0ff;
      box-shadow: 0 0 0 4px rgba(31, 91, 255, .12), 0 10px 28px rgba(15, 23, 42, .08);
    }
    .search-icon {
      width: 17px;
      height: 17px;
      margin-left: 13px;
      border: 2px solid #778195;
      border-radius: 50%;
      flex: 0 0 auto;
    }
    .search-icon::after {
      content: "";
      position: absolute;
      width: 7px;
      height: 2px;
      margin: 12px 0 0 -1px;
      background: #778195;
      transform: rotate(45deg);
      transform-origin: left center;
      border-radius: 2px;
    }
    #symbol {
      width: 100%;
      height: 42px;
      border: 0;
      border-radius: 10px;
      padding: 0 12px 0 10px;
      font-weight: 800;
      outline: none;
      background: transparent;
      text-transform: uppercase;
    }
    .symbol-menu {
      position: absolute;
      z-index: 30;
      top: calc(100% + 8px);
      left: 0;
      right: 0;
      display: none;
      max-height: 360px;
      overflow: auto;
      border: 1px solid #dfe5ef;
      border-radius: 12px;
      background: #fff;
      box-shadow: 0 20px 45px rgba(15, 23, 42, .16);
      padding: 6px;
    }
    .symbol-menu.open { display: block; }
    .symbol-option {
      width: 100%;
      min-height: 58px;
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: center;
      border: 0;
      border-radius: 8px;
      background: transparent;
      padding: 9px 10px;
      color: #101828;
      text-align: left;
      cursor: pointer;
    }
    .symbol-option:hover,
    .symbol-option.active {
      background: #f2f6ff;
    }
    .symbol-name {
      display: block;
      font-size: 14px;
      font-weight: 900;
      line-height: 1.2;
      color: #0b0f19;
    }
    .symbol-company {
      display: block;
      margin-top: 4px;
      color: #667085;
      font-size: 12px;
      font-weight: 650;
      line-height: 1.25;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .symbol-badges {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      justify-content: flex-end;
    }
    .symbol-badge {
      border-radius: 999px;
      background: #eef2f7;
      color: #475467;
      padding: 4px 7px;
      font-size: 11px;
      font-weight: 900;
      white-space: nowrap;
    }
    .symbol-badge.fo {
      background: #eaf1ff;
      color: var(--blue);
    }
    .symbol-empty {
      padding: 13px 12px;
      color: #667085;
      font-size: 13px;
      font-weight: 700;
    }
    .refresh, .kite-login {
      padding: 0 14px;
      cursor: pointer;
      background: #111827;
      color: #fff;
      font-weight: 700;
    }
    .kite-login {
      display: inline-flex;
      align-items: center;
      text-decoration: none;
      background: #ff7a00;
      border-color: #ff7a00;
      white-space: nowrap;
    }
    .view-toggle {
      display: inline-flex;
      align-items: center;
      text-decoration: none;
      height: 40px;
      border: 1px solid #d9dde5;
      border-radius: 6px;
      background: #fff;
      color: #111827;
      padding: 0 12px;
      font: inherit;
      font-weight: 750;
      cursor: pointer;
      white-space: nowrap;
    }
    .gauges {
      display: grid;
      grid-template-columns: minmax(300px, 1fr) minmax(380px, 1.18fr) minmax(300px, 1fr);
      align-items: start;
      gap: 26px;
    }
    .gauge-card {
      min-width: 0;
      text-align: center;
      background: transparent;
      padding: 6px 18px 24px;
    }
    .gauge-card:not(.summary) {
      margin-top: 60px;
    }
    h2 {
      margin: 0 0 6px;
      font-size: 19px;
      font-weight: 700;
      line-height: 1.2;
      letter-spacing: 0;
      color: #0b0f19;
    }
    .gauge-card.summary h2 { margin-bottom: 10px; }
    .gauge-host {
      width: 100%;
      height: 225px;
      display: grid;
      place-items: center;
      overflow: visible;
    }
    .summary .gauge-host { height: 280px; }
    .gauge {
      width: 100%;
      max-width: 330px;
      height: auto;
      overflow: visible;
    }
    .summary .gauge { max-width: 470px; }
    .arc-track {
      fill: none;
      stroke: #ededee;
      stroke-width: 14;
      stroke-linecap: butt;
    }
    .arc-segment {
      fill: none;
      stroke-width: 14;
      stroke-linecap: butt;
    }
    .summary .arc-track,
    .summary .arc-segment { stroke-width: 16; }
    .needle {
      transition: none;
    }
    .needle line { stroke: #111318; stroke-width: 3; stroke-linecap: round; }
    .needle circle { fill: #111318; }
    .mark {
      fill: #a7adb8;
      font-weight: 700;
      font-size: 14px;
    }
    .mark.active { fill: var(--blue); font-weight: 800; }
    .summary .mark { font-size: 15px; }
    .value {
      margin-top: -12px;
      color: var(--blue);
      font-size: 28px;
      font-weight: 800;
      background: transparent;
    }
    .value.sell { color: var(--red); }
    .value.neutral { color: #7d8492; }
    .value.buy { color: var(--blue); background: transparent; }
    .value.sell { background: transparent; }
    .value.neutral { background: transparent; }
    .summary .value {
      margin-top: -18px;
      font-size: 40px;
    }
    .counts {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      width: min(330px, 100%);
      margin: 24px auto 0;
    }
    .summary .counts { width: min(360px, 100%); }
    .count-label {
      font-size: 18px;
      font-weight: 800;
      line-height: 1.2;
    }
    .count-value {
      margin-top: 8px;
      font-size: 24px;
      line-height: 1;
    }
    .meta {
      margin-top: 18px;
      min-height: 22px;
      color: #6b7280;
      text-align: center;
      font-size: 14px;
    }
    .meta a {
      color: var(--blue);
      font-weight: 800;
      text-decoration: none;
    }
    .confirmation {
      min-height: 24px;
      margin: 8px auto 0;
      text-align: center;
      color: #667085;
      font-size: 13px;
      font-weight: 700;
    }
    .confirmation span {
      display: inline-flex;
      align-items: center;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      padding: 5px 10px;
      background: #f8fafc;
    }
    .confirmation .applied {
      color: #155eef;
      background: #edf3ff;
      border-color: #c7d7fe;
    }
    .confirmation .warn {
      color: #b42318;
      background: #fff0f2;
      border-color: #ffc9d0;
    }
    .tables {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 22px;
      margin-top: 30px;
      padding-top: 24px;
      border-top: 1px solid var(--line);
    }
    .table-panel h3 {
      margin: 0 0 10px;
      font-size: 17px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 14px;
      overflow-wrap: anywhere;
    }
    th { color: #747b89; font-weight: 700; }
    .signal {
      display: inline-flex;
      min-width: 68px;
      justify-content: center;
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 800;
    }
    .buy { color: var(--blue); background: #edf3ff; }
    .sell { color: var(--red); background: #fff0f2; }
    .neutral { color: #6d7480; background: #f2f3f5; }
    .hidden { display: none !important; }
    .chain-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 10px;
    }
    .chain-head h2 {
      margin: 0;
      font-size: 24px;
    }
    .chain-controls {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .expiry-field {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      min-height: 44px;
      border: 1px solid #dfe5ef;
      border-radius: 10px;
      background: #fff;
      padding: 0 10px 0 13px;
      box-shadow: 0 8px 24px rgba(15, 23, 42, .05);
    }
    .expiry-field span {
      color: #667085;
      font-size: 13px;
      font-weight: 900;
      text-transform: uppercase;
    }
    select {
      height: 40px;
      border: 1px solid #d9dde5;
      border-radius: 6px;
      background: #fff;
      padding: 0 10px;
      font: inherit;
      font-weight: 650;
    }
    .expiry-field select {
      min-width: 140px;
      border: 0;
      padding: 0 2px;
      outline: none;
      font-weight: 850;
    }
    .chain-summary {
      margin: 0 0 14px;
      color: var(--muted);
      font-size: 14px;
    }
    .chain-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 26px;
      align-items: start;
    }
    .option-board {
      min-width: 0;
      border: 3px solid var(--board);
      background: #fff;
    }
    .option-board.call { --board: #119b45; --head: #09a74d; --soft-head: #e9fff0; --grid: #a7d8b8; }
    .option-board.put { --board: #c51f3c; --head: #811337; --soft-head: #fff0f3; --grid: #e7a4b1; }
    .option-board h3 {
      margin: 0;
      padding: 9px 10px;
      color: #fff;
      background: var(--head);
      text-align: center;
      font-size: 20px;
      line-height: 1.1;
      letter-spacing: 0;
      text-transform: uppercase;
    }
    .chain-scroll { overflow-x: auto; }
    .option-table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    .option-table th {
      padding: 8px 6px;
      border: 2px solid var(--grid);
      background: var(--head);
      color: #fff;
      text-align: center;
      font-size: 13px;
      font-weight: 900;
      line-height: 1.1;
      white-space: normal;
      text-transform: uppercase;
    }
    .option-table th.strike-head {
      background: var(--soft-head);
      color: #126b46;
      text-decoration: underline;
    }
    .put .option-table th.strike-head { color: #811337; }
    .option-table td {
      padding: 7px 6px;
      border: 2px solid var(--grid);
      color: #111827;
      background: #fff;
      text-align: center;
      font-size: 15px;
      font-weight: 800;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .option-table .strike-cell {
      background: #f8fff9;
      color: #111827;
      font-weight: 900;
    }
    .put .option-table .strike-cell { background: #fff8fa; }
    .option-table .atm-cell {
      background: #0f86a6 !important;
      color: #06121a;
    }
    .oi-change.positive {
      background: #20b52b;
      color: #071b08;
    }
    .oi-change.negative {
      background: #ff3b18;
      color: #1f0801;
    }
    .oi-change.flat {
      background: #f8fafc;
      color: #111827;
    }
    @media (max-width: 1180px) {
      .topbar { align-items: stretch; flex-direction: column; margin-bottom: 28px; }
      .topbar-left { align-items: flex-start; flex-direction: column; gap: 14px; }
      .symbol-form { justify-content: flex-start; }
      .gauges { grid-template-columns: 1fr; gap: 18px; }
      .summary { order: -1; }
      .tables { grid-template-columns: 1fr; }
      .chain-grid { grid-template-columns: 1fr; }
      .chain-head { align-items: stretch; flex-direction: column; }
    }
    @media (max-width: 560px) {
      main { width: min(100vw - 24px, 1440px); padding-top: 14px; }
      .timeframes { overflow-x: auto; flex-wrap: nowrap; padding-bottom: 4px; }
      .tf { font-size: 15px; height: 38px; padding: 0 11px; }
      .search-combobox { width: 100%; min-width: 0; }
      .refresh, .view-toggle, .kite-login { flex: 0 0 auto; }
      .gauge-card { padding: 18px 12px 20px; }
      .gauge-host { height: 205px; }
      .summary .gauge-host { height: 238px; }
      .count-label { font-size: 15px; }
      .count-value { font-size: 21px; }
    }
  </style>
</head>
<body>
<main>
  <div class="topbar">
    <div class="topbar-left">
      <a class="app-logo" href="/" aria-label="StockRadar home">
        <span class="app-logo-mark">
          <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path d="M3 17 L9 10 L13 14 L21 5" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M15 5 H21 V11" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </span>
        <span class="app-logo-text">Stock<span>Radar</span></span>
      </a>
      <nav class="timeframes" aria-label="Timeframes">
        <button class="tf" type="button" data-interval="1m">1 minute</button>
        <button class="tf" type="button" data-interval="5m">5 minutes</button>
        <button class="tf" type="button" data-interval="15m">15 minutes</button>
        <button class="tf active" type="button" data-interval="30m">30 minutes</button>
      </nav>
    </div>
    <form class="symbol-form" id="controls">
      <div class="search-combobox">
        <div class="search-shell">
          <span class="search-icon" aria-hidden="true"></span>
          <input id="symbol" value="ASIANPAINT" aria-label="Search stock" autocomplete="off" spellcheck="false">
        </div>
        <div class="symbol-menu" id="symbolSuggestions" role="listbox"></div>
      </div>
      <select id="autoRefresh" aria-label="Auto refresh">
        <option value="0">Auto: Off</option>
        <option value="1000">Auto: 1s</option>
        <option value="2000">Auto: 2s</option>
        <option value="5000">Auto: 5s</option>
      </select>
      <button class="view-toggle" id="toggleView" type="button">Option Chain</button>
      <a class="kite-login" href="/kite/login">Connect Kite</a>
      <a class="view-toggle" href="/logout">Logout</a>
    </form>
  </div>

  <section id="ratingsView">
    <section class="gauges">
      <article class="gauge-card">
        <h2>Oscillators</h2>
        <div class="gauge-host" id="oscGauge"></div>
        <div class="value" id="oscValue">Loading</div>
        <div class="counts">
          <div><div class="count-label">Sell</div><div class="count-value" id="oscSell">0</div></div>
          <div><div class="count-label">Neutral</div><div class="count-value" id="oscNeutral">0</div></div>
          <div><div class="count-label">Buy</div><div class="count-value" id="oscBuy">0</div></div>
        </div>
      </article>

      <article class="gauge-card summary">
        <h2>Summary</h2>
        <div class="gauge-host" id="summaryGauge"></div>
        <div class="value" id="summaryValue">Loading</div>
        <div class="counts">
          <div><div class="count-label">Sell</div><div class="count-value" id="summarySell">0</div></div>
          <div><div class="count-label">Neutral</div><div class="count-value" id="summaryNeutral">0</div></div>
          <div><div class="count-label">Buy</div><div class="count-value" id="summaryBuy">0</div></div>
        </div>
      </article>

      <article class="gauge-card">
        <h2>Moving Averages</h2>
        <div class="gauge-host" id="maGauge"></div>
        <div class="value" id="maValue">Loading</div>
        <div class="counts">
          <div><div class="count-label">Sell</div><div class="count-value" id="maSell">0</div></div>
          <div><div class="count-label">Neutral</div><div class="count-value" id="maNeutral">0</div></div>
          <div><div class="count-label">Buy</div><div class="count-value" id="maBuy">0</div></div>
        </div>
      </article>
    </section>

    <p class="meta" id="meta">Waiting for data</p>
    <p class="confirmation" id="confirmation"></p>

    <section class="tables">
      <div class="table-panel">
        <h3>Oscillator Signals</h3>
        <table><thead><tr><th>Indicator</th><th>Value</th><th>Signal</th></tr></thead><tbody id="oscRows"></tbody></table>
      </div>
      <div class="table-panel">
        <h3>Moving Average Signals</h3>
        <table><thead><tr><th>Indicator</th><th>Value</th><th>Signal</th></tr></thead><tbody id="maRows"></tbody></table>
      </div>
    </section>
  </section>

  <section id="chainView" class="hidden">
    <div class="chain-head">
      <h2>Option Chain</h2>
      <div class="chain-controls">
        <label class="expiry-field">
          <span>Expiry</span>
          <select id="expirySelect" aria-label="Expiry"></select>
        </label>
      </div>
    </div>
    <p class="chain-summary" id="chainMeta">Waiting for option chain</p>
    <div class="chain-grid">
      <section class="option-board call">
        <h3 id="callTitle">Call Option</h3>
        <div class="chain-scroll">
          <table class="option-table">
            <thead>
              <tr>
                <th class="strike-head">Strike</th>
                <th>Last</th>
                <th>Open Int</th>
                <th>Change In OI</th>
                <th>OI %</th>
              </tr>
            </thead>
            <tbody id="callRows"></tbody>
          </table>
        </div>
      </section>
      <section class="option-board put">
        <h3 id="putTitle">Put Option</h3>
        <div class="chain-scroll">
          <table class="option-table">
            <thead>
              <tr>
                <th class="strike-head">Strike</th>
                <th>Last</th>
                <th>Open Int</th>
                <th>Change In OI</th>
                <th>OI %</th>
              </tr>
            </thead>
            <tbody id="putRows"></tbody>
          </table>
        </div>
      </section>
    </div>
  </section>
</main>

<script>
const form = document.getElementById("controls");
const tfButtons = [...document.querySelectorAll(".tf")];
const toggleViewButton = document.getElementById("toggleView");
const ratingsView = document.getElementById("ratingsView");
const chainView = document.getElementById("chainView");
const expirySelect = document.getElementById("expirySelect");
const symbolInput = document.getElementById("symbol");
const symbolSuggestions = document.getElementById("symbolSuggestions");
const autoRefreshSelect = document.getElementById("autoRefresh");
let activeInterval = "30m";
let activeView = "ratings";
let symbolTimer = 0;
let autoRefreshTimer = 0;
let refreshInFlight = false;
let symbolResults = [];
let activeSymbolIndex = -1;
const previousGaugeScores = {};

function labelForSignal(value) {
  return value > 0 ? "Buy" : value < 0 ? "Sell" : "Neutral";
}

function classForSignal(value) {
  return value > 0 ? "buy" : value < 0 ? "sell" : "neutral";
}

function labelForScore(score) {
  if (score < -0.5) return "Strong sell";
  if (score < -0.1) return "Sell";
  if (score <= 0.1) return "Neutral";
  if (score <= 0.5) return "Buy";
  return "Strong buy";
}

function classForScore(score) {
  if (score < -0.1) return "sell";
  if (score <= 0.1) return "neutral";
  return "buy";
}

function setValue(elId, score) {
  const el = document.getElementById(elId);
  el.textContent = labelForScore(score);
  el.className = `value ${classForScore(score)}`;
}

function counts(signals) {
  return Object.values(signals).reduce((acc, value) => {
    if (value > 0) acc.buy += 1;
    else if (value < 0) acc.sell += 1;
    else acc.neutral += 1;
    return acc;
  }, { sell: 0, neutral: 0, buy: 0 });
}

const valueKeys = {
  RSI14: "RSI14",
  Stochastic: "StochK",
  CCI20: "CCI20",
  ADX14: "ADX14",
  AO: "AO",
  Momentum10: "Momentum10",
  MACD: "MACD",
  StochRSI: "StochRSIK",
  WilliamsR: "WilliamsR",
  BullBearPower: "BullPower13",
  UltimateOscillator: "UltimateOscillator",
  HMA9: "HMA9",
  VWMA20: "VWMA20",
  Ichimoku: "IchimokuBase"
};

function formatValue(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toFixed(2);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, char => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  }[char]));
}

function renderRows(targetId, signals, values) {
  document.getElementById(targetId).innerHTML = Object.entries(signals).map(([name, value]) => {
    const displayValue = values[valueKeys[name] || name];
    return `<tr><td>${escapeHtml(name)}</td><td>${formatValue(displayValue)}</td><td><span class="signal ${classForSignal(value)}">${labelForSignal(value)}</span></td></tr>`;
  }).join("");
}

function polar(cx, cy, radius, angle) {
  const radians = (angle - 180) * Math.PI / 180;
  return { x: cx + radius * Math.cos(radians), y: cy + radius * Math.sin(radians) };
}

function arcPath(cx, cy, radius, startAngle, endAngle) {
  const start = polar(cx, cy, radius, endAngle);
  const end = polar(cx, cy, radius, startAngle);
  const largeArc = endAngle - startAngle <= 180 ? 0 : 1;
  return `M ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 0 ${end.x} ${end.y}`;
}

function zoneIndex(score) {
  if (score < -0.5) return 0;
  if (score < -0.1) return 1;
  if (score <= 0.1) return 2;
  if (score <= 0.5) return 3;
  return 4;
}

function angleForScore(score) {
  return Math.max(-1, Math.min(1, score)) * 76;
}

function gaugeSvg(id, score, compact, needleScore = score) {
  const w = compact ? 420 : 500;
  const h = compact ? 250 : 282;
  const cx = compact ? 210 : 250;
  const cy = compact ? 204 : 230;
  const r = compact ? 128 : 154;
  const labelR = compact ? 176 : 214;
  const needleLength = compact ? 86 : 104;
  const clamped = Math.max(-1, Math.min(1, score));
  const angle = angleForScore(needleScore);
  const active = zoneIndex(clamped);
  const labels = [
    "Strong sell",
    "Sell",
    "Neutral",
    "Buy",
    "Strong buy"
  ];
  const positions = [8, 45, 90, 135, 172];
  const fillEnd = Math.max(0.1, Math.min(179.9, ((clamped + 1) / 2) * 180));
  const gradId = `gaugeGradient-${id}`;
  const left = polar(cx, cy, r, 0);
  const right = polar(cx, cy, r, 180);
  return `
    <svg class="gauge" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${id} rating gauge">
      <defs>
        <linearGradient id="${gradId}" gradientUnits="userSpaceOnUse" x1="${left.x}" y1="${left.y}" x2="${right.x}" y2="${right.y}">
          <stop offset="0%" stop-color="#f23645"/>
          <stop offset="34%" stop-color="#d83c79"/>
          <stop offset="58%" stop-color="#914bbb"/>
          <stop offset="82%" stop-color="#5b55dd"/>
          <stop offset="100%" stop-color="#2962ff"/>
        </linearGradient>
      </defs>
      <path class="arc-track" d="${arcPath(cx, cy, r, 0, 180)}"/>
      <path class="arc-segment" stroke="url(#${gradId})" d="${arcPath(cx, cy, r, 0, fillEnd)}"/>
      <g class="needle" id="${id}Needle" data-cx="${cx}" data-cy="${cy}" data-angle="${angle}" transform="rotate(${angle} ${cx} ${cy})">
        <line x1="${cx}" y1="${cy}" x2="${cx}" y2="${cy - needleLength}"/>
        <circle cx="${cx}" cy="${cy}" r="6.5"/>
      </g>
      ${labels.map((text, i) => {
        const extra = i === 0 || i === 4 ? (compact ? 22 : 28) : 0;
        const p = polar(cx, cy, labelR + extra, positions[i]);
        const cls = i === active ? "active" : "";
        return `<text class="mark ${cls}" x="${p.x}" y="${p.y + 5}" text-anchor="middle">${text}</text>`;
      }).join("")}
    </svg>`;
}

function renderGauge(targetId, gaugeId, score, compact) {
  const previous = previousGaugeScores[gaugeId];
  const startScore = previous === undefined ? score : previous;
  const host = document.getElementById(targetId);
  host.innerHTML = gaugeSvg(gaugeId, score, compact, startScore);
  previousGaugeScores[gaugeId] = score;
  requestAnimationFrame(() => animateNeedle(gaugeId, angleForScore(startScore), angleForScore(score)));
}

function animateNeedle(gaugeId, fromAngle, toAngle) {
  const needle = document.getElementById(`${gaugeId}Needle`);
  if (!needle) return;
  const cx = Number(needle.dataset.cx);
  const cy = Number(needle.dataset.cy);
  const started = performance.now();
  const duration = 950;
  function step(now) {
    const progress = Math.min(1, (now - started) / duration);
    const eased = 1 - Math.pow(1 - progress, 3);
    const angle = fromAngle + (toAngle - fromAngle) * eased;
    needle.setAttribute("transform", `rotate(${angle} ${cx} ${cy})`);
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

function setCounts(prefix, data) {
  document.getElementById(`${prefix}Sell`).textContent = data.sell;
  document.getElementById(`${prefix}Neutral`).textContent = data.neutral;
  document.getElementById(`${prefix}Buy`).textContent = data.buy;
}

function formatTvRecommendation(value) {
  return String(value || "NEUTRAL").replace("_", " ").toLowerCase().replace(/\b\w/g, char => char.toUpperCase());
}

function renderConfirmation(confirmation) {
  const host = document.getElementById("confirmation");
  if (!confirmation || !confirmation.checked) {
    host.textContent = "";
    return;
  }
  if (confirmation.error) {
    host.innerHTML = `<span class="warn">TradingView confirmation failed: ${escapeHtml(confirmation.error)}</span>`;
    return;
  }
  if (!confirmation.summary) {
    host.innerHTML = `<span>${escapeHtml(confirmation.reason || "TradingView confirmation skipped")}</span>`;
    return;
  }
  const cacheText = confirmation.cache === "hit" ? `cached ${confirmation.age_seconds || 0}s` : "fresh";
  const cls = confirmation.applied ? "applied" : "";
  host.innerHTML = `<span class="${cls}">TradingView ${formatTvRecommendation(confirmation.summary)} | Osc ${formatTvRecommendation(confirmation.oscillators)} | MA ${formatTvRecommendation(confirmation.moving_averages)} | ${cacheText}</span>`;
}

function render(data, bars, confirmation) {
  const maCounts = counts(data.moving_averages);
  const oscCounts = counts(data.oscillators);
  const summaryCounts = counts({ ...data.moving_averages, ...data.oscillators });

  renderGauge("oscGauge", "osc", data.oscillator_score, true);
  renderGauge("summaryGauge", "summary", data.overall_score, false);
  renderGauge("maGauge", "ma", data.ma_score, true);

  setValue("oscValue", data.oscillator_score);
  setValue("summaryValue", data.overall_score);
  setValue("maValue", data.ma_score);

  setCounts("osc", oscCounts);
  setCounts("summary", summaryCounts);
  setCounts("ma", maCounts);

  document.getElementById("meta").textContent =
    `${data.symbol} | ${activeInterval} | Last price ${data.price.toFixed(2)} | ${bars ?? "--"} bars`;
  renderConfirmation(confirmation);
  renderRows("oscRows", data.oscillators, data.indicator_values);
  renderRows("maRows", data.moving_averages, data.indicator_values);
}

async function load() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  document.getElementById("meta").textContent = "Loading market data...";
  try {
    const qs = new URLSearchParams({
      symbol: symbolInput.value,
      interval: activeInterval,
      range: "2y"
    });
    const response = await fetch(`/api/ratings?${qs.toString()}`);
    const payload = await response.json();
    if (!payload.ok) throw new Error(payload.error || "Unable to calculate ratings");
    render(payload.data, payload.bars, payload.confirmation);
  } finally {
    refreshInFlight = false;
  }
}

function money(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toFixed(2);
}

function integer(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toLocaleString("en-IN");
}

function opt(side, key) {
  return side && side[key] !== undefined ? side[key] : null;
}

function oiChange(side) {
  const value = opt(side, "oi_change");
  return value === null || value === undefined ? null : Number(value);
}

function oiPercent(side) {
  const change = oiChange(side);
  const oi = opt(side, "oi");
  if (change === null || oi === null || oi === undefined || Number(oi) === 0) return null;
  const previous = Number(oi) - change;
  if (!Number.isFinite(previous) || previous <= 0) return null;
  return (change / previous) * 100;
}

function oiClass(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value)) || Number(value) === 0) return "flat";
  return Number(value) > 0 ? "positive" : "negative";
}

function percent(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  return `${Math.round(Number(value))} %`;
}

function optionRows(rows, sideKey, atmStrike) {
  return rows.map(row => {
    const side = row[sideKey] || {};
    const change = oiChange(side);
    const pct = oiPercent(side);
    const atm = Number(row.strike) === Number(atmStrike) ? " atm-cell" : "";
    return `<tr>
      <td class="strike-cell${atm}">${money(row.strike)}</td>
      <td>${money(opt(side, "ltp"))}</td>
      <td>${integer(opt(side, "oi"))}</td>
      <td class="oi-change ${oiClass(change)}">${integer(change)}</td>
      <td>${percent(pct)}</td>
    </tr>`;
  }).join("");
}

function renderOptionChain(data) {
  document.getElementById("chainMeta").textContent =
    `${data.symbol} | Spot ${money(data.spot)} | Expiry ${data.expiry}`;
  document.getElementById("callTitle").textContent = `${data.option_underlying || data.symbol} Call Option`;
  document.getElementById("putTitle").textContent = `${data.option_underlying || data.symbol} Put Option`;
  const previousExpiry = expirySelect.value;
  expirySelect.innerHTML = data.expiries.map(expiry => {
    const selected = expiry === data.expiry || expiry === previousExpiry ? "selected" : "";
    return `<option value="${escapeHtml(expiry)}" ${selected}>${escapeHtml(expiry)}</option>`;
  }).join("");
  const atmStrike = data.rows.reduce((best, row) => {
    if (best === null) return row.strike;
    return Math.abs(Number(row.strike) - Number(data.spot)) < Math.abs(Number(best) - Number(data.spot)) ? row.strike : best;
  }, null);
  document.getElementById("callRows").innerHTML = optionRows(data.rows, "CE", atmStrike);
  document.getElementById("putRows").innerHTML = optionRows(data.rows, "PE", atmStrike);
}

async function loadOptionChain() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  document.getElementById("chainMeta").textContent = "Loading option chain...";
  try {
    const qs = new URLSearchParams({
      symbol: symbolInput.value,
      expiry: expirySelect.value,
      strikes: "8"
    });
    const response = await fetch(`/api/option-chain?${qs.toString()}`);
    const payload = await response.json();
    if (!payload.ok) throw new Error(payload.error || "Unable to load option chain");
    renderOptionChain(payload.data);
  } finally {
    refreshInFlight = false;
  }
}

function setView(view) {
  activeView = view;
  ratingsView.classList.toggle("hidden", view !== "ratings");
  chainView.classList.toggle("hidden", view !== "chain");
  toggleViewButton.textContent = view === "ratings" ? "Option Chain" : "Technicals";
  if (view === "chain") {
    loadOptionChain().catch(showChainError);
  } else {
    load().catch(showError);
  }
}

async function loadSymbolSuggestions() {
  const q = symbolInput.value.trim();
  if (q.length < 2) {
    hideSymbolSuggestions();
    return;
  }
  renderSymbolSuggestionState("Searching...");
  const response = await fetch(`/api/symbols?q=${encodeURIComponent(q)}&limit=30`);
  const payload = await response.json();
  if (!payload.ok) {
    hideSymbolSuggestions();
    return;
  }
  symbolResults = payload.data || [];
  activeSymbolIndex = symbolResults.length ? 0 : -1;
  renderSymbolSuggestions();
}

function renderSymbolSuggestionState(message) {
  symbolResults = [];
  activeSymbolIndex = -1;
  symbolSuggestions.innerHTML = `<div class="symbol-empty">${escapeHtml(message)}</div>`;
  symbolSuggestions.classList.add("open");
}

function renderSymbolSuggestions() {
  if (!symbolResults.length) {
    renderSymbolSuggestionState("No matching stocks found");
    return;
  }
  symbolSuggestions.innerHTML = symbolResults.map((item, index) => {
    const active = index === activeSymbolIndex ? " active" : "";
    const kind = item.kind || "Stock";
    const fo = item.optionable ? `<span class="symbol-badge fo">F&O</span>` : "";
    return `<button class="symbol-option${active}" type="button" role="option" data-index="${index}" aria-selected="${index === activeSymbolIndex}">
      <span>
        <span class="symbol-name">${escapeHtml(item.tradingsymbol || item.symbol)}</span>
        <span class="symbol-company">${escapeHtml(item.name || "")}</span>
      </span>
      <span class="symbol-badges">
        <span class="symbol-badge">${escapeHtml(kind)}</span>
        ${fo}
      </span>
    </button>`;
  }).join("");
  symbolSuggestions.classList.add("open");
}

function hideSymbolSuggestions() {
  symbolSuggestions.classList.remove("open");
  activeSymbolIndex = -1;
}

function chooseSymbol(index) {
  const item = symbolResults[index];
  if (!item) return;
  symbolInput.value = item.symbol || item.tradingsymbol;
  hideSymbolSuggestions();
  runCurrentRefresh();
}

tfButtons.forEach(button => {
  button.addEventListener("click", () => {
    activeInterval = button.dataset.interval;
    tfButtons.forEach(item => item.classList.toggle("active", item === button));
    if (activeView === "ratings") {
      load().catch(showError);
    }
  });
});

function showError(error) {
  const meta = document.getElementById("meta");
  document.getElementById("confirmation").textContent = "";
  if (String(error.message).includes("Kite not connected")) {
    meta.innerHTML = `${escapeHtml(error.message)} <a href="/kite/login">Connect Kite</a>`;
  } else {
    meta.textContent = error.message;
  }
  ["oscValue", "summaryValue", "maValue"].forEach(id => document.getElementById(id).textContent = "Error");
}

function showChainError(error) {
  const chainMeta = document.getElementById("chainMeta");
  if (String(error.message).includes("Kite session")) {
    chainMeta.innerHTML = `${escapeHtml(error.message)} <a href="/kite/login">Connect Kite</a>`;
  } else {
    chainMeta.textContent = error.message;
  }
}

function runCurrentRefresh() {
  if (activeView === "chain") {
    loadOptionChain().catch(showChainError);
  } else {
    load().catch(showError);
  }
}

function configureAutoRefresh() {
  clearInterval(autoRefreshTimer);
  autoRefreshTimer = 0;
  const intervalMs = Number(autoRefreshSelect.value);
  if (intervalMs > 0) {
    autoRefreshTimer = setInterval(runCurrentRefresh, intervalMs);
  }
}

form.addEventListener("submit", event => {
  event.preventDefault();
  if (activeView === "chain") {
    loadOptionChain().catch(showChainError);
  } else {
    load().catch(showError);
  }
});

symbolInput.addEventListener("input", () => {
  clearTimeout(symbolTimer);
  symbolTimer = setTimeout(() => loadSymbolSuggestions().catch(() => {}), 180);
});

symbolInput.addEventListener("focus", () => {
  if (symbolResults.length) renderSymbolSuggestions();
});

symbolInput.addEventListener("keydown", event => {
  if (!symbolSuggestions.classList.contains("open")) return;
  if (event.key === "ArrowDown") {
    event.preventDefault();
    activeSymbolIndex = Math.min(symbolResults.length - 1, activeSymbolIndex + 1);
    renderSymbolSuggestions();
  } else if (event.key === "ArrowUp") {
    event.preventDefault();
    activeSymbolIndex = Math.max(0, activeSymbolIndex - 1);
    renderSymbolSuggestions();
  } else if (event.key === "Enter" && activeSymbolIndex >= 0) {
    event.preventDefault();
    chooseSymbol(activeSymbolIndex);
  } else if (event.key === "Escape") {
    hideSymbolSuggestions();
  }
});

symbolSuggestions.addEventListener("mousedown", event => {
  const option = event.target.closest(".symbol-option");
  if (!option) return;
  event.preventDefault();
  chooseSymbol(Number(option.dataset.index));
});

document.addEventListener("mousedown", event => {
  if (!event.target.closest(".search-combobox")) hideSymbolSuggestions();
});

autoRefreshSelect.addEventListener("change", configureAutoRefresh);

toggleViewButton.addEventListener("click", () => {
  setView(activeView === "ratings" ? "chain" : "ratings");
});

expirySelect.addEventListener("change", () => loadOptionChain().catch(showChainError));

load().catch(showError);
</script>
</body>
</html>
"""


def run_server(host: str, port: int) -> None:
    validate_runtime_config(host)
    ensure_events_table()
    start_oi_baseline_scheduler()
    server = ThreadingHTTPServer((host, port), RatingsHandler)
    log_event(
        "server.start",
        host=host,
        port=port,
        data_dir=str(DATA_DIR),
        log_to_file=os.environ.get("STOCKRADAR_LOG_TO_FILE", "true"),
        log_file=str(LOG_FILE_PATH) if LOG_FILE_PATH else "",
        tradingview_confirmation=tradingview_confirmation_enabled(),
        database_enabled=database_enabled(),
        oi_baseline_enabled=oi_baseline_enabled(),
        oi_baseline_capture_time="%02d:%02d" % oi_baseline_capture_time(),
        oi_baseline_source="nse.bhavcopy",
        nse_bhavcopy_lookback_days=nse_bhavcopy_lookback_days(),
    )
    print(f"Technical Ratings app running at http://{host}:{port}")
    if LOG_FILE_PATH:
        print(f"StockRadar logs: {LOG_FILE_PATH}")
    server.serve_forever()


def run_cli(symbol: str, csv_path: str | None, interval: str, range_: str) -> None:
    started = time.perf_counter()
    log_event("cli.ratings.start", symbol=symbol, interval=interval, range=range_, csv=bool(csv_path))
    interval = validate_interval(interval)
    range_ = validate_range(range_)
    df = load_csv(csv_path) if csv_path else fetch_kite_ohlcv(symbol, interval, range_)
    result = technical_ratings(df, symbol=validate_symbol(symbol))
    log_event(
        "cli.ratings.success",
        symbol=result.symbol,
        interval=interval,
        bars=len(df),
        overall=result.overall_label,
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    print(json.dumps(result_to_dict(result), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingView-style Technical Ratings gauge")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--interval", default="30m")
    parser.add_argument("--range", default="2y")
    parser.add_argument("--csv", default=None, help="CSV path with Open, High, Low, Close, Volume columns")
    parser.add_argument("--cli", action="store_true", help="Print JSON instead of starting the web app")
    parser.add_argument("--kite-login-url", action="store_true", help="Print Kite Connect login URL")
    parser.add_argument("--kite-request-token", default=None, help="Exchange Kite request_token for access_token")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=80)
    args = parser.parse_args()

    if args.kite_login_url:
        print(kite_login_url())
    elif args.kite_request_token:
        payload = exchange_kite_request_token(args.kite_request_token)
        print(json.dumps(payload.get("data", {}), indent=2))
    elif args.cli:
        run_cli(args.symbol, args.csv, args.interval, args.range)
    else:
        run_server(args.host, args.port)


if __name__ == "__main__":
    main()
