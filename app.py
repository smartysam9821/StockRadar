from __future__ import annotations

import argparse
import base64
import difflib
import hmac
import os
import json
import secrets
import time
import urllib.parse
from http import cookies
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect

from technical_ratings import RatingResult, technical_ratings


DEFAULT_SYMBOL = "ASIANPAINT.NS"
KITE_INSTRUMENT_CACHE = Path("data/kite_instruments.csv")
KITE_TOKEN_FILE = Path("data/kite_access_token.json")
KITE_ACCESS_TOKEN_MEMORY = ""
APP_SESSION_COOKIE = "stock_app_session"


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


def make_session_cookie(username: str) -> str:
    payload = {
        "u": username,
        "iat": int(time.time()),
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
    return payload.get("u") == app_username() and 0 <= age <= 12 * 60 * 60


def fetch_kite_ohlcv(symbol: str, interval: str = "1d", range_: str = "2y") -> pd.DataFrame:
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
    chunks = fetch_kite_historical_chunks(
        api_key, access_token, instrument_token, source_interval, from_dt, to_dt
    )
    if not chunks:
        raise ValueError(f"No Kite candles returned for {exchange}:{tradingsymbol}.")

    df = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["Date"]).sort_values("Date")
    if interval in {"2h", "4h", "1wk", "1mo"}:
        return resample_ohlcv(df, interval)
    return df.reset_index(drop=True)


def current_kite_access_token() -> str:
    if KITE_ACCESS_TOKEN_MEMORY:
        return KITE_ACCESS_TOKEN_MEMORY
    env_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token
    return load_saved_kite_access_token()


def load_saved_kite_access_token() -> str:
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
    KITE_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "access_token": access_token,
        "date": datetime.now().date().isoformat(),
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    KITE_TOKEN_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def kite_login_url() -> str:
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing KITE_API_KEY env var.")
    return f"https://kite.zerodha.com/connect/login?v=3&api_key={urllib.parse.quote(api_key)}"


def exchange_kite_request_token(request_token: str) -> dict:
    global KITE_ACCESS_TOKEN_MEMORY
    disable_kite_proxy_env()
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    api_secret = os.environ.get("KITE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise ValueError("Missing KITE_API_KEY or KITE_API_SECRET env var.")
    kite = KiteConnect(api_key=api_key, timeout=20)
    payload = kite.generate_session(request_token, api_secret=api_secret)
    access_token = payload.get("access_token", "")
    if not access_token:
        raise ValueError("Kite token exchange succeeded but no access_token returned.")
    KITE_ACCESS_TOKEN_MEMORY = access_token
    save_kite_access_token(access_token)
    return payload


def kite_client(access_token: str | None = None) -> KiteConnect:
    disable_kite_proxy_env()
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing KITE_API_KEY env var.")
    token = access_token if access_token is not None else current_kite_access_token()
    return KiteConnect(api_key=api_key, access_token=token or None, timeout=30)


def normalize_kite_symbol(symbol: str) -> tuple[str, str]:
    clean = symbol.strip().upper()
    if ":" in clean:
        exchange, tradingsymbol = clean.split(":", 1)
        return exchange, tradingsymbol
    if clean.endswith(".NS"):
        return "NSE", clean[:-3]
    return "NSE", clean


def kite_source_interval(interval: str) -> str:
    mapping = {
        "1m": "minute",
        "5m": "5minute",
        "15m": "15minute",
        "30m": "30minute",
        "1h": "60minute",
        "2h": "60minute",
        "4h": "60minute",
        "1d": "day",
        "1wk": "day",
        "1mo": "day",
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
        "1h": 365,
        "2h": 730,
        "4h": 1100,
        "1d": 900,
        "1wk": 2200,
        "1mo": 7500,
    }
    if interval in {"1m", "5m", "15m", "30m", "1h", "2h", "4h"}:
        days = minimum_days.get(interval, 120)
    elif requested_range == "max":
        days = minimum_days.get(interval, 900)
    else:
        days = max(_range_rank(requested_range), minimum_days.get(interval, 900))
    return to_dt - timedelta(days=days), to_dt


def fetch_kite_historical_chunks(
    api_key: str,
    access_token: str,
    instrument_token: int,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
) -> list[pd.DataFrame]:
    max_days = 60 if interval != "day" else 1900
    frames: list[pd.DataFrame] = []
    kite = kite_client(access_token)
    cursor = from_dt
    while cursor < to_dt:
        end = min(cursor + timedelta(days=max_days), to_dt)
        candles = kite.historical_data(
            instrument_token=instrument_token,
            from_date=cursor,
            to_date=end,
            interval=interval,
            continuous=False,
            oi=False,
        )
        if candles:
            frames.append(kite_candles_to_frame(candles))
        cursor = end + timedelta(seconds=1)
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
        elif q in haystack:
            score = 1.0 if tradingsymbol.startswith(q) else 0.85
        else:
            score = max(
                difflib.SequenceMatcher(None, q, tradingsymbol).ratio(),
                difflib.SequenceMatcher(None, q, name).ratio(),
            )
        if not q or score >= 0.45:
            optionable = tradingsymbol in option_names
            rows.append(
                {
                    "symbol": tradingsymbol,
                    "tradingsymbol": tradingsymbol,
                    "name": str(row["name"]),
                    "optionable": optionable,
                    "score": score + (0.05 if optionable else 0),
                }
            )
    rows.sort(key=lambda item: (-item["score"], item["tradingsymbol"]))
    for item in rows:
        item.pop("score", None)
    return rows[:limit]


def fetch_option_chain(symbol: str, expiry: str = "", strikes_each_side: int = 8) -> dict:
    access_token = current_kite_access_token()
    if not access_token:
        raise ValueError("Kite session not connected. Open /kite/login and complete Kite login once.")

    _, underlying = normalize_kite_symbol(symbol)
    instruments = load_kite_instruments()
    options = instruments[
        (instruments["exchange"].astype(str).str.upper() == "NFO")
        & (instruments["name"].astype(str).str.upper() == underlying)
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

    keys = [f"NFO:{row.tradingsymbol}" for row in chain.itertuples()]
    quotes = kite_client().quote(*keys) if keys else {}
    by_strike: dict[float, dict] = {}
    for row in chain.itertuples():
        strike_row = by_strike.setdefault(float(row.strike), {"strike": float(row.strike)})
        side = str(row.instrument_type).upper()
        quote = quotes.get(f"NFO:{row.tradingsymbol}", {})
        strike_row[side] = option_quote_payload(row, quote)

    rows = [by_strike[strike] for strike in sorted(by_strike)]
    return {
        "symbol": underlying,
        "spot": spot,
        "expiry": selected_expiry.isoformat(),
        "expiries": [item.isoformat() for item in future_expiries[:12]],
        "rows": rows,
    }


def kite_spot_price(underlying: str) -> float:
    quote_key = f"NSE:{underlying}"
    ltp = kite_client().ltp(quote_key)
    return float(ltp[quote_key]["last_price"])


def option_quote_payload(row, quote: dict) -> dict:
    return {
        "tradingsymbol": row.tradingsymbol,
        "ltp": quote.get("last_price"),
        "change": quote.get("net_change"),
        "oi": quote.get("oi"),
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
    if not KITE_INSTRUMENT_CACHE.exists() or cache_age_seconds(KITE_INSTRUMENT_CACHE) > 12 * 60 * 60:
        KITE_INSTRUMENT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        instruments = pd.DataFrame(kite_client(access_token="").instruments())
        instruments.to_csv(KITE_INSTRUMENT_CACHE, index=False)
    return pd.read_csv(KITE_INSTRUMENT_CACHE)


def cache_age_seconds(path: Path) -> float:
    return time.time() - path.stat().st_mtime


def resample_ohlcv(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    rule = {"2h": "2h", "4h": "4h", "1wk": "W-FRI", "1mo": "ME"}[interval]
    data = df.set_index("Date").sort_index()
    resampled = data.resample(rule).agg(
        {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
    )
    return resampled.dropna().reset_index()


def history_range_for_indicators(interval: str, requested_range: str) -> str:
    """Fetch enough bars for 200-period indicators, independent of visible range."""
    capped = {
        "1m": "7d",
        "2m": "60d",
        "5m": "60d",
        "15m": "60d",
        "30m": "60d",
        "60m": "730d",
        "90m": "60d",
        "1h": "730d",
        "2h": "730d",
        "4h": "730d",
    }
    if interval in capped:
        return capped[interval]

    minimums = {
        "1d": "2y",
        "5d": "5y",
        "1wk": "10y",
        "1mo": "max",
        "3mo": "max",
    }
    requested_rank = _range_rank(requested_range)
    minimum = minimums.get(interval, "2y")
    return requested_range if requested_rank >= _range_rank(minimum) else minimum


def _range_rank(range_: str) -> int:
    if range_ == "max":
        return 10_000_000
    unit = range_[-1]
    try:
        value = int(range_[:-1])
    except ValueError:
        return 0
    if unit == "d":
        return value
    if unit == "w":
        return value * 7
    if unit == "m":
        return value * 31
    if unit == "y":
        return value * 366
    return 0


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
        self.send_error(404, "Not found")

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/login":
            self.send_error(404, "Not found")
            return
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length).decode("utf-8")
        data = urllib.parse.parse_qs(body)
        username = data.get("username", [""])[0]
        password = data.get("password", [""])[0]
        if auth_configured() and username == app_username() and hmac.compare_digest(password, app_password()):
            self._set_session(username)
        else:
            self._send_html(LOGIN_HTML.replace("<!--ERROR-->", "<p class='error'>Invalid login or APP_PASSWORD is not set.</p>"), status=401)

    def log_message(self, format: str, *args: object) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {format % args}")

    def _is_authenticated(self) -> bool:
        jar = cookies.SimpleCookie(self.headers.get("Cookie", ""))
        morsel = jar.get(APP_SESSION_COOKIE)
        return verify_session_cookie(morsel.value if morsel else "")

    def _handle_ratings(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        symbol = params.get("symbol", [DEFAULT_SYMBOL])[0].strip() or DEFAULT_SYMBOL
        interval = params.get("interval", ["1d"])[0]
        range_ = params.get("range", ["2y"])[0]
        csv_path = params.get("csv", [""])[0].strip()
        try:
            df = load_csv(csv_path) if csv_path else fetch_kite_ohlcv(symbol, interval, range_)
            result = technical_ratings(df, symbol=symbol)
            self._send_json({"ok": True, "data": result_to_dict(result), "bars": len(df)})
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_option_chain(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        symbol = params.get("symbol", [DEFAULT_SYMBOL])[0].strip() or DEFAULT_SYMBOL
        expiry = params.get("expiry", [""])[0].strip()
        strikes = int(params.get("strikes", ["8"])[0])
        try:
            chain = fetch_option_chain(symbol, expiry=expiry, strikes_each_side=strikes)
            self._send_json({"ok": True, "data": chain})
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_symbols(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        q = params.get("q", [""])[0]
        limit = int(params.get("limit", ["25"])[0])
        try:
            self._send_json({"ok": True, "data": search_symbols(q, limit=limit)})
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _handle_kite_callback(self, query: str) -> None:
        params = urllib.parse.parse_qs(query)
        request_token = params.get("request_token", [""])[0].strip()
        if not request_token:
            self._send_html("<h1>Kite login failed</h1><p>No request_token received.</p>")
            return
        try:
            exchange_kite_request_token(request_token)
            self._redirect("/")
        except Exception as exc:
            self._send_html(f"<h1>Kite token exchange failed</h1><p>{exc}</p>")

    def _send_html(self, body: str, status: int = 200) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, data: dict, status: int = 200) -> None:
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _redirect(self, url: str) -> None:
        self.send_response(302)
        self.send_header("Location", url)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _set_session(self, username: str) -> None:
        secure = "; Secure" if os.environ.get("APP_COOKIE_SECURE", "").lower() in {"1", "true", "yes"} else ""
        self.send_response(302)
        self.send_header("Location", "/")
        self.send_header(
            "Set-Cookie",
            f"{APP_SESSION_COOKIE}={make_session_cookie(username)}; HttpOnly; SameSite=Lax; Path=/; Max-Age=43200{secure}",
        )
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _logout(self) -> None:
        self.send_response(302)
        self.send_header("Location", "/login")
        self.send_header(
            "Set-Cookie",
            f"{APP_SESSION_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0",
        )
        self.send_header("Cache-Control", "no-store")
        self.end_headers()


LOGIN_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock Console Login</title>
  <style>
    :root {
      --bg: #f6f8fb;
      --panel: #ffffff;
      --text: #0b0f19;
      --muted: #667085;
      --line: #e3e8ef;
      --blue: #2563ff;
      --orange: #ff7a00;
      --red: #f23645;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at 18% 18%, rgba(37, 99, 255, .14), transparent 30%),
        radial-gradient(circle at 82% 24%, rgba(255, 122, 0, .16), transparent 28%),
        linear-gradient(145deg, #ffffff 0%, var(--bg) 55%, #eef3ff 100%);
      padding: 24px;
    }
    .shell {
      width: min(980px, 100%);
      display: grid;
      grid-template-columns: 1fr 410px;
      min-height: 560px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: rgba(255, 255, 255, .82);
      box-shadow: 0 28px 80px rgba(15, 23, 42, .14);
      overflow: hidden;
      backdrop-filter: blur(18px);
    }
    .brand {
      padding: 46px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background: #0b0f19;
      color: #fff;
    }
    .brand h1 {
      margin: 0;
      font-size: clamp(34px, 5vw, 58px);
      line-height: .98;
      letter-spacing: 0;
    }
    .brand p {
      margin: 18px 0 0;
      max-width: 440px;
      color: #aab2c1;
      font-size: 17px;
      line-height: 1.6;
    }
    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 30px;
    }
    .chip {
      border: 1px solid rgba(255,255,255,.18);
      border-radius: 999px;
      padding: 8px 12px;
      color: #d8deea;
      font-weight: 700;
      font-size: 13px;
    }
    .panel {
      padding: 42px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      background: var(--panel);
    }
    .panel h2 {
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: 0;
    }
    .panel .sub {
      margin: 0 0 28px;
      color: var(--muted);
      line-height: 1.5;
    }
    label {
      display: block;
      margin: 16px 0 7px;
      color: #344054;
      font-weight: 800;
      font-size: 14px;
    }
    input {
      width: 100%;
      height: 46px;
      border: 1px solid #d0d7e2;
      border-radius: 10px;
      padding: 0 13px;
      font: inherit;
      font-weight: 650;
      outline: none;
      transition: border-color .18s, box-shadow .18s;
    }
    input:focus {
      border-color: var(--blue);
      box-shadow: 0 0 0 4px rgba(37, 99, 255, .12);
    }
    button {
      width: 100%;
      height: 48px;
      margin-top: 24px;
      border: 0;
      border-radius: 10px;
      color: #fff;
      background: linear-gradient(90deg, var(--blue), #6938ef);
      font: inherit;
      font-weight: 900;
      cursor: pointer;
      box-shadow: 0 12px 26px rgba(37, 99, 255, .25);
    }
    .setup, .error {
      border-radius: 10px;
      padding: 12px 13px;
      font-size: 14px;
      line-height: 1.45;
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
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    code {
      color: #111827;
      font-weight: 800;
    }
    @media (max-width: 820px) {
      .shell { grid-template-columns: 1fr; }
      .brand { min-height: 280px; padding: 32px; }
      .panel { padding: 30px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="brand">
      <div>
        <h1>Stock Market Console</h1>
        <p>Technical ratings, Kite option chain, and live market views behind a private session.</p>
        <div class="chips">
          <span class="chip">Kite Connect</span>
          <span class="chip">NSE Stocks</span>
          <span class="chip">Technical Ratings</span>
          <span class="chip">Option Chain</span>
        </div>
      </div>
      <p>Use HTTPS and a strong password before exposing this server publicly.</p>
    </section>
    <section class="panel">
      <h2>Sign In</h2>
      <p class="sub">Access your market dashboard.</p>
      <!--ERROR-->
      <form method="post" action="/login">
        <label for="username">Username</label>
        <input id="username" name="username" value="admin" autocomplete="username" required>
        <label for="password">Password</label>
        <input id="password" name="password" type="password" autocomplete="current-password" required>
        <button type="submit">Enter Dashboard</button>
      </form>
      <p class="foot">Set <code>APP_USERNAME</code>, <code>APP_PASSWORD</code>, and <code>APP_SESSION_SECRET</code> before public deployment.</p>
      """ + ("" if auth_configured() else "<p class='setup'>APP_PASSWORD is not set. Login is disabled until you configure it.</p>") + r"""
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
      width: min(1620px, calc(100vw - 40px));
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
    .timeframes {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .tf {
      height: 44px;
      border: 0;
      border-radius: 8px;
      background: transparent;
      color: #05070c;
      padding: 0 16px;
      font-size: 17px;
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
    }
    input, .refresh, .kite-login {
      height: 40px;
      border-radius: 6px;
      border: 1px solid #d9dde5;
      background: #fff;
      color: var(--text);
      font: inherit;
    }
    input {
      width: 165px;
      padding: 0 12px;
      font-weight: 650;
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
      margin-bottom: 16px;
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
    select {
      height: 40px;
      border: 1px solid #d9dde5;
      border-radius: 6px;
      background: #fff;
      padding: 0 10px;
      font: inherit;
      font-weight: 650;
    }
    .chain-summary {
      margin-bottom: 14px;
      color: var(--muted);
      font-size: 14px;
    }
    .option-table {
      width: 100%;
      border-collapse: collapse;
      table-layout: auto;
    }
    .option-table th {
      background: #f8fafc;
      color: #4b5563;
      font-size: 13px;
      white-space: nowrap;
    }
    .option-table td {
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .strike-cell {
      background: #fbfdff;
      color: #111827;
      font-weight: 850;
      text-align: center;
    }
    .call-side { color: #126b46; }
    .put-side { color: #9f1239; }
    .chain-scroll { overflow-x: auto; }
    @media (max-width: 1180px) {
      .topbar { align-items: stretch; flex-direction: column; margin-bottom: 28px; }
      .symbol-form { justify-content: flex-start; }
      .gauges { grid-template-columns: 1fr; gap: 18px; }
      .summary { order: -1; }
      .tables { grid-template-columns: 1fr; }
      .chain-head { align-items: stretch; flex-direction: column; }
    }
    @media (max-width: 560px) {
      main { width: min(100vw - 24px, 1440px); padding-top: 14px; }
      .timeframes { overflow-x: auto; flex-wrap: nowrap; padding-bottom: 4px; }
      .tf { font-size: 15px; height: 38px; padding: 0 11px; }
      input { flex: 1 1 160px; }
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
    <nav class="timeframes" aria-label="Timeframes">
      <button class="tf" type="button" data-interval="1m">1 minute</button>
      <button class="tf" type="button" data-interval="5m">5 minutes</button>
      <button class="tf" type="button" data-interval="15m">15 minutes</button>
      <button class="tf active" type="button" data-interval="30m">30 minutes</button>
      <button class="tf" type="button" data-interval="1h">1 hour</button>
      <button class="tf" type="button" data-interval="2h">2 hours</button>
      <button class="tf" type="button" data-interval="4h">4 hours</button>
      <button class="tf" type="button" data-interval="1d">1 day</button>
    </nav>
    <form class="symbol-form" id="controls">
      <input id="symbol" value="ASIANPAINT" aria-label="Symbol" list="symbolSuggestions" autocomplete="off">
      <datalist id="symbolSuggestions"></datalist>
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
        <select id="expirySelect" aria-label="Expiry"></select>
      </div>
    </div>
    <p class="chain-summary" id="chainMeta">Waiting for option chain</p>
    <div class="chain-scroll">
      <table class="option-table">
        <thead>
          <tr>
            <th class="call-side">CE OI</th><th class="call-side">CE Vol</th><th class="call-side">CE Bid</th><th class="call-side">CE LTP</th><th class="call-side">CE Ask</th>
            <th>Strike</th>
            <th class="put-side">PE Bid</th><th class="put-side">PE LTP</th><th class="put-side">PE Ask</th><th class="put-side">PE Vol</th><th class="put-side">PE OI</th>
          </tr>
        </thead>
        <tbody id="chainRows"></tbody>
      </table>
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

function renderRows(targetId, signals, values) {
  document.getElementById(targetId).innerHTML = Object.entries(signals).map(([name, value]) => {
    const displayValue = values[valueKeys[name] || name];
    return `<tr><td>${name}</td><td>${formatValue(displayValue)}</td><td><span class="signal ${classForSignal(value)}">${labelForSignal(value)}</span></td></tr>`;
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

function render(data, bars) {
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
    render(payload.data, payload.bars);
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

function renderOptionChain(data) {
  document.getElementById("chainMeta").textContent =
    `${data.symbol} | Spot ${money(data.spot)} | Expiry ${data.expiry}`;
  const previousExpiry = expirySelect.value;
  expirySelect.innerHTML = data.expiries.map(expiry => {
    const selected = expiry === data.expiry || expiry === previousExpiry ? "selected" : "";
    return `<option value="${expiry}" ${selected}>${expiry}</option>`;
  }).join("");
  document.getElementById("chainRows").innerHTML = data.rows.map(row => {
    const ce = row.CE || {};
    const pe = row.PE || {};
    return `<tr>
      <td class="call-side">${integer(opt(ce, "oi"))}</td>
      <td class="call-side">${integer(opt(ce, "volume"))}</td>
      <td class="call-side">${money(opt(ce, "bid"))}</td>
      <td class="call-side">${money(opt(ce, "ltp"))}</td>
      <td class="call-side">${money(opt(ce, "ask"))}</td>
      <td class="strike-cell">${money(row.strike)}</td>
      <td class="put-side">${money(opt(pe, "bid"))}</td>
      <td class="put-side">${money(opt(pe, "ltp"))}</td>
      <td class="put-side">${money(opt(pe, "ask"))}</td>
      <td class="put-side">${integer(opt(pe, "volume"))}</td>
      <td class="put-side">${integer(opt(pe, "oi"))}</td>
    </tr>`;
  }).join("");
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
  if (q.length < 2) return;
  const response = await fetch(`/api/symbols?q=${encodeURIComponent(q)}&limit=30`);
  const payload = await response.json();
  if (!payload.ok) return;
  symbolSuggestions.innerHTML = payload.data.map(item => {
    const tag = item.optionable ? " | F&O" : "";
    return `<option value="${item.symbol}" label="${item.tradingsymbol}${tag} - ${item.name}"></option>`;
  }).join("");
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
  if (String(error.message).includes("Kite not connected")) {
    meta.innerHTML = `${error.message} <a href="/kite/login">Connect Kite</a>`;
  } else {
    meta.textContent = error.message;
  }
  ["oscValue", "summaryValue", "maValue"].forEach(id => document.getElementById(id).textContent = "Error");
}

function showChainError(error) {
  const chainMeta = document.getElementById("chainMeta");
  if (String(error.message).includes("Kite session")) {
    chainMeta.innerHTML = `${error.message} <a href="/kite/login">Connect Kite</a>`;
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
    server = ThreadingHTTPServer((host, port), RatingsHandler)
    print(f"Technical Ratings app running at http://{host}:{port}")
    server.serve_forever()


def run_cli(symbol: str, csv_path: str | None, interval: str, range_: str) -> None:
    df = load_csv(csv_path) if csv_path else fetch_kite_ohlcv(symbol, interval, range_)
    result = technical_ratings(df, symbol=symbol)
    print(json.dumps(result_to_dict(result), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingView-style Technical Ratings gauge")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--range", default="2y")
    parser.add_argument("--csv", default=None, help="CSV path with Open, High, Low, Close, Volume columns")
    parser.add_argument("--cli", action="store_true", help="Print JSON instead of starting the web app")
    parser.add_argument("--kite-login-url", action="store_true", help="Print Kite Connect login URL")
    parser.add_argument("--kite-request-token", default=None, help="Exchange Kite request_token for access_token")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
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
