# StockRadar

StockRadar calculates TradingView-style Technical Ratings, renders a modern three-gauge dashboard, and fetches NSE cash/option data through Kite Connect.

## Run

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Create your local environment file:

```cmd
copy .env.example .env
```

Edit `.env` and set your app login and Kite credentials:

```env
APP_USERNAME=admin
APP_PASSWORD=use-a-strong-password
APP_SESSION_SECRET=replace-with-a-generated-random-secret
KITE_API_KEY=your_kite_api_key
KITE_API_SECRET=your_kite_api_secret
APP_COOKIE_SECURE=false
APP_ALLOW_HTTP_CSV=false
TRADINGVIEW_CONFIRMATION_ENABLED=true
TRADINGVIEW_CONFIRMATION_TTL_SECONDS=300
STOCKRADAR_LOG_LEVEL=INFO
STOCKRADAR_LOG_TO_FILE=true
# DATABASE_URL=postgresql://user:password@host:5432/database
```

Generate `APP_SESSION_SECRET` from **cmd.exe**:

```cmd
powershell -NoProfile -Command "[Convert]::ToBase64String((1..48 | ForEach-Object { Get-Random -Maximum 256 }))"
```

Generate it from **PowerShell**:

```powershell
[Convert]::ToBase64String((1..48 | ForEach-Object { Get-Random -Maximum 256 }))
```

Paste the generated value into `.env` as `APP_SESSION_SECRET`.
Values in `.env` are loaded at app startup and override same-named variables already present in the shell or service environment.

```powershell
python app.py
```

By default, this binds to `0.0.0.0:80`, so open the server with your public IP or local address:

```text
http://127.0.0.1/
http://YOUR_PUBLIC_IP/
```

If you do not already have a daily access token, login to the app and click **Connect Kite**.

After Kite redirects back with a `request_token`, the app exchanges it for an access token and keeps it in server memory.
It also saves the token locally at `data/kite_access_token.json` for the current date, so server restarts do not require login again until Kite expires the token.

If you already have an access token, set it directly:

```env
KITE_ACCESS_TOKEN=your_daily_access_token
```

## CLI

```powershell
python app.py --cli --symbol ASIANPAINT.NS --interval 5m --range 2y
```

Print Kite login URL:

```powershell
python app.py --kite-login-url
```

Exchange a Kite request token:

```powershell
python app.py --kite-request-token "request_token_from_redirect"
```

You can also calculate from a CSV:

```powershell
python app.py --cli --csv path\to\ohlcv.csv
```

The CSV must contain `Open`, `High`, `Low`, `Close`, and `Volume` columns.

## Public Deployment

For internet exposure, bind to a public interface only after setting `APP_PASSWORD` and `APP_SESSION_SECRET`:

```powershell
python app.py --host 0.0.0.0 --port 80
```

Run behind HTTPS, such as IIS/Nginx/Caddy reverse proxy or a cloud load balancer. If HTTPS terminates before Python, set:

```powershell
$env:APP_COOKIE_SECURE="true"
```

Runtime files are stored in `data/` by default. To keep tokens/cache outside the repo:

```powershell
$env:STOCKRADAR_DATA_DIR="D:\StockRadarData"
```

HTTP CSV loading is disabled by default for safety. Keep CSV analysis on the CLI, or explicitly enable it only on trusted private deployments:

```powershell
$env:APP_ALLOW_HTTP_CSV="true"
```

## TradingView Extreme Confirmation

StockRadar normally calculates ratings from Kite candles. To reduce TradingView traffic, the app only calls TradingView through `tradingview-ta` when all three local gauges are aligned on one side:

- Oscillators is `Sell` or `Strong Sell`, Summary is `Sell` or `Strong Sell`, and Moving Averages is `Sell` or `Strong Sell`
- Oscillators is `Buy` or `Strong Buy`, Summary is `Buy` or `Strong Buy`, and Moving Averages is `Buy` or `Strong Buy`

The UI is updated only when TradingView returns the same extreme on all three groups: Summary, Oscillators, and Moving Averages must all be `STRONG_SELL`, or all three must be `STRONG_BUY`. Results are cached by symbol and timeframe to avoid repeated requests:

```env
TRADINGVIEW_CONFIRMATION_ENABLED=true
TRADINGVIEW_CONFIRMATION_TTL_SECONDS=300
```

Increase `TRADINGVIEW_CONFIRMATION_TTL_SECONDS` if you want fewer TradingView requests during auto-refresh.

## Background Signal Scanner

StockRadar can run a background scanner that periodically fetches Kite candles, calculates local technical ratings, checks TradingView only when the three local gauges align on buy/sell, and inserts a row into `events` only when TradingView confirms all three groups as `STRONG_BUY` or `STRONG_SELL`.

Add these settings to `.env`:

```env
BACKGROUND_SIGNAL_SCANNER_ENABLED=true
BACKGROUND_SIGNAL_SCANNER_SYMBOLS=ASIANPAINT.NS,NIFTY 50
BACKGROUND_SIGNAL_SCANNER_INTERVALS=5m,15m
BACKGROUND_SIGNAL_SCANNER_RANGE=2y
BACKGROUND_SIGNAL_SCANNER_POLL_SECONDS=60
BACKGROUND_SIGNAL_SCANNER_DELAY_SECONDS=2
```

Notes:

- The scanner uses the same event logging logic as `/api/ratings`.
- `BACKGROUND_SIGNAL_SCANNER_SYMBOLS` is comma-separated and accepts normal stock symbols plus index aliases such as `NIFTY 50`.
- `BACKGROUND_SIGNAL_SCANNER_INTERVALS` must use supported app intervals: `5m` and `15m`.
- Keep `BACKGROUND_SIGNAL_SCANNER_DELAY_SECONDS` above `0` when scanning many symbols to avoid bursts of Kite/TradingView requests.

## Logging

StockRadar writes structured JSON logs to stdout and, by default, to:

```text
data/stockradar.log
```

Every UI/API request gets a `request_id`, with `request.start` and `request.end` events. Kite calls, option-chain calls, symbol searches, TradingView confirmations, login attempts, redirects, and errors are logged with timings.
The same id is returned in the `X-Request-ID` response header.

Configure logging in `.env`:

```env
STOCKRADAR_LOG_LEVEL=INFO
STOCKRADAR_LOG_TO_FILE=true
STOCKRADAR_LOG_FILE=D:\StockRadarData\stockradar.log
```

Use `DEBUG` only while troubleshooting because auto-refresh can create many log lines.

If you run with redirected output, logs are written to stdout and to the file above. Older deployments may still show logs in `server.err.log`; restart after pulling this version to move stream logs to stdout.

To verify logging after deployment, open the app and then request:

```text
/api/health
```

You should see `request.start`, `health.success`, and `request.end` entries in the log file.

## Event Storage

Set `DATABASE_URL` to enable PostgreSQL event logging:

```env
DATABASE_URL=postgresql://user:password@host:5432/database
```

On startup, StockRadar creates the `events` table if it does not exist. A row is inserted only when:

- all three local gauges are on the same side,
- TradingView is checked,
- TradingView returns all three groups as `STRONG_BUY` or all three as `STRONG_SELL`,
- the confirmed strong signal is applied to the UI.

The event row includes symbol, timeframe, signal, price, bars, local scores/labels, TradingView recommendations/counts, payload JSON, and request id.

## Daily Option OI Baseline

StockRadar can save one option OI opening snapshot per trading day from Kite quotes. This keeps the option-chain `Change in OI` calculation on one source:

```text
Change in OI = current Kite OI - saved Kite opening OI
```

This avoids mixing Kite/INDmoney-style live OI with another EOD source, which can create false differences.

Add these settings to `.env`:

```env
OI_BASELINE_ENABLED=true
OI_BASELINE_CAPTURE_TIME=09:20
OI_BASELINE_POLL_SECONDS=60
OI_BASELINE_UNDERLYINGS=NIFTY,ADANIENT,ADANIPORTS,APOLLOHOSP,ASIANPAINT,AXISBANK,BAJAJ-AUTO,BAJFINANCE,BAJAJFINSV,BEL,BHARTIARTL,CIPLA,COALINDIA,DRREDDY,EICHERMOT,ETERNAL,GRASIM,HCLTECH,HDFCBANK,HDFCLIFE,HEROMOTOCO,HINDALCO,HINDUNILVR,ICICIBANK,INDUSINDBK,INFY,ITC,JIOFIN,JSWSTEEL,KOTAKBANK,LT,M&M,MARUTI,NESTLEIND,NTPC,ONGC,POWERGRID,RELIANCE,SBILIFE,SBIN,SHRIRAMFIN,SUNPHARMA,TATACONSUM,TATAMOTORS,TATASTEEL,TCS,TECHM,TITAN,TRENT,ULTRACEMCO,WIPRO
```

How it works:

- When the server is running after `OI_BASELINE_CAPTURE_TIME`, it checks once per poll interval.
- On weekdays, it snapshots all active CE/PE NFO contracts for the configured NIFTY index and NIFTY 50 underlyings through `kite.quote()`.
- It stores `trade_date`, contract, expiry, strike, option type, OI, last price, and source in `option_oi_daily` with source `kite.quote.opening`.
- The option-chain screen calculates `Change in OI` as `current Kite OI - latest saved Kite OI baseline`.
- If no Kite baseline exists for a contract, `Change in OI` is shown as `0` instead of comparing against another data source.

Change `OI_BASELINE_UNDERLYINGS` if the NIFTY 50 constituents change or if you want a smaller watchlist. This requires `DATABASE_URL`, a valid Kite session, and the app process running at or after the capture time.

## Accuracy Notes

The calculation follows TradingView's public Technical Ratings rules: 15 moving-average components, 11 oscillator components, each component contributing `-1`, `0`, or `+1`. The gauge thresholds are:

- `< -0.5`: Strong Sell
- `[-0.5, -0.1)`: Sell
- `[-0.1, 0.1]`: Neutral
- `(0.1, 0.5]`: Buy
- `> 0.5`: Strong Buy

Exact values can differ from TradingView if the OHLCV source, exchange session, corporate-action adjustment, intraday realtime bar, or timezone differs. Live fetch now uses Kite Connect historical candles. You can still use CSV input for testing or offline runs.
