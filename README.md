# StockRadar

StockRadar calculates TradingView-style Technical Ratings, renders a modern three-gauge dashboard, and fetches NSE cash/option data through Kite Connect.

## Run

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Set app and Kite credentials first:

```powershell
$env:APP_USERNAME="admin"
$env:APP_PASSWORD="use-a-strong-password"
$env:APP_SESSION_SECRET="use-a-long-random-secret"
$env:KITE_API_KEY="your_api_key"
$env:KITE_API_SECRET="your_api_secret"
```

```powershell
python app.py --host 127.0.0.1 --port 8765
```

Open:

```text
http://127.0.0.1:8765
```

If you do not already have a daily access token, open:

```text
http://127.0.0.1:8765/kite/login
```

After Kite redirects back with a `request_token`, the app exchanges it for an access token and keeps it in server memory.
It also saves the token locally at `data/kite_access_token.json` for the current date, so server restarts do not require login again until Kite expires the token.

If you already have an access token, set it directly:

```powershell
$env:KITE_ACCESS_TOKEN="your_daily_access_token"
```

## CLI

```powershell
python app.py --cli --symbol ASIANPAINT.NS --interval 1d --range 2y
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

## Accuracy Notes

The calculation follows TradingView's public Technical Ratings rules: 15 moving-average components, 11 oscillator components, each component contributing `-1`, `0`, or `+1`. The gauge thresholds are:

- `< -0.5`: Strong Sell
- `[-0.5, -0.1)`: Sell
- `[-0.1, 0.1]`: Neutral
- `(0.1, 0.5]`: Buy
- `> 0.5`: Strong Buy

Exact values can differ from TradingView if the OHLCV source, exchange session, corporate-action adjustment, intraday realtime bar, or timezone differs. Live fetch now uses Kite Connect historical candles. You can still use CSV input for testing or offline runs.
