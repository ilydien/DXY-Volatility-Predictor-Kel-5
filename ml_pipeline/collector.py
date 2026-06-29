import yfinance as yf
import pandas as pd
import psycopg2
import redis
import os
import logging
import time
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("collector")

TICKERS = ["DX-Y.NYB", "EURUSD=X", "USDJPY=X", "GBPUSD=X", "^GSPC"]
VIX_TICKER = "^VIX"
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
DRAGONFLY_PASSWORD = os.getenv("DRAGONFLY_PASSWORD", "")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")
COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "60"))

TICKER_MAP = {
    "DX-Y.NYB": "dxy",
    "EURUSD=X": "eur",
    "USDJPY=X": "jpy",
    "GBPUSD=X": "gbp",
    "^VIX": "vix",
    "^GSPC": "sp500",
}


def get_db():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            return conn, conn.cursor()
        except Exception as e:
            log.warning("Waiting for PostgreSQL: %s", e)
            time.sleep(3)


conn, cur = get_db()
r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)

BARS_TOTAL = Counter("collector_bars_total", "Total bars stored")
ERRORS = Counter("collector_errors_total", "Total errors")
LAST_SUCCESS = Gauge("collector_last_success_seconds", "Last success timestamp")
CYCLE_DURATION = Gauge("collector_cycle_duration_seconds", "Duration per cycle")
start_http_server(8000)


def collect_and_store():
    log.info("Fetching 1m bars at %s", datetime.now(timezone.utc).isoformat())
    df = yf.download(
        TICKERS, period="1d", interval="1m", group_by="ticker", progress=False
    )
    total_bars = 0

    for ticker in TICKERS:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                tdf = df[ticker].dropna()
            else:
                tdf = df.dropna()
            if tdf.empty:
                continue

            prefix = TICKER_MAP[ticker]

            for idx, row in tdf.iterrows():
                ts = idx.to_pydatetime()
                cur.execute(
                    """
                    INSERT INTO intraday_bars (ticker, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, timestamp) DO NOTHING
                    """,
                    (
                        ticker, ts,
                        float(row["Open"]), float(row["High"]),
                        float(row["Low"]), float(row["Close"]),
                        int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
                    ),
                )
            conn.commit()

            last = tdf.iloc[-1]
            r.set(f"latest:{prefix}:close", float(last["Close"]))
            r.set(f"latest:{prefix}:high", float(last["High"]))
            r.set(f"latest:{prefix}:low", float(last["Low"]))

            for _, row in tdf.iterrows():
                r.rpush(f"history:{prefix}:close", float(row["Close"]))
            r.ltrim(f"history:{prefix}:close", -200, -1)

            bar_count = len(tdf)
            total_bars += bar_count
            log.info("%s (%s): %d bars, close=%.4f", ticker, prefix, bar_count, float(last['Close']))

        except Exception as e:
            log.error("Error %s: %s", ticker, e)

    try:
        vix_df = yf.download(VIX_TICKER, period="1mo", interval="1d", progress=False)
        if vix_df.empty:
            vix_df = yf.download(VIX_TICKER, period="3mo", interval="1d", progress=False)
        if not vix_df.empty:
            vix_tdf = vix_df.dropna()
            prefix = TICKER_MAP[VIX_TICKER]
            for idx, row in vix_tdf.iterrows():
                ts = idx.to_pydatetime()
                cur.execute(
                    """
                    INSERT INTO intraday_bars (ticker, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, timestamp) DO NOTHING
                    """,
                    (VIX_TICKER, ts,
                     float(row["Open"]), float(row["High"]),
                     float(row["Low"]), float(row["Close"]),
                     0),
                )
            conn.commit()
            last = vix_tdf.iloc[-1]
            r.set(f"latest:{prefix}:close", float(last["Close"]))
            r.set(f"latest:{prefix}:high", float(last["High"]))
            r.set(f"latest:{prefix}:low", float(last["Low"]))
            log.info("%s (%s): %d bars (daily), close=%.4f", VIX_TICKER, prefix, len(vix_tdf), float(last["Close"]))
        else:
            log.warning("VIX download returned empty after fallback")
    except Exception as e:
        log.error("Error VIX daily: %s", e)

    dxy_data = None
    dxy_prefix = TICKER_MAP["DX-Y.NYB"]
    dxy_hist = r.lrange(f"history:{dxy_prefix}:close", -50, -1)
    if len(dxy_hist) >= 2:
        try:
            prices = [float(v) for v in dxy_hist]
            returns = [
                (prices[i] / prices[i - 1]) - 1
                for i in range(1, len(prices))
            ]
            window = min(20, len(returns))
            roll_vol = pd.Series(returns).rolling(window).std().iloc[-1]
            r.set("latest:dxy:volatility", float(roll_vol) if not pd.isna(roll_vol) else 0)
        except (TypeError, ValueError):
            pass

    BARS_TOTAL.inc(total_bars)
    LAST_SUCCESS.set(time.time())
    log.info("Done: %d bars stored", total_bars)


if __name__ == "__main__":
    log.info("Starting, interval=%ds", COLLECT_INTERVAL)
    while True:
        t0 = time.time()
        try:
            collect_and_store()
        except Exception as e:
            ERRORS.inc()
            log.error("Cycle error: %s", e)
        CYCLE_DURATION.set(time.time() - t0)
        time.sleep(COLLECT_INTERVAL)
