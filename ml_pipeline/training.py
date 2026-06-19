import yfinance as yf
import pandas as pd
import numpy as np
import json
import joblib
import os
import redis
import psycopg2
import logging
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("training")

TICKERS = ["DX-Y.NYB", "EURUSD=X", "USDJPY=X", "GBPUSD=X", "^VIX", "^GSPC"]
TICKER_MAP = {
    "DX-Y.NYB": "dxy",
    "EURUSD=X": "eur",
    "USDJPY=X": "jpy",
    "GBPUSD=X": "gbp",
    "^VIX": "vix",
    "^GSPC": "sp500",
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/dxy_model.pkl")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "20"))
VOL_SCALE = float(os.getenv("VOL_SCALE", "5.5"))


def get_db():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            return conn, conn.cursor()
        except Exception as e:
            log.warning("Waiting for PostgreSQL: %s", e)
            time.sleep(3)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode(),
    acks=1,
)
r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)
conn, cur = get_db()


def fetch_and_store_bars():
    log.info("Fetching latest 1m bars from yfinance...")
    df = yf.download(
        TICKERS, period="1d", interval="1m", group_by="ticker", progress=False
    )
    count = 0
    for ticker in TICKERS:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                tdf = df[ticker].dropna()
            else:
                tdf = df.dropna()
            if tdf.empty:
                continue
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
                count += 1
            conn.commit()
        except Exception as e:
            log.error("Fetch error %s: %s", ticker, e)
    log.info("Stored %d new bars", count)


def load_training_data():
    query = """
    SELECT ticker, timestamp, open, high, low, close
    FROM intraday_bars
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    ORDER BY ticker, timestamp
    """
    cur.execute(query)
    rows = cur.fetchall()

    if len(rows) < 50:
        log.warning("Insufficient data (%d rows), aborting", len(rows))
        return None

    df = pd.DataFrame(rows, columns=["ticker", "timestamp", "open", "high", "low", "close"])
    dfs = {}
    for ticker in TICKERS:
        prefix = TICKER_MAP[ticker]
        tdf = df[df["ticker"] == ticker].copy()
        if tdf.empty:
            return None
        tdf = tdf.set_index("timestamp").sort_index()
        tdf = tdf[~tdf.index.duplicated(keep="last")]
        dfs[prefix] = tdf

    return dfs


def compute_features(dfs):
    base_idx = set(dfs["dxy"].index)
    for p in ["eur", "jpy", "gbp"]:
        base_idx &= set(dfs[p].index)
    if not base_idx:
        return None
    base_idx = sorted(base_idx)
    base_df = pd.DataFrame(index=base_idx)

    feat_df = pd.DataFrame(index=base_idx)

    vix_close_series = None
    for prefix, tdf in dfs.items():
        tdf_small = tdf[["close", "high", "low"]]
        if prefix in ["vix", "sp500"]:
            merged = pd.merge_asof(base_df, tdf_small, left_index=True, right_index=True, direction="backward")
            close = merged["close"].astype(float).fillna(0)
            hl = (merged["high"].astype(float).fillna(0) - merged["low"].astype(float).fillna(0))
        else:
            aligned = tdf_small.reindex(base_idx).ffill().bfill()
            close = aligned["close"].astype(float)
            hl = aligned["high"].astype(float) - aligned["low"].astype(float)
        returns = close.pct_change()
        feat_df[f"{prefix}_return"] = returns
        feat_df[f"{prefix}_hl_pct"] = hl / close.replace(0, np.nan)
        if prefix == "vix":
            vix_close_series = close

    if vix_close_series is not None:
        feat_df["vix_close"] = vix_close_series

    returns_mat = feat_df[[c for c in feat_df.columns if c.endswith("_return")]]
    dxy_ret = returns_mat["dxy_return"]
    feat_df["dxy_roll_vol"] = dxy_ret.rolling(ROLLING_WINDOW).std()
    feat_df["dxy_roll_vol_60"] = dxy_ret.rolling(60).std()
    feat_df["dxy_roll_vol_120"] = dxy_ret.rolling(120).std()

    hl_pct = ((dfs["dxy"]["high"] - dfs["dxy"]["low"]) / dfs["dxy"]["close"])
    hl_pct = hl_pct.reindex(base_idx).fillna(0).astype(float)
    feat_df["dxy_hl_range"] = hl_pct
    feat_df["dxy_roll_hl_range"] = feat_df["dxy_hl_range"].rolling(ROLLING_WINDOW).mean()

    for col in feat_df.columns:
        feat_df[col] = feat_df[col].replace([np.inf, -np.inf], np.nan).clip(-5, 5)

    return feat_df.dropna()


def build_lag_features(feat_df):
    return_cols = [f"{p}_return" for p in TICKER_MAP.values()]
    hl_cols = [f"{p}_hl_pct" for p in TICKER_MAP.values()]

    for col in return_cols:
        for i in range(1, 4):
            feat_df[f"{col}_lag{i}"] = feat_df[col].shift(i)

    for col in hl_cols:
        feat_df[f"{col}_lag1"] = feat_df[col].shift(1)

    for i in range(1, 4):
        feat_df[f"dxy_roll_vol_lag{i}"] = feat_df["dxy_roll_vol"].shift(i)
        feat_df[f"dxy_roll_vol_60_lag{i}"] = feat_df["dxy_roll_vol_60"].shift(i)
        feat_df[f"dxy_roll_vol_120_lag{i}"] = feat_df["dxy_roll_vol_120"].shift(i)

    if "vix_close" in feat_df.columns:
        feat_df["vix_close_lag1"] = feat_df["vix_close"].shift(1)

    return feat_df.dropna()


log.info("Starting...")
fetch_and_store_bars()

dfs = load_training_data()
if dfs is None:
    log.warning("Not enough data, exiting")
    exit(1)

feat_df = compute_features(dfs)
if feat_df is None:
    log.error("Feature computation failed")
    exit(1)

feat_df = build_lag_features(feat_df)
if len(feat_df) < 5:
    log.warning("Only %d feature rows, need >= 5", len(feat_df))
    exit(1)

FEATURE_COLS = (
    [f"{p}_return_lag{i}" for p in TICKER_MAP.values() for i in range(1, 4)]
    + [f"{p}_hl_pct_lag1" for p in TICKER_MAP.values()]
    + ["dxy_roll_vol_lag1", "dxy_roll_vol_lag2", "dxy_roll_vol_lag3"]
    + ["dxy_roll_vol_60_lag1", "dxy_roll_vol_60_lag2", "dxy_roll_vol_60_lag3"]
    + ["dxy_roll_vol_120_lag1", "dxy_roll_vol_120_lag2", "dxy_roll_vol_120_lag3"]
    + ["vix_close_lag1"]
)

X = feat_df[FEATURE_COLS].values
y = feat_df["dxy_roll_hl_range"].values

SCALER_PATH = MODEL_PATH.replace(".pkl", "_scaler.pkl")
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
joblib.dump(scaler, SCALER_PATH)

model = Ridge(alpha=0.01)
model.fit(X_scaled, y)
joblib.dump(model, MODEL_PATH)
log.info("Vol model trained on %d rows, %d features (scaled)", len(feat_df), len(FEATURE_COLS))

dxy_close_series = dfs["dxy"]["close"]
dxy_rets = dxy_close_series.pct_change()
price_targets = pd.DataFrame(index=dxy_close_series.index)
price_targets["return_t1"] = dxy_rets.shift(-1)
price_targets["return_t3"] = dxy_rets.rolling(3).sum().shift(-3)
price_targets["return_t5"] = dxy_rets.rolling(5).sum().shift(-5)
price_targets["return_t30"] = dxy_rets.rolling(30).sum().shift(-30)
price_targets = price_targets.dropna()
common_idx = feat_df.index.intersection(price_targets.index)
if len(common_idx) >= 5:
    X_price = scaler.transform(feat_df.loc[common_idx, FEATURE_COLS].values)
    y_price = price_targets.loc[common_idx].values
    price_model = Ridge(alpha=0.01)
    price_model.fit(X_price, y_price)
    PRICE_MODEL_PATH = MODEL_PATH.replace(".pkl", "_price.pkl")
    joblib.dump(price_model, PRICE_MODEL_PATH)
    log.info("Price model trained on %d rows, 4 targets (t+1, t+3, t+5, t+30)", len(common_idx))
else:
    price_model = None
    log.warning("Not enough price data (%d rows), skipping price model", len(common_idx))

latest = feat_df.iloc[-1:]

pred_X = scaler.transform(latest[FEATURE_COLS].values)
predicted_vol = float(model.predict(pred_X)[0]) * VOL_SCALE
predicted_vol = max(predicted_vol, 0)

actual_vol = float(latest["dxy_roll_hl_range"].iloc[0]) * VOL_SCALE
dxy_latest = dfs["dxy"].loc[latest.index[0]]
dxy_close = float(dxy_latest["close"])

features = {col: float(latest[col].iloc[0]) for col in feat_df.columns if col != "date"}

pred_price_1m = pred_price_3m = pred_price_5m = pred_price_30m = None
if price_model is not None:
    price_X = scaler.transform(latest[FEATURE_COLS].values)
    price_raw = price_model.predict(price_X)[0]
    last_close = dxy_close
    pred_price_1m = round(last_close * (1 + price_raw[0]), 4)
    pred_price_3m = round(last_close * (1 + price_raw[1]), 4)
    pred_price_5m = round(last_close * (1 + price_raw[2]), 4)
    pred_price_30m = round(last_close * (1 + price_raw[3]), 4) if len(price_raw) > 3 else None

result = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "dxy_close": round(dxy_close, 4),
    "predicted_volatility": round(predicted_vol, 6),
    "actual_volatility": round(actual_vol, 6),
    "predicted_price_1m": pred_price_1m,
    "predicted_price_3m": pred_price_3m,
    "predicted_price_5m": pred_price_5m,
    "predicted_price_30m": pred_price_30m,
    "source": "batch",
    "features": {k: round(v, 6) if isinstance(v, float) else v for k, v in features.items()},
}

producer.send("dxy-predictions", value=result)
log.info("pred_vol=%.6f actual_vol=%.6f", predicted_vol, actual_vol)

updates = {}
for prefix in TICKER_MAP.values():
    tdf = dfs.get(prefix)
    if tdf is None:
        continue
    last = tdf.iloc[-1]
    updates[f"latest:{prefix}:close"] = str(float(last["close"]))
    updates[f"latest:{prefix}:high"] = str(float(last["high"]))
    updates[f"latest:{prefix}:low"] = str(float(last["low"]))

updates["latest:dxy:volatility"] = str(actual_vol)

with r.pipeline() as pipe:
    for k, v in updates.items():
        pipe.set(k, v)
    for prefix in TICKER_MAP.values():
        tdf = dfs.get(prefix)
        if tdf is None:
            continue
        for val in tdf["close"].astype(float).values:
            pipe.rpush(f"history:{prefix}:close", float(val))
        pipe.ltrim(f"history:{prefix}:close", -200, -1)
    pipe.execute()

log.info("Dragonfly cache updated")
