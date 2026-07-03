import pandas as pd
import numpy as np
import json
import joblib
import os
import time
import redis
import psycopg2
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("batch_predictor")

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
DRAGONFLY_PASSWORD = os.getenv("DRAGONFLY_PASSWORD", "")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/dxy_model.pkl")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "20"))
PREDICT_INTERVAL = int(os.getenv("PREDICT_INTERVAL", "60"))
VOL_SCALE = float(os.getenv("VOL_SCALE", "5.5"))

FEATURE_COLS = (
    [f"{p}_return_lag{i}" for p in TICKER_MAP.values() for i in range(1, 4)]
    + [f"{p}_hl_pct_lag1" for p in TICKER_MAP.values()]
    + ["dxy_roll_vol_lag1", "dxy_roll_vol_lag2", "dxy_roll_vol_lag3"]
    + ["dxy_roll_vol_60_lag1", "dxy_roll_vol_60_lag2", "dxy_roll_vol_60_lag3"]
    + ["dxy_roll_vol_120_lag1", "dxy_roll_vol_120_lag2", "dxy_roll_vol_120_lag3"]
    + ["vix_close_lag1"]
    + ["sin_hour", "cos_hour", "is_weekend"]
)


def get_db():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            return conn, conn.cursor()
        except Exception as e:
            log.warning("Waiting for PostgreSQL: %s", e)
            time.sleep(3)


def load_data(cur):
    cur.execute("""
        SELECT ticker, timestamp, open, high, low, close
        FROM intraday_bars
        WHERE timestamp >= NOW() - INTERVAL '30 days'
        ORDER BY ticker, timestamp
    """)
    rows = cur.fetchall()
    if len(rows) < 50:
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
    feat_df["dxy_roll_vol"] = dxy_ret.rolling(ROLLING_WINDOW, min_periods=5).std()
    feat_df["dxy_roll_vol_60"] = dxy_ret.rolling(60, min_periods=5).std()
    feat_df["dxy_roll_vol_120"] = dxy_ret.rolling(120, min_periods=5).std()
    hl_pct = ((dfs["dxy"]["high"] - dfs["dxy"]["low"]) / dfs["dxy"]["close"])
    hl_pct = hl_pct.reindex(base_idx).fillna(0).astype(float)
    feat_df["dxy_hl_range"] = hl_pct
    feat_df["dxy_roll_hl_range"] = feat_df["dxy_hl_range"].rolling(ROLLING_WINDOW, min_periods=5).mean()
    for col in feat_df.columns:
        feat_df[col] = feat_df[col].replace([np.inf, -np.inf], np.nan).clip(-5, 5)
    return feat_df.dropna()


def add_time_features(feat_df):
    hour = feat_df.index.hour
    feat_df["sin_hour"] = np.sin(2 * np.pi * hour / 24)
    feat_df["cos_hour"] = np.cos(2 * np.pi * hour / 24)
    feat_df["is_weekend"] = (feat_df.index.dayofweek >= 5).astype(float)
    return feat_df


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


ALL_PREFIXES = ["dxy", "eur", "jpy", "gbp", "vix", "sp500"]


def override_stale_bars(dfs, r):
    now = datetime.now(timezone.utc)
    now_minute = now.replace(second=0, microsecond=0)
    for prefix in ["dxy", "eur", "jpy", "gbp"]:
        tdf = dfs[prefix]
        last_ts = tdf.index[-1]
        age = (now_minute - last_ts).total_seconds()
        if age > 90:
            stream_close = r.get(f"latest:{prefix}:close")
            if stream_close:
                stream_close_f = float(stream_close)
                stream_high = r.get(f"latest:{prefix}:high")
                stream_low = r.get(f"latest:{prefix}:low")
                high = float(stream_high) if stream_high else stream_close_f
                low = float(stream_low) if stream_low else stream_close_f
                open_val = float(tdf.iloc[-1]["close"])
                new_row = pd.DataFrame({
                    "open": [open_val], "high": [high],
                    "low": [low], "close": [stream_close_f],
                }, index=[now_minute])
                dfs[prefix] = pd.concat([tdf, new_row])
                dfs[prefix] = dfs[prefix][~dfs[prefix].index.duplicated(keep="last")]
                log.info("Override %s stale bar (%ds old) with stream close=%.4f", prefix, int(age), stream_close_f)


def run_one_cycle(producer, conn, cur, r):
    dfs = load_data(cur)
    if dfs is None:
        log.warning("Insufficient data")
        return

    feat_df = compute_features(dfs)
    if feat_df is None or len(feat_df) < 5:
        log.warning("Feature computation failed (%d rows)", len(feat_df) if feat_df is not None else 0)
        return

    feat_df = build_lag_features(feat_df)
    feat_df = add_time_features(feat_df)
    if len(feat_df) < 1:
        return

    override_stale_bars(dfs, r)

    if not os.path.exists(MODEL_PATH):
        log.warning("Model not found at %s", MODEL_PATH)
        return

    model = joblib.load(MODEL_PATH)
    price_model_path = MODEL_PATH.replace(".pkl", "_price.pkl")
    price_model = joblib.load(price_model_path) if os.path.exists(price_model_path) else None
    scaler_path = MODEL_PATH.replace(".pkl", "_scaler.pkl")
    scaler = joblib.load(scaler_path) if os.path.exists(scaler_path) else None

    latest = feat_df.iloc[-1:]
    X = latest[FEATURE_COLS].values
    if scaler is not None:
        X = scaler.transform(X)
    predicted_vol = max(float(model.predict(X)[0]), 0) * VOL_SCALE
    inst_vol_str = r.get("latest:dxy:instant_volatility")
    actual_vol = float(inst_vol_str) if inst_vol_str else 0.0

    dxy_latest = dfs["dxy"].loc[latest.index[0]]
    dxy_close = float(dxy_latest["close"])

    pred_price_1m = pred_price_3m = pred_price_5m = pred_price_30m = None
    if price_model is not None:
        X_price = X if scaler is not None else latest[FEATURE_COLS].values
        price_raw = price_model.predict(X_price)[0]
        pred_price_1m = round(dxy_close * (1 + price_raw[0]), 4)
        pred_price_3m = round(dxy_close * (1 + price_raw[1]), 4)
        pred_price_5m = round(dxy_close * (1 + price_raw[2]), 4)
        pred_price_30m = round(dxy_close * (1 + price_raw[3]), 4) if len(price_raw) > 3 else None

    features = {col: float(latest[col].iloc[0]) for col in feat_df.columns}

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

    producer.send("dxy-predictions", value=result).get(timeout=5)
    log.info("pred_vol=%.6f actual_vol=%.6f dxy=%.4f", predicted_vol, actual_vol, dxy_close)


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        acks=1,
    )
    conn, cur = get_db()
    r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)

    CYCLES_TOTAL = Counter("batch_predictor_cycles_total", "Completed cycles")
    ERRORS = Counter("batch_predictor_errors_total", "Total errors")
    LAST_SUCCESS = Gauge("batch_predictor_last_success_seconds", "Last success timestamp")
    PREDICTED_VOL = Gauge("batch_predictor_predicted_vol", "Latest predicted volatility")
    ACTUAL_VOL = Gauge("batch_predictor_actual_vol", "Latest actual volatility")
    start_http_server(8002)

    log.info("Starting batch predictor every %ds", PREDICT_INTERVAL)

    while True:
        try:
            run_one_cycle(producer, conn, cur, r)
            CYCLES_TOTAL.inc()
            PREDICTED_VOL.set(float(producer.bootstrap_connected()))  # placeholder
            LAST_SUCCESS.set(time.time())
        except Exception as e:
            ERRORS.inc()
            log.error("Error: %s", e)
        time.sleep(PREDICT_INTERVAL)
