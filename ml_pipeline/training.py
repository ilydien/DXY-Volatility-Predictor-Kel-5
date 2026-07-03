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
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

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
DRAGONFLY_PASSWORD = os.getenv("DRAGONFLY_PASSWORD", "")
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
r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)
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
    WHERE timestamp >= NOW() - INTERVAL '7 days'
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
feat_df = add_time_features(feat_df)
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
    + ["sin_hour", "cos_hour", "is_weekend"]
)

X = feat_df[FEATURE_COLS].values
y = feat_df["dxy_roll_hl_range"].values

SCALER_PATH = MODEL_PATH.replace(".pkl", "_scaler.pkl")
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
joblib.dump(scaler, SCALER_PATH)

split_idx = int(len(X_scaled) * 0.8)
if split_idx >= 5:
    eval_model = RidgeCV(alphas=[1e-4, 5e-4, 1e-3, 5e-3, 0.01, 0.1, 1, 10])
    eval_model.fit(X_scaled[:split_idx], y[:split_idx])
    y_pred_vol = eval_model.predict(X_scaled[split_idx:])
    y_true_vol = y[split_idx:]

    y_pred_persist = np.full_like(y_true_vol, y_true_vol[0])
    persist_mae = mean_absolute_error(y_true_vol, y_pred_persist)

    r2_vol = r2_score(y_true_vol, y_pred_vol)
    mae_vol = mean_absolute_error(y_true_vol, y_pred_vol)
    rmse_vol = np.sqrt(mean_squared_error(y_true_vol, y_pred_vol))
    mape_vol = np.mean(np.abs((y_true_vol - y_pred_vol) / (y_true_vol + 1e-10))) * 100

    log.info(
        "Vol model eval — R²=%.4f, MAE=%.6f, RMSE=%.6f, MAPE=%.2f%% | Persistence MAE=%.6f",
        r2_vol, mae_vol, rmse_vol, mape_vol, persist_mae,
    )

    try:
        conn.rollback()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS training_metrics (
                timestamp TIMESTAMPTZ DEFAULT NOW(),
                model_type TEXT,
                r2 FLOAT, mae FLOAT, rmse FLOAT, mape FLOAT
            )
        """)
        cur.execute(
            "INSERT INTO training_metrics (model_type, r2, mae, rmse, mape) VALUES (%s, %s, %s, %s, %s)",
            ("volatility", float(round(r2_vol, 6)), float(round(mae_vol, 8)), float(round(rmse_vol, 8)), float(round(mape_vol, 4))),
        )
        conn.commit()
    except Exception as e:
        log.warning("Failed to save vol metrics: %s", e)

model = RidgeCV(alphas=[1e-4, 5e-4, 1e-3, 5e-3, 0.01, 0.1, 1, 10])
model.fit(X_scaled, y)
joblib.dump(model, MODEL_PATH)
log.info("Vol model trained on %d rows, %d features (scaled), best_alpha=%.6f", len(feat_df), len(FEATURE_COLS), model.alpha_)

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

    split_px = int(len(X_price) * 0.8)
    if split_px >= 5:
        eval_price = RidgeCV(alphas=[1e-4, 5e-4, 1e-3, 5e-3, 0.01, 0.1, 1, 10])
        eval_price.fit(X_price[:split_px], y_price[:split_px])
        y_pred_price = eval_price.predict(X_price[split_px:])
        y_true_price = y_price[split_px:]
        horizons = ["t+1", "t+3", "t+5", "t+30"]
        for i, h in enumerate(horizons):
            r2_price = r2_score(y_true_price[:, i], y_pred_price[:, i])
            mae_price = mean_absolute_error(y_true_price[:, i], y_pred_price[:, i])
            persist_mae = mean_absolute_error(
                y_true_price[:, i], np.zeros_like(y_true_price[:, i])
            )
            log.info(
                "Price %s eval — R²=%.4f, MAE=%.6f | Persistence MAE=%.6f",
                h, r2_price, mae_price, persist_mae,
            )
            try:
                conn.rollback()
                cur.execute(
                    "INSERT INTO training_metrics (model_type, r2, mae) VALUES (%s, %s, %s)",
                    (f"price_{h}", float(round(r2_price, 6)), float(round(mae_price, 8))),
                )
                conn.commit()
            except Exception as e:
                log.warning("Failed to save price %s metrics: %s", h, e)

    price_model = RidgeCV(alphas=[1e-4, 5e-4, 1e-3, 5e-3, 0.01, 0.1, 1, 10])
    price_model.fit(X_price, y_price)
    PRICE_MODEL_PATH = MODEL_PATH.replace(".pkl", "_price.pkl")
    joblib.dump(price_model, PRICE_MODEL_PATH)
    log.info("Price model trained on %d rows, 4 targets (t+1, t+3, t+5, t+30), best_alpha=%.6f", len(common_idx), price_model.alpha_)
else:
    price_model = None
    log.warning("Not enough price data (%d rows), skipping price model", len(common_idx))

latest = feat_df.iloc[-1:]

pred_X = scaler.transform(latest[FEATURE_COLS].values)
predicted_vol = float(model.predict(pred_X)[0]) * VOL_SCALE
predicted_vol = max(predicted_vol, 0)

inst_vol_str = r.get("latest:dxy:instant_volatility")
actual_vol = float(inst_vol_str) if inst_vol_str else 0.0
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
