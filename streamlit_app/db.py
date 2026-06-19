import os
import redis
import psycopg2
import pandas as pd

DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")

r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)

PREFIX_MAP = {
    "dxy": "DXY",
    "eur": "EUR/USD",
    "jpy": "USD/JPY",
    "gbp": "GBP/USD",
    "vix": "VIX",
    "sp500": "S&P 500",
}


def get_conn():
    return psycopg2.connect(POSTGRES_DSN)


def get_latest_prices():
    prices = {}
    for key, label in PREFIX_MAP.items():
        val = r.get(f"latest:{key}:close")
        prices[label] = float(val) if val else None
    return prices


def get_latest_vol():
    pred = r.get("latest:dxy:predicted_volatility")
    inst = r.get("latest:dxy:instant_volatility")
    return {
        "predicted": float(pred) if pred else None,
        "instant": float(inst) if inst else None,
    }


def get_predictions_24h(limit=100):
    conn = get_conn()
    query = f"""
    SELECT timestamp, predicted_close, actual_close
    FROM predictions
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if not df.empty:
        df = df.sort_values("timestamp")
    return df


def get_dxy_prices(hours=8):
    conn = get_conn()
    df = pd.read_sql(
        f"""
        SELECT timestamp, close
        FROM intraday_bars
        WHERE ticker = 'DX-Y.NYB'
          AND timestamp >= NOW() - INTERVAL '{hours} hours'
        ORDER BY timestamp
        """,
        conn,
    )
    conn.close()
    return df


def get_dxy_recent_bars(minutes=30):
    raw = r.lrange("history:dxy:close", -minutes, -1)
    if not raw:
        return pd.DataFrame({"index": [], "close": []})
    try:
        prices = [float(v) for v in raw]
    except (TypeError, ValueError):
        return pd.DataFrame({"index": [], "close": []})
    return pd.DataFrame({"index": list(range(len(prices))), "close": prices})


def get_latest_price_preds():
    pred_1m = r.get("latest:dxy:pred_price_1m")
    pred_3m = r.get("latest:dxy:pred_price_3m")
    pred_5m = r.get("latest:dxy:pred_price_5m")
    current_close = r.get("latest:dxy:close")
    if not pred_1m or not current_close:
        return None
    return {
        "current_close": float(current_close),
        "pred_1m": float(pred_1m),
        "pred_3m": float(pred_3m) if pred_3m else None,
        "pred_5m": float(pred_5m) if pred_5m else None,
    }


def get_price_pred_history():
    raw_1m = r.lrange("history:dxy:pred_price_1m", 0, -1)
    raw_3m = r.lrange("history:dxy:pred_price_3m", 0, -1)
    raw_5m = r.lrange("history:dxy:pred_price_5m", 0, -1)
    try:
        return {
            "pred_1m": [float(v) for v in raw_1m] if raw_1m else [],
            "pred_3m": [float(v) for v in raw_3m] if raw_3m else [],
            "pred_5m": [float(v) for v in raw_5m] if raw_5m else [],
        }
    except (TypeError, ValueError):
        return {"pred_1m": [], "pred_3m": [], "pred_5m": []}


def get_recent_features():
    keys = [
        "latest:dxy:close", "latest:eur:close", "latest:jpy:close",
        "latest:gbp:close", "latest:vix:close", "latest:sp500:close",
    ]
    vals = r.mget(keys)
    features = {
        "predictor_latest_close": None,
    }
    for key, val in zip(keys, vals):
        tag = key.replace("latest:", "").replace(":close", "")
        features[f"{tag}_close"] = float(val) if val else None
    return features
