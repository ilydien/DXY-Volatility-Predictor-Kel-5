import json
import os
import time
import joblib
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("predictor")
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
import redis

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/dxy_model.pkl")
ROLLING_WINDOW = int(os.getenv("ROLLING_WINDOW", "20"))
MAX_AGE_SECONDS = int(os.getenv("MAX_DATA_AGE", "120"))

TICKER_MAP = {
    "DX-Y.NYB": "dxy",
    "EURUSD=X": "eur",
    "USDJPY=X": "jpy",
    "GBPUSD=X": "gbp",
}

FEATURE_COLS = (
    [f"{p}_return_lag{i}" for p in ["dxy", "eur", "jpy", "gbp", "vix", "sp500"] for i in range(1, 4)]
    + [f"{p}_hl_pct_lag1" for p in ["dxy", "eur", "jpy", "gbp", "vix", "sp500"]]
    + ["dxy_roll_vol_lag1", "dxy_roll_vol_lag2", "dxy_roll_vol_lag3"]
)

ALL_PREFIXES = ["dxy", "eur", "jpy", "gbp", "vix", "sp500"]
WS_PREFIXES = ["dxy", "eur", "jpy", "gbp"]


def get_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                acks=1,
            )
        except Exception as e:
            log.warning("Waiting for Kafka producer: %s", e)
            time.sleep(3)


def get_kafka_consumer():
    while True:
        try:
            return KafkaConsumer(
                "market-data",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode()),
                group_id="predictor-stream-group",
                auto_offset_reset="latest",
            )
        except Exception as e:
            log.warning("Waiting for Kafka consumer: %s", e)
            time.sleep(3)


producer = get_kafka_producer()
consumer = get_kafka_consumer()
r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)

model = None
if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
    log.info("Loaded model from %s", MODEL_PATH)
else:
    log.warning("No model found, using heuristic")

PRICE_MODEL_PATH = MODEL_PATH.replace(".pkl", "_price.pkl")
price_model = None
if os.path.exists(PRICE_MODEL_PATH):
    price_model = joblib.load(PRICE_MODEL_PATH)
    log.info("Loaded price model from %s", PRICE_MODEL_PATH)
else:
    log.warning("No price model found")


def get_history(prefix, n=50):
    raw = r.lrange(f"history:{prefix}:close", -n, -1)
    try:
        return [float(v) for v in raw]
    except (TypeError, ValueError):
        return []


def compute_returns(prices):
    if len(prices) < 2:
        return []
    return [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]


def build_features(dxy_tick=None):
    prices = {}
    for prefix in ALL_PREFIXES:
        prices[prefix] = get_history(prefix)

    min_len = min(len(v) for v in prices.values())
    if min_len < 4:
        return None

    features = {}
    for prefix in ALL_PREFIXES:
        rets = compute_returns(prices[prefix])
        for i in range(1, 4):
            idx = -(i + 1)
            features[f"{prefix}_return_lag{i}"] = float(rets[idx]) if abs(idx) <= len(rets) else 0.0

        high = r.get(f"latest:{prefix}:high")
        low = r.get(f"latest:{prefix}:low")
        close_ = prices[prefix][-1]
        if high and low and close_:
            features[f"{prefix}_hl_pct_lag1"] = (float(high) - float(low)) / float(close_)
        else:
            features[f"{prefix}_hl_pct_lag1"] = 0.0

    if dxy_tick is not None and prices["dxy"]:
        features["dxy_return_lag1"] = (dxy_tick / prices["dxy"][-1]) - 1

    for prefix in ["eur", "jpy", "gbp"]:
        tick = r.get(f"latest:{prefix}:close")
        if tick and prices[prefix]:
            tick_f = float(tick)
            last_close = prices[prefix][-1]
            if last_close and abs(tick_f - last_close) / max(last_close, 0.0001) > 1e-8:
                features[f"{prefix}_return_lag1"] = (tick_f / last_close) - 1

    dxy_rets = compute_returns(prices["dxy"])
    for i in range(1, 4):
        if len(dxy_rets) >= ROLLING_WINDOW + i:
            sub = dxy_rets[-(ROLLING_WINDOW + i):-i]
            features[f"dxy_roll_vol_lag{i}"] = float(np.std(sub, ddof=0))
        elif len(dxy_rets) >= ROLLING_WINDOW:
            features[f"dxy_roll_vol_lag{i}"] = float(np.std(dxy_rets[-ROLLING_WINDOW:], ddof=0))
        else:
            features[f"dxy_roll_vol_lag{i}"] = 0.0

    return features


def compute_instant_vol():
    prices = get_history("dxy")
    tick = r.get("latest:dxy:close")
    if tick and prices:
        tick_f = float(tick)
        if abs(prices[-1] - tick_f) / max(prices[-1], 0.0001) > 1e-6:
            prices = prices + [tick_f]
    if len(prices) < 2:
        return 0.0
    rets = [(prices[i] / prices[i - 1]) - 1 for i in range(1, len(prices))]
    window = min(ROLLING_WINDOW, len(rets))
    return float(np.std(rets[-window:], ddof=0))


log.info("Waiting for streaming market data...")
last_predicted = None

for msg in consumer:
    try:
        body = msg.value
        data = body.get("data", {})
        ts_raw = body.get("timestamp")

        if ts_raw:
            try:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(ts_raw)).total_seconds()
                if age > MAX_AGE_SECONDS:
                    continue
            except (ValueError, TypeError):
                pass

        for ticker, ticker_data in data.items():
            prefix = TICKER_MAP.get(ticker)
            if not prefix:
                continue

            close = float(ticker_data.get("close", 0))
            r.set(f"latest:{prefix}:close", close)
            high = ticker_data.get("high")
            low = ticker_data.get("low")
            if high:
                r.set(f"latest:{prefix}:high", float(high))
            if low:
                r.set(f"latest:{prefix}:low", float(low))

            if ticker != "DX-Y.NYB":
                log.debug("Updated %s=%.4f", prefix, close)
                continue

            inst_vol = compute_instant_vol()
            r.set("latest:dxy:instant_volatility", inst_vol)

            features = build_features(dxy_tick=close)
            if features is None:
                continue

            if model is not None:
                X = np.array([[features.get(c, 0) for c in FEATURE_COLS]])
                predicted_vol = max(float(model.predict(X)[0]), 0)
            else:
                predicted_vol = inst_vol

            pred_price_1m = pred_price_3m = pred_price_5m = None
            if price_model is not None:
                X_price = np.array([[features.get(c, 0) for c in FEATURE_COLS]])
                price_raw = price_model.predict(X_price)[0]
                pred_price_1m = round(close * (1 + price_raw[0]), 4)
                pred_price_3m = round(close * (1 + price_raw[1]), 4)
                pred_price_5m = round(close * (1 + price_raw[2]), 4)

            same = last_predicted is not None and abs(predicted_vol - last_predicted) / max(last_predicted, 1e-10) < 0.001
            if same:
                continue

            result = {
                "timestamp": ts_raw or datetime.now(timezone.utc).isoformat(),
                "dxy_close": round(close, 4),
                "predicted_volatility": round(predicted_vol, 6),
                "actual_volatility": None,
                "instant_volatility": round(inst_vol, 6),
                "predicted_price_1m": pred_price_1m,
                "predicted_price_3m": pred_price_3m,
                "predicted_price_5m": pred_price_5m,
                "source": "stream",
                "features": {k: round(v, 6) if isinstance(v, float) else v for k, v in features.items()},
            }

            producer.send("dxy-predictions", value=result)
            last_predicted = predicted_vol
            log.info("pred_vol=%.6f inst_vol=%.6f", predicted_vol, inst_vol)

    except Exception as e:
        log.error("Error: %s", e)
