import json
import os
import time
import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import redis

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/dxy_model.pkl")


def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                acks=1,
            )
            print(f"[predictor] Connected to Kafka producer")
            return producer
        except Exception as e:
            print(f"[predictor] Waiting for Kafka producer: {e}")
            time.sleep(3)


def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "market-data",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode()),
                group_id="predictor-stream-group",
                auto_offset_reset="latest",
            )
            print(f"[predictor] Connected to Kafka consumer")
            return consumer
        except Exception as e:
            print(f"[predictor] Waiting for Kafka consumer: {e}")
            time.sleep(3)


producer = get_kafka_producer()
consumer = get_kafka_consumer()

r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)

model = None
if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
    print(f"[predictor] Loaded model from {MODEL_PATH}")
else:
    print("[predictor] No model found, using heuristic")

CACHE_KEYS = [
    "latest:eur:close", "latest:jpy:close", "latest:gbp:close",
    "latest:vix:close", "latest:sp500:close", "latest:dxy:close",
    "latest:dxy:high", "latest:dxy:low", "latest:dxy:volatility",
]


def read_cache():
    vals = r.mget(CACHE_KEYS)
    out = {}
    for key, val in zip(CACHE_KEYS, vals):
        name = key.replace("latest:", "").replace(":", "_")
        out[name] = float(val) if val else None
    return out


FEATURE_COLS = (
    [f"{c}_lag{i}" for c in ["eur_close", "jpy_close", "gbp_close", "vix_close", "sp500_close", "dxy_close"] for i in range(1, 4)]
    + ["dxy_volatility_lag1"]
)


def build_features(dxy_close, cache):
    features = {}
    for prefix in ["eur", "jpy", "gbp", "vix", "sp500"]:
        features[f"{prefix}_close"] = cache.get(f"{prefix}_close", 0)
    features["dxy_close"] = dxy_close
    for col in ["eur_close", "jpy_close", "gbp_close", "vix_close", "sp500_close", "dxy_close"]:
        for i in range(1, 4):
            features[f"{col}_lag{i}"] = features.get(col, 0)
    features["dxy_volatility_lag1"] = cache.get("dxy_volatility", 0)
    return features


print("[predictor] Waiting for streaming market data...")
for msg in consumer:
    try:
        body = msg.value
        data = body.get("data", {})

        dxy_data = data.get("DX-Y.NYB", {})
        if not dxy_data:
            continue

        dxy_close = float(dxy_data.get("close", 0))
        cache = read_cache()

        features = build_features(dxy_close, cache)

        if model is not None:
            df_feat = pd.DataFrame([{k: features.get(k, 0) for k in FEATURE_COLS}])
            predicted_vol = float(model.predict(df_feat)[0])
        else:
            predicted_vol = cache.get("dxy_volatility", 0)

        predicted_vol = max(predicted_vol, 0)

        result = {
            "timestamp": body.get("timestamp"),
            "dxy_close": round(dxy_close, 4),
            "predicted_volatility": round(predicted_vol, 4),
            "actual_volatility": None,
            "source": "stream",
            "features": {k: round(v, 4) if isinstance(v, float) else v for k, v in features.items()},
        }

        producer.send("dxy-predictions", value=result)
        print(f"[predictor] DXY={dxy_close:.4f} pred_vol={predicted_vol:.4f}")

    except Exception as e:
        print(f"[predictor] Error: {e}")
