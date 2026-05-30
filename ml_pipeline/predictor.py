import json
import os
import joblib
import numpy as np
import pandas as pd
from collections import deque
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MODEL_PATH = "/app/models/gold_model.pkl"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode(),
    acks=1,
)

consumer = KafkaConsumer(
    "market-data",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode()),
    group_id="predictor-group",
    auto_offset_reset="latest",
)

model = None
gold_history = deque(maxlen=10)

if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
    print(f"[predictor] Loaded model from {MODEL_PATH}")
else:
    print("[predictor] No model found, using heuristic (price ~= last close)")


def prepare_features(msg_data):
    gold = msg_data.get("XAUUSD=X", {})
    gold_close = gold.get("close", 0)
    gold_history.append(gold_close)

    closes = list(gold_history)
    while len(closes) < 5:
        closes = [closes[0]] + closes if closes else [gold_close]

    features = {
        "gold_lag1": closes[-1],
        "gold_lag2": closes[-2] if len(closes) >= 2 else closes[-1],
        "gold_lag3": closes[-3] if len(closes) >= 3 else closes[-1],
        "gold_lag4": closes[-4] if len(closes) >= 4 else closes[-1],
        "gold_lag5": closes[-5] if len(closes) >= 5 else closes[-1],
        "gold_ma5": np.mean(closes[-5:]),
        "dxy_close": msg_data.get("DX-Y.NYB", {}).get("close", 0),
        "vix_close": msg_data.get("^VIX", {}).get("close", 0),
        "sp500_close": msg_data.get("^GSPC", {}).get("close", 0),
        "oil_close": msg_data.get("CL=F", {}).get("close", 0),
    }

    return features, gold_close


def predict(features, gold_close):
    if model is not None:
        df = pd.DataFrame([features])
        predicted = float(model.predict(df)[0])
    else:
        predicted = gold_close

    return predicted


print("[predictor] Waiting for market data...")
for msg in consumer:
    try:
        body = msg.value
        data = body.get("data", {})

        if "XAUUSD=X" not in data:
            continue

        features, gold_close = prepare_features(data)
        predicted = predict(features, gold_close)

        result = {
            "timestamp": body.get("timestamp"),
            "actual_close": gold_close,
            "predicted_close": round(predicted, 2),
            "features": features,
        }

        producer.send("gold-predictions", value=result)
        print(f"[predictor] actual={gold_close:.2f} predicted={predicted:.2f}")

    except Exception as e:
        print(f"[predictor] Error: {e}")
