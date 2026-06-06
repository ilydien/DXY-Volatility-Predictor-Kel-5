import json
import os
import time
from kafka import KafkaConsumer
import redis
import psycopg2

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")


def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                "dxy-predictions",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda v: json.loads(v.decode()),
                group_id="sink-group",
                auto_offset_reset="latest",
            )
            print(f"[sink] Connected to Kafka")
            return consumer
        except Exception as e:
            print(f"[sink] Waiting for Kafka: {e}")
            time.sleep(3)


consumer = get_kafka_consumer()

r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)


def get_db():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            return conn, conn.cursor()
        except Exception as e:
            print(f"[sink] Waiting for PostgreSQL: {e}")
            time.sleep(3)


conn, cur = get_db()

print("[sink] Waiting for predictions on 'dxy-predictions'...")
for msg in consumer:
    try:
        body = msg.value

        predicted = body.get("predicted_volatility")
        actual = body.get("actual_volatility")
        ts = body.get("timestamp")
        dxy_close = body.get("dxy_close")
        source = body.get("source", "unknown")

        if predicted is None:
            continue

        r.set("latest:dxy:predicted_volatility", predicted)
        r.set("latest:dxy:predicted_timestamp", ts)
        r.set("latest:dxy:close", dxy_close)

        r.xadd(
            "dxy-predictions",
            {
                "timestamp": ts,
                "dxy_close": dxy_close,
                "predicted_volatility": predicted,
                "actual_volatility": actual if actual is not None else "",
                "source": source,
            },
            maxlen=10000,
        )

        cur.execute(
            """
            INSERT INTO predictions (timestamp, predicted_close, actual_close, features)
            VALUES (%s, %s, %s, %s)
        """,
            (
                ts,
                predicted,
                actual if actual is not None else None,
                json.dumps(body.get("features", {})),
            ),
        )
        conn.commit()

        print(f"[sink] source={source} pred_vol={predicted:.4f} actual_vol={actual}")

    except Exception as e:
        print(f"[sink] Error: {e}")
