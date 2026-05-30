import json
import os
import time
from kafka import KafkaConsumer
import redis
import psycopg2

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")

consumer = KafkaConsumer(
    "gold-predictions",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode()),
    group_id="sink-group",
    auto_offset_reset="latest",
)

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

print("[sink] Waiting for predictions...")
for msg in consumer:
    try:
        body = msg.value

        r.set("latest:prediction:close", body["predicted_close"])
        r.set("latest:prediction:timestamp", body["timestamp"])

        r.xadd(
            "gold-predictions",
            {
                "timestamp": body["timestamp"],
                "actual_close": body["actual_close"],
                "predicted_close": body["predicted_close"],
            },
            maxlen=10000,
        )

        cur.execute(
            """
            INSERT INTO predictions (timestamp, predicted_close, actual_close, features)
            VALUES (%s, %s, %s, %s)
        """,
            (
                body["timestamp"],
                body["predicted_close"],
                body["actual_close"],
                json.dumps(body.get("features", {})),
            ),
        )
        conn.commit()

        print(
            f"[sink] Predicted={body['predicted_close']:.2f} Actual={body['actual_close']:.2f}"
        )

    except Exception as e:
        print(f"[sink] Error: {e}")
