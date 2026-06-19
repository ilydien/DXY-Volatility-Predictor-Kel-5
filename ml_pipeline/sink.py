import json
import logging
import os
import time
from kafka import KafkaConsumer
import redis
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("sink")

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
            log.info("Connected to Kafka")
            return consumer
        except Exception as e:
            log.warning("Waiting for Kafka: %s", e)
            time.sleep(3)


consumer = get_kafka_consumer()

r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)


def get_db():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            return conn, conn.cursor()
        except Exception as e:
            log.warning("Waiting for PostgreSQL: %s", e)
            time.sleep(3)


conn, cur = get_db()

log.info("Waiting for predictions on 'dxy-predictions'...")
for msg in consumer:
    try:
        body = msg.value

        predicted = body.get("predicted_volatility")
        actual = body.get("actual_volatility")
        ts = body.get("timestamp")
        dxy_close = body.get("dxy_close")
        source = body.get("source", "unknown")
        pred_price_1m = body.get("predicted_price_1m")
        pred_price_3m = body.get("predicted_price_3m")
        pred_price_5m = body.get("predicted_price_5m")
        pred_price_30m = body.get("predicted_price_30m")

        if predicted is None:
            continue

        r.set("latest:dxy:predicted_volatility", predicted)
        r.set("latest:dxy:predicted_timestamp", ts)
        r.set("latest:dxy:close", dxy_close)

        if pred_price_1m is not None:
            r.set("latest:dxy:pred_price_1m", pred_price_1m)
            r.set("latest:dxy:pred_price_3m", pred_price_3m)
            r.set("latest:dxy:pred_price_5m", pred_price_5m)
            r.set("latest:dxy:pred_price_30m", pred_price_30m)
            r.rpush("history:dxy:pred_price_1m", pred_price_1m)
            r.ltrim("history:dxy:pred_price_1m", -200, -1)
            r.ltrim("history:dxy:pred_price_3m", -200, -1)
            r.ltrim("history:dxy:pred_price_5m", -200, -1)
            r.ltrim("history:dxy:pred_price_30m", -200, -1)

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
            INSERT INTO predictions (timestamp, predicted_close, actual_close, pred_price_1m, pred_price_3m, pred_price_5m, pred_price_30m, features)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                ts,
                predicted,
                actual if actual is not None else None,
                pred_price_1m,
                pred_price_3m,
                pred_price_5m,
                pred_price_30m,
                json.dumps(body.get("features", {})),
            ),
        )
        conn.commit()

        log.info("source=%s pred_vol=%.4f actual_vol=%s", source, predicted, actual)

    except Exception as e:
        print(f"[sink] Error: {e}")
