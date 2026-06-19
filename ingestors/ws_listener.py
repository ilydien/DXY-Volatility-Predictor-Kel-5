import json
import logging
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ws_listener")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WS_TICKERS = os.getenv("WS_TICKERS", "DX-Y.NYB,EURUSD=X,USDJPY=X,GBPUSD=X").split(",")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))


def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                acks=1,
            )
            log.info("Connected to Kafka")
            return producer
        except Exception as e:
            log.warning("Waiting for Kafka: %s", e)
            time.sleep(3)


producer = get_kafka_producer()


def send_price(ticker, price, ts=None, high=None, low=None):
    entry = {"close": float(price), "source": "ws"}
    if high is not None:
        entry["high"] = float(high)
    if low is not None:
        entry["low"] = float(low)
    data = {
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if isinstance(ts, (int, float))
        else datetime.now(timezone.utc).isoformat(),
        "data": {ticker: entry},
    }
    producer.send("market-data", value=data)
    log.info("%s: %s", ticker, price)


def poll_forever():
    log.info("REST polling every %ds for %s", POLL_INTERVAL, WS_TICKERS)
    tickers = [yf.Ticker(t) for t in WS_TICKERS]
    while True:
        try:
            for t, ticker_id in zip(tickers, WS_TICKERS):
                df = t.history(period="1d", interval="1m")
                if df.empty:
                    continue
                last = df.iloc[-1]
                send_price(
                    ticker_id,
                    float(last["Close"]),
                    high=float(last["High"]),
                    low=float(last["Low"]),
                )
        except Exception as e:
            log.error("Poll error: %s", e)
        time.sleep(POLL_INTERVAL)


log.info("Connecting, tickers=%s", WS_TICKERS)

ws_available = hasattr(yf, "WebSocket")
if ws_available:
    log.info("WebSocket available, trying streaming...")
    try:
        ws = yf.WebSocket(verbose=False)
        ws.subscribe(WS_TICKERS)
        ws.listen(lambda msg: send_price(
            msg.get("id"), msg.get("price"), msg.get("ts")
        ))
    except Exception as e:
        log.warning("WebSocket failed (%s), falling back to REST", e)
        poll_forever()
else:
    poll_forever()
