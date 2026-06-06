import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf
import pandas as pd

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WS_TICKERS = os.getenv("WS_TICKERS", "DX-Y.NYB").split(",")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))


def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
                acks=1,
            )
            print(f"[ws_listener] Connected to Kafka")
            return producer
        except Exception as e:
            print(f"[ws_listener] Waiting for Kafka: {e}")
            time.sleep(3)


producer = get_kafka_producer()


def send_price(ticker, price, ts=None):
    data = {
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if isinstance(ts, (int, float))
        else datetime.now(timezone.utc).isoformat(),
        "data": {ticker: {"close": float(price), "source": "ws"}},
    }
    producer.send("market-data", value=data)
    print(f"[ws_listener] {ticker}: {price}")


def poll_forever():
    print(f"[ws_listener] REST polling every {POLL_INTERVAL}s for {WS_TICKERS}")
    while True:
        try:
            df = yf.download(
                WS_TICKERS,
                period="1d",
                interval="1m",
                group_by="ticker",
                progress=False,
            )
            for ticker in WS_TICKERS:
                try:
                    if isinstance(df.columns, pd.MultiIndex):
                        tdf = df[ticker].dropna()
                    else:
                        tdf = df.dropna()
                    if tdf.empty:
                        continue
                    last = tdf.iloc[-1]
                    send_price(ticker, float(last["Close"]))
                except (KeyError, IndexError, ValueError, TypeError):
                    continue
        except Exception as e:
            print(f"[ws_listener] Poll error: {e}")
        time.sleep(POLL_INTERVAL)


print(f"[ws_listener] Connecting, tickers={WS_TICKERS}")

# Try WebSocket first, fallback to REST polling
ws_available = hasattr(yf, "WebSocket")
if ws_available:
    print("[ws_listener] WebSocket available, trying streaming...")
    try:
        ws = yf.WebSocket(verbose=False)
        ws.subscribe(WS_TICKERS)
        ws.listen(lambda msg: send_price(
            msg.get("id"), msg.get("price"), msg.get("ts")
        ))
    except Exception as e:
        print(f"[ws_listener] WebSocket failed ({e}), falling back to REST")
        poll_forever()
else:
    poll_forever()
