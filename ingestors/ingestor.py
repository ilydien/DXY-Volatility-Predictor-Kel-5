import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
import redis
import os

TICKERS = os.getenv("TICKERS", "DX-Y.NYB,EURUSD=X,USDJPY=X,GBPUSD=X,^VIX,^GSPC").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode(),
    acks=1,
)

r = redis.Redis(host=DRAGONFLY_HOST, port=6379, decode_responses=True)


def fetch_and_publish():
    df = yf.download(
        TICKERS,
        period="5d",
        interval="1d",
        group_by="ticker",
        progress=False,
    )

    timestamp = datetime.now(timezone.utc)
    data = {}

    for ticker in TICKERS:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                ticker_df = df[ticker]
            else:
                ticker_df = df

            if ticker_df.empty:
                continue

            last = ticker_df.dropna().iloc[-1]
            data[ticker] = {
                "close": float(last["Close"]),
                "open": float(last["Open"]),
                "high": float(last["High"]),
                "low": float(last["Low"]),
                "volume": int(last["Volume"]) if not pd.isna(last["Volume"]) else 0,
            }
        except (KeyError, IndexError, ValueError, TypeError):
            continue

    if not data or "DX-Y.NYB" not in data:
        print(f"[ingestor] No DXY data at {timestamp}")
        return

    message = {"timestamp": timestamp.isoformat(), "data": data}

    producer.send("market-data", value=message)
    print(f"[ingestor] Kafka: DXY={data['DX-Y.NYB']['close']}")

    stream_entry = {"timestamp": timestamp.isoformat()}
    for ticker, ticker_data in data.items():
        stream_entry[ticker] = json.dumps(ticker_data)
    r.xadd("market-data", stream_entry, maxlen=10000)

    r.set("latest:dxy:close", data["DX-Y.NYB"]["close"])
    r.set("latest:dxy:high", data["DX-Y.NYB"]["high"])
    r.set("latest:dxy:low", data["DX-Y.NYB"]["low"])
    r.set("latest:dxy:timestamp", timestamp.isoformat())

    if "EURUSD=X" in data:
        r.set("latest:eur:close", data["EURUSD=X"]["close"])
    if "USDJPY=X" in data:
        r.set("latest:jpy:close", data["USDJPY=X"]["close"])
    if "GBPUSD=X" in data:
        r.set("latest:gbp:close", data["GBPUSD=X"]["close"])
    if "^VIX" in data:
        r.set("latest:vix:close", data["^VIX"]["close"])
    if "^GSPC" in data:
        r.set("latest:sp500:close", data["^GSPC"]["close"])

    vol = data["DX-Y.NYB"]["high"] - data["DX-Y.NYB"]["low"]
    r.set("latest:dxy:volatility", vol)

    return message


if __name__ == "__main__":
    fetch_and_publish()
