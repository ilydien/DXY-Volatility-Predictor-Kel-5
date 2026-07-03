import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf
from prometheus_client import start_http_server, Counter, Gauge

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ws_listener")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
WS_TICKERS = os.getenv("WS_TICKERS", "DX-Y.NYB,EURUSD=X,USDJPY=X,GBPUSD=X").split(",")
REST_POLL_TICKERS = os.getenv("REST_POLL_TICKERS", "USDJPY=X,^VIX,^GSPC").split(",")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))
MAX_SILENT = int(os.getenv("MAX_WS_SILENT", "60"))


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

PUBLISHED_TOTAL = Counter("ws_listener_published_total", "Messages published")
ERRORS = Counter("ws_listener_errors_total", "Total errors")
LAST_SUCCESS = Gauge("ws_listener_last_success_seconds", "Last success timestamp")
WS_CONNECTED = Gauge("ws_listener_ws_connected", "WebSocket connected (1=yes, 0=no)")
MODE = Gauge("ws_listener_mode", "Data source mode (1=WebSocket, 0=REST)")
start_http_server(8004)

ws_last_data = 0.0
use_websocket = True
mode_lock = threading.Lock()
current_ws = None
ws_lock = threading.Lock()
ws_thread_active = False


def send_price(ticker, price, ts=None, source="ws"):
    entry = {"close": float(price), "source": source}
    data = {
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if isinstance(ts, (int, float))
        else datetime.now(timezone.utc).isoformat(),
        "data": {ticker: entry},
    }
    producer.send("market-data", value=data)
    PUBLISHED_TOTAL.inc()
    LAST_SUCCESS.set(time.time())
    log.info("%s: %s (source=%s)", ticker, price, source)


def on_ws_message(msg):
    global ws_last_data
    try:
        ticker = msg.get("id")
        price = msg.get("price")
        ts = msg.get("time", time.time())
        if ticker and price is not None and ticker in WS_TICKERS:
            ws_last_data = time.time()
            send_price(ticker, price, ts=ts)
    except Exception as e:
        log.warning("WS message handler error: %s", e)


def start_ws():
    global use_websocket, current_ws, ws_thread_active
    try:
        while True:
            try:
                log.info("Connecting to Yahoo Finance WebSocket...")
                with yf.WebSocket() as ws:
                    with ws_lock:
                        current_ws = ws
                    ws.subscribe(WS_TICKERS)
                    log.info("Subscribed to %s via WebSocket", WS_TICKERS)
                    WS_CONNECTED.set(1)
                    with mode_lock:
                        if use_websocket:
                            MODE.set(1)
                    ws.listen(on_ws_message)
            except Exception as e:
                log.warning("WebSocket error: %s", e)
            finally:
                with ws_lock:
                    current_ws = None
                WS_CONNECTED.set(0)
                with mode_lock:
                    if not use_websocket:
                        return
                log.info("Reconnecting WebSocket in 5s...")
                time.sleep(5)
    finally:
        ws_thread_active = False


def rest_poll_forever():
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
                    source="rest_fallback",
                )
        except Exception as e:
            ERRORS.inc()
            log.error("REST poll error: %s", e)
        time.sleep(POLL_INTERVAL)


def poll_ticker_forever(ticker_id, interval, period="1d", interval_str="1m"):
    log.info("REST polling %s every %ds", ticker_id, interval)
    t = yf.Ticker(ticker_id)
    while True:
        try:
            df = t.history(period=period, interval=interval_str)
            if df.empty:
                log.warning("No data for %s", ticker_id)
            else:
                last = df.iloc[-1]
                send_price(ticker_id, float(last["Close"]), source="rest_poll")
        except Exception as e:
            log.warning("REST poll %s error: %s", ticker_id, e)
        time.sleep(interval)


def watchdog():
    global use_websocket, ws_thread_active
    rest_started = False
    rest_begin = 0.0
    last_ws_retry = 0.0
    WS_RETRY_INTERVAL = 300
    while True:
        time.sleep(5)
        now = time.time()
        if not use_websocket:
            if not rest_started and rest_begin > 0:
                rest_started = True
                threading.Thread(target=rest_poll_forever, daemon=True).start()
            if rest_started and now - rest_begin > WS_RETRY_INTERVAL and now - last_ws_retry > WS_RETRY_INTERVAL and not ws_thread_active:
                log.info("Attempting to reconnect WebSocket...")
                last_ws_retry = now
                ws_thread_active = True
                with mode_lock:
                    use_websocket = True
                threading.Thread(target=start_ws, daemon=True).start()
            continue
        if ws_last_data > 0 and now - ws_last_data > MAX_SILENT:
            log.warning("WS silent for %ds, closing WS and switching to REST fallback", int(now - ws_last_data))
            with ws_lock:
                if current_ws is not None:
                    try:
                        current_ws.close()
                    except Exception:
                        pass
            with mode_lock:
                use_websocket = False
                MODE.set(0)
            WS_CONNECTED.set(0)
            rest_begin = time.time()


log.info("Starting ws_listener, tickers=%s", WS_TICKERS)

ws_thread_active = True
ws_thread = threading.Thread(target=start_ws, daemon=True)
ws_thread.start()

wd_thread = threading.Thread(target=watchdog, daemon=True)
wd_thread.start()

REST_POLL_CONFIGS = [
    ("USDJPY=X", 1, "1d", "1m"),
    ("^VIX", 1, "1mo", "1d"),
    ("^GSPC", 1, "1d", "1m"),
]
for ticker, interval, period, bar_interval in REST_POLL_CONFIGS:
    t = threading.Thread(
        target=poll_ticker_forever,
        args=(ticker, interval, period, bar_interval),
        daemon=True,
    )
    t.start()

while True:
    time.sleep(60)
