import os
import time
import requests
import redis
import psycopg2
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("alert_bot")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "0.001"))
SUMMARY_INTERVAL = int(os.getenv("ALERT_SUMMARY_INTERVAL", "60"))
HEARTBEAT_INTERVAL = int(os.getenv("ALERT_HEARTBEAT_INTERVAL", "300"))
DRAGONFLY_HOST = os.getenv("DRAGONFLY_HOST", "dragonfly")
DRAGONFLY_PASSWORD = os.getenv("DRAGONFLY_PASSWORD", "")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://gold:gold@postgres:5432/golddb")

previous_status = None
previous_close = None
last_heartbeat_ts = 0.0
last_tick_ts = None
last_summary_ts = 0.0
FLOW_NAMES = ["batch-predict", "dxy-training", "dbt-test"]


def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=5,
        )
    except Exception as e:
        log.warning("Telegram send failed: %s", e)


def build_status(r, conn, prev_close=None):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    inst_vol = r.get("latest:dxy:instant_volatility")
    dxy = r.get("latest:dxy:close")
    p1 = r.get("latest:dxy:pred_price_1m")
    p3 = r.get("latest:dxy:pred_price_3m")
    p5 = r.get("latest:dxy:pred_price_5m")
    p30 = r.get("latest:dxy:pred_price_30m")

    dyn_threshold = r.get("latest:dxy:alert_threshold")
    effective_threshold = float(dyn_threshold) if dyn_threshold else THRESHOLD

    inst_vol_f = float(inst_vol) if inst_vol else 0.0
    status = "WARNING" if inst_vol_f > effective_threshold else "NORMAL"
    status_icon = "⚠️" if status == "WARNING" else "✅"
    vol_str = f"{inst_vol_f:.6f}"
    dxy_f = float(dxy) if dxy else None

    dxy_line = f"DXY: {dxy_str}" if not dxy_f else "DXY: —"
    if dxy_f and prev_close is not None:
        delta = dxy_f - prev_close
        if delta > 0.00005:
            dir_icon = "📈"
        elif delta < -0.00005:
            dir_icon = "📉"
        else:
            dir_icon = "➡️"
        dxy_line = f"{dir_icon} DXY: {dxy_f:.4f} ({delta:+.4f})"
    elif dxy_f:
        dxy_line = f"➡️ DXY: {dxy_f:.4f}"

    cur = conn.cursor()
    flows = {}
    for name in FLOW_NAMES:
        cur.execute("""
            SELECT state_type FROM flow_run fr
            JOIN flow f ON f.id = fr.flow_id
            WHERE f.name = %s AND fr.start_time >= NOW() - INTERVAL '30 minutes'
            ORDER BY fr.start_time DESC LIMIT 1
        """, (name,))
        row = cur.fetchone()
        if row is None:
            flows[name] = "❓"
        elif row[0] == "COMPLETED":
            flows[name] = "✅"
        elif row[0] == "FAILED":
            flows[name] = "❌"
        elif row[0] == "RUNNING":
            flows[name] = "⏳"
        else:
            flows[name] = "⏸"
    cur.close()

    flow_batch = flows.get("batch-predict", "❓")
    flow_train = flows.get("dxy-training", "❓")
    flow_dbt = flows.get("dbt-test", "❓")

    p1_str = f"{float(p1):.4f}" if p1 else "—"
    p3_str = f"{float(p3):.4f}" if p3 else "—"
    p5_str = f"{float(p5):.4f}" if p5 else "—"
    p30_str = f"{float(p30):.4f}" if p30 else "—"

    msg = (
        f"📊 *DXY Pipeline Update* — {ts}\n"
        f"Status: {status} {status_icon}\n"
        f"{dxy_line}\n"
        f"Vol: {vol_str} (threshold {effective_threshold:.4f})\n"
        f"Pred: 1m={p1_str} | 3m={p3_str} | 5m={p5_str} | 30m={p30_str}\n"
        f"Flow batch: {flow_batch} | training: {flow_train} | dbt: {flow_dbt}"
    )
    return status, dxy_f if dxy_f else 0.0, msg


def main():
    global previous_status, previous_close, last_tick_ts, last_heartbeat_ts

    log.info("Connecting to Dragonfly...")
    r = redis.Redis(host=DRAGONFLY_HOST, port=6379, password=DRAGONFLY_PASSWORD, decode_responses=True)
    r.ping()
    log.info("Dragonfly OK")

    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(POSTGRES_DSN)
    log.info("PostgreSQL OK")

    last_tick_ts = r.get("latest:dxy:predicted_timestamp")
    status, current_close, msg = build_status(r, conn, prev_close=None)
    send_telegram("🚀 *DXY Alert Bot Started*\n\n" + msg)
    previous_status = status
    previous_close = current_close
    last_heartbeat_ts = time.time()
    log.info("Bot started — status: %s, last_tick: %s", status, last_tick_ts)

    while True:
        try:
            now = time.time()
            current_tick = r.get("latest:dxy:predicted_timestamp")

            if current_tick and current_tick != last_tick_ts:
                status, current_close, msg = build_status(r, conn, prev_close=previous_close)
                send_telegram(msg)
                last_tick_ts = current_tick
                previous_status = status
                previous_close = current_close
                last_heartbeat_ts = now
                log.info("New tick: %s — %s", current_tick, status)
            elif not current_tick:
                log.debug("No tick data yet")

            if now - last_heartbeat_ts >= HEARTBEAT_INTERVAL:
                send_telegram("💓 *Heartbeat* — Bot masih hidup")
                last_heartbeat_ts = now
                log.info("Heartbeat sent")

        except Exception as e:
            log.error("Check cycle failed: %s", e)

        time.sleep(1)


if __name__ == "__main__":
    main()
