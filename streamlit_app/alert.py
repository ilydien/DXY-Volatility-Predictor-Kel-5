from datetime import datetime, timezone


def check_alert(instant_vol, threshold, r):
    is_triggered = instant_vol is not None and instant_vol > threshold
    now = datetime.now(timezone.utc).isoformat()

    r.set("alert:dxy:threshold", str(threshold))

    if is_triggered:
        last = r.get("alert:dxy:last_value")
        r.set("alert:dxy:status", "TRIGGERED")
        r.set("alert:dxy:last_value", str(instant_vol))
        r.set("alert:dxy:last_timestamp", now)
        if last is None:
            r.set("alert:dxy:last_triggered", now)
        return True
    else:
        r.set("alert:dxy:status", "NORMAL")
        return False


def get_alert_state(r):
    return {
        "status": r.get("alert:dxy:status") or "NORMAL",
        "last_value": r.get("alert:dxy:last_value"),
        "last_timestamp": r.get("alert:dxy:last_timestamp"),
        "last_triggered": r.get("alert:dxy:last_triggered"),
        "threshold": r.get("alert:dxy:threshold") or "0.001",
    }
