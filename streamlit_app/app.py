import streamlit as st
import pandas as pd
import altair as alt
import time
import os
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Gauge

from db import (
    get_latest_prices, get_latest_vol, get_predictions_24h,
    get_latest_price_preds, get_actual_vs_predicted_pg,
    get_recent_features, r, PREFIX_MAP,
)
from alert import check_alert, get_alert_state

REFRESH_SECONDS = 2

st.set_page_config(page_title="DXY Volatility Dashboard", layout="wide", page_icon="📊")

DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "dxy2024")
if "authed" not in st.session_state:
    st.session_state.authed = False

if not st.session_state.authed:
    st.title("DXY Dashboard")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == DASHBOARD_PASSWORD:
            st.session_state.authed = True
            st.rerun()
        else:
            st.error("Wrong password")
    st.stop()

st.sidebar.title("DXY Dashboard")
threshold = st.sidebar.slider(
    "Volatility Alert Threshold",
    min_value=0.0, max_value=0.01, value=0.001, step=0.0001,
    format="%.4f",
)
r.set("latest:dxy:alert_threshold", threshold)
chart_minutes = st.sidebar.slider(
    "Price Chart (minutes)",
    min_value=10, max_value=2880, value=480, step=5,
)

SHORT_LABELS = {
    "DXY": "DXY",
    "EUR/USD": "EUR",
    "USD/JPY": "JPY",
    "GBP/USD": "GBP",
    "VIX": "VIX",
    "S&P 500": "SPX",
}

@st.cache_resource
def init_prometheus():
    refreshes = Counter("dashboard_refreshes_total", "Total refreshes")
    errors = Counter("dashboard_errors_total", "Total errors")
    last_ok = Gauge("dashboard_last_success_seconds", "Last success timestamp")
    start_http_server(8005)
    return refreshes, errors, last_ok

REFRESHES_TOTAL, ERRORS, LAST_SUCCESS = init_prometheus()

main = st.empty()

while True:
    REFRESHES_TOTAL.inc()
    LAST_SUCCESS.set(time.time())
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    prices = get_latest_prices()
    vol = get_latest_vol()
    features = get_recent_features()
    alert_state = get_alert_state(r)

    predicted_vol = vol.get("predicted") or 0
    inst_vol = vol.get("instant") or 0
    is_alert = check_alert(inst_vol, threshold, r)

    with main.container():
        st.title("DXY Volatility Dashboard")
        st.caption(f"Updated: {now} · Refresh: {REFRESH_SECONDS}s · Sidebar: threshold")

        cols = st.columns(6)
        for i, (label, short) in enumerate(SHORT_LABELS.items()):
            val = prices.get(label)
            delta = None
            if val is not None:
                prev = r.get(f"latest:{list(PREFIX_MAP.keys())[i] if i < 6 else ''}:close_prev")
            cols[i].metric(
                short, f"{val:.4f}" if val else "—", delta=delta,
                border=True,
            )

        vol_col1, vol_col2 = st.columns(2)
        vol_col1.metric("Predicted Volatility (Batch)", f"{predicted_vol:.6f}", border=True)
        vol_col2.metric("Instant Volatility (Stream)", f"{inst_vol:.6f}", border=True)

        alert_col1, alert_col2, alert_col3 = st.columns(3)
        emoji = "🔴" if is_alert else "🟢"
        alert_col1.metric("Alert Status", f"{emoji} {'TRIGGERED' if is_alert else 'NORMAL'}", border=True)
        alert_col2.metric("Last Alert", alert_state.get("last_triggered", "—")[:19] if alert_state.get("last_triggered") else "—", border=True)
        alert_col3.metric("Threshold", f"{threshold:.4f}", border=True)

        if is_alert:
            st.warning(f"Volatility spike: {inst_vol:.6f} exceeds threshold {threshold:.4f}", icon="🚨")

        vol_chart_col1, vol_chart_col2 = st.columns(2)

        with vol_chart_col1:
            st.subheader("Batch Volatility: Predicted vs Actual")
            df_batch = get_predictions_24h(limit=100, source="batch")
            if not df_batch.empty:
                df_batch["timestamp"] = pd.to_datetime(df_batch["timestamp"])
                chart = df_batch.rename(columns={
                    "predicted_close": "Predicted",
                    "actual_close": "Actual",
                }).set_index("timestamp")
                cols_to_show = [c for c in ["Predicted", "Actual"] if c in chart.columns]
                if cols_to_show:
                    st.line_chart(chart[cols_to_show])
                st.caption(f"Span: {df_batch['timestamp'].min().strftime('%m/%d %H:%M')} → {df_batch['timestamp'].max().strftime('%m/%d %H:%M')}")
            else:
                st.info("No batch predictions yet")

        with vol_chart_col2:
            st.subheader("Stream Instant Volatility")
            df_stream = get_predictions_24h(limit=100, source="stream")
            if not df_stream.empty:
                df_stream["timestamp"] = pd.to_datetime(df_stream["timestamp"])
                chart = df_stream.rename(columns={
                    "actual_close": "Instant Vol",
                }).set_index("timestamp")
                st.line_chart(chart[["Instant Vol"]])
                st.caption(f"Span: {df_stream['timestamp'].min().strftime('%m/%d %H:%M')} → {df_stream['timestamp'].max().strftime('%m/%d %H:%M')}")
            else:
                st.info("No stream predictions yet")

        st.subheader("DXY Price — Actual vs Predicted")
        df_ap = get_actual_vs_predicted_pg(hours=chart_minutes / 60)
        preds = get_latest_price_preds()
        if not df_ap.empty:
            chart_data = df_ap
            if preds:
                last_ts = chart_data[chart_data["type"] == "Actual"]["timestamp"].iloc[-1]
                pred_rows = []
                for offset, label, key in [(1, "Pred 1m", "pred_1m"), (3, "Pred 3m", "pred_3m"), (5, "Pred 5m", "pred_5m"), (30, "Pred 30m", "pred_30m")]:
                    val = preds.get(key)
                    if val:
                        pred_rows.append({"timestamp": last_ts + pd.Timedelta(minutes=offset), "price": val, "type": label})
                if pred_rows:
                    chart_data = pd.concat([chart_data, pd.DataFrame(pred_rows)], ignore_index=True)
            zoom = alt.selection_interval(bind="scales")
            chart = alt.Chart(chart_data).mark_line(point=True).add_params(zoom).encode(
                x=alt.X("timestamp:T", title="Time", axis=alt.Axis(format="%H:%M", grid=False)),
                y=alt.Y("price:Q", title="DXY Price", scale=alt.Scale(zero=False)),
                color=alt.Color("type:N", scale=alt.Scale(
                    domain=["Actual", "Predicted", "Pred 1m", "Pred 3m", "Pred 5m", "Pred 30m"],
                    range=["#00cc66", "#ff0000", "#ffcc00", "#ff8800", "#ff3300", "#9933ff"],
                )),
                strokeDash=alt.StrokeDash("type:N", scale=alt.Scale(
                    domain=["Actual", "Predicted", "Pred 1m", "Pred 3m", "Pred 5m", "Pred 30m"],
                    range=[[0], [6, 3], [6, 3], [6, 3], [6, 3], [4, 4]],
                )),
                opacity=alt.condition(
                    alt.datum.type == "Predicted",
                    alt.value(0.4),
                    alt.value(1.0)
                ),
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No DXY price data yet — waiting for collector")

        price_cols = st.columns(5)
        actual_price = prices.get("DXY")
        price_cols[0].metric("Actual DXY", f"{actual_price:.4f}" if actual_price else "—", border=True)
        if preds:
            for i, (label, key) in enumerate([("Pred 1m", "pred_1m"), ("Pred 3m", "pred_3m"), ("Pred 5m", "pred_5m"), ("Pred 30m", "pred_30m")], 1):
                val = preds.get(key)
                delta = (val - actual_price) if (val is not None and actual_price is not None) else None
                price_cols[i].metric(label, f"{val:.4f}" if val else "—", delta=f"{delta:+.4f}" if delta is not None else None, border=True)
        else:
            for i, label in enumerate(["Pred 1m", "Pred 3m", "Pred 5m", "Pred 30m"], 1):
                price_cols[i].metric(label, "—", border=True)

        with st.expander("Latest Market Snapshot"):
            st.json({k: round(v, 6) if isinstance(v, float) else v for k, v in features.items()})

    time.sleep(REFRESH_SECONDS)
