import streamlit as st
import pandas as pd
import altair as alt
import time
from datetime import datetime, timezone

from db import (
    get_latest_prices, get_latest_vol, get_predictions_24h,
    get_latest_price_preds, get_actual_vs_predicted_pg,
    get_recent_features, r, PREFIX_MAP,
)
from alert import check_alert, get_alert_state

REFRESH_SECONDS = 2

st.set_page_config(page_title="DXY Volatility Dashboard", layout="wide", page_icon="📊")

st.sidebar.title("DXY Dashboard")
threshold = st.sidebar.slider(
    "Volatility Alert Threshold",
    min_value=0.0, max_value=0.01, value=0.001, step=0.0001,
    format="%.4f",
)
chart_hours = st.sidebar.slider(
    "Price Chart (PostgreSQL hours)",
    min_value=1, max_value=48, value=8, step=1,
)

SHORT_LABELS = {
    "DXY": "DXY",
    "EUR/USD": "EUR",
    "USD/JPY": "JPY",
    "GBP/USD": "GBP",
    "VIX": "VIX",
    "S&P 500": "SPX",
}

main = st.empty()

while True:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    prices = get_latest_prices()
    vol = get_latest_vol()
    df_pred = get_predictions_24h()
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
        vol_col1.metric("Predicted Volatility", f"{predicted_vol:.6f}", border=True)
        vol_col2.metric(
            "Instant Volatility", f"{inst_vol:.6f}",
            delta=f"{(inst_vol - predicted_vol):.6f}" if predicted_vol else None,
            border=True,
        )

        alert_col1, alert_col2, alert_col3 = st.columns(3)
        emoji = "🔴" if is_alert else "🟢"
        alert_col1.metric("Alert Status", f"{emoji} {'TRIGGERED' if is_alert else 'NORMAL'}", border=True)
        alert_col2.metric("Last Alert", alert_state.get("last_triggered", "—")[:19] if alert_state.get("last_triggered") else "—", border=True)
        alert_col3.metric("Threshold", f"{threshold:.4f}", border=True)

        if is_alert:
            st.warning(f"Volatility spike: {inst_vol:.6f} exceeds threshold {threshold:.4f}", icon="🚨")

        st.subheader("Volatility (100 latest predictions)")
        if not df_pred.empty:
            df_pred["timestamp"] = pd.to_datetime(df_pred["timestamp"])
            chart = df_pred.rename(columns={
                "predicted_close": "Predicted",
                "actual_close": "Actual",
            }).set_index("timestamp")
            cols_to_show = [c for c in ["Predicted", "Actual"] if c in chart.columns]
            if cols_to_show:
                st.line_chart(chart[cols_to_show])
            st.caption(f"Span: {df_pred['timestamp'].min().strftime('%m/%d %H:%M')} → {df_pred['timestamp'].max().strftime('%m/%d %H:%M')}")
        else:
            st.info("No prediction data — run `docker compose exec training-flow python /app/ml_pipeline/training.py`")

        st.subheader("DXY Price — Actual vs Predicted")
        df_ap = get_actual_vs_predicted_pg(hours=chart_hours)
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
            chart = alt.Chart(chart_data).mark_line(point=True).encode(
                x=alt.X("timestamp:T", title="Time", axis=alt.Axis(format="%H:%M", grid=False)),
                y=alt.Y("price:Q", title="DXY Price", scale=alt.Scale(zero=False)),
                color=alt.Color("type:N", scale=alt.Scale(
                    domain=["Actual", "Predicted", "Pred 1m", "Pred 3m", "Pred 5m", "Pred 30m"],
                    range=["#00cc66", "#ff6600", "#ffcc00", "#ff8800", "#ff3300", "#9933ff"],
                )),
                strokeDash=alt.StrokeDash("type:N", scale=alt.Scale(
                    domain=["Actual", "Predicted", "Pred 1m", "Pred 3m", "Pred 5m", "Pred 30m"],
                    range=[[0], [0], [6, 3], [6, 3], [6, 3], [4, 4]],
                )),
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
