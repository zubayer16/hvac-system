"""Streamlit monitoring consumes the public API; no fabricated model outputs."""

import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st


def request(path: str, body: dict[str, Any] | None = None) -> Any:
    base = os.getenv("BP_API_URL", "http://127.0.0.1:8000")
    req = Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def main() -> None:
    st.set_page_config(page_title="BuildingPilot AI", page_icon="🌡️", layout="wide")
    st.title("BuildingPilot AI")
    st.caption("Synthetic building monitoring · recommendations are simulation only")
    try:
        buildings = request("/buildings")
    except (URLError, TimeoutError):
        st.error("Backend unavailable. Start the API and refresh.")
        return
    selected = st.sidebar.selectbox("Building", [b["building_id"] for b in buildings])
    st.sidebar.button("Refresh observations")
    interval = st.sidebar.select_slider("Recent readings", options=[100, 300, 1000], value=300)
    try:
        latest = request(f"/buildings/{selected}/telemetry/latest")
        # Read newest event-time window, not the first page of all historical observations.
        from datetime import datetime, timedelta

        timestamp = datetime.fromisoformat(latest["telemetry"]["timestamp"].replace("Z", "+00:00"))
        start = timestamp - timedelta(seconds=latest["telemetry"]["interval_seconds"] * interval)
        from urllib.parse import urlencode

        rows = request(
            f"/buildings/{selected}/telemetry?"
            + urlencode({"start": start.isoformat(), "limit": 1000})
        )
    except HTTPError as exc:
        st.info(f"Telemetry is not yet available (HTTP {exc.code}). Start ingestion.")
        return
    data = pd.DataFrame([row["telemetry"] for row in rows])
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    event = latest["telemetry"]
    columns = st.columns(4)
    columns[0].metric("Indoor temperature", f"{event['indoor_temperature_c']} °C")
    columns[1].metric("Occupancy", event["occupancy"])
    columns[2].metric("CO₂", f"{event['co2_ppm']:.0f} ppm")
    columns[3].metric("Energy in displayed window", f"{data['energy_kwh'].sum():.2f} kWh")
    st.line_chart(
        data.set_index("timestamp")[["indoor_temperature_c", "outdoor_temperature_c", "setpoint_c"]]
    )
    st.subheader("Energy consumption")
    st.line_chart(data.set_index("timestamp")[["energy_kwh"]])
    a, b = st.columns(2)
    with a:
        st.subheader("Fault detection")
        try:
            fault = request(f"/buildings/{selected}/faults/latest")
            st.warning("Fault alert") if fault["faulty"] else st.success("No model fault alert")
            st.metric("Estimated fault probability", f"{fault['fault_probability']:.1%}")
            st.bar_chart(
                pd.Series(fault["important_features"]).sort_values(ascending=False).head(8)
            )
            st.write("Observed conditions", fault["observed_conditions"])
            st.caption(fault["explanation"])
        except HTTPError as exc:
            st.info(f"Fault model unavailable (HTTP {exc.code}).")
    with b:
        st.subheader("Energy forecast")
        try:
            prediction = request(f"/buildings/{selected}/forecasts", {"horizon": 8})
            forecasts = pd.DataFrame(prediction["predictions"])
            forecasts["timestamp"] = pd.to_datetime(forecasts["timestamp"], utc=True)
            st.line_chart(forecasts.set_index("timestamp")[["load_kw"]])
            st.write("Held-out evaluation (kW)", prediction["held_out_metrics"])
            st.caption(
                "Forecasts are recursive; future weather/occupancy are held constant in this view."
            )
        except HTTPError as exc:
            st.info(f"Forecast unavailable (HTTP {exc.code}).")
    st.subheader("Simulated recommendations")
    if st.button("Evaluate proposed setpoints"):
        try:
            control = request(f"/buildings/{selected}/optimization", {"horizon": 4})
            st.write("Proposed setpoints (°C)", control["setpoints_c"])
            st.metric(
                "Estimated energy difference vs baseline",
                f"{control['estimated_savings_pct']:.2f}%",
            )
            st.write(
                "Feasible", control["feasible"], "Comfort violations", control["comfort_violations"]
            )
            st.write("Active constraints", control["active_constraints"])
            st.caption("Engineering assumptions: " + "; ".join(control["assumptions"]))
        except HTTPError as exc:
            st.info(f"Recommendation unavailable (HTTP {exc.code}).")
    with st.expander("Data quality and provenance"):
        st.write("Observation source", event["source"])
        st.write("Missing indoor readings", int(data["indoor_temperature_c"].isna().sum()))
        st.write("Late readings in window", sum(row["is_late"] for row in rows))
        st.json(request("/ingestion/status"))


if __name__ == "__main__":
    main()
