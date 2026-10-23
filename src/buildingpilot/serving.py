"""Use promoted artifacts with current operational telemetry; persist results locally."""

import hashlib
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import numpy as np

from buildingpilot.artifacts import load_current
from buildingpilot.features import FAULT_FEATURES
from buildingpilot.metrics import (
    COMFORT,
    DRIFT,
    FAULT_ALERTS,
    FAULT_PREDICTIONS,
    FEASIBILITY,
    INFERENCE,
)
from buildingpilot.optimization import Scenario, optimize
from buildingpilot.storage import Repository


def recent(repository: Repository, building_id: str, count: int = 3) -> list[dict[str, Any]]:
    repository.building(building_id)
    rows = repository.history(building_id, latest=True, limit=count)
    if not rows:
        raise ValueError("No telemetry for building")
    return [cast(dict[str, Any], row["telemetry"]) for row in reversed(rows)]


def persist_result(
    repository: Repository, family: str, building: str, result: dict[str, Any]
) -> None:
    payload = json.dumps(result, sort_keys=True, allow_nan=False)
    key = hashlib.sha256(payload.encode()).hexdigest()
    with repository.connection() as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS results (result_id TEXT PRIMARY KEY, "
            "family TEXT NOT NULL, building_id TEXT NOT NULL, payload TEXT NOT NULL)"
        )
        db.execute(
            "INSERT OR IGNORE INTO results VALUES (?, ?, ?, ?)", (key, family, building, payload)
        )


def predict_fault(repository: Repository, root: Path, building_id: str) -> dict[str, Any]:
    event = recent(repository, building_id)[-1]
    model, metadata = load_current(root, "fault")
    if event["source"] != metadata["source"]:
        raise ValueError("Model provenance does not match observations")
    values = np.asarray(
        [[event[name] if event[name] is not None else np.nan for name in FAULT_FEATURES]],
        dtype=float,
    )
    with INFERENCE.labels("fault").time():
        label = int(model.predict(values)[0])
    FAULT_PREDICTIONS.inc()
    if label:
        FAULT_ALERTS.inc()
    probabilities = model.predict_proba(values)[0]
    probability = (
        float(probabilities[list(model.classes_).index(1)]) if 1 in model.classes_ else 0.0
    )
    importance = metadata["feature_importance"]
    important = sorted(importance, key=lambda name: importance[name], reverse=True)[:5]
    result = {
        "building_id": building_id,
        "event_id": event["event_id"],
        "timestamp": event["timestamp"],
        "faulty": bool(label),
        "fault_probability": probability,
        "model_version": metadata["version"],
        "source": event["source"],
        "important_features": importance,
        "observed_conditions": {name: event[name] for name in important},
        "explanation": "Global feature importance and observed values; not a causal diagnosis",
    }
    persist_result(repository, "fault", building_id, result)
    return result


def forecast(
    repository: Repository,
    root: Path,
    building_id: str,
    horizon: int = 4,
    outdoor_c: list[float] | None = None,
    occupancy: list[int] | None = None,
) -> dict[str, Any]:
    rows = recent(repository, building_id)
    if len(rows) < 2:
        raise ValueError("At least two contiguous readings required for forecasting")
    event = rows[-1]
    interval = int(event["interval_seconds"])
    timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    previous_time = datetime.fromisoformat(rows[-2]["timestamp"].replace("Z", "+00:00"))
    if (timestamp - previous_time).total_seconds() != rows[-2]["interval_seconds"]:
        raise ValueError("Latest telemetry contains a gap; refusing unreliable lag features")
    if not 1 <= horizon <= 12:
        raise ValueError("Horizon must be between 1 and 12 intervals")
    weather = (
        outdoor_c if outdoor_c is not None else [float(event["outdoor_temperature_c"])] * horizon
    )
    people = occupancy if occupancy is not None else [int(event["occupancy"])] * horizon
    if len(weather) != horizon or len(people) != horizon:
        raise ValueError("Scenario lengths must match horizon")
    model, metadata = load_current(root, "forecast")
    if event["source"] != metadata["source"]:
        raise ValueError("Model provenance does not match observations")
    lag = float(event["energy_kwh"]) * 3600 / interval
    lag2 = float(rows[-2]["energy_kwh"]) * 3600 / int(rows[-2]["interval_seconds"])
    predictions = []
    for index in range(horizon):
        time = timestamp + timedelta(seconds=(index + 1) * interval)
        x = np.asarray(
            [
                [
                    lag,
                    lag2,
                    event["indoor_temperature_c"]
                    if event["indoor_temperature_c"] is not None
                    else np.nan,
                    weather[index],
                    people[index],
                    math.sin(time.hour * math.tau / 24),
                    math.cos(time.hour * math.tau / 24),
                ]
            ],
            dtype=float,
        )
        with INFERENCE.labels("forecast").time():
            load = max(0, float(model.predict(x)[0]))
        if "reference_mean" in metadata:
            mean = np.asarray(metadata["reference_mean"], dtype=float)
            scale = np.asarray(metadata["reference_std"], dtype=float)
            scale = np.where(np.isfinite(scale) & (scale > 1e-6), scale, 1.0)
            DRIFT.set(float(np.max(np.nan_to_num(np.abs((x[0] - mean) / scale)))))
        predictions.append(
            {"timestamp": time.isoformat(), "load_kw": load, "energy_kwh": load * interval / 3600}
        )
        lag2, lag = lag, load
    result = {
        "building_id": building_id,
        "generated_from": event["timestamp"],
        "interval_seconds": interval,
        "model_version": metadata["version"],
        "model_name": metadata["winner"],
        "source": event["source"],
        "predictions": predictions,
        "held_out_metrics": metadata["test"],
        "assumptions": [
            "Recursive one-interval forecast",
            "Indoor state held at latest observation",
            "Weather and occupancy persisted unless an explicit scenario is supplied",
        ],
    }
    persist_result(repository, "forecast", building_id, result)
    return result


def recommendation(
    repository: Repository,
    root: Path,
    building_id: str,
    horizon: int = 4,
    prices: list[float] | None = None,
) -> dict[str, Any]:
    event = recent(repository, building_id)[-1]
    if event["indoor_temperature_c"] is None:
        raise ValueError("Optimization requires a measured indoor temperature")
    predicted = forecast(repository, root, building_id, horizon)
    building = repository.building(building_id)
    scenario = Scenario(
        float(event["indoor_temperature_c"]),
        (float(event["outdoor_temperature_c"]),) * horizon,
        (int(event["occupancy"]),) * horizon,
        tuple(prices) if prices is not None else (0.2,) * horizon,
        float(event["setpoint_c"]),
        int(event["interval_seconds"]),
    )
    result = optimize(building, scenario, [row["load_kw"] for row in predicted["predictions"]])
    FEASIBILITY.labels(str(result["feasible"]).lower()).inc()
    COMFORT.inc(result["comfort_violations"])
    result.update(
        building_id=building_id,
        generated_from=event["timestamp"],
        source=event["source"],
        model_version=predicted["model_version"],
    )
    persist_result(repository, "optimization", building_id, result)
    return result
