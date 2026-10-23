from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi.testclient import TestClient
from sklearn.dummy import DummyClassifier

from buildingpilot.api import create_app
from buildingpilot.artifacts import promote, save_candidate
from buildingpilot.features import FAULT_FEATURES, FORECAST_FEATURES
from buildingpilot.forecasting import PersistenceForecast
from buildingpilot.research_data import synthetic_experiment
from buildingpilot.settings import Settings


def test_model_endpoints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "models"
    monkeypatch.setenv("BP_MODEL_ROOT", str(root))
    fault = DummyClassifier(strategy="most_frequent").fit(
        np.zeros((2, len(FAULT_FEATURES))), [0, 1]
    )
    metadata: dict[str, Any] = {
        "source": "synthetic",
        "feature_importance": {name: 0.0 for name in FAULT_FEATURES},
    }
    version = save_candidate(root, "fault", fault, metadata)
    promote(root, "fault", version)
    forecast = PersistenceForecast()
    version = save_candidate(
        root,
        "forecast",
        forecast,
        {
            "winner": "persistence",
            "source": "synthetic",
            "test": {"mae": 1.0, "rmse": 2.0},
            "features": FORECAST_FEATURES,
        },
    )
    promote(root, "forecast", version)
    from dataclasses import fields

    from buildingpilot.domain import Telemetry

    frame = (
        synthetic_experiment(days=3)
        .filter(__import__("polars").col("building_id") == "office")
        .head(5)
    )
    with TestClient(
        create_app(Settings(database=tmp_path / "api.db", transport="local"))
    ) as client:
        for row in frame.to_dicts():
            row["timestamp"] = row["timestamp"].isoformat()
            payload = {field.name: row[field.name] for field in fields(Telemetry)}
            assert client.post("/telemetry", json=payload).status_code == 202
        assert client.get("/buildings/office/faults/latest").status_code == 200
        response = client.post("/buildings/office/forecasts", json={"horizon": 4})
        assert response.status_code == 200
        assert len(response.json()["predictions"]) == 4
        response = client.post("/buildings/office/optimization", json={"horizon": 4})
        assert response.status_code == 200
        assert response.json()["simulation_only"] is True
        assert client.post("/buildings/office/forecasts", json={"horizon": 13}).status_code == 422
        assert client.get("/models").json()["forecast"]["winner"] == "persistence"


def test_dashboard_unavailable_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("BP_API_URL", "http://127.0.0.1:1")
    path = Path(__file__).resolve().parents[1] / "src/buildingpilot/dashboard.py"
    app = AppTest.from_file(str(path)).run(timeout=20)
    assert not app.exception
    assert len(app.error) == 1
