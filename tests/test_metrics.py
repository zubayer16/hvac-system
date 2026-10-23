from pathlib import Path

from fastapi.testclient import TestClient
from prometheus_client.parser import text_string_to_metric_families

from buildingpilot.api import create_app
from buildingpilot.metrics import persist_metric
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository


def test_metrics_scrape_and_persistence(tmp_path: Path) -> None:
    settings = Settings(database=tmp_path / "api.db", transport="local")
    with TestClient(create_app(settings)) as client:
        client.get("/buildings/unknown")
        repository = Repository(settings.database)
        persist_metric(repository, "pipeline_failures", 1, increment=True)
        persist_metric(repository, "forecast_validation_mae", 1.5)
        response = client.get("/metrics")
        assert response.status_code == 200
        families = list(text_string_to_metric_families(response.text))
        assert any(f.name == "bp_http_requests" for f in families)
        assert "bp_pipeline_failures_total 1.0" in response.text
        assert "bp_forecast_validation_mae_kw 1.5" in response.text
        assert 'route="/buildings/{building_id}"' in response.text
        assert "event_id=" not in response.text
