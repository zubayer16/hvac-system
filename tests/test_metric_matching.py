from datetime import UTC, datetime
from pathlib import Path

from buildingpilot.cli import generate
from buildingpilot.ingestion import IngestionService
from buildingpilot.metrics import match_forecast_errors
from buildingpilot.serving import persist_result
from buildingpilot.storage import Repository, canonical


def test_delayed_forecast_observation_match(tmp_path: Path) -> None:
    repository = Repository(tmp_path / "ops.db")
    repository.initialize()
    event = next(generate(1, datetime(2026, 1, 5, tzinfo=UTC), 300, 42))
    prediction = {
        "building_id": event.building_id,
        "source": "synthetic",
        "predictions": [
            {"timestamp": event.timestamp.isoformat(), "load_kw": event.energy_kwh * 12 + 2}
        ],
    }
    persist_result(repository, "forecast", event.building_id, prediction)
    assert match_forecast_errors(repository) == 0
    IngestionService(repository).ingest(canonical(event).encode(), "received-later")
    assert match_forecast_errors(repository) == 1
    with repository.connection() as db:
        value = db.execute(
            "SELECT value FROM operational_metrics WHERE name='forecast_observed_mae'"
        ).fetchone()[0]
    assert abs(value - 2) < 1e-6
