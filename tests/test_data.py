from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from buildingpilot.batch import run_batch
from buildingpilot.cli import generate
from buildingpilot.features import feature_frame, telemetry_frame
from buildingpilot.ingestion import IngestionService
from buildingpilot.lakehouse import Lakehouse
from buildingpilot.storage import Repository, canonical


def test_causal_features_and_building_isolation() -> None:
    events = list(generate(5, datetime(2026, 1, 5, tzinfo=UTC), 300, 42))
    frame = telemetry_frame([e.to_dict() for e in events])
    result = feature_frame(frame)
    first = result.filter(pl.col("building_id") == "office").row(0, named=True)
    assert first["lag_load_kw"] == events[0].energy_kwh * 12
    assert first["lag2_load_kw"] is None


def test_real_iceberg_roundtrip_and_replay(tmp_path: Path) -> None:
    repository = Repository(tmp_path / "operational.db")
    repository.initialize()
    ingestion = IngestionService(repository)
    for index, event in enumerate(generate(6, datetime(2026, 1, 5, tzinfo=UTC), 300, 42)):
        ingestion.ingest(canonical(event).encode(), str(index))
    lake = Lakehouse.local(tmp_path / "lake")
    first = run_batch(repository, lake, tmp_path / "reports")
    snapshot = first["snapshots"]["cleaned_telemetry"]
    run_batch(repository, lake, tmp_path / "reports")
    assert lake.read("cleaned_telemetry").num_rows == 18
    assert lake.read("cleaned_telemetry", snapshot).num_rows == 18
    assert lake.read("hourly_measurements").num_rows == 3
    assert lake.read("training_datasets").num_rows == 12
    reopened = Lakehouse.local(tmp_path / "lake")
    assert reopened.read("raw_events").num_rows == 18
