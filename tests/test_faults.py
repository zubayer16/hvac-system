import csv
from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from buildingpilot.artifacts import load_current, promote
from buildingpilot.faults import time_split, train_faults
from buildingpilot.features import FAULT_FEATURES
from buildingpilot.importer import import_csv
from buildingpilot.research_data import synthetic_experiment
from buildingpilot.storage import Repository


def test_fault_training_split_and_roundtrip(tmp_path: Path) -> None:
    frame = synthetic_experiment(days=4)
    train, validation, test = time_split(frame)
    assert cast(datetime, train["timestamp"].max()) < cast(datetime, validation["timestamp"].min())
    assert cast(datetime, validation["timestamp"].max()) < cast(datetime, test["timestamp"].min())
    metadata = train_faults(frame, tmp_path)
    assert set(metadata["validation"]) == {
        "dummy",
        "extra_trees",
        "random_forest",
        "gradient_boosting",
    }
    assert "fault_label" not in FAULT_FEATURES and "faulty" not in FAULT_FEATURES
    promote(tmp_path, "fault", metadata["version"])
    model, restored = load_current(tmp_path, "fault")
    assert restored["dataset_hash"] == metadata["dataset_hash"]
    assert len(model.predict(test.select(FAULT_FEATURES).to_numpy())) == test.height
    with pytest.raises(ValueError):
        train_faults(frame.with_columns(pl.lit(0).alias("faulty")), tmp_path)


def test_csv_units_and_provenance(tmp_path: Path) -> None:
    event = synthetic_experiment(days=3).row(0, named=True)
    # Only domain fields are accepted; map Fahrenheit and label explicitly.
    from dataclasses import fields

    from buildingpilot.domain import Telemetry

    defaults = {field.name: event[field.name] for field in fields(Telemetry) if field.name in event}
    defaults.pop("timestamp")
    defaults.pop("event_id")
    defaults["indoor_temperature_c"] = 21
    path = tmp_path / "research.csv"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["indoor_F", "label", "time"])
        writer.writeheader()
        writer.writerow({"indoor_F": 68, "label": "healthy", "time": "2026-01-05T00:00:00Z"})
        writer.writerow({"indoor_F": 70, "label": "bad", "time": "2026-01-05T00:05:00Z"})
    config = {
        "source": "experimental",
        "columns": {
            "indoor_temperature_c": "indoor_F",
            "fault_label": "label",
            "timestamp": "time",
        },
        "units": {"indoor_temperature_c": "F"},
        "fault_labels": {"healthy": "normal", "bad": "faulty"},
        "defaults": defaults,
    }
    repository = Repository(tmp_path / "import.db")
    repository.initialize()
    assert import_csv(path, config, repository)["accepted"] == 2
    assert import_csv(path, config, repository)["accepted"] == 2  # same receipt is a safe replay
    row = repository.history(defaults["building_id"])[0]["telemetry"]
    assert isinstance(row, dict)
    assert row["indoor_temperature_c"] == 20
    assert row["source"] == "experimental"
    second = repository.history(defaults["building_id"])[1]["telemetry"]
    assert isinstance(second, dict)
    assert second["fault_label"] == "faulty" and second["schema_version"] == "1.1"
