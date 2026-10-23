import json
from pathlib import Path
from typing import Any

import pytest

from buildingpilot.artifacts import promote, save_candidate
from buildingpilot.pipeline import promotion_decision, run_task


def test_promotion_rejects_bad_validation() -> None:
    fault: dict[str, Any] = {
        "winner": "trees",
        "validation": {"trees": {"f1": 0.5, "samples": 100}, "dummy": {"f1": 0.1}},
    }
    forecast: dict[str, Any] = {
        "winner": "flax",
        "validation_mae": {"flax": 10.0, "persistence": 2.0},
    }
    assert promotion_decision(fault, forecast)["fault"] is False
    assert promotion_decision(fault, forecast)["forecast"] is False
    fault["validation"]["trees"]["f1"] = 0.9
    forecast["validation_mae"]["flax"] = 1.0
    assert promotion_decision(fault, forecast)["fault"] is True


def test_repeated_promotion_preserves_rollback(tmp_path: Path) -> None:
    first = save_candidate(tmp_path, "fault", {"model": 1}, {"experiment": 1})
    second = save_candidate(tmp_path, "fault", {"model": 2}, {"experiment": 2})
    promote(tmp_path, "fault", first)
    promote(tmp_path, "fault", second)
    promote(tmp_path, "fault", second)
    previous = json.loads((tmp_path / "fault/previous.json").read_text())
    assert previous["version"] == first


def test_task_marker_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BP_DATABASE", str(tmp_path / "operations.db"))
    monkeypatch.setenv("BP_PIPELINE_ROOT", str(tmp_path / "runs"))
    first = run_task("ingest", "repeatable")
    assert run_task("ingest", "repeatable") == first
    with pytest.raises(ValueError):
        run_task("ingest", "../escape")
