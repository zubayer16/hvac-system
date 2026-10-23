"""Repeatable task entry points, shared by Airflow and local execution."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

import polars as pl

from buildingpilot.artifacts import artifact_root, code_fingerprint, load_current, promote
from buildingpilot.batch import run_batch
from buildingpilot.faults import train_faults
from buildingpilot.forecasting import train_forecasts
from buildingpilot.importer import import_csv
from buildingpilot.lakehouse import Lakehouse
from buildingpilot.serving import forecast, predict_fault
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository

TASKS = (
    "ingest",
    "quality",
    "features",
    "train_fault",
    "train_forecast",
    "evaluate",
    "promote",
    "infer",
)


def promotion_decision(fault: dict[str, Any], energy: dict[str, Any]) -> dict[str, Any]:
    fault_score = fault["validation"][fault["winner"]]
    dummy = fault["validation"]["dummy"]["f1"]
    forecast_scores = energy["validation_mae"]
    fault_ok = fault_score["f1"] >= max(0.6, dummy) and fault_score["samples"] >= 30
    energy_ok = forecast_scores[energy["winner"]] <= forecast_scores["persistence"] * 1.01
    # No test-set metric is used for selecting/promoting models.
    return {
        "fault": bool(fault_ok),
        "forecast": bool(energy_ok),
        "policy": "Validation F1>=0.6 and dummy; forecast MAE<=1.01*persistence; isolated source",
    }


def _run_task(task: str, run_id: str) -> dict[str, Any]:
    if task not in TASKS or re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", run_id) is None:
        raise ValueError("Invalid task/run identifier")
    output = Path(os.getenv("BP_PIPELINE_ROOT", "data/runs")) / run_id
    output.mkdir(parents=True, exist_ok=True)
    marker = output / f"{task}.json"
    if marker.exists():
        return json.loads(marker.read_text(encoding="utf-8"))  # type: ignore[no-any-return]
    repository = Repository(Settings.from_env().database)
    repository.initialize()
    root = artifact_root()
    result: dict[str, Any] = {}
    if task == "ingest":
        path = os.getenv("BP_IMPORT_CSV")
        result = (
            import_csv(
                Path(path),
                json.loads(Path(os.environ["BP_IMPORT_MAPPING"]).read_text()),
                repository,
            )
            if path
            else {"source": "existing streaming journal", "import_requested": False}
        )
    elif task == "quality":
        status = repository.status()
        if int(status["events"]) < 100:  # type: ignore[call-overload]
            raise ValueError("At least 100 valid observations required for lifecycle evaluation")
        with repository.connection() as db:
            sources = {
                json.loads(row[0])["source"] for row in db.execute("SELECT payload FROM telemetry")
            }
        result = {"events": status["events"], "sources": sorted(sources), "valid": True}
    elif task == "features":
        result = run_batch(repository, Lakehouse.from_env(), output)
    elif task in ("train_fault", "train_forecast"):
        family = "fault" if task == "train_fault" else "forecast"
        manifest = json.loads((output / "features.json").read_text())
        snapshot = manifest["snapshots"]["training_datasets"]
        lake = Lakehouse.from_env()
        frame = pl.from_arrow(lake.read("training_datasets", snapshot))
        assert isinstance(frame, pl.DataFrame)
        source = os.getenv("BP_TRAIN_SOURCE", "synthetic")
        frame = frame.filter(pl.col("source") == source)
        digest = hashlib.sha256(
            frame.sort(["timestamp", "event_id"]).write_json().encode()
        ).hexdigest()
        try:
            _, current = load_current(root, family)
        except FileNotFoundError:
            current = {}
        same_config = family == "fault" or current.get("epochs") == int(
            os.getenv("BP_TRAIN_EPOCHS", "80")
        )
        if (
            current.get("dataset_hash") == digest
            and current.get("code_hash") == code_fingerprint()
            and same_config
            and os.getenv("BP_FORCE_RETRAIN") != "1"
        ):
            result = current | {"retrained": False}
        else:
            result = (
                train_faults(frame, root, snapshot)
                if family == "fault"
                else train_forecasts(frame, root, int(os.getenv("BP_TRAIN_EPOCHS", "80")), snapshot)
            )
            result["retrained"] = True
    elif task == "evaluate":
        result = promotion_decision(
            json.loads((output / "train_fault.json").read_text()),
            json.loads((output / "train_forecast.json").read_text()),
        )
        from buildingpilot.metrics import persist_metric

        energy = json.loads((output / "train_forecast.json").read_text())
        persist_metric(
            repository, "forecast_validation_mae", energy["validation_mae"][energy["winner"]]
        )
    elif task == "promote":
        decision = json.loads((output / "evaluate.json").read_text())
        for family in ("fault", "forecast"):
            candidate = json.loads((output / f"train_{family}.json").read_text())
            if decision[family]:
                promote(root, family, candidate["version"])
            result[family] = {"promoted": decision[family], "version": candidate["version"]}
    elif task == "infer":
        outcomes: dict[str, Any] = {}
        for building in repository.buildings():
            try:
                outcomes[building.building_id] = {
                    "fault": predict_fault(repository, root, building.building_id),
                    "forecast": forecast(repository, root, building.building_id),
                }
            except (FileNotFoundError, ValueError) as exc:
                outcomes[building.building_id] = {"unavailable": str(exc)}
        result = {"buildings": outcomes}
        export_results(repository, Lakehouse.from_env())
        from buildingpilot.metrics import match_forecast_errors

        result["matched_forecast_points"] = match_forecast_errors(repository)
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, allow_nan=False), encoding="utf-8")
    temporary.replace(marker)
    return result


def run_task(task: str, run_id: str) -> dict[str, Any]:
    try:
        return _run_task(task, run_id)
    except Exception:
        from buildingpilot.metrics import persist_metric

        repository = Repository(Settings.from_env().database)
        repository.initialize()
        persist_metric(repository, "pipeline_failures", 1, increment=True)
        raise


def export_results(repository: Repository, lake: Lakehouse) -> None:
    import pyarrow as pa

    with repository.connection() as db:
        if db.execute("SELECT name FROM sqlite_master WHERE name='results'").fetchone() is None:
            return
        rows = [dict(row) for row in db.execute("SELECT * FROM results")]
    for name, families in (
        ("prediction_results", ("fault", "forecast")),
        ("optimization_recommendations", ("optimization",)),
    ):
        selected = [
            {
                "result_id": row["result_id"],
                "building_id": row["building_id"],
                "payload": row["payload"],
            }
            for row in rows
            if row["family"] in families
        ]
        if selected:
            lake.write(name, pa.Table.from_pylist(selected), ["result_id"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("task", choices=[*TASKS, "all"])
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    for task in TASKS if args.task == "all" else (args.task,):
        print(json.dumps({"task": task, "result": run_task(task, args.run_id)}))


if __name__ == "__main__":
    main()
