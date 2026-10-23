"""Explicit synthetic experiment and complete lifecycle; never implicitly imports fake history."""

import argparse
import json
import logging
import os
from dataclasses import fields

from buildingpilot.contracts import TELEMETRY
from buildingpilot.domain import Telemetry
from buildingpilot.ingestion import IngestionService
from buildingpilot.pipeline import TASKS, run_task
from buildingpilot.research_data import synthetic_experiment
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository, canonical


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=20)
    parser.add_argument("--epochs", type=int, default=80)
    args = parser.parse_args()
    if "BP_DATABASE" not in os.environ:
        os.environ["BP_DATABASE"] = "data/demo/operations.db"
    os.environ.setdefault("BP_MODEL_ROOT", "data/demo/models")
    os.environ.setdefault("BP_PIPELINE_ROOT", "data/demo/runs")
    os.environ.setdefault("BP_LAKE_ROOT", "data/demo/lake")
    os.environ["BP_TRAIN_EPOCHS"] = str(args.epochs)
    logging.getLogger("buildingpilot.ingestion").setLevel(logging.WARNING)
    repository = Repository(Settings.from_env().database)
    repository.initialize()
    service = IngestionService(repository)
    frame = synthetic_experiment(args.days)
    for row in frame.to_dicts():
        row["timestamp"] = row["timestamp"].isoformat()
        event = TELEMETRY.validate_python(
            {field.name: row[field.name] for field in fields(Telemetry)}
        )
        service.ingest(canonical(event).encode(), f"demo:{event.event_id}")
    run_id = f"synthetic-{args.days}d-{args.epochs}epochs"
    for task in TASKS:
        result = run_task(task, run_id)
        summary = {
            key: result[key]
            for key in ("winner", "version", "retrained", "fault", "forecast")
            if key in result
        }
        print(json.dumps({"task": task, "summary": summary}), flush=True)
    print(
        json.dumps(
            {
                "simulation_only": True,
                "database": str(repository.path),
                "models": os.environ["BP_MODEL_ROOT"],
                "run_id": run_id,
            }
        )
    )


if __name__ == "__main__":
    main()
