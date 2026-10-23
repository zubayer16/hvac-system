"""Airflow orchestrates the same tested CLI tasks; ML dependencies live in a separate venv."""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["buildingpilot", "simulation"],
)
def buildingpilot_lifecycle():
    @task
    def execute(name: str, run_key: str):
        executable = os.getenv("BP_PIPELINE_PYTHON", "/opt/buildingpilot/.venv/bin/python")
        subprocess.run(
            [executable, "-m", "buildingpilot.pipeline", name, "--run-id", run_key], check=True
        )

    previous = None
    for name in (
        "ingest",
        "quality",
        "features",
        "train_fault",
        "train_forecast",
        "evaluate",
        "promote",
        "infer",
    ):
        operation = execute.override(task_id=name)(name, "{{ ts_nodash }}")
        if previous is not None:
            previous >> operation
        previous = operation


buildingpilot_lifecycle()
