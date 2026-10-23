"""Idempotent journal export and feature materialization; late arrivals are recomputed."""

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from buildingpilot.features import arrow_table, feature_frame, hourly_frame, telemetry_frame
from buildingpilot.lakehouse import Lakehouse
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository


def run_batch(repository: Repository, lake: Lakehouse, output: Path) -> dict[str, Any]:
    lake.ensure_results()
    with repository.connection() as db:
        # One read transaction gives raw/cleaned a consistent SQLite view.
        db.execute("BEGIN")
        raw = [dict(row) for row in db.execute("SELECT * FROM receipts ORDER BY record_id")]
        clean = [json.loads(row[0]) for row in db.execute("SELECT payload FROM telemetry")]
    if not clean:
        raise ValueError("No validated telemetry; ingest or import data first")
    frame = telemetry_frame(clean)
    features = feature_frame(frame)
    raw_schema = pa.schema(
        [
            (name, pa.int64() if name in ("partition_id", "offset_id") else pa.string())
            for name in (
                "record_id",
                "received_at",
                "raw_base64",
                "outcome",
                "reason",
                "event_id",
                "topic",
                "partition_id",
                "offset_id",
            )
        ]
    )
    lake.write("raw_events", pa.Table.from_pylist(raw, schema=raw_schema), ["record_id"])
    lake.write("cleaned_telemetry", arrow_table(frame), ["event_id"])
    lake.write("hourly_measurements", arrow_table(hourly_frame(frame)))
    if features.height:
        lake.write("features", arrow_table(features))
        training = features.drop_nulls(["lag_load_kw", "lag2_load_kw"])
        if training.height:
            lake.write("training_datasets", arrow_table(training))
    output.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow_table(features), output / "features.parquet")
    # pandas is used for a human-readable statistical report, not the streaming hot path.
    report = (
        pd.DataFrame(frame.select("building_id", "energy_kwh").to_dicts())
        .groupby("building_id")
        .agg(["count", "sum", "mean"])
        .to_json()
    )
    (output / "energy-report.json").write_text(report, encoding="utf-8")
    return {
        "observations": frame.height,
        "features": features.height,
        "snapshots": lake.snapshots(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/batch"))
    args = parser.parse_args()
    repository = Repository(Settings.from_env().database)
    repository.initialize()
    print(json.dumps(run_batch(repository, Lakehouse.from_env(), args.output)))


if __name__ == "__main__":
    main()
