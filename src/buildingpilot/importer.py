"""Configurable CSV import, without assuming any research dataset is installed."""

import argparse
import csv
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from buildingpilot.contracts import TELEMETRY
from buildingpilot.ingestion import IngestionService
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository, canonical


def import_csv(path: Path, config: dict[str, Any], repository: Repository) -> dict[str, int]:
    source = config["source"]
    if source not in ("experimental", "real"):
        raise ValueError("External imports must explicitly declare experimental or real provenance")
    columns = config["columns"]
    if source == "real" and "timestamp" not in columns:
        raise ValueError("Real observations require observed timestamps")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:12]
    service = IngestionService(repository)
    counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
    with path.open(newline="", encoding="utf-8-sig") as stream:
        for index, row in enumerate(csv.DictReader(stream)):
            record_id = f"csv:{digest}:{config_hash}:{index}"
            try:
                item = dict(config.get("defaults", {}))
                for target, column in columns.items():
                    value: Any = row[column]
                    unit = config.get("units", {}).get(target)
                    if unit in ("F", "W", "Wh", "cfm", "fraction"):
                        value = float(value)
                        value = (
                            (value - 32) * 5 / 9
                            if unit == "F"
                            else value / 1000
                            if unit in ("W", "Wh")
                            else value * 0.00047194745
                            if unit == "cfm"
                            else value * 100
                        )
                    elif unit not in (None, "C", "kW", "kWh", "m3/s", "%", "ppm"):
                        raise ValueError(f"Unsupported unit {unit}")
                    item[target] = (
                        None if value == "" and target == "indoor_temperature_c" else value
                    )
                if "timestamp" not in columns:
                    start = datetime.fromisoformat(config["start"].replace("Z", "+00:00"))
                    item["timestamp"] = (
                        start + timedelta(seconds=index * int(item["interval_seconds"]))
                    ).isoformat()
                if "fault_label" in item:
                    item["fault_label"] = config.get("fault_labels", {}).get(
                        str(item["fault_label"]), item["fault_label"]
                    )
                item.update(event_id=f"csv-{digest[:16]}-{config_hash}-{index}", source=source)
                item["schema_version"] = config.get("schema_version", "1.1")
                event = TELEMETRY.validate_python(item)
                outcome = service.ingest(canonical(event).encode(), record_id)
            except (ValueError, KeyError, TypeError) as exc:
                outcome = repository.persist(
                    json.dumps(row).encode(), record_id, None, str(exc)[:500], 900
                )
            counts[outcome] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path)
    parser.add_argument("mapping", type=Path)
    args = parser.parse_args()
    repository = Repository(Settings.from_env().database)
    repository.initialize()
    print(json.dumps(import_csv(args.csv, json.loads(args.mapping.read_text()), repository)))


if __name__ == "__main__":
    main()
