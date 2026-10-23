"""Write an isolated real REST/S3 fixture for the Spark interoperability smoke test."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from buildingpilot.batch import run_batch
from buildingpilot.cli import generate
from buildingpilot.ingestion import IngestionService
from buildingpilot.lakehouse import Lakehouse
from buildingpilot.storage import Repository, canonical

root = Path(__file__).resolve().parents[1]
for line in (root / ".env").read_text(encoding="utf-8").splitlines():
    if line.startswith("BP_S3_") and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)
os.environ.update(
    BP_LAKE_CATALOG="rest",
    BP_CATALOG_URI="http://127.0.0.1:8181",
    BP_S3_ENDPOINT="http://127.0.0.1:9000",
)
lake = Lakehouse.from_env()
namespace = "verify_" + uuid4().hex[:12]
lake = Lakehouse(lake.catalog, namespace)
repository = Repository(root / "data" / namespace / "fixture.db")
repository.initialize()
ingestion = IngestionService(repository)
for index, event in enumerate(generate(12, datetime(2026, 1, 5, tzinfo=UTC), 300, 42)):
    ingestion.ingest(canonical(event).encode(), str(index))
run_batch(repository, lake, root / "data" / namespace)
run_batch(repository, lake, root / "data" / namespace)
assert lake.read("cleaned_telemetry").num_rows == 36
print(json.dumps({"namespace": namespace, "observations": 36, "snapshots": lake.snapshots()}))
(root / "data/lake-verification-namespace.txt").write_text(namespace, encoding="utf-8")
