# Phase 3 — analytical storage and transformations

The SQLite journal remains the durable operational boundary. `batch.py` reads one consistent
transaction, upserts receipts and cleaned telemetry into real Iceberg tables, then recomputes
hourly aggregates and causal features so late records are included. This first implementation
rebuilds derived tables; it favors reviewable correctness over incremental scale. One writer
must own each derived table. Snapshot IDs identify training inputs and allow time travel.

Tables: raw_events, cleaned_telemetry, hourly_measurements, features, training_datasets,
prediction_results and optimization_recommendations. Result tables start empty and are filled
by serving/pipeline work in subsequent phases. Never mix synthetic, experimental and real
provenance in model evaluation; features partition histories by provenance as well as equipment.

PyArrow provides concrete schemas and Parquet exchange; Polars computes ordered per-stream
lags and hourly transforms; pandas produces a statistical energy report. Spark reads the
same REST tables, overwrites hourly aggregates, compacts data files and expires eligible
snapshots conservatively. No control commands are generated in this phase.

## Local SQL catalog

```powershell
uv sync --locked
uv run python -m buildingpilot.batch
uv run pytest -q tests/test_data.py
```

Set BP_DATABASE to a populated operational database. Default batch output is data/batch;
catalog metadata and local objects are under data/lakehouse. Windows drive URI normalization
is isolated in `arrow_io.py`; no library monkeypatch is used. This is a real persistent
Iceberg SQL catalog, but it does not test REST/S3 or Spark.

## Shared REST/S3 and Spark

```powershell
uv run python scripts/init_local.py
docker compose -f compose.yaml -f compose.lake.yaml up -d object-store bucket catalog
docker compose -f compose.yaml -f compose.lake.yaml build api spark
docker compose -f compose.yaml -f compose.lake.yaml run --rm batch
docker compose -f compose.yaml -f compose.lake.yaml run --rm spark
```

The batch container reads the existing operational named volume. The REST fixture uses a
file-backed JDBC SQLite catalog and a persistent volume; a root initialization container
sets ownership, and the catalog itself runs as its unprivileged service account. Its exact
downloaded image digest is pinned. Object data lives in an independent S3-compatible RustFS
volume. Random local credentials are generated into ignored .env without overwriting existing
values. No credentials are committed. These are local development services, not a production
high-availability catalog or secured object-store deployment.

RustFS replaces the originally proposed MinIO because the MinIO image was unavailable. It
is the object store in the current official Iceberg quickstart. The S3 boundary keeps the
choice separate from domain logic. Spark 3.5.7 / Iceberg runtime and AWS bundle 1.9.2 are pinned
together in `deploy/Dockerfile.spark`; Spark's Python runs in its own image.

For an isolated interoperability fixture, run `uv run python scripts/verify_lake.py` first,
then pass its namespace to Spark:

```powershell
$namespace = Get-Content data/lake-verification-namespace.txt
docker compose -f compose.yaml -f compose.lake.yaml run --rm -e BP_LAKE_NAMESPACE=$namespace spark
```

Expected: 36 cleaned observations after replay, three hourly rows, and the Spark log marker
SPARK_CROSS_ENGINE_OK. Fixture namespaces are retained for inspection. Real integration
checks use actual REST requests, object reads/writes and Spark; local tests are not reported
as equivalent coverage.

References: [PyIceberg API](https://py.iceberg.apache.org/api/),
[official Spark quickstart](https://iceberg.apache.org/spark-quickstart/),
[Spark catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).
