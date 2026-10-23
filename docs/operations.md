# Phases 8–10 — lifecycle, observability and deployment

## Model lifecycle

The Airflow DAG invokes the same `buildingpilot.pipeline` tasks used by local commands:
import (when explicitly configured), quality, Iceberg features, fault training, forecasting,
evaluation, promotion and batch inference. Tasks have atomic completion markers under a
validated run ID. Retries reuse completed work. Use a new run ID for a new data snapshot;
rerunning a completed ID intentionally reuses its snapshot/results.

Training checks data, code and relevant training configuration fingerprints and skips unchanged data
unless BP_FORCE_RETRAIN=1. Each provenance is trained separately (BP_TRAIN_SOURCE defaults
to synthetic). Promotion requires validation F1 >= .6 and the dummy score, sufficient validation
rows, and forecast validation MAE no worse than 1.01 times persistence. Held-out test metrics
are reporting only, not promotion inputs. Rejected candidates stay saved; current models
remain active. Previous pointer JSON is retained for rollback via the artifact promotion helper.
These are initial quality gates, not substitutes for building-specific acceptance testing.

```powershell
uv run python -m buildingpilot.pipeline all --run-id manual-001
```

The default requires a populated journal with both classes for supervised fault training.
It does not manufacture missing fault labels. For a complete, explicitly synthetic experiment:

```powershell
uv run python -m buildingpilot.demo --days 12 --epochs 30
```

By default this uses data/demo/operations.db, models, local Iceberg and runs. Explicit
BP_DATABASE overrides must also point to an isolated demo database. It does not modify
existing JSONL files. To serve its artifacts with a native API:

```powershell
$env:BP_DATABASE='data/demo/operations.db'
$env:BP_MODEL_ROOT='data/demo/models'
$env:BP_TRANSPORT='local'
uv run uvicorn buildingpilot.api:app --host 127.0.0.1 --port 8002
# another terminal
$env:BP_API_URL='http://127.0.0.1:8002'
uv run streamlit run src/buildingpilot/dashboard.py
```

The Airflow image copies the application virtual environment into a separate path. Its
framework interpreter parses only the DAG; ML jobs use the application interpreter, avoiding
Airflow/SQLAlchemy/JAX version conflicts. Airflow 2.10.5 / Python 3.12 is pinned for this
local standalone setup. It is not an HA production scheduler. Newly discovered DAGs are paused.

```powershell
docker compose -f compose.yaml -f compose.apps.yaml build api airflow
docker compose -f compose.yaml -f compose.apps.yaml --profile orchestration up -d airflow
```

The local UI is http://localhost:8080. Standalone generates an admin password in its runtime
home; retrieve it locally from `/app/data/airflow/standalone_admin_password.txt`, never commit
or paste it into logs. Configure the dataset and unpause the lifecycle DAG when ready. The
standalone scheduler uses local SQL Iceberg by default; shared REST/S3 environment variables
can be supplied using the same names as compose.lake.yaml. Spark's scheduled hourly work is
available as a separate job/container; the DAG's first implementation uses Polars locally.

The development image tag remains `buildingpilot:phase2` for compatibility with the earlier
Compose setup; rebuilt images contain all current phases. It is not a phase-completion signal.
Use immutable release tags/digests when publishing images for an actual deployment.

## Monitoring

```powershell
uv run python scripts/init_local.py
uv run python scripts/build_dashboards.py
docker compose -f compose.yaml -f compose.monitoring.yaml up -d prometheus grafana
```

API metrics are at /metrics; the worker exporter is on internal port 9101. Prometheus at
localhost:9090 scrapes both. Grafana at localhost:3000 uses admin and the generated
BP_GRAFANA_PASSWORD from ignored .env. Provisioning loads backend, pipeline and model views.
Alert rules cover service loss, API errors, lag, feature shifts and pipeline failures.
Thresholds are illustrative and require operational calibration. Alerts appear in Prometheus;
no external notification service is configured or contacted.

Pipeline failures and evaluation gauges persist in SQLite so short-lived jobs remain visible.
Forecast-vs-observed errors match actual later measurements; absent observations do not become
zero error. The drift gauge is a standardized feature-distance indicator, not a calibrated
population drift test. Fault/ingestion counters count inference/processing attempts (including
replays); SQLite gauges report unique persisted events. Event IDs and building IDs are not
metric labels. Run one API process/exporter for these in-process metrics; multi-worker
Prometheus aggregation is not configured.

## Helm / Kubernetes

```powershell
helm lint deploy/helm/buildingpilot
helm template pilot deploy/helm/buildingpilot
```

The chart packages API, ingestion and optional simulator/dashboard and a batch CronJob.
Kafka, catalog, object storage, Airflow and monitoring remain external infrastructure.
Set kafka.bootstrapServers, initialize topics and load an image accessible to your cluster.
Enable lakehouse.mode=rest and supply catalogUri, s3Endpoint and an existing Kubernetes Secret
containing BP_S3_ACCESS_KEY/BP_S3_SECRET_KEY for shared object storage. No secret values are
committed or rendered by default.

SQLite WAL requires all application writers on one node with a suitable local/block-device
filesystem; pod affinity and one replica make that restriction explicit. Do not use SMB/NFS
for this store. Move to a tested network database adapter before horizontal scaling. PVCs
are retained across Helm uninstall. Resources, non-root IDs, dropped capabilities, health
probes and disabled API-token mounting are included. This is deployable application packaging,
not a claim of a tested production cluster or production security certification.

## Nix

On Linux, WSL2 or supported macOS with Nix installed:

```text
nix develop
uv sync --locked
uv run pytest -q
```

flake.nix and its lockfile pin developer tools (Python 3.12, uv, Ruff, mypy, Helm, kubectl,
JDK) separately from uv.lock's Python dependencies. Native Windows uses uv directly; Nix
validation here uses a Linux container. Docker remains host-managed and no daemon is installed
by entering the shell. Optional Azure guidance is in [azure.md](azure.md); no cloud resources
are provisioned.
