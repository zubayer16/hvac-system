# Architecture and implementation plan

## Boundaries and data flow

Domain: immutable Building and Telemetry, simulator physics and baseline control.
Application: generate typed events. Adapter: CLI serializes canonical JSONL. The generator
can later feed a Kafka producer without coupling physics to broker libraries. Configuration
is per building/equipment; the first three archetypes are code-defined and validated.

Phase 1 contracts use Celsius, UTC, interval kWh, thermal kW, m3/s, ppm and percent.
Schema 1.0 has event_id, building_id, equipment_id, timestamp, interval_seconds and source.
Source distinguishes synthetic/experimental/real; generated events always say synthetic.
Stable UUID5 IDs include configuration, seed, timestamp and scenario options. They identify
replays of a deterministic run; they are not content hashes of arbitrary external records.
The simulator rejects time gaps/overlaps; transport delay and reordering belong downstream.

Missing indoor readings are allowed explicitly; nonfinite and out-of-range values are
rejected. Phase 2 preserves raw payloads, quarantines invalid input, deduplicates by event ID,
records received_at independently of event time, and tracks watermarks per building/equipment.
Schema 1.0 requires canonical units; the historical importer provides explicit configured
unit conversion. Late arrivals remain queryable; batch jobs recompute hourly aggregates
from the retained observations. Missing values never become zeros implicitly.
No claim of exactly-once Kafka delivery: commit offsets only after durable writes and use
idempotent replay. Restart recovery and bounded duplicate tracking must be integration tested.

## Target technologies and interfaces (all ten phases have implementations)

| Component | Purpose and interface |
|---|---|
| Kafka | keyed building/equipment telemetry and separate fault events; consumer offsets and lag |
| Iceberg | raw, cleaned, hourly, features, training snapshots, predictions and recommendations |
| PyArrow | explicit columnar schemas and table exchange at lakehouse boundaries |
| Polars | small/local feature windows and transformations partitioned by building |
| pandas | statistical reports and interoperability, outside streaming hot path |
| Spark | substantial batch aggregation, backfills, compaction and snapshot maintenance |
| scikit-learn | dummy, Extra Trees, Random Forest, gradient boosting faults and regression baseline |
| JAX / Flax / Optax | CPU-first numerical arrays, neural forecaster, reproducible training updates |
| SciPy | bounded constrained setpoint search over simulated trajectories |
| FastAPI / Pydantic | configuration, telemetry, faults, forecasts, optimization, metadata and health |
| Streamlit | building monitoring, quality, explanations and comparison with baseline |
| Airflow | retryable partition jobs, evaluation and gated model lifecycle |
| Prometheus / Grafana | bounded-label metrics, operational dashboards and alerts |
| Docker / Kubernetes / Helm | local services and packaged application deployment |
| Nix | pinned reproducible development environment in Phase 10 |
| Azure | optional documented deployment; never provision resources automatically |

Implemented local lakehouse: Iceberg REST catalog backed by a persistent metadata database
and S3-compatible object storage (RustFS; MinIO image unavailable). These auxiliary services are needed for a shared
catalog and durable object access across Spark and Python containers; Phase 3 must explain
and verify their exact compatible versions before implementation. A disposable integration
fixture must create tables, append through Python, read through Spark, and verify snapshot
and replay behavior against real services. The local REST/S3 setup and Python-to-Spark fixture have been executed; see docs/phase3.md.

## Phased plan and gates

| Phase | Deliverable and testable gate |
|---|---|
| 1 | typed domain, 3 buildings, RC simulator, JSONL CLI, pytest/Ruff/mypy; implemented |
| 2 | FastAPI registration/history/health, Kafka producer/consumer, Compose; durable restart/replay tests; implemented |
| 3 | working REST catalog/object store, all Iceberg tables, Arrow/Polars features, Spark batch/maintenance; cross-engine read/write tests; implemented (verification limits documented) |
| 4 | optional mapped research CSV, four supervised classifiers; chronological/building splits, persisted artifacts and per-building metrics; implemented (verification limits documented) |
| 5 | persistence, sklearn and Flax forecasters, Optax training; walk-forward MAE/RMSE and honest comparison; implemented (verification limits documented) |
| 6 | SciPy constrained optimization using forecast estimates and RC rollout; feasibility and baseline comfort/energy simulation tests; implemented (verification limits documented) |
| 7 | Streamlit live/history/quality/alerts/forecast/setpoint explanations from real backend responses; implemented (verification limits documented) |
| 8 | Airflow ingestion/quality/features/train/evaluate/promote/retrain/inference; rerun idempotency and candidate rejection tests; implemented (verification limits documented) |
| 9 | API/pipeline/model metrics, Grafana dashboards/alerts; scrape and alert fixture validation; implemented (verification limits documented) |
| 10 | containers, Helm, Nix, optional Azure guide; chart lint/render and deployment smoke tests; implemented (verification limits documented) |

Each phase was introduced with its problem, technology, data flow, interfaces, output and
verification before implementation. The request to complete all remaining phases authorized
continuing through these gates without stopping between phases.

## ML and operational invariants

Fit scaling/oversampling only within training folds. Feature lags cannot cross buildings or
look ahead. Reserve chronological test data; validation determines model selection and
promotion. Fault reports include precision/recall/F1/confusion/false-alarm rate and ROC-AUC
only where class coverage permits, broken down by building and operating condition.
Forecasts use walk-forward evaluation; neural models need not beat baselines. Persist code,
seed, configuration, feature schema, dataset snapshot and metrics with each artifact.
Airflow timers trigger evaluation of freshness/drift, not unconditional retraining/promotion.
Promotion must satisfy fixed quality/coverage and regression gates with rollback available.

Optimization compares identical weather, occupancy, initial state and tariffs for candidate
and baseline. Report simulation assumptions and comfort violations alongside estimated
energy/cost differences. Feature importances and observed alert conditions are model
explanations; heat-transfer coefficients and comfort bounds are engineering assumptions.

Metrics cover request/inference latency, errors, Kafka lag, ingestion/invalid events, pipeline
failures, forecast residuals, drift, fault rates, feasibility and comfort. Use bounded service,
model-family and status labels; never event IDs. Version artifacts outside metric labels.

## Trade-offs, deployment and production evolution

Standard-library domain validation minimizes coupling. JSONL is inspectable and deterministic
but not a concurrent analytical store. Phase 2 adds a SQLite WAL operational store shared by
local API/consumer processes, with raw receipts and quarantine. It is not the analytical
lakehouse. See [Phase 2 decisions](docs/phase2.md). Python 3.12 is the developer target;
Spark and Airflow use separate container environments; Airflow delegates ML tasks to the
application virtual environment rather than forcing incompatible dependencies together.

Azure design will map Helm applications to AKS, objects to compatible Azure storage via a
verified Iceberg FileIO, secrets to Key Vault and credentials to workload identity. Broker,
catalog and Airflow choices require explicit operational/cost review. No credentials or paid
resources are needed in Phase 1.

Before real operation: calibrate each building against measured data, validate provenance
and sensor calibration, use weather forecasts available at decision time, enforce independent
hard safety interlocks, equipment rate limits, authenticated/authorized access, audit history,
human approval, canary rollout and rollback. Monitor residual/drift by building and season.
Real control would require a separate reviewed integration; this project never sends physical
actuation commands. No synthetic savings can be presented as measured field savings.

## Implementation and validation boundaries

The source now includes all ten phases. Native tests cover models and serving; the complete
synthetic lifecycle trained, evaluated, promoted and ran inference. The shared REST/S3
Iceberg fixture was read and transformed by Spark. Helm is linted/rendered without deploying
Kubernetes. Azure is a guide only. Nix/Airflow container validation and full test evidence
are recorded in the operations/verification documents. No field performance is claimed.
