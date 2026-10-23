# Verification record

All ten phases have source implementations. These checks were executed locally during
implementation; they do not claim real-building validation or an Azure/Kubernetes deployment.

| Area | Executed evidence |
|---|---|
| Regression | 60 pytest tests passed with BP_TEST_KAFKA=1, including schema 1.1 CSV compatibility, real fault-event publication and rollback idempotency |
| Quality | Ruff lint and formatting passed; strict mypy passed for 39 application/test files |
| Iceberg local | Actual SQL catalog writes, reopen, replay and snapshot reads passed |
| REST/S3 | Python wrote/replayed an isolated 36-observation fixture through the persistent REST catalog and RustFS |
| Live lakehouse | Exported 2,335 actual Kafka-ingested observations; live inference for all three buildings committed prediction/recommendation snapshots and matched 15 completed forecast points |
| Fault stream | Real-broker test published and read a versioned fault event in a separate uniquely named topic |
| Spark | Read those same tables, rebuilt hourly data, ran compaction/snapshot maintenance and exited with SPARK_CROSS_ENGINE_OK |
| Model training | Native synthetic 12-day lifecycle trained all fault/forecast candidates, evaluated, promoted and ran batch inference |
| JAX | Real CPU Flax/Optax training and inference executed, including walk-forward folds |
| Control | Feasible and infeasible scenarios tested; live HTTP recommendations returned simulation_only=true |
| Streamlit | AppTest verified unavailable-backend behavior; container health endpoint responded |
| Airflow | Real 2.10.5 image imported the eight-task DAG and completed an entire airflow dags test run successfully against a copied synthetic fixture |
| Prometheus | promtool accepted configuration and all five alert rules; API/ingestion targets reported up |
| Grafana | Authenticated provisioning check found backend, pipeline and models dashboard UIDs |
| Helm | Lint passed; optional-services rendering parsed as nine resources, including four one-replica Deployments and a CronJob |
| Nix | Generated flake.lock and passed Linux x86_64 flake check --no-build; complete toolchain materialization and other declared platforms were not exercised |
| Azure | Guide reviewed against official documentation; no credentials requested or resources provisioned |

The running Compose API/worker/simulator were updated without resetting Kafka or telemetry
volumes. Trusted synthetic model artifacts were copied into the local application volume
for serving checks. The Airflow fixture used a separate copied database; the live Kafka
journal and existing JSONL files were preserved. Scientific Python runtime images are large;
dependency installation is separated from source packaging to support later cached builds.

## Interpretation and remaining limits

The injected synthetic fault is intentionally recognizable through diagnostic proxies. For
example, a native synthetic held-out run produced fault F1=1.0 and forecast MAE about 0.87 kW.
Those values describe that generated scenario, not generalization to research datasets or
commercial buildings. Different data windows/hyperparameters can select different models.
The neural model was trained and compared, and was not forced to win.

Forecast evaluation is one interval ahead. Recursive horizon performance, timestep changes,
season/building transfer and causal control effects need separate validation/calibration.
The five-second demo stream and quarter-hour research scenario are explicitly synthetic;
do not treat their displayed held-out metrics as validation of a different sampling regime.
Weather/occupancy persist unless a scenario is supplied. No physical control is implemented.

Airflow standalone, SQLite operational state, the REST fixture and local S3 store are developer
services. Production HA, auth/TLS, operational database migration, journal retention,
incremental partition transforms, configuration history and calibrated drift/alert thresholds
remain work for a production rollout. Helm rendering is not a Kubernetes deployment test.
The Azure guide lists adapter and safety work rather than claiming those integrations ran.

Upstream deprecation warnings from Kafka/Starlette and an Iceberg empty-overwrite warning
were visible during tests; they were not suppressed or misreported as clean warning-free runs.
