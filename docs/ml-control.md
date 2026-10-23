# Phases 4–7 — trained models, forecasts, simulated control and monitoring UI

## Fault detection

`faults.py` compares DummyClassifier, Extra Trees, Random Forest and gradient boosting.
The first 60% of distinct UTC timestamps train, the next 20% select, and the final 20%
report held-out performance. Equal timestamps never cross splits. Imputation and scaling
fit inside each training pipeline. No oversampling is used. Features explicitly exclude
fault labels, source and event identity. Reports include precision, recall, F1, confusion,
false-alarm rate, ROC-AUC when defined, and building/operating-condition breakdowns.

Artifacts contain seed, feature schema, provenance, training data fingerprint, Iceberg
snapshot ID where available, splits, metrics and importance. Only trusted local artifacts
may be loaded: joblib/pickle is not a format for accepting files from untrusted users.

```powershell
uv run python -m buildingpilot.faults data/batch/features.parquet
uv run pytest -q tests/test_faults.py
```

The CSV importer accepts explicit `columns` (canonical name → CSV column), `defaults`,
`units`, `fault_labels`, and source `experimental` or `real`. All mandatory telemetry fields
must be mapped or explicitly defaulted. Supported input units include F, W, Wh, cfm and
fraction, normalized to canonical units. Real observations require measured timestamps.
Experimental rows without timestamps require an explicit `start` and interval; those times
are an assumption. Unknown labels/units and invalid rows are rejected/quarantined.
Simulator schema 1.0 retains normal/heating_degraded labels. CSV imports default to schema
1.1, which also accepts a generic faulty label without claiming a heating degradation.
Broader fault taxonomies still need an explicit domain extension.

```powershell
uv run python -m buildingpilot.importer path/to/research.csv path/to/mapping.json
```

No SZVAV/MZVAV/SZCAV files are assumed or downloaded. Synthetic fault labels identify an
injected engineering degradation; the resulting metrics are not validation on real equipment.

## Forecasting

`forecasting.py` compares persistence, sklearn histogram gradient boosting and a Flax MLP
with JAX arrays/gradients and Optax clipped Adam updates. Three expanding time windows
evaluate MAE/RMSE/bias; the final 20% remains held out. Normalization fits training only.
Series are partitioned by building/equipment/provenance; lags crossing missing intervals
are excluded. The target is average interval load in kW, not cumulative energy.
The Flax candidate is saved even when a baseline wins. CPU packages are installed by default;
accelerated JAX installation is an explicit environment choice, not a required GPU purchase.

```powershell
uv run python -m buildingpilot.forecasting data/batch/features.parquet --epochs 80
uv run pytest -q tests/test_forecasting.py
```

Serving recursively predicts up to 12 intervals. Future outdoor/occupancy scenarios may be
supplied; otherwise they and indoor state are persisted from current observations. This is
not a weather-service integration. Reported held-out metrics are one-interval evaluation,
not a guarantee of recursive multi-interval accuracy. Model provenance must match telemetry.

## Optimization

`optimization.py` uses SciPy SLSQP and exact RC rollouts. It constrains temperature (occupied
20–24 C; unoccupied 16–28 C), setpoints (16–28 C), step changes (default .5 C), and clips
thermal power to equipment bounds. The objective combines tariff-weighted estimated energy
and a small occupied comfort penalty. Candidate and baseline share weather, occupancy,
initial state, horizon, forecast residual and tariff.

The forecast estimates baseline load. Candidate control responses come from the uncalibrated
thermal model; the neural model is not claimed to have learned causal setpoint effects.
Infeasible solutions return the baseline with explicit failure/comfort violations. Estimates
may show no savings or worse energy performance; no measured savings are fabricated.
There is no actuation endpoint or physical HVAC driver.

## API and Streamlit

GET `/buildings/{id}/faults/latest`, POST `/buildings/{id}/forecasts`, POST
`/buildings/{id}/optimization`, and GET `/models` use promoted artifacts. Missing artifacts
return 503; missing/invalid state returns a clear client error. Model importance/observed
conditions are distinct from RC/comfort assumptions. Results are persisted in SQLite and
exported to Iceberg by the lifecycle.

In Kafka mode, positive fault predictions served by the API also publish schema-versioned
events to `buildingpilot.faults.v1`. Their stable event identity combines the source event
and model version; consumers must deduplicate replayed alerts. The topic is initialized
alongside telemetry, and broker readiness verifies both topics. Local mode does not claim
Kafka publication. The real broker integration test reads an actual published fault event.

```powershell
uv run streamlit run src/buildingpilot/dashboard.py
```

Set BP_API_URL to select a backend. The UI displays stored sensor/energy history, model
faults and explanations, forecasts and held-out scores, simulated setpoint comparisons,
provenance and missing/late counts. Refresh fetches current backend data. It does not invent
predictions when models are missing. Streamlit AppTest verifies unavailable-backend handling;
HTTP tests exercise model serving and optimization with temporary promoted fixtures.

References: [sklearn leakage guidance](https://scikit-learn.org/stable/common_pitfalls.html),
[JAX installation](https://docs.jax.dev/en/latest/installation.html),
[Optax updates](https://optax.readthedocs.io/en/latest/getting_started.html),
[SciPy SLSQP](https://docs.scipy.org/doc/scipy/reference/optimize.minimize-slsqp.html),
[Streamlit testing](https://docs.streamlit.io/develop/api-reference/app-testing).
