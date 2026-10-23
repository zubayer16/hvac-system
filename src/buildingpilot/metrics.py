"""Bounded-label metrics and durable gauges for short-lived pipeline jobs."""

import json
from datetime import datetime
from typing import cast

from prometheus_client import Counter, Gauge, Histogram

from buildingpilot.storage import Repository

HTTP_REQUESTS = Counter(
    "bp_http_requests_total", "Completed HTTP requests", ["method", "route", "status"]
)
HTTP_SECONDS = Histogram("bp_http_request_seconds", "HTTP duration", ["route"])
INGESTION = Counter(
    "bp_ingestion_events_total", "Ingestion attempts by durable outcome", ["outcome"]
)
INFERENCE = Histogram("bp_inference_seconds", "Inference duration", ["family"])
FAULT_ALERTS = Counter("bp_fault_alerts_total", "Fault alerts returned by inference")
FAULT_PREDICTIONS = Counter("bp_fault_predictions_total", "Fault predictions returned")
KAFKA_LAG = Gauge("bp_kafka_consumer_lag", "Uncommitted records in the consumer assignment")
FEASIBILITY = Counter("bp_optimization_total", "Simulated optimizer outcomes", ["feasible"])
COMFORT = Counter("bp_comfort_violations_total", "Simulated recommendation comfort violations")
DRIFT = Gauge("bp_feature_drift_indicator", "Maximum standardized feature distance; uncalibrated")
PIPELINE_FAILURES = Gauge("bp_pipeline_failures_total", "Persisted pipeline failures")
FORECAST_ERROR = Gauge("bp_forecast_validation_mae_kw", "Walk-forward forecast MAE")
OBSERVED_ERROR = Gauge("bp_forecast_observed_mae_kw", "Matched historical forecast/observation MAE")
INVALID = Gauge("bp_invalid_events", "Durably quarantined records")
VOLUME = Gauge("bp_valid_events", "Unique persisted telemetry events")


def persist_metric(
    repository: Repository, key: str, value: float, *, increment: bool = False
) -> None:
    with repository.connection() as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS operational_metrics "
            "(name TEXT PRIMARY KEY, value REAL NOT NULL)"
        )
        operation = "value + excluded.value" if increment else "excluded.value"
        db.execute(
            "INSERT INTO operational_metrics VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET "
            f"value={operation}",
            (key, value),
        )


def update_persisted_metrics(repository: Repository) -> None:
    status = repository.status()
    VOLUME.set(cast(int, status["events"]))
    INVALID.set(cast(dict[str, int], status["receipts"]).get("rejected", 0))
    with repository.connection() as db:
        exists = db.execute(
            "SELECT name FROM sqlite_master WHERE name='operational_metrics'"
        ).fetchone()
        values = dict(db.execute("SELECT name, value FROM operational_metrics")) if exists else {}
    for key, metric in (
        ("pipeline_failures", PIPELINE_FAILURES),
        ("forecast_validation_mae", FORECAST_ERROR),
        ("forecast_observed_mae", OBSERVED_ERROR),
    ):
        if key in values:
            metric.set(values[key])


def match_forecast_errors(repository: Repository) -> int:
    with repository.connection() as db:
        if db.execute("SELECT name FROM sqlite_master WHERE name='results'").fetchone() is None:
            return 0
        predictions = [
            json.loads(row[0])
            for row in db.execute("SELECT payload FROM results WHERE family='forecast'")
        ]
        actual = {}
        for row in db.execute("SELECT payload FROM telemetry"):
            event = json.loads(row[0])
            key = (
                event["building_id"],
                datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")),
                event["source"],
            )
            actual[key] = event["energy_kwh"] * 3600 / event["interval_seconds"]
    errors = []
    for prediction in predictions:
        for point in prediction["predictions"]:
            key = (
                prediction["building_id"],
                datetime.fromisoformat(point["timestamp"]),
                prediction["source"],
            )
            if key in actual:
                errors.append(abs(point["load_kw"] - actual[key]))
    if errors:
        persist_metric(repository, "forecast_observed_mae", sum(errors) / len(errors))
    return len(errors)
