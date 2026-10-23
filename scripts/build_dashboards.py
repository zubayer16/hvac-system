"""Generate provisioned Grafana dashboards with stable Prometheus queries."""

import json
from pathlib import Path

root = Path(__file__).resolve().parents[1] / "monitoring/grafana/dashboards"
root.mkdir(parents=True, exist_ok=True)
groups = {
    "backend": [
        ("Request rate", "sum by (route) (rate(bp_http_requests_total[5m]))"),
        (
            "p95 request latency",
            "histogram_quantile(0.95, sum by (le) (rate(bp_http_request_seconds_bucket[5m])))",
        ),
        ("HTTP errors", 'sum(rate(bp_http_requests_total{status=~"5.."}[5m]))'),
        ("Service availability", "up"),
    ],
    "pipeline": [
        ("Unique valid observations", "bp_valid_events"),
        ("Quarantined records", "bp_invalid_events"),
        ("Consumer lag", "bp_kafka_consumer_lag"),
        ("Ingestion attempts", "sum by (outcome) (rate(bp_ingestion_events_total[5m]))"),
        ("Pipeline failures", "bp_pipeline_failures_total"),
    ],
    "models": [
        (
            "Inference latency",
            "rate(bp_inference_seconds_sum[5m]) / "
            "clamp_min(rate(bp_inference_seconds_count[5m]), 0.001)",
        ),
        ("Forecast validation MAE (kW)", "bp_forecast_validation_mae_kw"),
        ("Matched forecast error (kW)", "bp_forecast_observed_mae_kw"),
        ("Feature distance indicator", "bp_feature_drift_indicator"),
        ("Fault alert rate", "rate(bp_fault_alerts_total[5m])"),
        ("Optimization feasibility", "sum by (feasible) (rate(bp_optimization_total[5m]))"),
        ("Simulated comfort violations", "rate(bp_comfort_violations_total[5m])"),
    ],
}
for name, queries in groups.items():
    panels = [
        {
            "id": index + 1,
            "title": title,
            "type": "timeseries",
            "datasource": {"type": "prometheus", "uid": "buildingpilot-prometheus"},
            "gridPos": {"x": index % 2 * 12, "y": index // 2 * 8, "w": 12, "h": 8},
            "targets": [{"expr": expression, "refId": "A"}],
        }
        for index, (title, expression) in enumerate(queries)
    ]
    (root / f"{name}.json").write_text(
        json.dumps(
            {
                "uid": f"buildingpilot-{name}",
                "title": f"BuildingPilot · {name}",
                "schemaVersion": 39,
                "version": 1,
                "refresh": "10s",
                "time": {"from": "now-1h", "to": "now"},
                "panels": panels,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
print("Generated backend, pipeline and model dashboards")
