"""Reproducible labeled scenario for experiments, not a field-validated dataset."""

from datetime import UTC, datetime, timedelta

import polars as pl

from buildingpilot.domain import demo_buildings
from buildingpilot.features import feature_frame, telemetry_frame
from buildingpilot.simulator import Simulator


def synthetic_experiment(days: int = 20) -> pl.DataFrame:
    if days < 3:
        raise ValueError("At least three days required for experiments")
    rows = []
    start = datetime(2026, 1, 5, tzinfo=UTC)
    for number, building in enumerate(demo_buildings()):
        simulator = Simulator(building, seed=42)
        for index in range(days * 96):
            event = simulator.step(
                start + timedelta(minutes=15 * index),
                900,
                degraded_heating=(index // 96 + number) % 3 == 0,
                missing_indoor=index % 31 == 0,
            )
            rows.append(event.to_dict())
    return feature_frame(telemetry_frame(rows))
