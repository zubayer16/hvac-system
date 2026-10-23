"""Columnar telemetry and causal, per-building forecasting features."""

from typing import Any

import polars as pl
import pyarrow as pa

from buildingpilot.contracts import TELEMETRY

FAULT_FEATURES = [
    "indoor_temperature_c",
    "outdoor_temperature_c",
    "supply_air_temperature_c",
    "return_air_temperature_c",
    "humidity_pct",
    "co2_ppm",
    "occupancy",
    "airflow_m3_s",
    "heating_demand_kw",
    "cooling_demand_kw",
    "fan_speed_pct",
    "damper_position_pct",
    "valve_position_pct",
    "setpoint_c",
]
FORECAST_FEATURES = [
    "lag_load_kw",
    "lag2_load_kw",
    "indoor_temperature_c",
    "outdoor_temperature_c",
    "occupancy",
    "hour_sin",
    "hour_cos",
]


def telemetry_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    validated = [TELEMETRY.validate_python(row).to_dict() for row in rows]
    if not validated:
        raise ValueError("No telemetry to transform")
    return (
        pl.DataFrame(validated)
        .with_columns(
            pl.col("timestamp").str.to_datetime(time_zone="UTC"),
            pl.col("indoor_temperature_c").cast(pl.Float64),
        )
        .unique(subset=["event_id"])
        .sort(["building_id", "equipment_id", "timestamp", "event_id"])
    )


def feature_frame(frame: pl.DataFrame) -> pl.DataFrame:
    keys = ["building_id", "equipment_id", "source"]
    ordered = frame.sort(keys + ["timestamp", "event_id"])
    return (
        ordered.with_columns(
            (pl.col("energy_kwh") * 3600 / pl.col("interval_seconds")).alias("load_kw"),
            (pl.col("timestamp").dt.hour() * 2 * 3.141592653589793 / 24).sin().alias("hour_sin"),
            (pl.col("timestamp").dt.hour() * 2 * 3.141592653589793 / 24).cos().alias("hour_cos"),
            (pl.col("fault_label") != "normal").cast(pl.Int64).alias("faulty"),
        )
        .with_columns(
            pl.col("load_kw").shift(1).over(keys).alias("lag_load_kw"),
            pl.when(
                (pl.col("timestamp") - pl.col("timestamp").shift(2).over(keys)).dt.total_seconds()
                == pl.col("interval_seconds") * 2
            )
            .then(pl.col("load_kw").shift(2).over(keys))
            .otherwise(None)
            .alias("lag2_load_kw"),
            pl.col("timestamp").diff().over(keys).dt.total_seconds().alias("gap_seconds"),
        )
        .filter(pl.col("gap_seconds") == pl.col("interval_seconds"))
    )


def hourly_frame(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
        .group_by(["building_id", "equipment_id", "source", "hour"])
        .agg(
            pl.col("energy_kwh").sum(),
            pl.col("indoor_temperature_c").mean(),
            pl.col("outdoor_temperature_c").mean(),
            pl.col("occupancy").mean(),
            pl.len().alias("observations"),
        )
        .sort(["building_id", "hour"])
    )


def arrow_table(frame: pl.DataFrame) -> pa.Table:
    # Polars exports large_string; normalize once so Spark/Iceberg see stable Arrow types.
    table = frame.to_arrow()
    schema = pa.schema(
        [
            pa.field(
                field.name,
                pa.string()
                if pa.types.is_large_string(field.type)
                else pa.int64()
                if pa.types.is_unsigned_integer(field.type)
                else field.type,
            )
            for field in table.schema
        ]
    )
    return table.cast(schema)
