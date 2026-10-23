"""Infrastructure-independent domain contracts. All measurements use canonical SI-derived units."""

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Literal


@dataclass(frozen=True)
class Building:
    building_id: str
    equipment_id: str
    thermal_capacity_kwh_k: float
    envelope_kw_k: float
    hvac_capacity_kw: float
    max_occupancy: int
    max_airflow_m3_s: float
    setpoint_c: float = 21.0
    cop: float = 3.0
    utc_offset_hours: int = 1

    def __post_init__(self) -> None:
        if not self.building_id or not self.equipment_id:
            raise ValueError("Building and equipment identifiers must be nonempty")
        for name in (
            "thermal_capacity_kwh_k",
            "envelope_kw_k",
            "hvac_capacity_kw",
            "max_airflow_m3_s",
            "cop",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.max_occupancy <= 0 or not 16 <= self.setpoint_c <= 28:
            raise ValueError("Invalid occupancy or setpoint")
        if not -12 <= self.utc_offset_hours <= 14:
            raise ValueError("Invalid UTC offset")


@dataclass(frozen=True)
class Telemetry:
    event_id: str
    building_id: str
    equipment_id: str
    timestamp: datetime
    interval_seconds: int
    indoor_temperature_c: float | None
    outdoor_temperature_c: float
    supply_air_temperature_c: float
    return_air_temperature_c: float
    humidity_pct: float
    co2_ppm: float
    occupancy: int
    airflow_m3_s: float
    heating_demand_kw: float
    cooling_demand_kw: float
    fan_speed_pct: float
    damper_position_pct: float
    valve_position_pct: float
    setpoint_c: float
    energy_kwh: float
    operating_status: Literal["heating", "cooling", "ventilating"]
    fault_label: Literal["normal", "heating_degraded", "faulty"]
    schema_version: str = "1.0"
    source: Literal["synthetic", "experimental", "real"] = "synthetic"

    def __post_init__(self) -> None:
        if self.timestamp.utcoffset() != timedelta(0):
            raise ValueError("timestamp must be timezone-aware UTC")
        if not all((self.event_id, self.building_id, self.equipment_id)):
            raise ValueError("Missing identifiers")
        if not 1 <= self.interval_seconds <= 900 or self.occupancy < 0:
            raise ValueError("Invalid interval or occupancy")
        values = asdict(self)
        for key, value in values.items():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"Non-finite measurement: {key}")
        for key in ("humidity_pct", "fan_speed_pct", "damper_position_pct", "valve_position_pct"):
            if not 0 <= values[key] <= 100:
                raise ValueError(f"Out-of-range percentage: {key}")
        for key in ("energy_kwh", "airflow_m3_s", "heating_demand_kw", "cooling_demand_kw"):
            if values[key] < 0:
                raise ValueError(f"Negative measurement: {key}")
        for key in (
            "indoor_temperature_c",
            "outdoor_temperature_c",
            "supply_air_temperature_c",
            "return_air_temperature_c",
            "setpoint_c",
        ):
            if values[key] is not None and not -60 <= values[key] <= 80:
                raise ValueError(f"Out-of-range temperature: {key}")
        if not 250 <= self.co2_ppm <= 5000:
            raise ValueError("Out-of-range CO2")
        if self.heating_demand_kw > 0 and self.cooling_demand_kw > 0:
            raise ValueError("Simultaneous heating and cooling unsupported")
        if self.source not in ("synthetic", "experimental", "real"):
            raise ValueError("Unknown provenance")
        if self.operating_status not in ("heating", "cooling", "ventilating"):
            raise ValueError("Unknown equipment status")
        if (
            self.fault_label not in ("normal", "heating_degraded", "faulty")
            or self.schema_version not in ("1.0", "1.1")
            or (self.fault_label == "faulty" and self.schema_version != "1.1")
        ):
            raise ValueError("Unsupported label or schema version")

    def to_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat().replace("+00:00", "Z")
        return result


def demo_buildings() -> tuple[Building, ...]:
    return (
        Building("office", "ahu-office", 120, 3, 150, 120, 3.0),
        Building("retail", "ahu-retail", 200, 5, 220, 200, 5.0, 22),
        Building("school", "ahu-school", 160, 4, 180, 180, 4.0, 21),
    )
