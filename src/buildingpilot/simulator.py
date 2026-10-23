"""Seeded single-zone RC model, with a proportional baseline controller."""

import hashlib
import math
import random
from datetime import datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from buildingpilot.domain import Building, Telemetry


def thermal_step(
    indoor_c: float,
    outdoor_c: float,
    heat_kw: float,
    gains_kw: float,
    capacity_kwh_k: float,
    loss_kw_k: float,
    hours: float,
) -> float:
    """Exact solution for constant inputs during one interval."""
    if capacity_kwh_k <= 0 or loss_kw_k <= 0 or hours <= 0:
        raise ValueError("Thermal parameters and timestep must be positive")
    equilibrium = outdoor_c + (heat_kw + gains_kw) / loss_kw_k
    return equilibrium + (indoor_c - equilibrium) * math.exp(-loss_kw_k * hours / capacity_kwh_k)


class Simulator:
    def __init__(self, building: Building, seed: int = 42, indoor_c: float = 21.0) -> None:
        if not math.isfinite(indoor_c) or not 5 <= indoor_c <= 40:
            raise ValueError("Initial temperature must be between 5 and 40 C")
        self.building = building
        self.seed = seed
        digest = hashlib.sha256(f"{seed}:{building.building_id}".encode()).digest()
        self.rng = random.Random(int.from_bytes(digest[:8], "big"))
        self.indoor_c = indoor_c
        self.last_timestamp: datetime | None = None
        self.last_interval_seconds: int | None = None

    def step(
        self,
        timestamp: datetime,
        interval_seconds: int = 300,
        *,
        degraded_heating: bool = False,
        missing_indoor: bool = False,
    ) -> Telemetry:
        if timestamp.utcoffset() != timedelta(0):
            raise ValueError("Use timezone-aware UTC timestamps")
        if not 1 <= interval_seconds <= 900:
            raise ValueError("interval_seconds must be in [1, 900]")
        if self.last_timestamp is not None and timestamp != self.last_timestamp + timedelta(
            seconds=self.last_interval_seconds or 0
        ):
            raise ValueError("Simulator steps must be contiguous; reorder events downstream")
        b = self.building
        local = timestamp + timedelta(hours=b.utc_offset_hours)
        hour = local.hour + local.minute / 60
        occupied = local.weekday() < 5 and 8 <= hour < 18
        occupancy = int(b.max_occupancy * self.rng.uniform(0.5, 0.95)) if occupied else 0
        season = math.cos(2 * math.pi * (timestamp.timetuple().tm_yday - 15) / 365.25)
        outdoor = 10 - 13 * season + 3 * math.sin(2 * math.pi * (hour - 9) / 24)
        outdoor += self.rng.gauss(0, 0.15)
        airflow = b.max_airflow_m3_s * (0.2 + 0.8 * occupancy / b.max_occupancy)
        # Air density * specific heat = approximately 1.2 kJ/(m3 K).
        loss = b.envelope_kw_k + 1.2 * airflow
        gains = occupancy * 0.1 + 2.0
        requested = loss * (b.setpoint_c - outdoor) - gains + 12 * (b.setpoint_c - self.indoor_c)
        heat = max(0.0, min(b.hvac_capacity_kw, requested))
        cool = max(0.0, min(b.hvac_capacity_kw, -requested))
        delivered_heat = heat * (0.45 if degraded_heating else 1.0)
        hours = interval_seconds / 3600
        previous = self.indoor_c
        next_indoor = thermal_step(
            previous, outdoor, delivered_heat - cool, gains, b.thermal_capacity_kwh_k, loss, hours
        )
        fan_pct = 100 * airflow / b.max_airflow_m3_s
        fan_kw = 5 * (fan_pct / 100) ** 3
        event = Telemetry(
            event_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"buildingpilot:1:{self.seed}:{b!r}:"
                    f"{timestamp.isoformat()}:{interval_seconds}:"
                    f"{degraded_heating}:{missing_indoor}",
                )
            ),
            building_id=b.building_id,
            equipment_id=b.equipment_id,
            timestamp=timestamp,
            interval_seconds=interval_seconds,
            indoor_temperature_c=None if missing_indoor else previous,
            outdoor_temperature_c=outdoor,
            supply_air_temperature_c=previous + 12 * (delivered_heat - cool) / b.hvac_capacity_kw,
            return_air_temperature_c=previous,
            humidity_pct=min(75, max(20, 40 + occupancy / b.max_occupancy * 15)),
            co2_ppm=420 + 650 * occupancy / b.max_occupancy,
            occupancy=occupancy,
            airflow_m3_s=airflow,
            heating_demand_kw=heat,
            cooling_demand_kw=cool,
            fan_speed_pct=fan_pct,
            damper_position_pct=fan_pct,
            valve_position_pct=100 * max(heat, cool) / b.hvac_capacity_kw,
            setpoint_c=b.setpoint_c,
            energy_kwh=((heat + cool) / b.cop + fan_kw) * hours,
            operating_status="heating" if heat else "cooling" if cool else "ventilating",
            fault_label="heating_degraded" if degraded_heating and heat > 0 else "normal",
        )
        self.indoor_c = next_indoor
        self.last_timestamp = timestamp
        self.last_interval_seconds = interval_seconds
        return event
