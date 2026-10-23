"""Constrained simulation-only recommendations; no physical actuation interface exists."""

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.optimize import minimize

from buildingpilot.domain import Building
from buildingpilot.simulator import thermal_step


@dataclass(frozen=True)
class Scenario:
    indoor_c: float
    outdoor_c: tuple[float, ...]
    occupancy: tuple[int, ...]
    prices: tuple[float, ...]
    previous_setpoint_c: float = 21
    interval_seconds: int = 900
    max_change_c: float = 0.5

    def __post_init__(self) -> None:
        horizon = len(self.outdoor_c)
        if not 1 <= horizon <= 12 or len(self.occupancy) != horizon or len(self.prices) != horizon:
            raise ValueError("Scenario vectors must share a horizon of 1..12 intervals")
        values = (
            self.indoor_c,
            self.previous_setpoint_c,
            self.max_change_c,
            *self.outdoor_c,
            *self.prices,
        )
        if not all(math.isfinite(v) for v in values) or any(v < 0 for v in self.prices):
            raise ValueError("Scenario values must be finite; prices must be nonnegative")
        if not 1 <= self.interval_seconds <= 900 or not 0 < self.max_change_c <= 2:
            raise ValueError("Invalid timestep/change bound")
        if any(value < 0 for value in self.occupancy) or not 16 <= self.previous_setpoint_c <= 28:
            raise ValueError("Invalid occupancy or previous setpoint")


def rollout(building: Building, scenario: Scenario, setpoints: Any) -> dict[str, Any]:
    indoor = scenario.indoor_c
    temperatures, energy, violations = [], [], []
    for outdoor, occupancy, setpoint in zip(
        scenario.outdoor_c, scenario.occupancy, setpoints, strict=True
    ):
        airflow = building.max_airflow_m3_s * (
            0.2 + 0.8 * min(occupancy / building.max_occupancy, 1)
        )
        loss = building.envelope_kw_k + 1.2 * airflow
        gains = 0.1 * occupancy + 2
        command = np.clip(
            loss * (setpoint - outdoor) - gains + 12 * (setpoint - indoor),
            -building.hvac_capacity_kw,
            building.hvac_capacity_kw,
        )
        hours = scenario.interval_seconds / 3600
        indoor = thermal_step(
            indoor, outdoor, float(command), gains, building.thermal_capacity_kwh_k, loss, hours
        )
        electrical = (
            abs(float(command)) / building.cop + 5 * (airflow / building.max_airflow_m3_s) ** 3
        ) * hours
        low, high = (20, 24) if occupancy else (16, 28)
        temperatures.append(indoor)
        energy.append(electrical)
        violations.append(max(low - indoor, indoor - high, 0))
    return {
        "temperatures_c": temperatures,
        "physical_energy_kwh": energy,
        "comfort_violation_c": violations,
    }


def optimize(
    building: Building, scenario: Scenario, baseline_forecast_kw: list[float] | None = None
) -> dict[str, Any]:
    horizon = len(scenario.outdoor_c)
    if any(value > building.max_occupancy for value in scenario.occupancy):
        raise ValueError("Occupancy exceeds building configuration")
    if baseline_forecast_kw is not None and (
        len(baseline_forecast_kw) != horizon
        or not all(math.isfinite(v) and v >= 0 for v in baseline_forecast_kw)
    ):
        raise ValueError("Forecast must contain finite nonnegative loads for the full horizon")
    baseline = np.full(horizon, scenario.previous_setpoint_c)
    baseline_result = rollout(building, scenario, baseline)
    baseline_energy = np.asarray(baseline_result["physical_energy_kwh"])
    residual = (
        np.asarray(baseline_forecast_kw) * scenario.interval_seconds / 3600 - baseline_energy
        if baseline_forecast_kw is not None
        else np.zeros(horizon)
    )

    def evaluate(setpoints: Any) -> tuple[dict[str, Any], Any]:
        result = rollout(building, scenario, setpoints)
        return result, np.maximum(0, np.asarray(result["physical_energy_kwh"]) + residual)

    def objective(setpoints: Any) -> float:
        result, energy = evaluate(setpoints)
        comfort = sum(
            (temp - 22) ** 2 if occ else 0
            for temp, occ in zip(result["temperatures_c"], scenario.occupancy, strict=True)
        )
        return float(np.dot(energy, scenario.prices) + 0.02 * comfort)

    def constraints(setpoints: Any) -> Any:
        result, _ = evaluate(setpoints)
        values = []
        previous = scenario.previous_setpoint_c
        for temperature, occupied, setpoint in zip(
            result["temperatures_c"], scenario.occupancy, setpoints, strict=True
        ):
            low, high = (20, 24) if occupied else (16, 28)
            values.extend(
                [
                    temperature - low,
                    high - temperature,
                    scenario.max_change_c - abs(setpoint - previous),
                ]
            )
            previous = setpoint
        return np.asarray(values)

    solution = minimize(
        objective,
        baseline,
        method="SLSQP",
        bounds=[(16, 28)] * horizon,
        constraints=[{"type": "ineq", "fun": constraints}],
        options={"maxiter": 120, "ftol": 1e-7},
    )
    feasible = bool(solution.success and np.min(constraints(solution.x)) >= -1e-5)
    chosen = solution.x if feasible and objective(solution.x) <= objective(baseline) else baseline
    result, estimated_energy = evaluate(chosen)
    _, baseline_estimated = evaluate(baseline)
    active = []
    if not feasible:
        active.append("No feasible optimizer solution: baseline returned for simulation comparison")
    if any(
        abs(a - b) >= scenario.max_change_c - 1e-3
        for a, b in zip(
            chosen, np.concatenate(([scenario.previous_setpoint_c], chosen[:-1])), strict=True
        )
    ):
        active.append("Maximum setpoint change")
    if any(
        abs(temp - (20 if occupied else 16)) < 0.02 or abs(temp - (24 if occupied else 28)) < 0.02
        for temp, occupied in zip(result["temperatures_c"], scenario.occupancy, strict=True)
    ):
        active.append("Thermal comfort boundary")
    base_total = float(np.sum(baseline_estimated))
    optimized_total = float(np.sum(estimated_energy))
    return {
        "simulation_only": True,
        "feasible": feasible,
        "solver_message": str(solution.message),
        "setpoints_c": [float(v) for v in chosen],
        "next_setpoint_c": float(chosen[0]),
        "baseline_setpoint_c": scenario.previous_setpoint_c,
        "baseline_energy_kwh": base_total,
        "estimated_energy_kwh": optimized_total,
        "estimated_savings_pct": 100 * (base_total - optimized_total) / base_total
        if base_total
        else 0,
        "baseline_cost": float(np.dot(baseline_estimated, scenario.prices)),
        "estimated_cost": float(np.dot(estimated_energy, scenario.prices)),
        "temperatures_c": result["temperatures_c"],
        "comfort_violations": sum(v > 1e-5 for v in result["comfort_violation_c"]),
        "baseline_comfort_violations": sum(
            v > 1e-5 for v in baseline_result["comfort_violation_c"]
        ),
        "active_constraints": active,
        "forecast_used": baseline_forecast_kw is not None,
        "assumptions": [
            "Single-zone uncalibrated RC response",
            "Perfect supplied weather/occupancy scenario",
            "Forecast supplies baseline residual; control effects come from engineering physics",
            "Illustrative tariff; simulated estimates, not measured savings",
        ],
    }
