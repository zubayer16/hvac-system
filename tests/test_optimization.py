import pytest

from buildingpilot.domain import demo_buildings
from buildingpilot.optimization import Scenario, optimize


def test_constraints_forecast_and_baseline() -> None:
    scenario = Scenario(21, (-5.0,) * 4, (60,) * 4, (0.2, 0.3, 0.4, 0.2))
    result = optimize(demo_buildings()[0], scenario, [40.0] * 4)
    assert result["simulation_only"] and result["feasible"] and result["forecast_used"]
    previous = scenario.previous_setpoint_c
    for setpoint in result["setpoints_c"]:
        assert 16 <= setpoint <= 28
        assert abs(setpoint - previous) <= 0.50001
        previous = setpoint
    assert result["comfort_violations"] == 0
    assert result["estimated_cost"] >= 0


def test_infeasible_is_explicit() -> None:
    result = optimize(demo_buildings()[0], Scenario(5, (-40.0,), (120,), (0.2,)))
    assert result["feasible"] is False
    assert result["comfort_violations"] > 0
    assert result["estimated_savings_pct"] == 0
    with pytest.raises(ValueError):
        Scenario(21, (-5.0,), (), (0.2,))
