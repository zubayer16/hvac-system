import json
import math
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from buildingpilot.cli import generate, main
from buildingpilot.domain import demo_buildings
from buildingpilot.simulator import Simulator, thermal_step

START = datetime(2026, 1, 5, tzinfo=UTC)


def test_thermal_solution() -> None:
    assert thermal_step(20, 0, 0, 0, 100, 5, 1) == pytest.approx(20 * math.exp(-0.05))
    assert thermal_step(20, 0, 100, 0, 100, 5, 1) == pytest.approx(20)
    with pytest.raises(ValueError):
        thermal_step(20, 0, 0, 0, 0, 5, 1)


def test_reproducibility_and_identity() -> None:
    first = list(generate(12, START, 300, 42))
    assert first == list(generate(12, START, 300, 42))
    assert first != list(generate(12, START, 300, 43))
    assert len({e.event_id for e in first}) == 36
    assert {e.building_id for e in first} == {"office", "retail", "school"}
    assert all(e.source == "synthetic" for e in first)
    assert first[0].to_dict()["timestamp"] == "2026-01-05T00:00:00Z"


def test_building_stream_isolation() -> None:
    events = list(generate(3, START, 300, 42))
    solo = Simulator(demo_buildings()[0])
    assert [solo.step(START + timedelta(seconds=300 * i)) for i in range(3)] == events[::3]


def test_missing_reading_preserves_state() -> None:
    a, b = Simulator(demo_buildings()[0]), Simulator(demo_buildings()[0])
    assert a.step(START, missing_indoor=True).indoor_temperature_c is None
    b.step(START)
    assert a.indoor_c == b.indoor_c
    assert list(generate(2, START, 300, 42, missing_every=2))[-1].indoor_temperature_c is None


def test_fault_reduces_delivered_heat() -> None:
    normal, faulty = Simulator(demo_buildings()[0]), Simulator(demo_buildings()[0])
    healthy = normal.step(START)
    fault = faulty.step(START, degraded_heating=True)
    assert fault.fault_label == "heating_degraded"
    assert fault.energy_kwh == healthy.energy_kwh
    assert faulty.indoor_c < normal.indoor_c
    assert fault.supply_air_temperature_c < healthy.supply_air_temperature_c


@pytest.mark.parametrize(
    "changes",
    [
        {"humidity_pct": 101},
        {"energy_kwh": -1},
        {"co2_ppm": 100},
        {"indoor_temperature_c": float("nan")},
        {"fan_speed_pct": float("inf")},
        {"timestamp": datetime(2026, 1, 1)},
        {"event_id": ""},
        {"indoor_temperature_c": 90},
        {"interval_seconds": 0},
    ],
)
def test_invalid_telemetry(changes: dict[str, object]) -> None:
    event = Simulator(demo_buildings()[0]).step(START)
    with pytest.raises(ValueError):
        replace(event, **changes)  # type: ignore[arg-type]


def test_building_validation() -> None:
    with pytest.raises(ValueError):
        replace(demo_buildings()[0], cop=0)
    with pytest.raises(ValueError):
        replace(demo_buildings()[0], thermal_capacity_kwh_k=float("nan"))


def test_time_validation_does_not_mutate_state() -> None:
    sim = Simulator(demo_buildings()[0])
    with pytest.raises(ValueError):
        sim.step(START.replace(tzinfo=None))
    first = sim.step(START)
    with pytest.raises(ValueError):
        sim.step(START)
    assert first == Simulator(demo_buildings()[0]).step(START)
    sim.step(START + timedelta(seconds=300))


def test_month_ranges_and_energy() -> None:
    events = list(generate(31 * 24 * 4, START, 900, 42))
    assert len(events) == 8928
    assert all(e.energy_kwh >= 0 and e.occupancy >= 0 for e in events)
    assert all(
        e.indoor_temperature_c is not None and 10 < e.indoor_temperature_c < 30 for e in events
    )
    assert max(e.occupancy for e in events) > 0
    assert sum(e.energy_kwh for e in events) > 0


def test_cli_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "telemetry.jsonl"
    main(["--steps", "288", "--output", str(output)])
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(rows) == 864
    assert json.loads(capsys.readouterr().out)["events"] == 864
    assert rows[0]["source"] == "synthetic"
    original = output.read_bytes()
    with pytest.raises(SystemExit):
        main(["--output", str(output)])
    assert output.read_bytes() == original


@pytest.mark.parametrize(
    "arguments",
    [
        ["--steps", "0"],
        ["--interval-seconds", "0"],
        ["--start", "2026-01-01"],
        ["--missing-every", "-1"],
    ],
)
def test_cli_rejects_invalid(arguments: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(arguments)
    assert exc.value.code == 2


def test_subprocess_cli(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "buildingpilot.cli",
            "--steps",
            "1",
            "--output",
            str(tmp_path / "end-to-end.jsonl"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout)["events"] == 3
