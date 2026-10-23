"""JSONL adapter; the iterator is independent of storage and wall-clock timing."""

import argparse
import json
import time
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path

from buildingpilot.domain import Telemetry, demo_buildings
from buildingpilot.simulator import Simulator


def generate(
    steps: int | None,
    start: datetime,
    interval: int,
    seed: int,
    fault_building: str | None = None,
    missing_every: int = 0,
) -> Iterator[Telemetry]:
    if steps is not None and steps < 1:
        raise ValueError("steps must be positive")
    if missing_every < 0:
        raise ValueError("missing_every must be nonnegative")
    buildings = demo_buildings()
    if fault_building is not None and fault_building not in {b.building_id for b in buildings}:
        raise ValueError("Unknown fault building")
    simulators = [Simulator(b, seed) for b in buildings]
    index = 0
    while steps is None or index < steps:
        timestamp = start + timedelta(seconds=index * interval)
        for simulator in simulators:
            yield simulator.step(
                timestamp,
                interval,
                degraded_heating=simulator.building.building_id == fault_building,
                missing_indoor=missing_every > 0 and (index + 1) % missing_every == 0,
            )
        index += 1


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic HVAC telemetry only")
    parser.add_argument("--steps", type=int, default=288, help="Intervals per building")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", default="2026-01-05T00:00:00Z")
    parser.add_argument("--output", type=Path, default=Path("data/telemetry.jsonl"))
    parser.add_argument("--fault-building", choices=[b.building_id for b in demo_buildings()])
    parser.add_argument("--missing-every", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        if start.utcoffset() != timedelta(0):
            raise ValueError("start must include UTC timezone")
        if not 1 <= args.interval_seconds <= 900 or args.steps < 1 or args.missing_every < 0:
            raise ValueError("Invalid steps, interval or missing-reading frequency")
    except ValueError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    try:
        # Exclusive creation protects existing telemetry from accidental truncation.
        with args.output.open("x", encoding="utf-8") as stream:
            for event in generate(
                None if args.continuous else args.steps,
                start,
                args.interval_seconds,
                args.seed,
                args.fault_building,
                args.missing_every,
            ):
                stream.write(json.dumps(event.to_dict(), allow_nan=False) + "\n")
                count += 1
                if count % 3 == 0:
                    stream.flush()
                    if args.continuous:
                        time.sleep(args.interval_seconds)
    except FileExistsError:
        parser.error("Output already exists; choose a new --output path")
    except KeyboardInterrupt:
        pass
    print(
        json.dumps(
            {"events": count, "buildings": 3, "source": "synthetic", "output": str(args.output)}
        )
    )


if __name__ == "__main__":
    main()
