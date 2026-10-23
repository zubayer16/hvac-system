"""Publish the existing simulator's events to Kafka, without changing Phase 1 CLI."""

import argparse
import logging
import signal
from collections.abc import Sequence
from datetime import UTC, datetime
from threading import Event
from types import FrameType

from buildingpilot.broker import KafkaPublisher
from buildingpilot.cli import generate
from buildingpilot.domain import demo_buildings
from buildingpilot.logging_config import configure_logging
from buildingpilot.settings import Settings


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic telemetry to Kafka")
    parser.add_argument("--steps", type=int, default=288)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", help="UTC ISO timestamp; defaults to current UTC time")
    parser.add_argument("--fault-building", choices=[b.building_id for b in demo_buildings()])
    parser.add_argument("--missing-every", type=int, default=0)
    args = parser.parse_args(argv)
    try:
        start = (
            datetime.fromisoformat(args.start.replace("Z", "+00:00"))
            if args.start
            else (datetime.now(UTC).replace(microsecond=0))
        )
        if start.utcoffset() != UTC.utcoffset(start):
            raise ValueError("start must be timezone-aware UTC")
        if args.steps < 1 or not 1 <= args.interval_seconds <= 900 or args.missing_every < 0:
            raise ValueError("Invalid simulation arguments")
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging()
    stopped = Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    publisher = KafkaPublisher(Settings.from_env())
    count = 0
    try:
        for event in generate(
            None if args.continuous else args.steps,
            start,
            args.interval_seconds,
            args.seed,
            args.fault_building,
            args.missing_every,
        ):
            if stopped.is_set():
                break
            publisher.publish(event)
            count += 1
            if args.continuous and count % 3 == 0:
                stopped.wait(args.interval_seconds)
    finally:
        publisher.close()
        logging.getLogger(__name__).info("producer_stopped", extra={"details": {"events": count}})


if __name__ == "__main__":
    main()
