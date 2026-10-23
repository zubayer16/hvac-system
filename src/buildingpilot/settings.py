"""Environment configuration shared by independently running processes."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True)
class Settings:
    database: Path = Path("data/buildingpilot.db")
    transport: Literal["kafka", "local"] = "kafka"
    bootstrap_servers: str = "127.0.0.1:9092"
    topic: str = "buildingpilot.telemetry.v1"
    fault_topic: str = "buildingpilot.faults.v1"
    group_id: str = "buildingpilot-ingestion-v1"
    lateness_seconds: int = 900

    def __post_init__(self) -> None:
        if self.transport not in ("kafka", "local") or self.lateness_seconds < 0:
            raise ValueError("Invalid transport or lateness configuration")

    @classmethod
    def from_env(cls) -> "Settings":
        transport = os.getenv("BP_TRANSPORT", "kafka")
        if transport not in ("kafka", "local"):
            raise ValueError("BP_TRANSPORT must be kafka or local")
        return cls(
            database=Path(os.getenv("BP_DATABASE", "data/buildingpilot.db")),
            transport="local" if transport == "local" else "kafka",
            bootstrap_servers=os.getenv("BP_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topic=os.getenv("BP_KAFKA_TOPIC", "buildingpilot.telemetry.v1"),
            fault_topic=os.getenv("BP_KAFKA_FAULT_TOPIC", "buildingpilot.faults.v1"),
            group_id=os.getenv("BP_KAFKA_GROUP_ID", "buildingpilot-ingestion-v1"),
            lateness_seconds=int(os.getenv("BP_ALLOWED_LATENESS_SECONDS", "900")),
        )
