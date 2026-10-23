"""Boundary validation leaves the Phase 1 domain independent of Pydantic."""

import re
from dataclasses import dataclass

from pydantic import ConfigDict, TypeAdapter

from buildingpilot.domain import Building, Telemetry


def validate_identifier(value: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is None:
        raise ValueError("Identifiers must be 1..128 ASCII letters/digits/dots/underscores/hyphens")


@dataclass(frozen=True)
class BuildingInput(Building):
    __pydantic_config__ = ConfigDict(extra="forbid", allow_inf_nan=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        validate_identifier(self.building_id)
        validate_identifier(self.equipment_id)


@dataclass(frozen=True)
class TelemetryInput(Telemetry):
    __pydantic_config__ = ConfigDict(extra="forbid", allow_inf_nan=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        for value in (self.event_id, self.building_id, self.equipment_id):
            validate_identifier(value)


BUILDING = TypeAdapter(BuildingInput)
TELEMETRY = TypeAdapter(TelemetryInput)
