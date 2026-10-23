"""Small durable local operational store, not the Phase 3 analytical lakehouse."""

import base64
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from buildingpilot.contracts import BUILDING, TELEMETRY
from buildingpilot.domain import Building, Telemetry, demo_buildings

Outcome = Literal["accepted", "duplicate", "rejected"]


class ConflictError(ValueError):
    pass


def canonical(event: Telemetry) -> str:
    return json.dumps(event.to_dict(), sort_keys=True, allow_nan=False)


def utc_text(value: datetime) -> str:
    if value.utcoffset() != timedelta(0):
        raise ValueError("Use timezone-aware UTC")
    return value.isoformat(timespec="microseconds")


class Repository:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS buildings (
                    building_id TEXT PRIMARY KEY, config TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telemetry (
                    event_id TEXT PRIMARY KEY,
                    building_id TEXT NOT NULL REFERENCES buildings(building_id),
                    equipment_id TEXT NOT NULL, timestamp TEXT NOT NULL,
                    received_at TEXT NOT NULL, is_late INTEGER NOT NULL,
                    out_of_order INTEGER NOT NULL, payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS telemetry_history
                    ON telemetry(building_id, timestamp, event_id);
                CREATE TABLE IF NOT EXISTS watermarks (
                    building_id TEXT NOT NULL, equipment_id TEXT NOT NULL,
                    max_event_time TEXT NOT NULL,
                    PRIMARY KEY (building_id, equipment_id)
                );
                CREATE TABLE IF NOT EXISTS receipts (
                    record_id TEXT PRIMARY KEY, received_at TEXT NOT NULL,
                    raw_base64 TEXT NOT NULL, outcome TEXT NOT NULL,
                    reason TEXT, event_id TEXT, topic TEXT, partition_id INTEGER,
                    offset_id INTEGER
                );
            """)
            for building in demo_buildings():
                db.execute(
                    "INSERT OR IGNORE INTO buildings VALUES (?, ?)",
                    (building.building_id, json.dumps(asdict(building))),
                )

    def buildings(self) -> list[Building]:
        with self.connection() as db:
            return [
                BUILDING.validate_json(row[0])
                for row in db.execute("SELECT config FROM buildings ORDER BY building_id")
            ]

    def building(self, building_id: str) -> Building:
        with self.connection() as db:
            row = db.execute(
                "SELECT config FROM buildings WHERE building_id=?", (building_id,)
            ).fetchone()
        if row is None:
            raise KeyError(building_id)
        return BUILDING.validate_json(row[0])

    def register(self, building: Building, *, update: bool = False) -> None:
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                "SELECT config FROM buildings WHERE building_id=?", (building.building_id,)
            ).fetchone()
            if update and current is None:
                raise KeyError(building.building_id)
            if not update and current is not None:
                raise ConflictError("Building already registered")
            if current is not None:
                old = BUILDING.validate_json(current[0])
                if old.equipment_id != building.equipment_id:
                    raise ConflictError(
                        "Equipment identity cannot be changed by configuration update"
                    )
            db.execute(
                "INSERT INTO buildings VALUES (?, ?) ON CONFLICT(building_id) "
                "DO UPDATE SET config=excluded.config",
                (building.building_id, json.dumps(asdict(building))),
            )

    def persist(
        self,
        raw: bytes,
        record_id: str,
        event: Telemetry | None,
        reason: str | None,
        lateness_seconds: int,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> Outcome:
        """Raw receipt, event and watermark are committed atomically before Kafka ACK."""
        encoded = base64.b64encode(raw).decode("ascii")
        received_at = utc_text(datetime.now(UTC))
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            receipt = db.execute(
                "SELECT outcome, raw_base64 FROM receipts WHERE record_id=?", (record_id,)
            ).fetchone()
            if receipt is not None:
                if receipt[1] != encoded:
                    raise ConflictError("Transport position was reused with different bytes")
                return cast(Outcome, receipt[0])
            outcome: Outcome = "rejected"
            if event is not None:
                building = db.execute(
                    "SELECT config FROM buildings WHERE building_id=?", (event.building_id,)
                ).fetchone()
                if building is None:
                    reason = "Unregistered building"
                elif BUILDING.validate_json(building[0]).equipment_id != event.equipment_id:
                    reason = "Equipment does not match building registration"
                else:
                    previous = db.execute(
                        "SELECT payload FROM telemetry WHERE event_id=?", (event.event_id,)
                    ).fetchone()
                    payload = canonical(event)
                    if previous is not None:
                        if previous[0] == payload:
                            outcome = "duplicate"
                        else:
                            reason = "Conflicting payload for existing event_id"
                    else:
                        timestamp = utc_text(event.timestamp)
                        watermark = db.execute(
                            "SELECT max_event_time FROM watermarks "
                            "WHERE building_id=? AND equipment_id=?",
                            (event.building_id, event.equipment_id),
                        ).fetchone()
                        out_of_order = watermark is not None and timestamp < watermark[0]
                        late = watermark is not None and event.timestamp < (
                            datetime.fromisoformat(watermark[0])
                            - timedelta(seconds=lateness_seconds)
                        )
                        db.execute(
                            "INSERT INTO telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                event.event_id,
                                event.building_id,
                                event.equipment_id,
                                timestamp,
                                received_at,
                                int(late),
                                int(out_of_order),
                                payload,
                            ),
                        )
                        db.execute(
                            "INSERT INTO watermarks VALUES (?, ?, ?) "
                            "ON CONFLICT(building_id, equipment_id) DO UPDATE SET "
                            "max_event_time=MAX(max_event_time, excluded.max_event_time)",
                            (event.building_id, event.equipment_id, timestamp),
                        )
                        outcome = "accepted"
            db.execute(
                "INSERT INTO receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record_id,
                    received_at,
                    encoded,
                    outcome,
                    reason,
                    event.event_id if event else None,
                    topic,
                    partition,
                    offset,
                ),
            )
        return outcome

    def history(
        self,
        building_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
        latest: bool = False,
    ) -> list[dict[str, object]]:
        clauses = ["building_id=?"]
        parameters: list[str | int] = [building_id]
        if start is not None:
            clauses.append("timestamp>=?")
            parameters.append(utc_text(start))
        if end is not None:
            clauses.append("timestamp<?")
            parameters.append(utc_text(end))
        parameters.extend([limit, offset])
        order = "timestamp DESC, event_id DESC" if latest else "timestamp, event_id"
        with self.connection() as db:
            rows = db.execute(
                "SELECT payload, received_at, is_late, out_of_order FROM telemetry "
                f"WHERE {' AND '.join(clauses)} ORDER BY {order} LIMIT ? OFFSET ?",
                parameters,
            ).fetchall()
        return [
            {
                "telemetry": TELEMETRY.validate_json(row[0]).to_dict(),
                "received_at": row[1],
                "is_late": bool(row[2]),
                "out_of_order": bool(row[3]),
            }
            for row in rows
        ]

    def status(self) -> dict[str, object]:
        with self.connection() as db:
            counts = {
                row[0]: row[1]
                for row in db.execute("SELECT outcome, COUNT(*) FROM receipts GROUP BY outcome")
            }
            events = db.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0]
            late = db.execute("SELECT COUNT(*) FROM telemetry WHERE is_late=1").fetchone()[0]
            marks = [dict(row) for row in db.execute("SELECT * FROM watermarks")]
        return {"events": events, "late_events": late, "receipts": counts, "watermarks": marks}
