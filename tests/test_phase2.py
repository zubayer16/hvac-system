import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from fastapi.testclient import TestClient
from kafka.errors import KafkaTimeoutError

from buildingpilot.api import create_app
from buildingpilot.broker import Record, process_record
from buildingpilot.cli import generate
from buildingpilot.domain import Telemetry, demo_buildings
from buildingpilot.ingestion import IngestionService
from buildingpilot.settings import Settings
from buildingpilot.storage import ConflictError, Repository, canonical

START = datetime(2026, 1, 5, tzinfo=UTC)


@pytest.fixture
def repository(tmp_path: Path) -> Repository:
    repository = Repository(tmp_path / "operational.db")
    repository.initialize()
    return repository


def sample() -> Telemetry:
    return next(generate(1, START, 300, 42))


def test_durable_replay_and_conflicting_id(repository: Repository) -> None:
    event = sample()
    ingest = IngestionService(repository)
    assert ingest.ingest(canonical(event).encode(), "record1") == "accepted"
    # A new connection/process can replay the same transport position after a lost ACK.
    reopened = Repository(repository.path)
    reopened.initialize()
    ingest = IngestionService(reopened)
    assert ingest.ingest(canonical(event).encode(), "record1") == "accepted"
    assert ingest.ingest(canonical(event).encode(), "record2") == "duplicate"
    changed = replace(event, energy_kwh=event.energy_kwh + 1)
    assert ingest.ingest(canonical(changed).encode(), "record3") == "rejected"
    assert reopened.status()["events"] == 1
    assert reopened.status()["receipts"] == {"accepted": 1, "duplicate": 1, "rejected": 1}
    with pytest.raises(ConflictError):
        ingest.ingest(b"different bytes", "record1")


def test_concurrent_duplicate_delivery(repository: Repository) -> None:
    ingestion = IngestionService(repository)
    raw = canonical(sample()).encode()
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda index: ingestion.ingest(raw, str(index)), range(12)))
    assert results.count("accepted") == 1
    assert results.count("duplicate") == 11
    assert repository.status()["events"] == 1


@pytest.mark.parametrize("payload", [b"not json", b"\xff", b"{}", b"null", b"[]"])
def test_raw_quarantine(repository: Repository, payload: bytes) -> None:
    import base64

    assert IngestionService(repository).ingest(payload, "bad") == "rejected"
    with repository.connection() as db:
        row = db.execute("SELECT raw_base64, reason FROM receipts").fetchone()
    assert base64.b64decode(row[0]) == payload
    assert row[1]


def test_unknown_building_and_equipment(repository: Repository) -> None:
    ingest = IngestionService(repository)
    assert (
        ingest.ingest(canonical(replace(sample(), building_id="unknown")).encode(), "1")
        == "rejected"
    )
    assert (
        ingest.ingest(canonical(replace(sample(), equipment_id="other")).encode(), "2")
        == "rejected"
    )
    assert repository.status()["events"] == 0


def test_out_of_order_and_late_events(repository: Repository) -> None:
    ingest = IngestionService(repository, lateness_seconds=600)
    event = sample()
    for minutes in (30, 25, 0):
        item = replace(event, event_id=str(minutes), timestamp=START + timedelta(minutes=minutes))
        assert ingest.ingest(canonical(item).encode(), str(minutes)) == "accepted"
    rows = repository.history("office")
    assert [cast(dict[str, object], row["telemetry"])["event_id"] for row in rows] == [
        "0",
        "25",
        "30",
    ]
    assert rows[0]["is_late"] is True
    assert rows[1]["is_late"] is False
    assert rows[1]["out_of_order"] is True
    latest = repository.history("office", latest=True, limit=1)[0]
    assert cast(dict[str, object], latest["telemetry"])["event_id"] == "30"
    assert repository.status()["late_events"] == 1


def test_no_ack_on_storage_failure(repository: Repository, monkeypatch: pytest.MonkeyPatch) -> None:
    commits: list[Record] = []
    record = Record("telemetry", 0, 12, canonical(sample()).encode())

    def fail(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(repository, "persist", fail)
    with pytest.raises(sqlite3.OperationalError):
        process_record(record, IngestionService(repository), commits.append)
    assert commits == []


def test_lost_ack_and_poison_record(repository: Repository) -> None:
    record = Record("telemetry", 1, 12, canonical(sample()).encode())
    ingest = IngestionService(repository)

    def lost_ack(record: Record) -> None:
        raise RuntimeError("lost commit response")

    with pytest.raises(RuntimeError):
        process_record(record, ingest, lost_ack)
    commits: list[Record] = []
    process_record(record, ingest, commits.append)
    poison = Record("telemetry", 1, 13, b"broken payload")
    process_record(poison, ingest, commits.append)
    assert commits == [record, poison]
    assert repository.status()["events"] == 1
    assert repository.status()["receipts"] == {"accepted": 1, "rejected": 1}


def test_transaction_rollback(repository: Repository) -> None:
    with repository.connection() as db:
        db.execute(
            "CREATE TRIGGER simulate_disk_failure BEFORE INSERT ON receipts "
            "BEGIN SELECT RAISE(ABORT, 'disk failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        IngestionService(repository).ingest(canonical(sample()).encode(), "1")
    assert repository.status() == {"events": 0, "late_events": 0, "receipts": {}, "watermarks": []}


def test_api_registration_history_and_restart(tmp_path: Path) -> None:
    settings = Settings(database=tmp_path / "api.db", transport="local")
    with TestClient(create_app(settings)) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").json()["transport"] == "local"
        assert len(client.get("/buildings").json()) == 3
        building = asdict(replace(demo_buildings()[0], building_id="new", equipment_id="new-ahu"))
        assert client.post("/buildings", json=building).status_code == 201
        assert client.post("/buildings", json=building).status_code == 409
        building["setpoint_c"] = 22
        assert client.put("/buildings/new", json=building).status_code == 200
        assert client.get("/buildings/new").json()["setpoint_c"] == 22
        building["equipment_id"] = "different"
        assert client.put("/buildings/new", json=building).status_code == 409
        assert client.get("/buildings/unknown").status_code == 404
        assert client.get("/buildings/office/telemetry/latest").status_code == 404
        for event in generate(3, START, 300, 42):
            response = client.post("/telemetry", json=event.to_dict())
            assert response.status_code == 202
            assert response.json()["delivery"] == "persisted"
            assert response.headers["X-Request-ID"]
        rows = client.get("/buildings/office/telemetry", params={"limit": 2, "offset": 1}).json()
        assert len(rows) == 2
        assert rows[0]["telemetry"]["timestamp"].startswith("2026-01-05T00:05:00")
        bounded = client.get(
            "/buildings/office/telemetry",
            params={"start": "2026-01-05T00:05:00Z", "end": "2026-01-05T00:10:00Z"},
        )
        assert len(bounded.json()) == 1
        assert client.get("/ingestion/status").json()["events"] == 9
        assert "/telemetry" in client.get("/openapi.json").json()["paths"]
        assert client.get("/docs").status_code == 200
    with TestClient(create_app(settings)) as restarted:
        assert restarted.get("/ingestion/status").json()["events"] == 9
        assert restarted.get("/buildings/new").json()["setpoint_c"] == 22
        latest = restarted.get("/buildings/office/telemetry/latest").json()
        assert latest["telemetry"]["timestamp"].startswith("2026-01-05T00:10:00")


@pytest.mark.parametrize(
    "changes",
    [
        {"humidity_pct": 101},
        {"timestamp": "2026-01-05T00:00:00"},
        {"source": "invented"},
        {"energy_kwh": "invalid"},
        {"unexpected_unit": "F"},
        {"schema_version": "2"},
        {"heating_demand_kw": 10, "cooling_demand_kw": 10},
    ],
)
def test_api_invalid_telemetry(tmp_path: Path, changes: dict[str, object]) -> None:
    with TestClient(
        create_app(Settings(database=tmp_path / "api.db", transport="local"))
    ) as client:
        payload = sample().to_dict() | changes
        assert client.post("/telemetry", json=payload).status_code == 422
        assert client.get("/ingestion/status").json()["events"] == 0


def test_api_query_and_config_validation(tmp_path: Path) -> None:
    with TestClient(
        create_app(Settings(database=tmp_path / "api.db", transport="local"))
    ) as client:
        for params in (
            {"limit": 1001},
            {"offset": -1},
            {"start": "2026-01-01"},
            {"start": "2026-01-02T00:00:00Z", "end": "2026-01-01T00:00:00Z"},
        ):
            assert client.get("/buildings/office/telemetry", params=params).status_code == 422
        assert (
            client.post("/buildings", json=asdict(demo_buildings()[0]) | {"cop": 0}).status_code
            == 422
        )
        assert (
            client.post(
                "/telemetry", json=sample().to_dict() | {"building_id": "missing"}
            ).status_code
            == 404
        )


class UnavailablePublisher:
    def publish(self, event: Telemetry) -> None:
        raise KafkaTimeoutError("broker unreachable")

    def ready(self) -> bool:
        return False

    def close(self) -> None:
        pass


def test_api_broker_unavailable(tmp_path: Path) -> None:
    with TestClient(
        create_app(Settings(database=tmp_path / "api.db"), UnavailablePublisher())
    ) as client:
        assert client.get("/health/ready").status_code == 503
        assert client.get("/health/live").status_code == 200
        assert client.post("/telemetry", json=sample().to_dict()).status_code == 503
