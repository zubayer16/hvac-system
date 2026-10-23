"""Opt-in test against a real broker; never silently substitute an in-memory broker."""

import json
import os
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from buildingpilot.api import create_app
from buildingpilot.broker import (
    KafkaPublisher,
    Record,
    commit_record,
    make_consumer,
    process_record,
)
from buildingpilot.cli import generate
from buildingpilot.ingestion import IngestionService
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(
        os.getenv("BP_TEST_KAFKA") != "1", reason="Set BP_TEST_KAFKA=1 with a running Kafka broker"
    ),
]


def test_broker_api_and_restart_replay(tmp_path: Path) -> None:
    suffix = uuid4().hex
    settings = Settings(
        database=tmp_path / "integration.db",
        bootstrap_servers=os.getenv("BP_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic=f"buildingpilot-test-{suffix}",
        fault_topic=f"buildingpilot-fault-test-{suffix}",
        group_id=f"buildingpilot-test-{suffix}",
    )
    admin = KafkaAdminClient(bootstrap_servers=settings.bootstrap_servers)
    publisher = None
    consumer = None
    restarted = None
    try:
        admin.create_topics(
            [
                NewTopic(topic, num_partitions=1, replication_factor=1)
                for topic in (settings.topic, settings.fault_topic)
            ]
        )
        repository = Repository(settings.database)
        repository.initialize()
        ingestion = IngestionService(repository)
        events = list(generate(1, datetime(2026, 1, 5, tzinfo=UTC), 300, 42))
        publisher = KafkaPublisher(settings)
        publisher.publish_fault(
            {
                "event_id": events[0].event_id,
                "model_version": "test",
                "building_id": "office",
                "faulty": True,
                "source": "synthetic",
            }
        )
        alerts = KafkaConsumer(
            settings.fault_topic,
            bootstrap_servers=settings.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        )
        try:
            alert = json.loads(next(alerts).value)
            assert alert["event_type"] == "fault_prediction"
            assert alert["source_event_id"] == events[0].event_id
        finally:
            alerts.close(autocommit=False)
        with TestClient(create_app(settings, publisher)) as client:
            assert client.get("/health/ready").status_code == 200
            for event in events:
                assert client.post("/telemetry", json=event.to_dict()).status_code == 202
            publisher.publish(events[0])
            publisher.publish(replace(events[0], energy_kwh=events[0].energy_kwh + 1))
            publisher.client.send(settings.topic, value=b"invalid JSON").get(timeout=15)
            consumer = make_consumer(settings)
            records: list[Record] = []
            deadline = time.monotonic() + 30
            while len(records) < 6 and time.monotonic() < deadline:
                for messages in consumer.poll(timeout_ms=1000).values():
                    for message in messages:
                        records.append(
                            Record(message.topic, message.partition, message.offset, message.value)
                        )
            assert len(records) == 6
            # Persist the first record and simulate process death before committing its offset.
            first = records[0]
            ingestion.ingest(
                first.value,
                f"kafka:{first.topic}:{first.partition}:{first.offset}",
                topic=first.topic,
                partition=first.partition,
                offset=first.offset,
            )
            consumer.close(autocommit=False)
            consumer = None
            restarted = make_consumer(settings)
            replayed: list[Record] = []
            deadline = time.monotonic() + 30
            while len(replayed) < 6 and time.monotonic() < deadline:
                for messages in restarted.poll(timeout_ms=1000).values():
                    for message in messages:
                        record = Record(
                            message.topic, message.partition, message.offset, message.value
                        )
                        process_record(
                            record, ingestion, lambda item: commit_record(restarted, item)
                        )
                        replayed.append(record)
            assert len(replayed) == 6
            assert replayed[0].offset == first.offset
            assert restarted.committed(TopicPartition(settings.topic, 0)) == 6
            assert client.get("/ingestion/status").json()["events"] == 3
            assert client.get("/ingestion/status").json()["receipts"] == {
                "accepted": 3,
                "duplicate": 1,
                "rejected": 2,
            }
            for event in events:
                response = client.get(f"/buildings/{event.building_id}/telemetry/latest")
                assert response.status_code == 200
                assert response.json()["telemetry"]["event_id"] == event.event_id
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
        if restarted is not None:
            restarted.close(autocommit=False)
        if publisher is not None:
            publisher.close()
        admin.delete_topics([settings.topic, settings.fault_topic])
        admin.close()
