"""Kafka adapter: acknowledged publication and explicit per-record offset commits."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from kafka.admin import KafkaAdminClient

from buildingpilot.domain import Telemetry
from buildingpilot.ingestion import IngestionService
from buildingpilot.settings import Settings
from buildingpilot.storage import canonical


class Publisher(Protocol):
    def publish(self, event: Telemetry) -> None: ...
    def ready(self) -> bool: ...
    def close(self) -> None: ...


class KafkaPublisher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.topic = settings.topic
        self.client = KafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            client_id="buildingpilot-publisher",
            acks="all",
            enable_idempotence=True,
            max_block_ms=10000,
            request_timeout_ms=10000,
        )

    def publish(self, event: Telemetry) -> None:
        key = f"{event.building_id}:{event.equipment_id}".encode()
        self.client.send(self.topic, key=key, value=canonical(event).encode()).get(timeout=15)

    def publish_fault(self, prediction: dict[str, Any]) -> None:
        payload = prediction | {
            "schema_version": "1.0",
            "event_type": "fault_prediction",
            "source_event_id": prediction["event_id"],
            "event_id": f"fault-{prediction['event_id']}-{prediction['model_version']}",
        }
        self.client.send(
            self.settings.fault_topic,
            key=str(prediction["building_id"]).encode(),
            value=json.dumps(payload, allow_nan=False).encode(),
        ).get(timeout=15)

    def ready(self) -> bool:
        # Use a network request rather than cached producer metadata to detect outages.
        admin = KafkaAdminClient(
            bootstrap_servers=self.settings.bootstrap_servers,
            request_timeout_ms=3000,
            bootstrap_timeout_ms=3000,
        )
        try:
            topics = admin.describe_topics([self.topic, self.settings.fault_topic])
            return bool(
                len(topics) == 2
                and all(topic["error_code"] == 0 and topic["partitions"] for topic in topics)
            )
        finally:
            admin.close()

    def close(self) -> None:
        self.client.close(timeout=10)


class LocalPublisher:
    """Explicit development fallback: persists directly and never claims Kafka delivery."""

    def __init__(self, ingestion: IngestionService) -> None:
        self.ingestion = ingestion

    def publish(self, event: Telemetry) -> None:
        result = self.ingestion.ingest(canonical(event).encode(), f"local:{uuid4()}")
        if result == "rejected":
            raise ValueError("Event was quarantined; inspect the ingestion journal")

    def ready(self) -> bool:
        return True

    def close(self) -> None:
        pass


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    value: bytes


def process_record(
    record: Record,
    ingestion: IngestionService,
    commit: Callable[[Record], None],
) -> None:
    ingestion.ingest(
        record.value,
        f"kafka:{record.topic}:{record.partition}:{record.offset}",
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
    )
    # A crash here causes replay, safely handled by durable receipt/event identities.
    commit(record)


def make_consumer(settings: Settings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.topic,
        bootstrap_servers=settings.bootstrap_servers,
        group_id=settings.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        client_id="buildingpilot-ingestion",
        max_poll_records=100,
    )


def commit_record(consumer: KafkaConsumer, record: Record) -> None:
    consumer.commit(
        {
            TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                record.offset + 1, "", -1
            )
        }
    )
