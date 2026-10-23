"""Separate consumer process; storage errors stop consumption without acknowledging data."""

import logging
import signal
from threading import Event
from types import FrameType

from prometheus_client import start_http_server

from buildingpilot.broker import Record, commit_record, make_consumer, process_record
from buildingpilot.ingestion import IngestionService
from buildingpilot.logging_config import configure_logging
from buildingpilot.metrics import KAFKA_LAG
from buildingpilot.settings import Settings
from buildingpilot.storage import Repository


def main() -> None:
    configure_logging()
    settings = Settings.from_env()
    repository = Repository(settings.database)
    repository.initialize()
    ingestion = IngestionService(repository, settings.lateness_seconds)
    start_http_server(9101)
    stopped = Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = make_consumer(settings)
    try:
        while not stopped.is_set():
            for messages in consumer.poll(timeout_ms=1000, max_records=100).values():
                for message in messages:
                    if stopped.is_set():
                        break
                    record = Record(message.topic, message.partition, message.offset, message.value)
                    process_record(record, ingestion, lambda item: commit_record(consumer, item))
            assigned = consumer.assignment()
            if assigned:
                ends = consumer.end_offsets(assigned)
                KAFKA_LAG.set(
                    sum(
                        max(0, ends[partition] - (consumer.committed(partition) or 0))
                        for partition in assigned
                    )
                )
    except Exception:
        logging.getLogger(__name__).exception("ingestion_worker_failed")
        raise
    finally:
        consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
