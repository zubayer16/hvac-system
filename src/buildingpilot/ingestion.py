"""Validate broker bytes and preserve malformed input in a durable quarantine."""

import logging

from pydantic import ValidationError

from buildingpilot.contracts import TELEMETRY
from buildingpilot.metrics import INGESTION
from buildingpilot.storage import Outcome, Repository

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, repository: Repository, lateness_seconds: int = 900) -> None:
        self.repository = repository
        self.lateness_seconds = lateness_seconds

    def ingest(
        self,
        raw: bytes,
        record_id: str,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> Outcome:
        event = None
        reason = None
        try:
            event = TELEMETRY.validate_json(raw)
        except ValidationError as exc:
            # Do not duplicate potentially sensitive raw input in logs/error messages.
            reason = str(exc.errors(include_input=False, include_context=False))[:1000]
        result = self.repository.persist(
            raw,
            record_id,
            event,
            reason,
            self.lateness_seconds,
            topic=topic,
            partition=partition,
            offset=offset,
        )
        logger.info("ingestion_result", extra={"details": {"outcome": result, "topic": topic}})
        INGESTION.labels(outcome=result).inc()
        return result
