"""HTTP boundary: synchronous broker/SQLite work runs in FastAPI's thread pool."""

import logging
import sqlite3
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from typing import Annotated
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from kafka.errors import KafkaError
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from buildingpilot import serving
from buildingpilot.artifacts import artifact_root, load_current
from buildingpilot.broker import KafkaPublisher, LocalPublisher, Publisher
from buildingpilot.contracts import BuildingInput, TelemetryInput
from buildingpilot.ingestion import IngestionService
from buildingpilot.logging_config import configure_logging
from buildingpilot.metrics import HTTP_REQUESTS, HTTP_SECONDS, update_persisted_metrics
from buildingpilot.settings import Settings
from buildingpilot.storage import ConflictError, Repository, utc_text

logger = logging.getLogger(__name__)


class Accepted(BaseModel):
    event_id: str
    transport: str
    delivery: str


class Reading(BaseModel):
    telemetry: TelemetryInput
    received_at: datetime
    is_late: bool
    out_of_order: bool


class ForecastRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    horizon: Annotated[int, Field(ge=1, le=12)] = 4
    outdoor_c: list[Annotated[float, Field(ge=-60, le=80)]] | None = None
    occupancy: list[Annotated[int, Field(ge=0)]] | None = None


class OptimizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    horizon: Annotated[int, Field(ge=1, le=12)] = 4
    prices: list[Annotated[float, Field(ge=0)]] | None = None


def create_app(settings: Settings | None = None, publisher: Publisher | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    repository = Repository(settings.database)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        repository.initialize()
        app.state.publisher = publisher or (
            KafkaPublisher(settings)
            if settings.transport == "kafka"
            else LocalPublisher(IngestionService(repository, settings.lateness_seconds))
        )
        try:
            yield
        finally:
            app.state.publisher.close()

    app = FastAPI(
        title="BuildingPilot AI",
        version="0.2.0",
        lifespan=lifespan,
        description="Phase 2 telemetry API. Synthetic simulation only; no equipment control.",
    )

    @app.middleware("http")
    async def log_request(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = str(uuid4())
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            route = request.scope.get("route")
            route_name = getattr(route, "path", "unmatched")
            HTTP_REQUESTS.labels(request.method, route_name, str(status)).inc()
            HTTP_SECONDS.labels(route_name).observe(time.perf_counter() - start)
            logger.info(
                "http_request",
                extra={
                    "details": {
                        "request_id": request_id,
                        "method": request.method,
                        "route": getattr(route, "path", "unmatched"),
                        "status": status,
                        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
                    }
                },
            )

    @app.exception_handler(KeyError)
    async def unknown(request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Building not found"})

    @app.exception_handler(ConflictError)
    async def conflict(request: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(sqlite3.Error)
    async def storage_error(request: Request, exc: sqlite3.Error) -> JSONResponse:
        logger.exception("storage_unavailable")
        return JSONResponse(status_code=503, content={"detail": "Storage unavailable; retry later"})

    @app.exception_handler(FileNotFoundError)
    async def model_missing(request: Request, exc: FileNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "No promoted model; run training and promotion"}
        )

    @app.exception_handler(ValueError)
    async def invalid_scenario(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        update_persisted_metrics(repository)
        return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    @app.get("/health/ready")
    def ready(request: Request) -> dict[str, str]:
        repository.status()
        active: Publisher = request.app.state.publisher
        try:
            available = active.ready()
        except KafkaError:
            available = False
        if not available:
            raise HTTPException(503, "Kafka unavailable")
        return {"status": "ready", "transport": settings.transport}

    @app.get("/buildings", response_model=list[BuildingInput])
    def buildings() -> list[dict[str, object]]:
        return [asdict(building) for building in repository.buildings()]

    @app.post("/buildings", status_code=201, response_model=BuildingInput)
    def register(building: BuildingInput) -> BuildingInput:
        repository.register(building)
        return building

    @app.get("/buildings/{building_id}", response_model=BuildingInput)
    def configuration(building_id: str) -> dict[str, object]:
        return asdict(repository.building(building_id))

    @app.put("/buildings/{building_id}", response_model=BuildingInput)
    def update(building_id: str, building: BuildingInput) -> BuildingInput:
        if building_id != building.building_id:
            raise HTTPException(422, "Path and payload building_id must match")
        repository.register(building, update=True)
        return building

    @app.post("/telemetry", status_code=202, response_model=Accepted)
    def publish(event: TelemetryInput, request: Request) -> Accepted:
        building = repository.building(event.building_id)
        if building.equipment_id != event.equipment_id:
            raise HTTPException(422, "Equipment does not match registration")
        active: Publisher = request.app.state.publisher
        try:
            active.publish(event)
        except KafkaError as exc:
            raise HTTPException(
                503, "Kafka delivery unconfirmed; retry with same event_id"
            ) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        return Accepted(
            event_id=event.event_id,
            transport=settings.transport,
            delivery="broker_acknowledged" if settings.transport == "kafka" else "persisted",
        )

    @app.get("/buildings/{building_id}/telemetry/latest", response_model=Reading)
    def latest(building_id: str) -> dict[str, object]:
        repository.building(building_id)
        rows = repository.history(building_id, limit=1, latest=True)
        if not rows:
            raise HTTPException(404, "No telemetry for this building")
        return rows[0]

    @app.get("/buildings/{building_id}/telemetry", response_model=list[Reading])
    def history(
        building_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> list[dict[str, object]]:
        repository.building(building_id)
        try:
            for bound in (start, end):
                if bound is not None:
                    utc_text(bound)
            if start is not None and end is not None and start >= end:
                raise ValueError("start must be before end")
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return repository.history(building_id, start=start, end=end, limit=limit, offset=offset)

    @app.get("/ingestion/status")
    def ingestion_status() -> dict[str, object]:
        return repository.status()

    @app.get("/buildings/{building_id}/faults/latest")
    def fault(building_id: str, request: Request) -> dict[str, object]:
        prediction = serving.predict_fault(repository, artifact_root(), building_id)
        active: Publisher = request.app.state.publisher
        if prediction["faulty"] and isinstance(active, KafkaPublisher):
            try:
                active.publish_fault(prediction)
            except KafkaError as exc:
                raise HTTPException(503, "Fault computed; alert delivery unconfirmed") from exc
        return prediction

    @app.post("/buildings/{building_id}/forecasts")
    def forecasts(building_id: str, body: ForecastRequest) -> dict[str, object]:
        return serving.forecast(
            repository, artifact_root(), building_id, body.horizon, body.outdoor_c, body.occupancy
        )

    @app.post("/buildings/{building_id}/optimization")
    def optimization(building_id: str, body: OptimizationRequest) -> dict[str, object]:
        return serving.recommendation(
            repository, artifact_root(), building_id, body.horizon, body.prices
        )

    @app.get("/models")
    def models() -> dict[str, object]:
        result: dict[str, object] = {}
        for family in ("fault", "forecast"):
            try:
                _, metadata = load_current(artifact_root(), family)
                result[family] = metadata
            except FileNotFoundError:
                result[family] = {"status": "unavailable"}
        return result

    return app


app = create_app()
