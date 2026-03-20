from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg2
from fastapi import Body, FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg2.extras import RealDictCursor
from starlette.responses import Response


LOGGER = logging.getLogger("backend-fastapi")
TRACER = trace.get_tracer("backend.fastapi")

DB_SETTINGS = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "bluesky"),
    "user": os.getenv("DB_USER", "bluesky"),
    "password": os.getenv("DB_PASSWORD", "bluesky"),
}

APP_CORS_ORIGINS = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "*").split(",") if origin.strip()]
POLL_INTERVAL_SECONDS = float(os.getenv("WS_POLL_INTERVAL_SECONDS", "1.0"))

API_REQUESTS = Counter("fastapi_requests_total", "Total API requests", ["endpoint"])
WS_PUSHED = Counter("ws_signals_pushed_total", "Total websocket pushed signals")
DB_QUERY_LATENCY = Histogram("fastapi_db_query_latency_seconds", "DB query latency seconds", ["query_name"])
SIGNAL_E2E_LATENCY = Histogram(
    "signal_end_to_end_latency_seconds",
    "End-to-end latency from event timestamp to websocket push visibility.",
)
FRONTEND_VITALS = Histogram(
    "frontend_web_vitals_value",
    "Frontend Web Vitals values reported by browser clients.",
    ("metric_name", "rating"),
)
FRONTEND_VITALS_TOTAL = Counter(
    "frontend_web_vitals_total",
    "Total frontend Web Vitals reports received.",
    ("metric_name", "status"),
)

app = FastAPI(title="Bluesky Strategy API", version="m4-v1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=APP_CORS_ORIGINS if APP_CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def init_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/") + "/v1/traces"
    )
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    LOGGER.info("otel tracing enabled endpoint=%s service=%s", endpoint, service_name)


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self.lock:
            self.connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self.lock:
            self.connections.discard(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        async with self.lock:
            targets = list(self.connections)
        for ws in targets:
            try:
                await ws.send_json(payload)
                WS_PUSHED.inc()
            except Exception:
                await self.disconnect(ws)


manager = ConnectionManager()


def get_conn():
    return psycopg2.connect(**DB_SETTINGS)


def normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(val) for key, val in value.items()}
    return value


def normalize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_value(dict(row)) for row in rows]


def parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def query_recent_signals(limit: int, signal_type: str | None) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("recent_signals").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if signal_type:
                    cursor.execute(
                        """
                        SELECT id, signal_type, author_did, uri, score, reasons, tickers, sentiment_score,
                               event_ts, created_at, payload
                        FROM strategy_signals
                        WHERE signal_type = %s
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        (signal_type, limit),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT id, signal_type, author_did, uri, score, reasons, tickers, sentiment_score,
                               event_ts, created_at, payload
                        FROM strategy_signals
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                return normalize_rows(cursor.fetchall())


def query_recent_insights(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("recent_insights").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, model_name, summary, risk_tags, recommendation, confidence, created_at, payload
                    FROM llm_insights
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return normalize_rows(cursor.fetchall())


def query_signal_counts() -> dict[str, int]:
    with DB_QUERY_LATENCY.labels("signal_counts").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE signal_type = 'candidate') AS candidate_count,
                        COUNT(*) FILTER (WHERE signal_type = 'final') AS final_count
                    FROM strategy_signals
                    """
                )
                row = cursor.fetchone() or {}
                return {
                    "candidate_count": int(row.get("candidate_count") or 0),
                    "final_count": int(row.get("final_count") or 0),
                }


def query_new_final_signals(last_seen_id: int, limit: int = 100) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("new_final_signals").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, signal_type, author_did, uri, score, reasons, tickers, sentiment_score,
                           event_ts, created_at, payload
                    FROM strategy_signals
                    WHERE signal_type = 'final' AND id > %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (last_seen_id, limit),
                )
                return normalize_rows(cursor.fetchall())


async def db_call(fn, *args):
    return await asyncio.to_thread(fn, *args)


async def signal_broadcast_loop() -> None:
    last_seen_id = 0
    try:
        latest = await db_call(query_recent_signals, 1, "final")
        if latest:
            last_seen_id = int(latest[0]["id"])
    except Exception as exc:
        LOGGER.warning("failed to bootstrap websocket poll state: %s", exc)

    while True:
        try:
            new_signals = await db_call(query_new_final_signals, last_seen_id)
            for signal in new_signals:
                last_seen_id = max(last_seen_id, int(signal["id"]))
                with TRACER.start_as_current_span("api.ws.push") as span:
                    span.set_attribute("signal.id", int(signal["id"]))
                    span.set_attribute("signal.type", str(signal.get("signal_type") or ""))
                    event_ts = parse_timestamp(signal.get("event_ts"))
                    if event_ts:
                        latency_seconds = max(
                            0.0,
                            (datetime.now(tz=timezone.utc) - event_ts.astimezone(timezone.utc)).total_seconds(),
                        )
                        SIGNAL_E2E_LATENCY.observe(latency_seconds)
                        span.set_attribute("signal.end_to_end_latency_seconds", latency_seconds)
                    await manager.broadcast({"event": "strategy_signal_final", "data": signal})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("websocket broadcast loop failed: %s", exc)
        await asyncio.sleep(max(0.2, POLL_INTERVAL_SECONDS))


@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_tracing(os.getenv("OTEL_SERVICE_NAME", "backend-fastapi"))
    app.state.broadcast_task = asyncio.create_task(signal_broadcast_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "broadcast_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@app.get("/health")
async def health() -> dict[str, str]:
    with TRACER.start_as_current_span("api.health"):
        API_REQUESTS.labels("/health").inc()
        return {"status": "ok"}


@app.get("/api/signals")
async def api_signals(
    limit: int = Query(default=50, ge=1, le=500),
    signal_type: str | None = Query(default="final"),
) -> dict[str, Any]:
    with TRACER.start_as_current_span("api.signals.query") as span:
        API_REQUESTS.labels("/api/signals").inc()
        normalized_signal_type = signal_type if signal_type in {"candidate", "final"} else None
        span.set_attribute("signals.limit", limit)
        span.set_attribute("signals.filter_type", normalized_signal_type or "all")
        rows = await db_call(query_recent_signals, limit, normalized_signal_type)
        span.set_attribute("signals.result_count", len(rows))
        return {"items": rows, "count": len(rows)}


@app.get("/api/insights")
async def api_insights(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    with TRACER.start_as_current_span("api.insights.query") as span:
        API_REQUESTS.labels("/api/insights").inc()
        span.set_attribute("insights.limit", limit)
        rows = await db_call(query_recent_insights, limit)
        span.set_attribute("insights.result_count", len(rows))
        return {"items": rows, "count": len(rows)}


@app.get("/api/overview")
async def api_overview() -> dict[str, Any]:
    with TRACER.start_as_current_span("api.overview.query"):
        API_REQUESTS.labels("/api/overview").inc()
        counts = await db_call(query_signal_counts)
        return counts


@app.post("/api/frontend/vitals")
async def api_frontend_vitals(payload: dict[str, Any] = Body(default={})) -> dict[str, str]:
    with TRACER.start_as_current_span("api.frontend.vitals") as span:
        API_REQUESTS.labels("/api/frontend/vitals").inc()
        metric_name = str(payload.get("name") or "").strip().upper()
        rating = str(payload.get("rating") or "unknown").strip().lower()
        value = payload.get("value")
        if not metric_name:
            FRONTEND_VITALS_TOTAL.labels(metric_name="unknown", status="invalid").inc()
            span.set_attribute("frontend.metric.valid", False)
            return {"status": "ignored"}
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            FRONTEND_VITALS_TOTAL.labels(metric_name=metric_name, status="invalid").inc()
            span.set_attribute("frontend.metric.name", metric_name)
            span.set_attribute("frontend.metric.valid", False)
            return {"status": "ignored"}

        FRONTEND_VITALS.labels(metric_name=metric_name, rating=rating).observe(max(0.0, numeric_value))
        FRONTEND_VITALS_TOTAL.labels(metric_name=metric_name, status="ok").inc()
        span.set_attribute("frontend.metric.name", metric_name)
        span.set_attribute("frontend.metric.rating", rating)
        span.set_attribute("frontend.metric.valid", True)
        return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.websocket("/ws/signals")
async def ws_signals(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await manager.disconnect(websocket)

