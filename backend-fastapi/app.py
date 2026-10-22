from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import time
from collections.abc import Iterable
from decimal import Decimal
from typing import Any
from pathlib import Path

import psycopg2
from fastapi import Body, FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg2.extras import RealDictCursor
from starlette.responses import Response

# The image includes /app/evidence.py; local checkouts use skills/.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parent.parent / "skills"))
from evidence import verify_evidence
from trace_api import clean, cycle_list, observability_status, research_detail, research_list, routing_list, trace_detail
from source_context import source_context


LOGGER = logging.getLogger("backend-fastapi")

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

app = FastAPI(title="AI Attention Proxy API", version="v2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=APP_CORS_ORIGINS if APP_CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_value(val) for key, val in value.items()}
    return value


def normalize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_value(dict(row)) for row in rows]


# ---------------------------------------------------------------------------
# Query functions — adapted for unified envelope payload structure
# payload fields: source, source_id, content_type, timestamp, data, raw, ...
# ---------------------------------------------------------------------------

def query_overview() -> dict[str, Any]:
    with DB_QUERY_LATENCY.labels("overview").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM raw_messages) AS raw_count,
                        (SELECT COUNT(*) FROM source_clean) AS clean_count,
                        (SELECT COUNT(*) FROM attention_tickets) AS ticket_count,
                        (SELECT COUNT(*) FROM annotations) AS annotation_count,
                        (SELECT COUNT(*) FROM decision_packets) AS decision_count,
                        (SELECT COUNT(*) FROM vector_embeddings) AS embedding_count
                """)
                row = dict(cur.fetchone() or {})
                # source breakdown
                cur.execute("""
                    SELECT payload->>'source' AS source, COUNT(*) AS cnt
                    FROM raw_messages GROUP BY payload->>'source'
                """)
                sources = {r["source"]: int(r["cnt"]) for r in cur.fetchall()}
                row["sources"] = sources
                # Summarize persistent working memory in the database. Do not
                # expose full agent state, source documents or thesis snapshots.
                cur.execute("""
                    SELECT agent_id, updated_at,
                        CASE jsonb_typeof(state->'pending_themes')
                            WHEN 'array' THEN jsonb_array_length(state->'pending_themes')
                            WHEN 'object' THEN (
                                SELECT COALESCE(SUM(CASE jsonb_typeof(value)
                                    WHEN 'array' THEN jsonb_array_length(value)
                                    ELSE 1
                                END), 0)
                                FROM jsonb_each(state->'pending_themes')
                            )
                            ELSE 0
                        END AS pending_theme_count,
                        CASE jsonb_typeof(state->'theses')
                            WHEN 'array' THEN jsonb_array_length(state->'theses')
                            WHEN 'object' THEN (SELECT COUNT(*) FROM jsonb_object_keys(state->'theses'))
                            ELSE 0
                        END AS thesis_count
                    FROM agent_state ORDER BY updated_at DESC, agent_id
                """)
                agents = normalize_rows(cur.fetchall())
                mid_ids = {"claude-mid", "codex-mid", os.getenv("MID_AGENT_ID", "claude-mid")}
                strong_ids = {"claude-strong", "codex-strong", os.getenv("STRONG_AGENT_ID", "claude-strong")}
                for agent in agents:
                    agent["role"] = ("mid" if agent["agent_id"] in mid_ids else
                                     "strong" if agent["agent_id"] in strong_ids else None)
                row["agents"] = agents
                return normalize_value(row)


def query_decision_packets(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("decision_packets").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM decision_packets
                    ORDER BY id DESC LIMIT %s
                """, (limit,))
                return normalize_rows(cur.fetchall())


def query_annotations(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("annotations").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM annotations
                    ORDER BY id DESC LIMIT %s
                """, (limit,))
                return normalize_rows(cur.fetchall())


def query_attention_tickets(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("attention_tickets").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM attention_tickets
                    ORDER BY id DESC LIMIT %s
                """, (limit,))
                return normalize_rows(cur.fetchall())


def query_new_decisions(last_seen_id: int, limit: int = 50) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("new_decisions").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM decision_packets
                    WHERE id > %s
                    ORDER BY id ASC LIMIT %s
                """, (last_seen_id, limit))
                return normalize_rows(cur.fetchall())


def query_new_annotations(last_seen_id: int, limit: int = 50) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("new_annotations").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM annotations
                    WHERE id > %s
                    ORDER BY id ASC LIMIT %s
                """, (last_seen_id, limit))
                return normalize_rows(cur.fetchall())


def query_feed(limit: int) -> list[dict[str, Any]]:
    """Unified feed: decisions and annotations merged and sorted."""
    with DB_QUERY_LATENCY.labels("feed").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    (SELECT id, created_at, payload, 'decision_packet' AS item_type
                     FROM decision_packets ORDER BY id DESC LIMIT %s)
                    UNION ALL
                    (SELECT id, created_at, payload, 'annotation' AS item_type
                     FROM annotations ORDER BY id DESC LIMIT %s)
                    ORDER BY created_at DESC LIMIT %s
                """, (limit, limit, limit))
                return normalize_rows(cur.fetchall())


def query_annotations_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    with DB_QUERY_LATENCY.labels("annotations_by_ids").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM annotations
                    WHERE id = ANY(%s)
                    ORDER BY id DESC
                """, (ids,))
                return normalize_rows(cur.fetchall())


def query_ticket_by_id(ticket_id: int) -> dict[str, Any] | None:
    with DB_QUERY_LATENCY.labels("ticket_by_id").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM attention_tickets
                    WHERE id = %s
                """, (ticket_id,))
                row = cur.fetchone()
                return normalize_value(dict(row)) if row else None


def query_tickets_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    with DB_QUERY_LATENCY.labels("tickets_by_ids").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, created_at, payload
                    FROM attention_tickets
                    WHERE id = ANY(%s)
                    ORDER BY id DESC
                """, (ids,))
                return normalize_rows(cur.fetchall())


def query_decision_evidence(decision_id: int) -> dict[str, Any] | None:
    with DB_QUERY_LATENCY.labels("decision_evidence").time():
        conn = get_conn()
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM decision_packets WHERE id = %s", (decision_id,))
                    row = cur.fetchone()
                if row is None:
                    return None
                return verify_evidence(conn, decision_id, row[0])
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# WebSocket broadcast loop
# ---------------------------------------------------------------------------

async def db_call(fn, *args):
    return await asyncio.to_thread(fn, *args)


def _current_max_id(table: str) -> int:
    """Fetch current max id from a table, or 0 if empty / error."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}")  # noqa: S608 – table name is hardcoded by callers
                return int(cur.fetchone()[0])
    except Exception:
        return 0


async def signal_broadcast_loop() -> None:
    # Start from current max so we only broadcast truly new items
    last_decision_id = await asyncio.to_thread(_current_max_id, "decision_packets")
    last_annotation_id = await asyncio.to_thread(_current_max_id, "annotations")
    LOGGER.info("broadcast loop starting: last_decision_id=%d, last_annotation_id=%d",
                last_decision_id, last_annotation_id)
    while True:
        try:
            new_decisions = await db_call(query_new_decisions, last_decision_id)
            for row in new_decisions:
                last_decision_id = max(last_decision_id, int(row["id"]))
                await manager.broadcast({
                    "event": "new_decision",
                    "data": row,
                })
            new_annotations = await db_call(query_new_annotations, last_annotation_id)
            for row in new_annotations:
                last_annotation_id = max(last_annotation_id, int(row["id"]))
                await manager.broadcast({
                    "event": "new_annotation",
                    "data": row,
                })
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("broadcast loop error: %s", exc)
        await asyncio.sleep(max(0.2, POLL_INTERVAL_SECONDS))


@app.on_event("startup")
async def startup() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app.state.broadcast_task = asyncio.create_task(signal_broadcast_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "broadcast_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    API_REQUESTS.labels("/health").inc()
    return {"status": "ok"}


@app.get("/api/overview")
async def api_overview() -> dict[str, Any]:
    API_REQUESTS.labels("/api/overview").inc()
    return await db_call(query_overview)


@app.get("/api/feed")
async def api_feed(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/feed").inc()
    rows = await db_call(query_feed, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/research")
async def api_research(limit: int = Query(default=50, ge=1, le=100)) -> dict[str, Any]:
    return await db_call(research_list, get_conn, limit)


@app.get("/api/research/detail")
async def api_research_detail(agent_id: str = Query(max_length=100),
                              thesis_id: str = Query(max_length=300)) -> Any:
    result = await db_call(research_detail, get_conn, agent_id, thesis_id)
    if result is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return result


@app.get("/api/traces/{kind}/{record_id}")
async def api_trace(kind: str, record_id: int) -> Any:
    if kind not in {"raw", "ticket", "annotation", "decision"} or record_id <= 0:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=400, content={"detail": "unsupported record reference"})
    result = await db_call(trace_detail, get_conn, kind, record_id)
    if result is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return result


@app.get("/api/cycles")
async def api_cycles(limit: int = Query(default=30, ge=1, le=100)) -> dict[str, Any]:
    return await db_call(cycle_list, limit)


@app.get("/api/observability")
async def api_observability() -> dict[str, Any]:
    return await db_call(observability_status)


@app.get("/api/routing")
async def api_routing(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    return await db_call(routing_list, get_conn, limit)


@app.get("/api/raw/{record_id}/context")
async def api_source_context(record_id: int, limit: int = Query(default=30, ge=1, le=50)) -> Any:
    if record_id <= 0:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=400, content={"detail": "raw record ID must be positive"})
    result = await db_call(source_context, get_conn, record_id, limit)
    if result is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": "not found"})
    # Same browser display policy as other trace endpoints. Hashes were computed
    # from complete authoritative originals before any display redaction.
    result["warnings"].append("Displayed source fields may be redacted or bounded; raw hashes cover the complete stored originals.")
    return clean(result)


@app.get("/api/decisions")
async def api_decisions(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/decisions").inc()
    rows = await db_call(query_decision_packets, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/annotations")
async def api_annotations(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/annotations").inc()
    rows = await db_call(query_annotations, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/decisions/{decision_id}/evidence")
async def api_decision_evidence(decision_id: int) -> Any:
    API_REQUESTS.labels("/api/decisions/{id}/evidence").inc()
    result = await db_call(query_decision_evidence, decision_id)
    if result is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return result


@app.get("/api/tickets/by-ids")
async def api_tickets_by_ids(ids: str = Query(default="")) -> dict[str, Any]:
    API_REQUESTS.labels("/api/tickets/by-ids").inc()
    parsed = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    rows = await db_call(query_tickets_by_ids, parsed[:50])
    return {"items": rows, "count": len(rows)}


@app.get("/api/annotations/by-ids")
async def api_annotations_by_ids(ids: str = Query(default="")) -> dict[str, Any]:
    API_REQUESTS.labels("/api/annotations/by-ids").inc()
    parsed = [int(x) for x in ids.split(",") if x.strip().isdigit()]
    rows = await db_call(query_annotations_by_ids, parsed[:50])
    return {"items": rows, "count": len(rows)}


@app.get("/api/tickets")
async def api_tickets(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/tickets").inc()
    rows = await db_call(query_attention_tickets, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/tickets/{ticket_id}")
async def api_ticket_by_id(ticket_id: int) -> Any:
    API_REQUESTS.labels("/api/tickets/{id}").inc()
    row = await db_call(query_ticket_by_id, ticket_id)
    if row is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": "not found"})
    return row


# Legacy endpoints for backwards compat
@app.get("/api/signals")
async def api_signals(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_decisions(limit=limit)


@app.get("/api/recent-signals")
async def api_recent_signals(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_decisions(limit=limit)


@app.get("/api/insights")
async def api_insights(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_annotations(limit=limit)


@app.get("/api/recent-insights")
async def api_recent_insights(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_annotations(limit=limit)


@app.get("/api/decision-chains")
async def api_decision_chains(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    return await api_decisions(limit=limit)


@app.post("/api/frontend/vitals")
async def api_frontend_vitals(payload: dict[str, Any] = Body(default={})) -> dict[str, str]:
    API_REQUESTS.labels("/api/frontend/vitals").inc()
    metric_name = str(payload.get("name") or "").strip().upper()
    rating = str(payload.get("rating") or "unknown").strip().lower()
    value = payload.get("value")
    if not metric_name:
        FRONTEND_VITALS_TOTAL.labels(metric_name="unknown", status="invalid").inc()
        return {"status": "ignored"}
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        FRONTEND_VITALS_TOTAL.labels(metric_name=metric_name, status="invalid").inc()
        return {"status": "ignored"}

    FRONTEND_VITALS.labels(metric_name=metric_name, rating=rating).observe(max(0.0, numeric_value))
    FRONTEND_VITALS_TOTAL.labels(metric_name=metric_name, status="ok").inc()
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
    finally:
        await manager.disconnect(websocket)
