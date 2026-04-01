from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import psycopg2
from fastapi import Body, FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg2.extras import RealDictCursor
from starlette.responses import Response


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

app = FastAPI(title="Bluesky Strategy API", version="contract-v1")
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


def _to_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def query_recent_signals(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("recent_signals").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, created_at, payload
                    FROM decision_packets
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = normalize_rows(cursor.fetchall())
                return [
                    {
                        "id": row.get("id"),
                        "created_at": row.get("created_at"),
                        "cid": (row.get("payload") or {}).get("cid"),
                        "report": (row.get("payload") or {}).get("report"),
                        "payload": row.get("payload"),
                    }
                    for row in rows
                ]


def query_recent_insights(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("recent_insights").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, created_at, payload
                    FROM annotations
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = normalize_rows(cursor.fetchall())
                return [
                    {
                        "id": row.get("id"),
                        "created_at": row.get("created_at"),
                        "cid": (row.get("payload") or {}).get("cid"),
                        "mid_agent_relevant_cids": (row.get("payload") or {}).get("mid_agent_relevant_cids") or [],
                        "payload": row.get("payload"),
                    }
                    for row in rows
                ]


def query_signal_counts() -> dict[str, int]:
    with DB_QUERY_LATENCY.labels("signal_counts").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM attention_tickets) AS candidate_count,
                        (SELECT COUNT(*) FROM decision_packets) AS final_count
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
                    SELECT id, created_at, payload
                    FROM decision_packets
                    WHERE id > %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (last_seen_id, limit),
                )
                rows = normalize_rows(cursor.fetchall())
                return [
                    {
                        "id": row.get("id"),
                        "created_at": row.get("created_at"),
                        "cid": (row.get("payload") or {}).get("cid"),
                        "report": (row.get("payload") or {}).get("report"),
                        "payload": row.get("payload"),
                    }
                    for row in rows
                ]


def query_decision_chains(limit: int) -> list[dict[str, Any]]:
    with DB_QUERY_LATENCY.labels("decision_chains").time():
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, created_at, payload
                    FROM decision_packets
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                decisions = normalize_rows(cursor.fetchall())
                if not decisions:
                    return []

                all_related_cids: list[str] = []
                chains: list[dict[str, Any]] = []
                for row in decisions:
                    payload = row.get("payload") or {}
                    decision_raw = payload.get("raw") if isinstance(payload, dict) else {}
                    if not isinstance(decision_raw, dict):
                        decision_raw = {}
                    related_cids = _to_str_list(decision_raw.get("mid_agent_relevant_cids"))
                    if not related_cids and payload.get("cid"):
                        related_cids = [str(payload.get("cid"))]
                    all_related_cids.extend(related_cids)
                    chains.append(
                        {
                            "decision": {
                                "id": row.get("id"),
                                "created_at": row.get("created_at"),
                                "cid": payload.get("cid"),
                                "report": payload.get("report"),
                            },
                            "annotations": [
                                {
                                    "cid": decision_raw.get("cid"),
                                    "mid_agent_relevant_cids": related_cids,
                                    "raw_messages": [],
                                }
                            ],
                        }
                    )

                all_related_cids = list(dict.fromkeys(all_related_cids))
                raw_by_cid: dict[str, dict[str, Any]] = {}
                if all_related_cids:
                    cursor.execute(
                        """
                        SELECT payload, raw, created_at
                        FROM raw_messages
                        WHERE payload->>'cid' = ANY(%s::text[])
                        """,
                        (all_related_cids,),
                    )
                    for row in normalize_rows(cursor.fetchall()):
                        payload = row.get("payload") or {}
                        cid = str(payload.get("cid") or "")
                        if not cid:
                            continue
                        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
                        commit = raw.get("commit") if isinstance(raw, dict) else {}
                        record = commit.get("record") if isinstance(commit, dict) else {}
                        raw_by_cid[cid] = {
                            "cid": cid,
                            "created_at": row.get("created_at"),
                            "text": record.get("text"),
                        }

                for chain in chains:
                    for annotation in chain["annotations"]:
                        related_cids = _to_str_list(annotation.get("mid_agent_relevant_cids"))
                        annotation["raw_messages"] = [raw_by_cid[cid] for cid in related_cids if cid in raw_by_cid]
                return chains


async def db_call(fn, *args):
    return await asyncio.to_thread(fn, *args)


async def signal_broadcast_loop() -> None:
    last_seen_id = 0
    while True:
        try:
            new_signals = await db_call(query_new_final_signals, last_seen_id)
            for signal in new_signals:
                last_seen_id = max(last_seen_id, int(signal["id"]))
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
    API_REQUESTS.labels("/health").inc()
    return {"status": "ok"}


@app.get("/api/signals")
async def api_signals(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/signals").inc()
    rows = await db_call(query_recent_signals, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/recent-signals")
async def api_recent_signals(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_signals(limit=limit)


@app.get("/api/insights")
async def api_insights(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/insights").inc()
    rows = await db_call(query_recent_insights, limit)
    return {"items": rows, "count": len(rows)}


@app.get("/api/recent-insights")
async def api_recent_insights(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
    return await api_insights(limit=limit)


@app.get("/api/overview")
async def api_overview() -> dict[str, Any]:
    API_REQUESTS.labels("/api/overview").inc()
    return await db_call(query_signal_counts)


@app.get("/api/decision-chains")
async def api_decision_chains(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    API_REQUESTS.labels("/api/decision-chains").inc()
    rows = await db_call(query_decision_chains, limit)
    return {"items": rows, "count": len(rows)}


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
