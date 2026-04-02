#!/usr/bin/env python3
"""Query PostgreSQL tables. Designed to be called by external agents (Claude Code, Codex, etc.).

Usage:
    python skills/db_query.py '{"table": "attention_tickets", "since": "2026-03-31T00:00:00", "limit": 20}'
    python skills/db_query.py '{"table": "raw_messages", "where": {"source": "bluesky"}, "limit": 10}'
    python skills/db_query.py '{"sql": "SELECT count(*) FROM attention_tickets"}'
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras

ALLOWED_TABLES = {
    "raw_messages",
    "source_clean",
    "vector_embeddings",
    "attention_tickets",
    "annotations",
    "decision_packets",
    "agent_state",
}

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "bluesky"),
    "user": os.environ.get("DB_USER", "bluesky"),
    "password": os.environ.get("DB_PASSWORD", "bluesky"),
}


def run_query(params: dict) -> list[dict]:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Raw SQL mode (read-only safety: reject writes)
            if "sql" in params:
                sql = params["sql"].strip()
                sql_upper = sql.upper()
                if any(kw in sql_upper for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE"]):
                    return [{"error": "Only SELECT queries are allowed"}]
                cur.execute(sql)
                rows = cur.fetchall()
                return [dict(r) for r in rows]

            # Structured query mode
            table = params.get("table", "")
            if table not in ALLOWED_TABLES:
                return [{"error": f"Table '{table}' not in allowed list: {sorted(ALLOWED_TABLES)}"}]

            limit = min(int(params.get("limit", 50)), 500)
            conditions = []
            values = []

            # Since filter
            since = params.get("since")
            if since:
                conditions.append("created_at >= %s")
                values.append(since)

            # Where filter on payload fields
            where = params.get("where", {})
            for key, val in where.items():
                conditions.append(f"payload->>%s = %s")
                values.extend([key, str(val)])

            sql = f"SELECT id, payload, created_at FROM {table}"
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)
            sql += " ORDER BY created_at DESC LIMIT %s"
            values.append(limit)

            cur.execute(sql, values)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python db_query.py '<json_params>'"}))
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    try:
        result = run_query(params)
        print(json.dumps(result, default=default_serializer, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
