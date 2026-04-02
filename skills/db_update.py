#!/usr/bin/env python3
"""Update existing rows in PostgreSQL. Designed for agent annotation enrichment.

Usage:
    # Append ticket IDs to an annotation's attention_ticket_ids array
    python skills/db_update.py '{"table": "annotations", "id": 42, "append": {"attention_ticket_ids": [10, 11]}}'

    # Overwrite specific payload fields on an annotation
    python skills/db_update.py '{"table": "annotations", "id": 42, "set": {"analysis": "updated analysis", "urgency": "high"}}'
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras

ALLOWED_TABLES = {"annotations", "decision_packets"}

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "bluesky"),
    "user": os.environ.get("DB_USER", "bluesky"),
    "password": os.environ.get("DB_PASSWORD", "bluesky"),
}


def _validate_ids_exist(conn, table: str, ids: list[int]) -> tuple[list[int], list[int]]:
    """Return (valid, invalid) IDs for the given table."""
    if not ids:
        return [], []
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", (ids,))
        found = {row[0] for row in cur.fetchall()}
    valid = [i for i in ids if i in found]
    invalid = [i for i in ids if i not in found]
    return valid, invalid


def run_update(params: dict) -> dict:
    table = params.get("table", "")
    if table not in ALLOWED_TABLES:
        return {"error": f"Table '{table}' not allowed. Use: {sorted(ALLOWED_TABLES)}"}

    row_id = params.get("id")
    if not row_id:
        return {"error": "id is required"}

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        # Validate appended ticket IDs before updating
        append = params.get("append", {})
        if table == "annotations" and "attention_ticket_ids" in append:
            ticket_ids = append["attention_ticket_ids"]
            if not isinstance(ticket_ids, list):
                ticket_ids = [ticket_ids]
            _, invalid = _validate_ids_exist(conn, "attention_tickets", ticket_ids)
            if invalid:
                return {"error": f"attention_ticket_ids {invalid} do not exist in DB. Query tickets first with db_query.py."}

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Fetch current payload
            cur.execute(f"SELECT id, payload FROM {table} WHERE id = %s", (row_id,))
            row = cur.fetchone()
            if not row:
                return {"error": f"Row {row_id} not found in {table}"}

            payload = row["payload"] or {}

            # Append mode: merge array fields (deduplicated)
            for key, values in append.items():
                if not isinstance(values, list):
                    values = [values]
                existing = payload.get(key, [])
                if not isinstance(existing, list):
                    existing = [existing] if existing else []
                merged = list(dict.fromkeys(existing + values))  # deduplicate, preserve order
                payload[key] = merged

            # Set mode: overwrite specific fields
            set_fields = params.get("set", {})
            for key, value in set_fields.items():
                payload[key] = value

            # Write back
            cur.execute(
                f"UPDATE {table} SET payload = %s WHERE id = %s RETURNING id",
                (json.dumps(payload, ensure_ascii=False), row_id),
            )
            conn.commit()
            return {"status": "ok", "id": row_id, "table": table, "payload": payload}
    finally:
        conn.close()


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python db_update.py '<json_params>'"}))
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    try:
        result = run_update(params)
        print(json.dumps(result, default=default_serializer, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
