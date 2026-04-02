#!/usr/bin/env python3
"""Persistent agent state across trigger cycles.

Usage:
    python skills/agent_state.py '{"agent_id": "claude-mid", "action": "get"}'
    python skills/agent_state.py '{"agent_id": "claude-mid", "action": "set", "state": {"cycle_count": 1}}'
    python skills/agent_state.py '{"agent_id": "claude-mid", "action": "merge", "state": {"pending_themes": {"oil": {"ticket_ids": [1]}}}}'
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "bluesky"),
    "user": os.environ.get("DB_USER", "bluesky"),
    "password": os.environ.get("DB_PASSWORD", "bluesky"),
}


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Lists are replaced, not appended."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_state(agent_id: str) -> dict:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT state, updated_at FROM agent_state WHERE agent_id = %s",
                (agent_id,),
            )
            row = cur.fetchone()
            if row is None:
                return {"agent_id": agent_id, "state": {}, "updated_at": None}
            return {
                "agent_id": agent_id,
                "state": row["state"],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            }
    finally:
        conn.close()


def set_state(agent_id: str, state: dict) -> dict:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO agent_state (agent_id, state, updated_at)
                   VALUES (%s, %s, NOW())
                   ON CONFLICT (agent_id)
                   DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()""",
                (agent_id, json.dumps(state)),
            )
        conn.commit()
        return {"agent_id": agent_id, "state": state, "status": "ok"}
    finally:
        conn.close()


def merge_state(agent_id: str, partial: dict) -> dict:
    current = get_state(agent_id)["state"]
    merged = deep_merge(current, partial)
    return set_state(agent_id, merged)


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: agent_state.py '{\"agent_id\": ..., \"action\": \"get|set|merge\"}'"}, indent=2))
        sys.exit(1)

    params = json.loads(sys.argv[1])
    agent_id = params.get("agent_id")
    action = params.get("action", "get")

    if not agent_id:
        print(json.dumps({"error": "agent_id is required"}))
        sys.exit(1)

    if action == "get":
        result = get_state(agent_id)
    elif action == "set":
        state = params.get("state", {})
        result = set_state(agent_id, state)
    elif action == "merge":
        state = params.get("state", {})
        result = merge_state(agent_id, state)
    else:
        result = {"error": f"Unknown action: {action}. Use get/set/merge."}

    def default_ser(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Not serializable: {type(obj)}")

    print(json.dumps(result, indent=2, default=default_ser, ensure_ascii=False))


if __name__ == "__main__":
    main()
