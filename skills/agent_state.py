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


class StateValidationError(ValueError):
    """A proposed state update would discard durable research identities."""


def _record_ids(records: list, field: str) -> set[str]:
    identity = "theme_id" if field == "pending_themes" else "thesis_id"
    result = set()
    for record in records:
        value = record.get(identity) if isinstance(record, dict) else None
        if not isinstance(value, str) or not value.strip():
            raise StateValidationError(f"{field} list records require a nonempty {identity}")
        if value in result:
            raise StateValidationError(f"{field} list records require unique {identity} values")
        result.add(value)
    return result


def _collection_ids(value, field: str) -> set[tuple[str, str]]:
    if isinstance(value, list):
        return {("record", key) for key in _record_ids(value, field)}
    if not isinstance(value, dict):
        raise StateValidationError(f"{field} must remain a list or object")
    result = set()
    identity = "theme_id" if field == "pending_themes" else "thesis_id"
    for key, record in value.items():
        if not isinstance(key, str) or not key.strip():
            raise StateValidationError(f"{field} object keys must be nonempty strings")
        result.add(("key", key))
        if isinstance(record, list):
            # Older Mid state also groups theme records under batch keys.
            result.update(("record", item) for item in _record_ids(record, field))
        elif isinstance(record, dict):
            if identity in record:
                item = record[identity]
                if not isinstance(item, str) or not item.strip():
                    raise StateValidationError(f"{field} records require a nonempty {identity}")
                result.add(("record", item))
        else:
            raise StateValidationError(f"{field} object values must be records or record lists")
    return result


def validate_state_update(current: dict, proposed: dict) -> None:
    """Retain collection identities while allowing record contents to be compacted."""
    if not isinstance(current, dict) or not isinstance(proposed, dict):
        raise StateValidationError("Agent state must be an object")
    for field in ("pending_themes", "theses"):
        if field not in current and field not in proposed:
            continue
        if field not in proposed:
            raise StateValidationError(f"State update cannot remove {field}")
        new_ids = _collection_ids(proposed[field], field)
        if field in current:
            if type(current[field]) is not type(proposed[field]):
                raise StateValidationError(f"State update cannot change the {field} container type")
            missing = _collection_ids(current[field], field) - new_ids
            if missing:
                raise StateValidationError(f"State update cannot drop {len(missing)} existing {field} identities")

    field = "handled_ticket_ids"
    if field in current or field in proposed:
        if field not in proposed:
            raise StateValidationError(f"State update cannot remove {field}")
        for value in (current.get(field, []), proposed[field]):
            if not isinstance(value, list) or any(type(item) is not int or item <= 0 for item in value):
                raise StateValidationError(f"{field} must be a list of positive integer IDs")
        if not set(current.get(field, [])) <= set(proposed[field]):
            raise StateValidationError(f"State update cannot drop existing {field}")


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


def _write_state(agent_id: str, state: dict, *, merge: bool) -> dict:
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise StateValidationError("agent_id must be a nonempty string")
    if not isinstance(state, dict):
        raise StateValidationError("Agent state must be an object")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                # The insert also serializes first writers; a failed validation
                # rolls this placeholder back together with the whole transaction.
                cur.execute(
                    "INSERT INTO agent_state (agent_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (agent_id,),
                )
                cur.execute("SELECT state FROM agent_state WHERE agent_id = %s FOR UPDATE", (agent_id,))
                current = cur.fetchone()[0]
                updated = deep_merge(current, state) if merge else state
                validate_state_update(current, updated)
                cur.execute(
                    "UPDATE agent_state SET state = %s, updated_at = clock_timestamp() WHERE agent_id = %s",
                    (json.dumps(updated, allow_nan=False), agent_id),
                )
        return {"agent_id": agent_id, "state": updated, "status": "ok"}
    finally:
        conn.close()


def set_state(agent_id: str, state: dict) -> dict:
    return _write_state(agent_id, state, merge=False)


def merge_state(agent_id: str, partial: dict) -> dict:
    return _write_state(agent_id, partial, merge=True)


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

    try:
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
    except StateValidationError as exc:
        print(json.dumps({"error": str(exc), "status": "rejected"}))
        sys.exit(1)

    def default_ser(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Not serializable: {type(obj)}")

    print(json.dumps(result, indent=2, default=default_ser, ensure_ascii=False))


if __name__ == "__main__":
    main()
