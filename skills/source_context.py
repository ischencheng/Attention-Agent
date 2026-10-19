#!/usr/bin/env python3
"""Read locally collected reply ancestors from authoritative source originals.

Usage: python skills/source_context.py '{"raw_id":42,"limit":30}'
No network source fetches, author/time similarity joins, or inferred thread IDs.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import json
import os
import re
import sys

import psycopg2
from psycopg2.extras import RealDictCursor

from evidence import digest


MAX_RECORDS = 50
MAX_RAW_BYTES = 65536
BOUNDARY = ("Current locally collected source context; surrounding records were not necessarily "
            "read or cited when a decision was made. Native reply references identify messages, "
            "not a particular historical version of their content.")


def native_id(value):
    if type(value) is int:
        value = str(value)
    return value if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,31}", value) else None


def extract_native_context(source, source_id, raw):
    """Inspect known root fields only; embedded quoted posts are other messages."""
    result = {"message_id": None, "parent_message_id": None, "event_ts": None,
              "thread_id": None, "chat_id": None, "identity_status": "unsupported_source"}
    if source != "twitter" or not isinstance(raw, dict):
        return result
    message = native_id(raw.get("rest_id"))
    reply = raw.get("reply_to_results")
    parent = native_id(reply.get("rest_id")) if isinstance(reply, dict) else None
    details = raw.get("details")
    milliseconds = details.get("created_at_ms") if isinstance(details, dict) else None
    if type(milliseconds) in {int, float}:
        try:
            result["event_ts"] = datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    result.update({"message_id": message, "parent_message_id": parent,
                   "identity_status": "verified_native_identity" if message and source_id == f"twitter-{message}"
                                      else "missing_native_identity" if message is None else "identity_mismatch"})
    return result


def source_text(raw):
    if not isinstance(raw, dict):
        return None
    note = raw
    for key in ("note_tweet", "note_tweet_results", "result"):
        note = note.get(key, {}) if isinstance(note, dict) else {}
    details = raw.get("details", {})
    return note.get("text") if isinstance(note, dict) and isinstance(note.get("text"), str) else (
        details.get("full_text") if isinstance(details, dict) and isinstance(details.get("full_text"), str) else None)


def display_raw(raw, context):
    """Keep normal provider records intact; bound unusually large source bodies."""
    if len(json.dumps(raw, ensure_ascii=False, default=str).encode()) <= MAX_RAW_BYTES:
        return raw, False
    if not isinstance(raw, dict):
        return {"display": "Unsupported large source object omitted from this bounded view."}, True
    text = source_text(raw)
    return {"rest_id": context["message_id"],
            "details": {"full_text": text[:16000] if text else None,
                        "created_at_ms": raw["details"].get("created_at_ms") if isinstance(raw.get("details"), dict) else None},
            "reply_to_results": {"rest_id": context["parent_message_id"]}}, True


def record_projection(row, anchor_id):
    envelope = row["payload"] if isinstance(row["payload"], dict) else {}
    raw = row["raw"]
    context = extract_native_context(envelope.get("source"), envelope.get("source_id"), raw)
    body, limited = display_raw(raw, context)
    observed = row["created_at"].isoformat() if isinstance(row["created_at"], datetime) else row["created_at"]
    text = source_text(raw)
    return {"id": row["id"], "created_at": observed, "sha256": digest(raw),
            "hash_status": "computed_from_authoritative_raw", "is_anchor": row["id"] == anchor_id,
            "native_context": context, "display_limited": limited,
            "payload": {"source": envelope.get("source"), "source_id": envelope.get("source_id"),
                        "content_type": envelope.get("content_type"), "timestamp": context["event_ts"],
                        "data": {"text": text[:16000] if text else None, "published_at": context["event_ts"],
                                 "observed_at": observed}, "raw": body}}


def _rows(conn, sql, params):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def source_context(connect, raw_id, limit=30):
    """Return a bounded current-context view in one read-only DB snapshot."""
    if type(raw_id) is not int or raw_id <= 0:
        raise ValueError("raw_id must be a positive integer")
    if type(limit) is not int or not 1 <= limit <= MAX_RECORDS:
        raise ValueError(f"limit must be an integer between 1 and {MAX_RECORDS}")
    conn = connect()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout='5000ms'")
            anchor = _rows(conn, "SELECT id,payload,raw,created_at FROM raw_messages WHERE id=%s", (raw_id,))
            if not anchor:
                return None
            initial = record_projection(anchor[0], raw_id)
            result = {"anchor_raw_id": raw_id, "status": "ok", "coverage": "locally_collected",
                      "context_scope": "recorded_reply_ancestors", "frozen_evidence": False,
                      "records": [initial], "edges": [], "missing_references": [],
                      "identity_conflicts": [], "cycles": [], "warnings": [],
                      "limits": {"requested": limit, "returned": 1, "truncated": False,
                                 "max_records": MAX_RECORDS}, "boundary": BOUNDARY}
            context = initial["native_context"]
            if context["identity_status"] != "verified_native_identity":
                result["status"] = "unsupported" if context["identity_status"] in {"unsupported_source", "missing_native_identity"} else "identity_mismatch"
                result["warnings"].append("No supported, verified native message identity is available; no source context was inferred.")
                return result
            queue = deque([(initial, (context["message_id"],))])
            seen = {raw_id: initial}
            cache = {}
            while queue:
                child, ancestry = queue.popleft()
                native = child["native_context"]
                parent_id = native["parent_message_id"]
                if parent_id is None:
                    continue
                source = child["payload"]["source"]
                source_id = f"twitter-{parent_id}"
                reference = {"from_raw_id": child["id"], "source": source,
                             "source_id": source_id, "native_message_id": parent_id}
                loop = parent_id in ancestry
                if loop:
                    result["cycles"].append({**reference, "reason": "native_reply_cycle"})
                    result["warnings"].append("A native reply cycle was recorded; traversal stopped at that reference.")
                remaining = limit - len(seen)
                # Already displayed versions can close a cycle without another query.
                existing = [record for record in seen.values() if record["native_context"]["message_id"] == parent_id]
                if existing:
                    candidates = existing
                elif loop:
                    candidates = []
                elif remaining <= 0:
                    result["limits"]["truncated"] = True
                    result["missing_references"].append({**reference, "reason": "limit_reached"})
                    continue
                else:
                    key = (source, source_id)
                    if key not in cache:
                        # Uses idx_raw_messages_source_id plus source scoping.
                        found = _rows(conn, "SELECT id,payload,raw,created_at FROM raw_messages "
                                      "WHERE payload->>'source_id'=%s AND payload->>'source'=%s "
                                      "ORDER BY id LIMIT %s", (source_id, source, remaining + 1))
                        cache[key] = [record_projection(row, raw_id) for row in found]
                    candidates = cache[key]
                    if len(candidates) > remaining:
                        result["limits"]["truncated"] = True
                    if len(candidates) > 1:
                        result["warnings"].append(f"{source_id} has multiple stored originals; the reply reference does not select a historical content version.")
                matched = 0
                for parent in candidates:
                    if parent["native_context"]["identity_status"] != "verified_native_identity" or parent["native_context"]["message_id"] != parent_id:
                        result["identity_conflicts"].append({**reference, "candidate_raw_id": parent["id"],
                                                             "reason": "native_envelope_identity_mismatch"})
                        continue
                    if parent["id"] not in seen:
                        if len(seen) >= limit:
                            result["limits"]["truncated"] = True
                            continue
                        seen[parent["id"]] = parent
                        if not loop:
                            queue.append((parent, (*ancestry, parent_id)))
                    matched += 1
                    edge = {"from_raw_id": child["id"], "to_raw_id": parent["id"],
                            "relation": "reply_to", "verification": "native_parent_reference",
                            "version_scope": "message_identity_only"}
                    if edge not in result["edges"]:
                        result["edges"].append(edge)
                if not matched and not loop:
                    result["missing_references"].append({**reference, "reason": "identity_mismatch" if candidates else "not_collected"})
            result["records"] = sorted(seen.values(), key=lambda row: (row["native_context"]["event_ts"] or "9999", row["id"]))
            result["limits"]["returned"] = len(result["records"])
            if any(row["display_limited"] for row in result["records"]):
                result["warnings"].append("Large original bodies are displayed as bounded projections; hashes cover the complete stored raw objects.")
            if result["identity_conflicts"]:
                result["warnings"].append("Stored candidates with conflicting native and envelope identities were excluded; see identity_conflicts for the affected original IDs.")
            result["warnings"] = list(dict.fromkeys(result["warnings"]))
            return result
    finally:
        conn.close()


def connect_database():
    return psycopg2.connect(host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
                            dbname=os.getenv("DB_NAME", "bluesky"), user=os.getenv("DB_USER", "bluesky"),
                            password=os.getenv("DB_PASSWORD", "bluesky"), connect_timeout=5)


def main():
    try:
        params = json.loads(sys.argv[1]) if len(sys.argv) == 2 else None
        if not isinstance(params, dict):
            raise ValueError('Usage: source_context.py \'{"raw_id":42,"limit":30}\'')
        result = source_context(connect_database, params.get("raw_id"), params.get("limit", 30))
        print(json.dumps(result if result is not None else {"status": "not_found"}, ensure_ascii=False))
        return 0 if result is not None else 2
    except (ValueError, TypeError) as error:
        print(json.dumps({"status": "invalid", "message": str(error)}))
        return 2
    except psycopg2.Error as error:
        print(json.dumps({"status": "error", "error_type": type(error).__name__, "message": "Source context database query failed."}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
