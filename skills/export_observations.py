#!/usr/bin/env python3
"""Export PostgreSQL originals for the longitudinal attention judgment environment.

Example:
    python skills/export_observations.py --since 2026-10-01T00:00:00Z \
        --output observations.jsonl

The default has no row limit and no source/topic/price filter. An explicit limit
exports the oldest available prefix, with a continuation cursor in the summary.
Only raw_messages.raw is original evidence; enriched payload.data is never used.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

import psycopg2


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def timestamp(value):
    """Parse an explicit-zone ISO timestamp without guessing a local timezone."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("timestamp must be an ISO string with an explicit timezone")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp requires an explicit timezone")
    return parsed.astimezone(timezone.utc)


def db_settings():
    """Use the project's existing DB_* environment contract, without logging it."""
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "bluesky"),
        "user": os.environ.get("DB_USER", "bluesky"),
        "password": os.environ.get("DB_PASSWORD", "bluesky"),
        "connect_timeout": 10,
    }


def observation_from_row(row):
    row_id, payload, raw, created_at = row
    if not isinstance(payload, dict):
        raise ValueError(f"raw_messages row {row_id} has no object envelope")
    source, source_id = payload.get("source"), payload.get("source_id")
    if not isinstance(source, str) or not source.strip():
        raise ValueError(f"raw_messages row {row_id} has no valid source")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError(f"raw_messages row {row_id} has no valid source_id")
    if raw is None:
        raise ValueError(f"raw_messages row {row_id} has no non-null original raw value")
    available = timestamp(created_at)
    try:
        published = timestamp(payload.get("timestamp"))
        published_origin = "envelope.timestamp"
    except (ValueError, TypeError, OverflowError):
        published = available
        published_origin = "raw_messages.created_at"
    content_hash = digest(raw)
    return {
        "id": digest([source, source_id, content_hash]),
        "event_id": digest([source, source_id]),
        "source": source,
        "source_id": source_id,
        "published_at": published.isoformat(),
        "available_at": available.isoformat(),
        "raw": raw,
        "raw_sha256": content_hash,
        "database_row_id": row_id,
        "published_at_origin": published_origin,
        "envelope_timestamp": payload.get("timestamp"),
    }


def export_observations(output, since=None, limit=None, *, after_row_id=None, settings=None):
    """Atomically export original rows, preserving all arrivals and revisions.

    ``since`` is an inclusive availability cutoff. For a bounded export, resume
    with both values from ``next_cursor``; this handles rows sharing one arrival
    timestamp without dropping ties or repeatedly exporting the same first page.
    ``settings`` accepts psycopg2 connection keywords for controlled local tests;
    production defaults to DB_* environment variables.
    """
    cutoff = timestamp(since) if since is not None else None
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("limit must be a positive integer, or omitted for all rows")
    if after_row_id is not None:
        if cutoff is None:
            raise ValueError("after_row_id requires since from a previous export cursor")
        if type(after_row_id) is not int or after_row_id < 0:
            raise ValueError("after_row_id must be a nonnegative integer")
    output = Path(output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    query = "SELECT id, payload, raw, created_at FROM raw_messages"
    parameters = []
    if after_row_id is not None:
        query += " WHERE (created_at, id) > (%s, %s)"
        parameters += [cutoff, after_row_id]
    elif cutoff is not None:
        query += " WHERE created_at >= %s"
        parameters.append(cutoff)
    query += " ORDER BY created_at ASC, id ASC"
    if limit is not None:
        query += " LIMIT %s"
        parameters.append(limit + 1)

    report = {
        "output": str(output), "count": 0, "source_counts": {},
        "since": cutoff.isoformat() if cutoff else None, "limit": limit,
        "has_more": False, "next_cursor": None, "timestamp_fallbacks": 0,
        "original_column": "raw_messages.raw", "filters": "availability only",
    }
    conn = None
    temporary = None
    try:
        conn = psycopg2.connect(**(db_settings() if settings is None else settings))
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent,
                                             prefix=f".{output.name}.", suffix=".tmp", delete=False) as stream:
                temporary = Path(stream.name)
                with conn.cursor(name="attention_originals_export") as cursor:
                    cursor.itersize = 1000
                    cursor.execute(query, parameters)
                    for row in cursor:
                        if limit is not None and report["count"] >= limit:
                            report["has_more"] = True
                            break
                        item = observation_from_row(row)
                        stream.write(canonical(item) + "\n")
                        report["count"] += 1
                        source = item["source"]
                        report["source_counts"][source] = report["source_counts"].get(source, 0) + 1
                        report["timestamp_fallbacks"] += item["published_at_origin"] != "envelope.timestamp"
                        report["next_cursor"] = {"since": item["available_at"], "after_row_id": item["database_row_id"]}
                stream.flush()
                os.fsync(stream.fileno())
        os.replace(temporary, output)
        temporary = None
        return report
    finally:
        if conn is not None:
            conn.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", help="Inclusive DB-arrival cutoff, ISO8601 with timezone")
    parser.add_argument("--limit", type=int, help="Export an oldest-first prefix; default exports all rows")
    parser.add_argument("--after-row-id", type=int, help="Use with --since from next_cursor to continue past timestamp ties")
    parser.add_argument("--output", required=True, type=Path, help="Destination JSONL file, replaced only after a complete export")
    args = parser.parse_args()
    try:
        report = export_observations(args.output, args.since, args.limit, after_row_id=args.after_row_id)
    except psycopg2.Error as exc:
        # Database exception messages can include connection details. Keep them
        # out of stdout/stderr, including when the caller configured a password.
        print(json.dumps({"error": f"Database export failed ({type(exc).__name__}); check DB_* settings and database availability"}), file=sys.stderr)
        return 1
    except (ValueError, TypeError, OSError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
