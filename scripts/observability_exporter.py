#!/usr/bin/env python3
"""Export bounded, recorded routing/cycle audits to OTLP without business writes.

These are independent audit spans. No parent relationships, per-tool timings,
prompts, outputs, or exception messages are reconstructed. Explicit --replay N
resends at most N recent retained records of each kind after backend data loss.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import heapq
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import threading
from urllib.parse import urlparse

import psycopg2
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
from runtime_traces import trace_identity

RUNTIME = ROOT / "environments/attention_judgment/runs/local-pipeline"
MAX_BATCH = 500
MAX_RESPONSE = 64_000
USAGE_KEYS = {"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"}
SAFE_LABEL = re.compile(r"[A-Za-z0-9_.-]{1,128}\Z")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("missing timestamp")
    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    value = value.astimezone(timezone.utc)
    delta = value - EPOCH
    nanos = (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000
    if nanos < 0:
        raise ValueError("timestamp precedes epoch")
    return value.isoformat(), nanos


def scalar_attributes(values):
    result = []
    for key, value in values.items():
        if type(value) is bool:
            encoded = {"boolValue": value}
        elif type(value) is int and -(2**63) <= value < 2**63:
            encoded = {"intValue": str(value)}
        elif isinstance(value, str):
            encoded = {"stringValue": value}
        else:
            continue
        result.append({"key": key, "value": encoded})
    return result


def label(value):
    # Do not pass free text, URLs, tokens or authorization strings as metadata.
    if (isinstance(value, str) and SAFE_LABEL.fullmatch(value)
            and not re.match(r"(?:sk-|gh[pousr]_)", value)):
        return value
    return None


def add_usage(attributes, value, *, historical=False):
    if not isinstance(value, dict):
        return
    prefix = "attention.historical_usage." if historical else "attention.reported_usage."
    for key in USAGE_KEYS:
        count = value.get(key)
        if type(count) is int and 0 <= count < 2**63:
            attributes[prefix + key] = count


def audit_span(kind, key, record):
    """Project only declared scalars; all payload content stays in its source."""
    attributes = {"attention.trace_origin": "recorded_audit", "attention.audit_kind": kind}
    error = False
    if kind == "routing":
        if not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("invalid routing identity")
        stage = record.get("stage")
        outcome = record.get("outcome")
        if stage not in {"relay", "filter", "fast"} or outcome not in {"pass", "reject"}:
            raise ValueError("invalid routing record")
        name, service = f"{stage}.recorded_handling", "attention-routing"
        start, start_ns = timestamp(record.get("started_at"))
        end, end_ns = timestamp(record.get("completed_at"))
        attributes.update({"attention.event_id": key, "attention.stage": stage,
                           "attention.outcome": outcome,
                           "attention.timing_scope": ("recorded_handling_through_delivery_before_audit_and_commit"
                                                      if stage == "relay" else "recorded_handling_before_delivery_and_commit")})
        classifier = record.get("classifier")
        classifier = classifier if isinstance(classifier, dict) else {}
        for field in ("provider", "model", "classification_mode"):
            attributes[f"attention.{field}"] = label(classifier.get(field))
        for field in ("classified_by_model", "cached", "full_review_required"):
            if type(classifier.get(field)) is bool:
                attributes[f"attention.{field}"] = classifier[field]
        # A cache hit's usage describes the original invocation, not this span.
        add_usage(attributes, classifier.get("usage"), historical=classifier.get("cached") is True)
        attributes["attention.usage_scope"] = "historical_cached_result" if classifier.get("cached") is True else "reported_routing_result"
        coordinates = record.get("input")
        if isinstance(coordinates, dict):
            attributes["messaging.destination.name"] = label(coordinates.get("topic"))
            for field in ("partition", "offset"):
                if type(coordinates.get(field)) is int and coordinates[field] >= 0:
                    attributes[f"attention.kafka.{field}"] = coordinates[field]
        raw_hash = record.get("raw_sha256")
        if isinstance(raw_hash, str) and re.fullmatch(r"[0-9a-f]{64}", raw_hash):
            attributes["attention.raw_sha256"] = raw_hash
    elif kind == "cycle":
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", key):
            raise ValueError("invalid cycle identity")
        role, event = record.get("role"), record.get("event")
        if role not in {"mid", "strong"} or event not in {"completed", "failed"}:
            raise ValueError("cycle has no valid terminal event")
        name, service = "agent.recorded_cycle", f"attention-{role}"
        start, start_ns = timestamp(record.get("started_at"))
        end, end_ns = timestamp(record.get("failed_at" if event == "failed" else "completed_at"))
        error = event == "failed"
        attributes.update({"attention.cycle_id": key, "attention.role": role,
                           "attention.outcome": event, "attention.model": label(record.get("model")),
                           "attention.timing_scope": "recorded_cycle_attempt",
                           "attention.tool_timing": "not_recorded"})
        attributes["error.type"] = label(record.get("error_type")) if error else None
        for field in ("state_verified", "daily_review", "forced"):
            if type(record.get(field)) is bool:
                attributes[f"attention.{field}"] = record[field]
        for field in ("published_count", "pending_count", "tool_event_count"):
            if type(record.get(field)) is int and record[field] >= 0:
                attributes[f"attention.{field}"] = record[field]
        for field in ("selected_input_ids", "reviewed_input_ids"):
            values = record.get(field)
            if isinstance(values, list):
                ids = [value for value in values[:100] if type(value) is int and value > 0]
                attributes[f"attention.{field}"] = ",".join(map(str, ids))
        add_usage(attributes, record.get("usage"))
    else:
        raise ValueError("unsupported audit kind")
    if end_ns < start_ns:
        raise ValueError("reversed audit timestamps")
    identity = trace_identity(kind, key)
    span = {"traceId": identity["trace_id"], "spanId": identity["span_id"], "name": name,
            "kind": 1, "startTimeUnixNano": str(start_ns), "endTimeUnixNano": str(end_ns),
            "attributes": scalar_attributes(attributes), "status": {"code": 2 if error else 1}}
    return {"kind": kind, "key": key, **identity, "service": service, "name": name, "start": start, "end": end,
            "resource": {"resource": {"attributes": scalar_attributes({"service.name": service})},
                         "scopeSpans": [{"scope": {"name": "attention.recorded_audit", "version": "1"}, "spans": [span]}]}}


class ExportError(RuntimeError):
    def __init__(self, error_type, *, retryable):
        super().__init__(error_type)
        self.error_type, self.retryable = error_type, retryable


def send_batch(endpoint, records, session=None):
    if not records:
        return
    sender = session or requests
    try:
        with sender.post(endpoint, json={"resourceSpans": [record["resource"] for record in records]},
                         headers={"Content-Type": "application/json", "Accept": "application/json"},
                         timeout=(3, 10), allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                raise ExportError(f"HTTP{response.status_code}", retryable=response.status_code in {408, 429, 500, 502, 503, 504})
            body = bytearray()
            for chunk in response.iter_content(8192):
                body.extend(chunk)
                if len(body) > MAX_RESPONSE:
                    raise ExportError("OversizedResponse", retryable=False)
            try:
                result = json.loads(body)
                if not isinstance(result, dict):
                    raise ValueError
                partial = result.get("partialSuccess")
                if partial is not None:
                    if not isinstance(partial, dict):
                        raise ValueError
                    rejected = partial.get("rejectedSpans", 0)
                    if type(rejected) is bool or not re.fullmatch(r"[0-9]+", str(rejected)):
                        raise ValueError
                    if int(rejected):
                        # OTLP does not identify the rejected members. Retain the whole
                        # batch unacknowledged; explicit replay is required after repair.
                        raise ExportError("RejectedSpans", retryable=False)
            except (ValueError, TypeError, UnicodeDecodeError) as exc:
                raise ExportError("InvalidResponse", retryable=False) from exc
    except requests.RequestException as exc:
        raise ExportError(type(exc).__name__, retryable=True) from exc


def connect_db():
    conn = psycopg2.connect(host=os.getenv("DB_HOST", "127.0.0.1"), port=os.getenv("DB_PORT", "5432"),
                            dbname=os.getenv("DB_NAME", "bluesky"), user=os.getenv("DB_USER", "bluesky"),
                            password=os.getenv("DB_PASSWORD", "bluesky"), connect_timeout=5)
    conn.set_session(readonly=True)
    return conn


def routing_records(after, limit, *, replay=False, connect=connect_db):
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '5s'")
            cur.execute("SELECT id,event_id,payload FROM routing_events WHERE id > %s ORDER BY id "
                        + ("DESC" if replay else "ASC") + " LIMIT %s", (0 if replay else after, limit + 1))
            records = cur.fetchall()
        return [(row[0], row[1], row[2]) for row in records[:limit]], len(records) > limit
    finally:
        conn.close()


def cycle_snapshot(folder):
    value = {}
    try:
        if folder.is_symlink():
            raise ValueError("symlink cycle audit")
        for event in ("started", "completed", "failed"):
            path = folder / f"{event}.json"
            if path.is_file():
                if path.is_symlink() or path.stat().st_size > 64_000:
                    raise ValueError("unreadable cycle audit")
                item = json.loads(path.read_text())
                if not isinstance(item, dict) or item.get("cycle_id") != folder.name or item.get("event") != event:
                    raise ValueError("inconsistent cycle audit")
                if event == "failed" and value.get("event") == "completed":
                    raise ValueError("conflicting cycle terminal records")
                value.update(item)
    except (OSError, ValueError, TypeError):
        # Writers create immutable files directly. Retry partial publication.
        value = {"read_pending": True}
    return folder.name, folder.name, value or {"read_pending": True}


def cycle_records(directory, after, limit, *, replay=False, unseen=None):
    if not directory.is_dir():
        return [], False
    # Bound memory and JSON reads even when the archive contains many directories.
    candidates = (path for path in directory.iterdir() if path.is_dir() and not path.is_symlink()
                  and (replay or (unseen(path.name) if unseen else path.name > after)))
    selected = (heapq.nlargest if replay else heapq.nsmallest)(limit + 1, candidates, key=lambda path: path.name)
    records = [cycle_snapshot(folder) for folder in selected[:limit]]
    return records, len(selected) > limit


class Exporter:
    def __init__(self, directory, cycle_directory, endpoint, *, batch_size=100, routing_reader=routing_records, session=None):
        if not 1 <= batch_size <= MAX_BATCH:
            raise ValueError("invalid batch size")
        self.directory, self.cycle_directory = Path(directory), Path(cycle_directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.endpoint, self.batch_size, self.routing_reader, self.session = endpoint, batch_size, routing_reader, session
        self.db = sqlite3.connect(self.directory / "exports.sqlite3", timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("CREATE TABLE IF NOT EXISTS exports (kind TEXT,record_key TEXT,trace_id TEXT,span_id TEXT,exported_at TEXT,PRIMARY KEY(kind,record_key))")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (name TEXT PRIMARY KEY,value TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS blocked_exports (kind TEXT,record_key TEXT,error_type TEXT,PRIMARY KEY(kind,record_key))")
        self.db.execute("CREATE TABLE IF NOT EXISTS pending_cycles (record_key TEXT PRIMARY KEY)")
        self.db.commit()

    def state(self, name, default=""):
        row = self.db.execute("SELECT value FROM state WHERE name=?", (name,)).fetchone()
        return row[0] if row else default

    def set_state(self, name, value):
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (name, str(value)))

    def known(self, table, kind, key):
        assert table in {"exports", "blocked_exports"}
        return bool(self.db.execute(f"SELECT 1 FROM {table} WHERE kind=? AND record_key=?", (kind, key)).fetchone())

    def block(self, kind, key, error_type):
        self.db.execute("INSERT OR REPLACE INTO blocked_exports VALUES (?,?,?)", (kind, key, error_type))
        if kind == "cycle":
            self.db.execute("DELETE FROM pending_cycles WHERE record_key=?", (key,))

    def read_cycles(self, cursor, limit, replay):
        if replay:
            return (*cycle_records(self.cycle_directory, cursor, limit, replay=True), cursor)
        total = self.db.execute("SELECT count(*) FROM pending_cycles").fetchone()[0]
        # Reserve work for both new and unfinished cycles. With a one-record
        # limit, alternate so neither side can starve the other.
        reserved = min(total, max(1, limit // 2))
        if limit == 1 and total:
            reserved = 1 if self.state("pending_turn", "1") == "1" else 0
            self.set_state("pending_turn", str(1 - reserved))
        pending_keys = []
        if reserved:
            after = self.state("pending_cursor")
            pending_keys = self.db.execute("SELECT record_key FROM pending_cycles WHERE record_key>? ORDER BY record_key LIMIT ?", (after, reserved)).fetchall()
            if not pending_keys:
                pending_keys = self.db.execute("SELECT record_key FROM pending_cycles ORDER BY record_key LIMIT ?", (reserved,)).fetchall()
            if pending_keys:
                self.set_state("pending_cursor", pending_keys[-1][0])
        def unseen(key):
            # Cycle IDs reflect attempt start, while directories appear only
            # after scheduler reads. A slow older attempt can publish late.
            return (not self.known("exports", "cycle", key)
                    and not self.known("blocked_exports", "cycle", key)
                    and not self.db.execute("SELECT 1 FROM pending_cycles WHERE record_key=?", (key,)).fetchone())
        records, more = cycle_records(self.cycle_directory, cursor, limit - len(pending_keys), unseen=unseen)
        next_cursor = records[-1][0] if records else cursor
        known = {key for _, key, _ in records}
        records += [cycle_snapshot(self.cycle_directory / key) for (key,) in pending_keys if key not in known]
        return records, more or total > len(pending_keys), next_cursor

    def run_once(self, *, replay=0):
        limit = replay or self.batch_size
        if not 1 <= limit <= MAX_BATCH:
            raise ValueError("invalid replay bound")
        pending = {"routing": 0, "cycle": 0}
        truncated, failures, staged, cursors = {}, [], [], {}
        for kind in ("routing", "cycle"):
            cursor = self.state(f"cursor.{kind}", "0" if kind == "routing" else "")
            try:
                if kind == "routing":
                    records, more = self.routing_reader(int(cursor), limit, replay=bool(replay))
                    next_cursor = records[-1][0] if records else cursor
                else:
                    records, more, next_cursor = self.read_cycles(cursor, limit, bool(replay))
                truncated[kind] = more
                cursors[kind] = next_cursor
                for _, key, payload in records:
                    if not replay and self.known("exports", kind, key):
                        continue
                    pending[kind] += 1
                    if not replay and self.known("blocked_exports", kind, key):
                        continue
                    if kind == "cycle" and (payload.get("event") == "started" or payload.get("read_pending")):
                        self.db.execute("INSERT OR IGNORE INTO pending_cycles VALUES (?)", (key,))
                        continue  # Revisit without inventing an end or rewinding history.
                    try:
                        staged.append(audit_span(kind, key, payload))
                    except (ValueError, TypeError, AttributeError):
                        self.block(kind, key, "InvalidAudit")
            except Exception as exc:
                failures.append(type(exc).__name__)
        try:
            send_batch(self.endpoint, staged, self.session)
            if staged:
                at = now()
                for record in staged:
                    self.db.execute("INSERT OR REPLACE INTO exports VALUES (?,?,?,?,?)",
                                    (record["kind"], record["key"], record["trace_id"], record["span_id"], at))
                    self.db.execute("DELETE FROM blocked_exports WHERE kind=? AND record_key=?", (record["kind"], record["key"]))
                    if record["kind"] == "cycle":
                        self.db.execute("DELETE FROM pending_cycles WHERE record_key=?", (record["key"],))
                    pending[record["kind"]] -= 1
                self.set_state("last_success_at", at)
                recent = [{key: row[key] for key in ("kind", "key", "trace_id", "service", "name", "start", "end")} for row in staged[-10:]]
                self.set_state("recent_traces", json.dumps(recent))
        except ExportError as exc:
            failures.append(exc.error_type)
            if exc.retryable:
                cursors.clear()  # Retry the same unacknowledged window next poll.
            else:
                for record in staged:
                    self.block(record["kind"], record["key"], exc.error_type)
        if not replay:
            for kind, cursor in cursors.items():
                self.set_state(f"cursor.{kind}", cursor)
        self.db.commit()
        blocked = dict(self.db.execute("SELECT kind,count(*) FROM blocked_exports GROUP BY kind"))
        status = {"status": "degraded" if failures else "blocked" if blocked else "healthy", "updated_at": now(),
                  "last_success_at": self.state("last_success_at") or None,
                  "exported_counts": {"routing": 0, "cycle": 0, **dict(self.db.execute("SELECT kind,count(*) FROM exports GROUP BY kind"))},
                  "pending_counts": pending, "pending_scope": "scanned_window", "scan_truncated": truncated,
                  "blocked_counts": {"routing": 0, "cycle": 0, **blocked},
                  "error_type": failures[0] if failures else "BlockedExports" if blocked else None,
                  "recent_traces": json.loads(self.state("recent_traces", "[]")),
                  "trace_origin": "recorded_audit", "receipt_scope": "collector_accepted"}
        temporary = self.directory / "status.tmp"
        with temporary.open("w") as stream:
            json.dump(status, stream, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.directory / "status.json")
        return status

    def close(self):
        self.db.close()


@contextmanager
def single_instance(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "exporter.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def main():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--replay", type=int, default=0, metavar="N", help="Explicitly resend up to N latest records of each kind, then exit (1–500)")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--directory", type=Path, default=RUNTIME / "observability")
    parser.add_argument("--cycle-directory", type=Path, default=RUNTIME / "cycle-audit")
    parser.add_argument("--endpoint", default=os.getenv("ATTENTION_OTLP_ENDPOINT", "http://127.0.0.1:4318/v1/traces"))
    args = parser.parse_args()
    url = urlparse(args.endpoint)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment:
        parser.error("endpoint must be an HTTP(S) URL without credentials, query or fragment")
    if not 1 <= args.batch_size <= MAX_BATCH or not 0 <= args.replay <= MAX_BATCH or not 1 <= args.interval <= 300:
        parser.error("batch/replay must be bounded by 500; interval must be 1–300 seconds")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    with single_instance(args.directory):
        exporter = Exporter(args.directory, args.cycle_directory, args.endpoint, batch_size=args.batch_size)
        try:
            while not stop.is_set():
                status = exporter.run_once(replay=args.replay)
                if args.once or args.replay:
                    print(json.dumps(status))
                    return 1 if status["status"] != "healthy" else 0
                stop.wait(args.interval)
        finally:
            exporter.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
