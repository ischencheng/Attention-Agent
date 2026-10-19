"""Bounded, read-only projections of recorded research and execution evidence.

Edges are declared references, verified source hashes, or broker coordinates.
No temporal adjacency joins and no model transcript reconstruction.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3

from psycopg2.extras import RealDictCursor
from evidence import digest, verify_evidence


MAX_LINKS = 50
TABLES = {"raw": "raw_messages", "ticket": "attention_tickets",
          "annotation": "annotations", "decision": "decision_packets"}
PRIVATE_KEYS = {"authorization", "api_key", "apikey", "access_token", "refresh_token",
                "password", "secret", "gateway_token", "reasoning", "reasoning_content",
                "chain_of_thought", "transcript", "messages", "system_prompt", "prompt"}
THESIS_KEYS = {"title", "claim", "status", "current_status", "confidence", "revisions",
               "next_review_at", "pending_questions", "source_identity", "source_ingested_at",
               "source_published_at", "supporting_raw_ids", "contradicting_raw_ids",
               "supporting_annotation_ids", "contradicting_annotation_ids",
               "invalidation_conditions", "expected_horizon", "what_changed"}
REVISION_KEYS = THESIS_KEYS - {"revisions", "source_identity"} | {
    "timestamp", "revision_id", "prior_revision", "created_at", "rationale", "uncertainty"}


def clean(value, depth=0):
    """Bound the display payload and redact common credentials/private traces."""
    if depth > 12:
        return "[depth limit]"
    if isinstance(value, dict):
        return {str(key): ("[redacted]" if str(key).lower() in PRIVATE_KEYS else clean(item, depth + 1))
                for key, item in list(value.items())[:100]}
    if isinstance(value, (list, tuple)):
        return [clean(item, depth + 1) for item in value[:100]]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        value = re.sub(r"(?i)(bearer\s+)[\w.\-]+", r"\1[redacted]", value)
        value = re.sub(r"(?i)((?:api[_-]?key|token|password|secret)\s*[=:]\s*)[^\s,;&]+", r"\1[redacted]", value)
        value = re.sub(r"\b(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9]{12,})\b", "[redacted]", value)
        return value[:16000]
    return value


def ids(value):
    return sorted({item for item in value if type(item) is int and item > 0})[:MAX_LINKS] if isinstance(value, list) else []


def thesis_items(state):
    theses = state.get("theses", {}) if isinstance(state, dict) else {}
    if isinstance(theses, dict):
        return [(str(key), value) for key, value in theses.items() if isinstance(value, dict)][:100]
    if isinstance(theses, list):
        return [(str(value["thesis_id"]), value) for value in theses[:100]
                if isinstance(value, dict) and value.get("thesis_id")]
    return []


def public_thesis(value):
    result = {key: item for key, item in value.items() if key in THESIS_KEYS}
    if isinstance(result.get("revisions"), list):
        result["revisions"] = [{key: item for key, item in revision.items() if key in REVISION_KEYS}
                               for revision in result["revisions"][:100] if isinstance(revision, dict)]
    if isinstance(result.get("source_identity"), dict):
        result["source_identity"] = {key: item for key, item in result["source_identity"].items()
                                     if key in {"raw_id", "source", "source_id", "raw_sha256"}}
    return clean(result)


@contextmanager
def reader(connect):
    conn = connect()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '5000ms'")
            yield conn
    finally:
        conn.close()


def rows(conn, sql, args=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, args)
        return [dict(row) for row in cur.fetchall()]


def states(conn):
    keys = sorted({"claude-strong", "codex-strong", os.getenv("STRONG_AGENT_ID", "claude-strong")})
    return rows(conn, "SELECT agent_id, updated_at, state FROM agent_state WHERE agent_id=ANY(%s) ORDER BY updated_at DESC", (keys,))


def archived_research(limit=200, *, with_projection=False):
    path = Path(os.getenv("ATTENTION_PRODUCTION_LEDGER", "/runtime/production/trace-projection.sqlite3"))
    projection = {"status": "unavailable", "captured_at": None, "age_seconds": None}
    def result(output, notes):
        return (output, notes, projection) if with_projection else (output, notes)
    if not path.is_file():
        return result([], ["The research ledger projection is not available; immutable versions cannot be displayed yet."])
    try:
        # This published backup uses DELETE journal mode. Do not use immutable=1:
        # local diagnostics may still deliberately point at the live WAL ledger.
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            metadata_table = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trace_projection_metadata'").fetchone()
            if metadata_table:
                metadata = json.loads(db.execute("SELECT payload FROM trace_projection_metadata WHERE id=1").fetchone()[0])
                captured = datetime.fromisoformat(metadata["captured_at"].replace("Z", "+00:00"))
                age = max(0, (datetime.now(timezone.utc) - captured).total_seconds())
                projection = {**metadata, "status": "available", "age_seconds": round(age, 1), "stale": age > 900}
            else:
                projection = {"status": "direct_ledger_without_capture_time", "captured_at": None, "age_seconds": None}
            found = db.execute("SELECT sequence,key,recorded_at,payload,previous_hash,hash FROM entries "
                               "WHERE kind='production_research_state' ORDER BY sequence DESC LIMIT ?", (limit,)).fetchall()
            output = []
            for item in found:
                entry = dict(item)
                if len(entry["payload"]) > 4_000_000:
                    continue
                payload = json.loads(entry["payload"])
                expected = digest({"kind": "production_research_state", "key": entry["key"],
                                   "recorded_at": entry["recorded_at"], "payload": payload,
                                   "previous_hash": entry["previous_hash"]})
                entry["payload"] = payload
                entry["entry_hash_verified"] = expected == entry["hash"] and digest(payload.get("state")) == payload.get("state_sha256")
                output.append(entry)
            notes = ["Research archives preserve application state; they are not independently verified investment decisions."]
            if len(found) == limit:
                notes.append(f"Only the latest {limit} archived research states are included.")
            if projection.get("stale"):
                notes.append("The research archive projection is over 15 minutes old; current agent state may be newer.")
            return result(output, notes)
    except (sqlite3.Error, ValueError, TypeError, KeyError, OSError):
        projection = {"status": "unavailable", "captured_at": None, "age_seconds": None}
        return result([], ["The production research ledger projection could not be read; no versions were reconstructed."])


def research_list(connect, limit=50):
    archives, limitations, projection = archived_research(with_projection=True)
    with reader(connect) as conn:
        current = states(conn)
    items = []
    for row in current:
        for key, thesis in thesis_items(row["state"]):
            revisions = thesis.get("revisions", [])
            latest = revisions[-1] if isinstance(revisions, list) and revisions and isinstance(revisions[-1], dict) else thesis
            count = sum(1 for entry in archives if entry["payload"].get("agent_id") == row["agent_id"]
                        and key in dict(thesis_items(entry["payload"].get("state"))))
            items.append({"thesis_id": key, "agent_id": row["agent_id"], "updated_at": row["updated_at"],
                          "status": latest.get("current_status", latest.get("status", "research")),
                          "claim": latest.get("claim", thesis.get("claim", key)),
                          "confidence": latest.get("confidence"), "next_review_at": thesis.get("next_review_at"),
                          "pending_questions": thesis.get("pending_questions", []),
                          "revision_count": len(revisions) if isinstance(revisions, list) else 0,
                          "frozen_version_count": count, "evaluation_status": "unscored_application_research"})
    return clean({"items": items[:limit], "count": min(limit, len(items)), "limitations": limitations,
                  "archive_projection": projection})


def routing_list(connect, limit=20):
    items = []
    notes = ["These are recorded routing outcomes. Rejection ends that route; it is not an investment judgment.",
             "Original matching uses source identity and the authoritative raw hash; duplicate deliveries may share an original."]
    with reader(connect) as conn:
        if not rows(conn, "SELECT to_regclass('public.routing_events') AS present")[0]["present"]:
            return {"items": [], "count": 0, "limitations": ["No routing audit table is available."]}
        found = rows(conn, "SELECT id,created_at,payload FROM routing_events ORDER BY id DESC LIMIT %s", (limit,))
        # Multiple stages of one observation reuse the same bounded original lookup.
        matches = {}
        for row in found:
            payload = row["payload"]
            identity = (payload.get("source"), payload.get("source_id"), payload.get("raw_sha256"))
            if identity not in matches:
                candidates = rows(conn, "SELECT id,payload,raw FROM raw_messages WHERE payload->>'source'=%s "
                                  "AND payload->>'source_id'=%s ORDER BY id LIMIT 100", identity[:2])
                matches[identity] = next((item for item in candidates if item["raw"] is not None
                                          and digest(item["raw"]) == identity[2]), None)
            original = matches[identity]
            data = original["payload"].get("data", {}) if original else {}
            items.append({**row, "raw_message_id": original["id"] if original else None,
                          "original_status": "source_identity_and_raw_sha256" if original else "unresolved",
                          "title": data.get("title") if isinstance(data, dict) else None,
                          "excerpt": str(data.get("text", ""))[:500] if isinstance(data, dict) else None,
                          "stage": payload.get("stage"), "outcome": payload.get("outcome"),
                          "reason": payload.get("reason"), "model": payload.get("classifier", {}).get("model"),
                          "source": identity[0], "source_id": identity[1], "raw_sha256": identity[2]})
    return clean({"items": items, "count": len(items), "limitations": notes})


class Graph:
    def __init__(self, conn):
        self.conn = conn
        self.nodes = {}
        self.edges = []
        self.limitations = ["Only recorded references, matching original hashes, and broker coordinates create links.",
                            "Recorded rationale is an application output; private model reasoning is not exposed."]
        self.seen = set()

    def edge(self, source, target, relation, verification="declared_reference"):
        value = {"source": source, "target": target, "relation": relation, "verification": verification}
        if value not in self.edges:
            self.edges.append(value)

    def node(self, kind, record_id, payload, timestamp=None, status=None, **extra):
        key = f"{kind}:{record_id}"
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        title = (payload.get("summary") or payload.get("claim") or
                 data.get("title") or f"{kind.title()} #{record_id}")
        self.nodes[key] = {"id": key, "kind": kind, "record_id": record_id, "title": title,
                           "status": status, "timestamp": timestamp, "payload": payload, **extra}
        return key

    def fetch(self, kind, record_id):
        if kind not in TABLES or type(record_id) is not int or record_id <= 0:
            return None
        raw_column = ",raw AS original_raw" if kind == "raw" else ""
        found = rows(self.conn, f"SELECT id,created_at,payload,kafka_topic,kafka_partition,kafka_offset{raw_column} FROM {TABLES[kind]} WHERE id=%s", (record_id,))
        return found[0] if found else None

    def expand(self, kind, record_id):
        key = f"{kind}:{record_id}"
        if key in self.seen:
            return key if key in self.nodes else None
        self.seen.add(key)
        if len(self.seen) > 150 or len(self.nodes) >= 150:
            self.limitations.append("Trace expansion reached its 150-record limit.")
            return None
        row = self.fetch(kind, record_id)
        if not row:
            self.limitations.append(f"Missing recorded reference: {key}.")
            return None
        payload = row["payload"]
        if not isinstance(payload, dict):
            self.limitations.append(f"Malformed payload: {key}.")
            return None
        if kind == "raw":
            if payload.get("raw") != row["original_raw"]:
                self.limitations.append(f"Original {record_id}: envelope raw differs from the authoritative raw column.")
            payload = {**payload, "raw": row["original_raw"]}
            row["payload"] = payload
        self.node(kind, record_id, payload, row["created_at"],
                  "recorded_original" if kind == "raw" else payload.get("verdict", payload.get("status")),
                  kafka={"topic": row["kafka_topic"], "partition": row["kafka_partition"], "offset": row["kafka_offset"]})
        if kind == "raw":
            self.nodes[key].update({"sha256": digest(row["original_raw"]) if row["original_raw"] is not None else None,
                                    "hash_status": "computed_from_authoritative_raw" if row["original_raw"] is not None else "missing_original"})
        if kind == "annotation":
            for ticket_id in ids(payload.get("attention_ticket_ids")):
                source = self.expand("ticket", ticket_id)
                if source:
                    self.edge(source, key, "referenced_by_annotation")
            for raw_id in ids(payload.get("raw_ids")):
                source = self.expand("raw", raw_id)
                if source:
                    self.edge(source, key, "cited_original")
        elif kind == "ticket":
            self.ticket_original(row)
            # Legacy envelope verdicts are retained but not invented as timed spans.
            if not payload.get("fast_agent", {}).get("reason"):
                self.limitations.append(f"Ticket {record_id}: historical Fast rationale may not have been recorded.")
        elif kind == "raw":
            self.routing(row)
        return key

    def ticket_original(self, row):
        payload = row["payload"]
        if payload.get("raw") is None:
            self.limitations.append(f"Ticket {row['id']} has no original body to verify.")
            return
        found = rows(self.conn, "SELECT id,payload,raw FROM raw_messages WHERE payload->>'source'=%s "
                     "AND payload->>'source_id'=%s ORDER BY id LIMIT 100", (payload.get("source"), payload.get("source_id")))
        expected = digest(payload["raw"])
        for original in found:
            envelope = original["payload"]
            if isinstance(envelope, dict) and original["raw"] is not None and digest(original["raw"]) == expected:
                source = self.expand("raw", original["id"])
                if source:
                    self.edge(source, f"ticket:{row['id']}", "same_original", "source_identity_and_raw_sha256")
                    self.routing(self.fetch("raw", original["id"]), ticket=row)
                return
        self.limitations.append(f"Ticket {row['id']}: no matching original was found in the bounded identity lookup.")

    def routing(self, original, ticket=None):
        envelope = {**original["payload"], "raw": original.get("original_raw", original["payload"].get("raw"))}
        if envelope.get("raw") is None:
            return
        if not rows(self.conn, "SELECT to_regclass('public.routing_events') AS present")[0]["present"]:
            self.limitations.append("Routing audit is not available for this deployment.")
            return
        remaining = min(50, 150 - len(self.nodes))
        if remaining <= 0:
            self.limitations.append("Trace expansion reached its 150-record limit.")
            return
        found = rows(self.conn, "SELECT id,created_at,payload FROM routing_events WHERE source=%s AND source_id=%s "
                     "AND raw_sha256=%s ORDER BY id DESC LIMIT %s", (envelope.get("source"), envelope.get("source_id"), digest(envelope["raw"]), remaining))
        if not found:
            self.limitations.append(f"Original {original['id']}: no persisted routing attempt was recorded; historical verdicts are not backfilled.")
        for row in found:
            payload = row["payload"]
            key = self.node("routing", row["id"], payload, payload.get("recorded_at", row["created_at"]), payload.get("outcome"))
            propagated = payload.get("trace")
            if isinstance(propagated, dict) and re.fullmatch(r"[0-9a-f]{32}", str(propagated.get("trace_id", ""))):
                self.nodes[key]["runtime_trace_id"] = propagated["trace_id"]
                self.nodes[key]["runtime_trace_status"] = "propagated_header_only"
            self.edge(f"raw:{original['id']}", key, "same_original", "source_identity_and_raw_sha256")
            out = payload.get("output")
            if ticket and isinstance(out, dict) and (out.get("topic"), out.get("partition"), out.get("offset")) == (ticket["kafka_topic"], ticket["kafka_partition"], ticket["kafka_offset"]):
                self.edge(key, f"ticket:{ticket['id']}", "published_ticket", "broker_coordinates")
        for source in found:
            out = source["payload"].get("output")
            if not isinstance(out, dict) or any(out.get(name) is None for name in ("topic", "partition", "offset")):
                continue
            for target in found:
                incoming = target["payload"].get("input")
                if source["id"] != target["id"] and isinstance(incoming, dict) and all(out[name] == incoming.get(name) for name in ("topic", "partition", "offset")):
                    self.edge(f"routing:{source['id']}", f"routing:{target['id']}", "forwarded_input", "broker_coordinates")

    def decision(self, record_id):
        row = self.fetch("decision", record_id)
        if not row:
            return None
        candidate = row["payload"].get("evidence_snapshot", {})
        if isinstance(candidate, dict) and any(isinstance(candidate.get(name), list) and len(candidate[name]) > MAX_LINKS
                                               for name in ("annotations", "tickets", "raw_refs")):
            self.limitations.append("The historical snapshot exceeds this explorer's 50-reference limit; verification was not run here.")
            return self.node("decision", record_id, row["payload"], row["created_at"], "not_expanded_limit")
        verification = verify_evidence(self.conn, record_id, row["payload"])
        target = self.node("decision", record_id, row["payload"], row["created_at"], verification["status"])
        snapshot = verification.get("evidence_snapshot")
        if verification["status"] != "verified":
            self.limitations.extend(verification["errors"])
            return target
        # Historical annotation snapshots are authoritative; never replace them
        # with the current mutable annotation row.
        for item in snapshot["annotations"]:
            source = self.node("annotation", item["id"], item["payload"], item["payload"].get("timestamp"),
                               "verified_historical_snapshot", snapshot_sha256=item["sha256"])
            self.edge(source, target, "frozen_evidence", "verified_historical_snapshot")
            for ticket_id in ids(item["payload"].get("attention_ticket_ids")):
                self.edge(f"ticket:{ticket_id}", source, "snapshot_reference", "verified_historical_snapshot")
        for item in snapshot["tickets"]:
            self.node("ticket", item["id"], item, status="frozen_source_reference")
            self.edge(f"raw:{item['raw_message_id']}", f"ticket:{item['id']}", "same_original", "verified_historical_snapshot")
        for item in verification["raw_messages"]:
            self.node("raw", item["id"], item, status="verified_original",
                      sha256=item.get("sha256"), hash_status="verified_against_frozen_decision_reference")
            original = self.fetch("raw", item["id"])
            if original:
                self.routing(original)
        self.limitations.append("Frozen ticket references do not contain broker coordinates; routing events are associated by original hash, not asserted as the historical decision execution.")
        return target

    def result(self):
        return clean({"nodes": list(self.nodes.values()), "edges": self.edges,
                      "limitations": list(dict.fromkeys(self.limitations)),
                      "coverage": "bounded_recorded_evidence"})


def trace_detail(connect, kind, record_id):
    with reader(connect) as conn:
        graph = Graph(conn)
        result = graph.decision(record_id) if kind == "decision" else graph.expand(kind, record_id)
        return graph.result() if result else None


def research_detail(connect, agent_id, thesis_id):
    archives, limitations, projection = archived_research(with_projection=True)
    with reader(connect) as conn:
        state_row = next((row for row in states(conn) if row["agent_id"] == agent_id), None)
        thesis = dict(thesis_items(state_row["state"])).get(thesis_id) if state_row else None
        if thesis is None:
            return None
        graph = Graph(conn)
        graph.limitations.extend(limitations)
        revisions = thesis.get("revisions", [])
        latest = revisions[-1] if isinstance(revisions, list) and revisions and isinstance(revisions[-1], dict) else thesis
        target = graph.node("research", f"{agent_id}:{thesis_id}", public_thesis(thesis), state_row["updated_at"],
                            latest.get("current_status", latest.get("status", "research")))
        for name, kind, relation in (("supporting_annotation_ids", "annotation", "supports"),
                                     ("contradicting_annotation_ids", "annotation", "contradicts"),
                                     ("supporting_raw_ids", "raw", "supports"),
                                     ("contradicting_raw_ids", "raw", "contradicts")):
            for record_id in ids(latest.get(name, thesis.get(name))):
                source = graph.expand(kind, record_id)
                if source:
                    graph.edge(source, target, relation, "application_declared_reference")
        versions = []
        for entry in archives:
            frozen = entry["payload"]
            old = dict(thesis_items(frozen.get("state"))).get(thesis_id)
            if frozen.get("agent_id") == agent_id and old is not None:
                versions.append({key: frozen.get(key) for key in ("version", "frozen_at", "updated_at", "state_sha256", "previous_version_hash")} |
                                {"hash": entry["hash"], "entry_hash_verified": entry["entry_hash_verified"], "thesis": public_thesis(old)})
        return clean({"thesis_id": thesis_id, "agent_id": agent_id, "updated_at": state_row["updated_at"],
                      "thesis": public_thesis(thesis), "revisions": public_thesis(thesis).get("revisions", []),
                      "versions": versions[:50], "graph": graph.result(),
                      "archive_projection": projection,
                      "evaluation_status": "unscored_application_research"})


def cycle_list(limit=30):
    directory = Path(os.getenv("ATTENTION_CYCLE_AUDIT_DIR", "/runtime/cycle-audit"))
    items, limitations = [], []
    if directory.is_dir():
        for folder in sorted((path for path in directory.iterdir() if path.is_dir()), reverse=True)[:limit]:
            value = {}
            try:
                for name in ("started", "completed", "failed"):
                    path = folder / f"{name}.json"
                    if path.is_file() and path.stat().st_size < 64_000:
                        value.update(json.loads(path.read_text()))
                if value:
                    if value.get("event") == "started":
                        value["status"] = "running_or_interrupted"
                        value["summary"] = "No terminal summary has been recorded; this attempt may still be running or was interrupted."
                    items.append(value)
            except (OSError, ValueError):
                limitations.append("An incomplete cycle summary could not be read.")
    legacy = Path(os.getenv("ATTENTION_LEGACY_CYCLE_DIR", "/runtime/legacy-cycles"))
    known = {item.get("cycle_id") for item in items}
    for role in ("mid", "strong"):
        path = legacy / f"{role}.json"
        if not path.is_file() or path.stat().st_size > 4_000_000:
            continue
        try:
            value = json.loads(path.read_text())
            if value.get("cycle_id") in known:
                continue
            final = value.get("final", {})
            failed = value.get("status") == "failed"
            items.append({"cycle_id": f"legacy:{role}:{value.get('completed_at', 'unknown')}",
                          "role": role, "origin": "imported_latest_summary", "status": value.get("status"),
                          "completed_at": value.get("completed_at"), "failed_at": value.get("failed_at"),
                          "selected_input_ids": [] if failed else value.get("selected_input_ids", []),
                          "reviewed_input_ids": [] if failed else final.get("reviewed_ticket_ids" if role == "mid" else "reviewed_annotation_ids", []),
                          "summary": "A legacy attempt failed; its selected inputs and tool events were not retained separately." if failed else final.get("summary"),
                          "usage": {} if failed else value.get("usage", {}),
                          "state_verified": False if failed else value.get("state_verified", False),
                          "tool_detail_status": "unavailable", "tool_events": [], "tool_event_count": 0})
            limitations.append("Imported latest summaries do not recover older attempts, tool spans, or missing timings.")
        except (OSError, ValueError):
            limitations.append(f"The legacy {role} summary could not be read.")
    items.sort(key=lambda value: value.get("started_at") or value.get("completed_at") or value.get("failed_at") or "", reverse=True)
    limitations.append("Cycle summaries contain recorded outcomes and usage, not private model transcripts. Model/tool span detail is unavailable here.")
    return clean({"items": items[:limit], "count": min(limit, len(items)), "limitations": list(dict.fromkeys(limitations))})
