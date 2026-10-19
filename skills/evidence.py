"""Build and verify historical evidence using authoritative PostgreSQL originals.

This module has no model or Kafka dependency. Callers own the connection and
transaction; publication uses a read-only REPEATABLE READ transaction.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def positive_ids(value: Any, label: str, *, nonempty: bool = True) -> list[int]:
    if not isinstance(value, list) or (nonempty and not value):
        raise ValueError(f"{label} must be {'a nonempty' if nonempty else 'a'} list of positive integer IDs")
    if any(type(item) is not int or item <= 0 for item in value):
        raise ValueError(f"{label} must contain positive integer IDs; booleans are not IDs")
    return sorted(set(value))


def identity(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def fetch_payloads(conn: Any, table: str, ids: list[int]) -> dict[int, dict]:
    if table not in {"annotations", "attention_tickets"}:
        raise ValueError("Unsupported evidence table")
    if not ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(f"SELECT id, payload FROM {table} WHERE id = ANY(%s)", (ids,))
        rows = dict(cur.fetchall())
    missing = sorted(set(ids) - rows.keys())
    if missing:
        raise ValueError(f"Missing {table} IDs: {missing}")
    if any(not isinstance(payload, dict) for payload in rows.values()):
        raise ValueError(f"{table} payloads must be JSON objects")
    return rows


def build_snapshot(conn: Any, annotation_ids: list[int]) -> dict:
    annotations = fetch_payloads(conn, "annotations", annotation_ids)
    ticket_ids: set[int] = set()
    annotation_entries = []
    for annotation_id in sorted(annotations):
        payload = annotations[annotation_id]
        ticket_ids.update(positive_ids(
            payload.get("attention_ticket_ids"),
            f"annotations[{annotation_id}].attention_ticket_ids",
        ))
        annotation_entries.append({
            "id": annotation_id, "payload": copy.deepcopy(payload), "sha256": digest(payload),
        })

    tickets = fetch_payloads(conn, "attention_tickets", sorted(ticket_ids))
    ticket_entries = []
    raw_refs = {}
    for ticket_id in sorted(tickets):
        ticket = tickets[ticket_id]
        source = identity(ticket.get("source"), f"tickets[{ticket_id}].source")
        source_id = identity(ticket.get("source_id"), f"tickets[{ticket_id}].source_id")
        if ticket.get("raw") is None:
            raise ValueError(f"tickets[{ticket_id}] needs non-null raw evidence")
        raw_hash = digest(ticket["raw"])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, payload, raw FROM raw_messages "
                "WHERE payload->>'source' = %s AND payload->>'source_id' = %s ORDER BY id",
                (source, source_id),
            )
            candidates = cur.fetchall()
        matching_id = None
        for raw_id, raw_payload, raw in candidates:
            if (isinstance(raw_payload, dict)
                    and raw_payload.get("source") == source
                    and raw_payload.get("source_id") == source_id
                    and raw is not None and digest(raw) == raw_hash):
                matching_id = raw_id
                break
        if matching_id is None:
            raise ValueError(f"No matching original for ticket {ticket_id} ({source}, {source_id})")
        ticket_entries.append({
            "id": ticket_id, "source": source, "source_id": source_id,
            "raw_message_id": matching_id, "raw_sha256": raw_hash,
        })
        raw_refs[matching_id] = {
            "id": matching_id, "source": source, "source_id": source_id, "sha256": raw_hash,
        }
    return {
        "version": 1,
        "annotations": annotation_entries,
        "tickets": ticket_entries,
        "raw_refs": [raw_refs[raw_id] for raw_id in sorted(raw_refs)],
    }


def _entries(value: Any, label: str) -> dict[int, dict]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"snapshot.{label} must be a nonempty list")
    result = {}
    for entry in value:
        if not isinstance(entry, dict):
            raise ValueError(f"snapshot.{label} entries must be objects")
        entry_id = entry.get("id")
        positive_ids([entry_id], f"snapshot.{label}.id")
        if entry_id in result:
            raise ValueError(f"Duplicate snapshot.{label} ID {entry_id}")
        result[entry_id] = entry
    return result


def _hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{label} must be a SHA256 hexadecimal digest")
    return value


def verify_evidence(conn: Any, decision_id: int, payload: Any) -> dict:
    result = {
        "decision_id": decision_id, "status": "broken", "evidence_snapshot": None,
        "raw_messages": [], "errors": [],
    }
    if not isinstance(payload, dict):
        result["errors"].append("Decision payload is not a JSON object")
        return result
    if "evidence_snapshot" not in payload:
        result["status"] = "legacy_unverified"
        result["errors"].append("No historical evidence snapshot was recorded for this decision")
        return result

    snapshot = copy.deepcopy(payload["evidence_snapshot"])
    result["evidence_snapshot"] = snapshot
    try:
        if not isinstance(snapshot, dict) or type(snapshot.get("version")) is not int or snapshot["version"] != 1:
            raise ValueError("Unsupported or malformed evidence snapshot version")
        annotations = _entries(snapshot.get("annotations"), "annotations")
        tickets = _entries(snapshot.get("tickets"), "tickets")
        raw_refs = _entries(snapshot.get("raw_refs"), "raw_refs")
        for name, entries in (("annotations", annotations), ("tickets", tickets), ("raw_refs", raw_refs)):
            snapshot[name] = [entries[entry_id] for entry_id in sorted(entries)]

        expected_annotations = positive_ids(payload.get("related_annotations"), "related_annotations")
        if set(expected_annotations) != annotations.keys():
            raise ValueError("Decision annotation references do not match the snapshot graph")
        expected_tickets = set()
        for annotation_id, annotation in annotations.items():
            annotation_payload = annotation.get("payload")
            if not isinstance(annotation_payload, dict):
                raise ValueError(f"Annotation {annotation_id} snapshot payload must be an object")
            if digest(annotation_payload) != _hash(annotation.get("sha256"), "annotation.sha256"):
                raise ValueError(f"Annotation {annotation_id} snapshot hash mismatch")
            expected_tickets.update(positive_ids(
                annotation_payload.get("attention_ticket_ids"),
                f"annotation[{annotation_id}].attention_ticket_ids",
            ))
        if expected_tickets != tickets.keys():
            raise ValueError("Annotation ticket references do not match the snapshot graph")

        expected_raw_ids = set()
        for ticket_id, ticket in tickets.items():
            source = identity(ticket.get("source"), f"ticket[{ticket_id}].source")
            source_id = identity(ticket.get("source_id"), f"ticket[{ticket_id}].source_id")
            raw_id = positive_ids([ticket.get("raw_message_id")], "ticket.raw_message_id")[0]
            raw_hash = _hash(ticket.get("raw_sha256"), "ticket.raw_sha256")
            expected_raw_ids.add(raw_id)
            raw_ref = raw_refs.get(raw_id)
            if raw_ref is None or (source, source_id, raw_hash) != (
                    raw_ref.get("source"), raw_ref.get("source_id"), raw_ref.get("sha256")):
                raise ValueError(f"Ticket {ticket_id} does not match its original reference")
        if expected_raw_ids != raw_refs.keys():
            raise ValueError("Ticket raw references do not match the snapshot graph")

        with conn.cursor() as cur:
            cur.execute("SELECT id, payload, raw FROM raw_messages WHERE id = ANY(%s) ORDER BY id", (sorted(raw_refs),))
            originals = cur.fetchall()
        found = set()
        for raw_id, raw_payload, raw in originals:
            found.add(raw_id)
            ref = raw_refs[raw_id]
            if not isinstance(raw_payload, dict):
                result["errors"].append(f"Original {raw_id} has a malformed envelope")
                continue
            actual_hash = digest(raw)
            source, source_id = raw_payload.get("source"), raw_payload.get("source_id")
            result["raw_messages"].append({
                "id": raw_id, "source": source, "source_id": source_id,
                "raw": raw, "sha256": actual_hash,
            })
            if raw is None or (source, source_id, actual_hash) != (ref["source"], ref["source_id"], ref["sha256"]):
                result["errors"].append(f"Original {raw_id} identity or hash mismatch")
        for raw_id in sorted(raw_refs.keys() - found):
            result["errors"].append(f"Missing original {raw_id}")
        if not result["errors"]:
            result["status"] = "verified"
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        result["errors"].append(str(exc))
    return result


def json_equal(left: Any, right: Any) -> bool:
    """JSON structural equality, keeping booleans distinct from numbers."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(json_equal(left[key], right[key]) for key in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(json_equal(a, b) for a, b in zip(left, right))
    return left == right
