"""Independent integration outcomes for the frozen attention provenance task.

These cases use only the harness's public World interface and the task's JSON
contract. They do not import a candidate implementation or inspect its source.
"""

from __future__ import annotations

import hashlib
import json


def clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def digest(value):
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def decision(annotation_ids):
    return {
        "agent_id": "synthetic-strong-agent",
        "signal": "review",
        "reason": "Preserve the original evidence, including its uncertainty.",
        "related_annotations": list(annotation_ids),
        "framework_analysis": {
            "traversability": "Synthetic scenario only.",
            "barbell_position": "No real allocation.",
            "moat": "Not assessed as an investment.",
            "cycle_position": "Fictional event sequence.",
            "prohibitions_check": "No execution requested.",
            "technical_analysis": "No price or trading assertion.",
        },
    }


def add_original(world, source, source_id, raw):
    envelope = {
        "source": source,
        "source_id": source_id,
        "data": {"text": "Envelope text is not the authoritative original."},
        "raw": clone(raw),
    }
    return world.insert("raw_messages", envelope, raw=raw)


def add_ticket(world, source, source_id, raw):
    payload = {
        "source": source,
        "source_id": source_id,
        "data": {"text": "ENRICHED COPY: this deliberately differs from raw."},
        "summary": "Do not use this summary as evidence.",
        "raw": clone(raw),
    }
    return world.insert("attention_tickets", payload, raw=raw), payload


def add_annotation(world, ticket_ids, label="original interpretation"):
    payload = {
        "agent_id": "synthetic-mid-agent",
        "theme": label,
        "attention_ticket_ids": list(ticket_ids),
        "facts": [{"text": "尚未证实", "attributes": {"tags": ["α", "β"]}}],
        "analysis": {"confidence": "uncertain", "alternatives": ["A", "B"]},
    }
    return world.insert("annotations", payload), payload


def chain(world, suffix="base", raw=None):
    if raw is None:
        raw = {
            "text": "原文≠摘要 🛰️\nSecond line",
            "nested": {"items": [None, False, 0, {"z": "é", "a": "é"}]},
            "number": world.seed + 17,
        }
    source = "synthetic-feed-" + suffix
    source_id = f"fictional-{world.seed}-{suffix}"
    raw_id = add_original(world, source, source_id, raw)
    ticket_id, ticket = add_ticket(world, source, source_id, raw)
    annotation_id, annotation = add_annotation(world, [ticket_id])
    return {
        "source": source, "source_id": source_id, "raw": raw,
        "raw_id": raw_id, "ticket_id": ticket_id, "ticket": ticket,
        "annotation_id": annotation_id, "annotation": annotation,
    }


def expected_snapshot(annotations, tickets, originals):
    return {
        "version": 1,
        "annotations": [
            {"id": row_id, "payload": clone(payload), "sha256": digest(payload)}
            for row_id, payload in sorted(annotations.items())
        ],
        "tickets": [
            {
                "id": row_id, "source": source, "source_id": source_id,
                "raw_message_id": raw_id, "raw_sha256": digest(raw),
            }
            for row_id, (source, source_id, raw_id, raw) in sorted(tickets.items())
        ],
        "raw_refs": [
            {"id": row_id, "source": source, "source_id": source_id,
             "sha256": digest(raw)}
            for row_id, (source, source_id, raw) in sorted(originals.items())
        ],
    }


def chain_snapshot(c):
    return expected_snapshot(
        {c["annotation_id"]: c["annotation"]},
        {c["ticket_id"]: (c["source"], c["source_id"], c["raw_id"], c["raw"])},
        {c["raw_id"]: (c["source"], c["source_id"], c["raw"])},
    )


def expected_raws(originals):
    return [
        {"id": row_id, "source": source, "source_id": source_id,
         "raw": clone(raw), "sha256": digest(raw)}
        for row_id, (source, source_id, raw) in sorted(originals.items())
    ]


def evidence(world, decision_id):
    response = world.api(f"/api/decisions/{decision_id}/evidence")
    assert response["status_code"] == 200, (
        f"Existing decision {decision_id} evidence must return HTTP 200: {response}"
    )
    assert isinstance(response["body"], dict), "Evidence response must be a JSON object"
    return response["body"]


def assert_verified(world, decision_id, snapshot, originals):
    body = evidence(world, decision_id)
    assert body.get("decision_id") == decision_id, "Evidence has the wrong decision ID"
    assert body.get("status") == "verified", f"Valid evidence was not verified: {body}"
    assert body.get("evidence_snapshot") == snapshot, "Published snapshot was changed or incomplete"
    assert body.get("raw_messages") == expected_raws(originals), (
        "Evidence must return exact original JSON, canonical hashes, and ascending unique IDs"
    )
    assert body.get("errors") == [], "Verified evidence must have no diagnostics"
    return body


def stored_payload(world, table, row_id):
    rows = world.sql(f"SELECT payload FROM {table} WHERE id = %s", (row_id,))
    assert len(rows) == 1, f"Expected persisted {table} row {row_id}"
    return rows[0][0]


def assert_rejected(result, label):
    assert isinstance(result, dict) and result.get("ok") is False, (
        f"{label} must fail explicitly, got {result}"
    )
    assert result.get("error"), f"{label} must explain its rejection"


def assert_update_rejected(world, params, label):
    before = stored_payload(world, params["table"], params["id"])
    try:
        result = world.update(params)
    except Exception:
        result = {"error": "worker raised"}
    assert isinstance(result, dict) and result.get("error"), (
        f"{label} must report failure: {result}"
    )
    assert stored_payload(world, params["table"], params["id"]) == before, (
        f"{label} partially changed a row despite failed validation"
    )


def original_preservation(world):
    c = chain(world, "unicode")
    payload = decision([c["annotation_id"]])
    before = clone(payload)
    prepared = world.prepare(payload)
    assert prepared.get("ok") is True, f"Valid decision preparation failed: {prepared}"
    assert prepared["result"]["evidence_snapshot"] == chain_snapshot(c)
    for key, value in before.items():
        expected = sorted(set(value)) if key == "related_annotations" else value
        assert prepared["result"].get(key) == expected, f"Preparation changed or dropped decision field {key}"
    assert payload == before, "prepare_payload mutated its argument"
    decision_id, published = world.publish(payload)
    for key, value in before.items():
        expected = sorted(set(value)) if key == "related_annotations" else value
        assert published.get(key) == expected, f"Publication changed or dropped decision field {key}"
    assert published.get("evidence_snapshot") == chain_snapshot(c), (
        "The actual main/producer path did not publish the generated snapshot"
    )
    assert stored_payload(world, "decision_packets", decision_id) == published
    assert_verified(world, decision_id, chain_snapshot(c), {
        c["raw_id"]: (c["source"], c["source_id"], c["raw"]),
    })
    for value in (False, 0, "", ["original", None, {"nested": True}]):
        scalar = chain(world, "json-" + str(type(value).__name__), raw=value)
        scalar_id, _ = world.publish(decision([scalar["annotation_id"]]))
        assert_verified(world, scalar_id, chain_snapshot(scalar), {
            scalar["raw_id"]: (scalar["source"], scalar["source_id"], value),
        })


def identity_replay(world):
    source_id = f"shared-id-{world.seed}"
    source = "feed-correct"
    raw = {"revision": 2, "text": "同一编号不等于同一条证据", "items": [2, 1]}
    add_original(world, "feed-wrong", source_id, raw)
    add_original(world, source, source_id, {"revision": 1, "text": "stale"})
    correct_id = add_original(world, source, source_id, raw)
    replay_id = add_original(world, source, source_id, clone(raw))
    assert correct_id < replay_id
    first_id, _ = add_ticket(world, source, source_id, raw)
    second_id, _ = add_ticket(world, source, source_id, raw)
    other_raw = {"revision": 1, "text": "other source", "null": None}
    other_id = add_original(world, "feed-other", source_id, other_raw)
    third_id, _ = add_ticket(world, "feed-other", source_id, other_raw)
    first_aid, first_ann = add_annotation(world, [second_id, first_id, second_id])
    second_aid, second_ann = add_annotation(world, [third_id, first_id, third_id])
    snapshot = expected_snapshot(
        {first_aid: first_ann, second_aid: second_ann},
        {first_id: (source, source_id, correct_id, raw),
         second_id: (source, source_id, correct_id, raw),
         third_id: ("feed-other", source_id, other_id, other_raw)},
        {correct_id: (source, source_id, raw), other_id: ("feed-other", source_id, other_raw)},
    )
    decision_id, prepared = world.publish(decision([second_aid, first_aid, second_aid]))
    assert prepared.get("evidence_snapshot") == snapshot, (
        "Identity/replay resolution must match source, source_id, and raw digest, choosing minimum ID"
    )
    assert_verified(world, decision_id, snapshot, {
        correct_id: (source, source_id, raw), other_id: ("feed-other", source_id, other_raw),
    })


def historical_consistency(world):
    c = chain(world, "historic")
    later = chain(world, "later", raw={"text": "correction", "revision": 3})
    decision_id, _ = world.publish(decision([c["annotation_id"]]))
    snapshot = chain_snapshot(c)
    old_fact = clone(c["annotation"]["facts"][0])
    # Same JSON object with another key insertion order must remain one fact.
    reordered_fact = {"attributes": old_fact["attributes"], "text": old_fact["text"]}
    new_fact = {"text": "已更正", "attributes": {"tags": ["γ"], "details": [1, None]}}
    update = world.update({
        "table": "annotations", "id": c["annotation_id"],
        "append": {"facts": [reordered_fact, new_fact, clone(new_fact)]},
        "set": {"analysis": {"confidence": "corrected"},
                "attention_ticket_ids": [later["ticket_id"]]},
    })
    assert not update.get("error"), f"Valid nested object append failed: {update}"
    current = stored_payload(world, "annotations", c["annotation_id"])
    assert current["facts"] == [old_fact, new_fact], "Object facts must deduplicate by JSON equality"
    assert current["attention_ticket_ids"] == [later["ticket_id"]]
    assert current["analysis"] == {"confidence": "corrected"}
    assert_verified(world, decision_id, snapshot, {
        c["raw_id"]: (c["source"], c["source_id"], c["raw"]),
    })
    # A subsequent decision must capture the corrected annotation, not a cache.
    new_id, new_payload = world.publish(decision([c["annotation_id"]]))
    corrected = expected_snapshot(
        {c["annotation_id"]: current},
        {later["ticket_id"]: (later["source"], later["source_id"], later["raw_id"], later["raw"])},
        {later["raw_id"]: (later["source"], later["source_id"], later["raw"])},
    )
    assert new_payload["evidence_snapshot"] == corrected
    assert_verified(world, new_id, corrected, {
        later["raw_id"]: (later["source"], later["source_id"], later["raw"]),
    })


def atomic_reference_updates(world):
    c = chain(world, "updates")
    missing = 9_000_000 + world.seed
    for mode in ("append", "set"):
        for invalid in ([missing], [True], [0], [-1], [str(c["ticket_id"])]):
            params = {
                "table": "annotations", "id": c["annotation_id"],
                mode: {"attention_ticket_ids": invalid},
            }
            if mode == "append":
                params["set"] = {"theme": "MUST NOT PARTIALLY COMMIT"}
            else:
                params["append"] = {"facts": [{"text": "MUST NOT PARTIALLY COMMIT"}]}
            assert_update_rejected(world, params, f"{mode} invalid ticket references {invalid}")
    legacy_id = world.insert("decision_packets", decision([c["annotation_id"]]))
    for mode in ("append", "set"):
        assert_update_rejected(world, {
            "table": "decision_packets", "id": legacy_id,
            mode: {"related_annotations": [missing]},
        }, f"{mode} must validate decision annotation references")
    published_id, _ = world.publish(decision([c["annotation_id"]]))
    another = chain(world, "another")
    for mode in ("append", "set"):
        assert_update_rejected(world, {
            "table": "decision_packets", "id": published_id,
            mode: {"related_annotations": [another["annotation_id"]]},
        }, f"{mode} cannot rewrite published evidence links")
    assert_update_rejected(world, {
        "table": "decision_packets", "id": published_id,
        "set": {"evidence_snapshot": None, "reason": "MUST NOT COMMIT"},
    }, "Published snapshot cannot be cleared through db_update")
    reason = "The analyst corrected a non-evidence note."
    updated = world.update({
        "table": "decision_packets", "id": published_id,
        "set": {"reason": reason},
    })
    assert not updated.get("error"), f"Ordinary decision fields must remain editable through db_update: {updated}"
    current = stored_payload(world, "decision_packets", published_id)
    assert current.get("reason") == reason, "Decision update reported success without persisting the note"
    assert current.get("related_annotations") == [c["annotation_id"]]
    assert current.get("evidence_snapshot") == chain_snapshot(c)
    assert_verified(world, published_id, chain_snapshot(c), {
        c["raw_id"]: (c["source"], c["source_id"], c["raw"]),
    })


def invalid_evidence(world):
    c = chain(world, "invalid")
    control = world.prepare(decision([c["annotation_id"]]))
    assert control.get("ok") is True, f"Rejection checks need a working positive control: {control}"
    assert control["result"].get("evidence_snapshot") == chain_snapshot(c)
    missing = 8_000_000 + world.seed
    for refs in (None, [], [missing], [0], [-1], [True], [str(c["annotation_id"])], "1"):
        payload = decision([c["annotation_id"]])
        payload["related_annotations"] = refs
        assert_rejected(world.prepare(payload), f"Invalid decision references {refs!r}")
    payload = decision([c["annotation_id"]])
    del payload["related_annotations"]
    assert_rejected(world.prepare(payload), "Missing related_annotations")
    for refs in (None, [], [missing], [0], [-2], [True], [str(c["ticket_id"])], "1"):
        ann = clone(c["annotation"])
        ann["attention_ticket_ids"] = refs
        aid = world.insert("annotations", ann)
        assert_rejected(world.prepare(decision([aid])), f"Invalid ticket references {refs!r}")
    bad_tickets = []
    for field, values in (("source", (None, "", 7)), ("source_id", (None, "", 7))):
        for value in values:
            ticket = clone(c["ticket"])
            ticket[field] = value
            bad_tickets.append((f"invalid {field}: {value!r}", ticket))
    for mode in ("missing", "null", "different revision", "other feed"):
        ticket = clone(c["ticket"])
        if mode == "missing":
            del ticket["raw"]
        elif mode == "null":
            ticket["raw"] = None
        elif mode == "different revision":
            ticket["raw"] = {"text": "No original has this revision"}
        else:
            ticket["source"] = "feed-with-no-original"
        bad_tickets.append((mode, ticket))
    for label, ticket in bad_tickets:
        tid = world.insert("attention_tickets", ticket)
        aid, _ = add_annotation(world, [tid])
        assert_rejected(world.prepare(decision([aid])), label)
    for supplied in (None, {}, chain_snapshot(c)):
        payload = decision([c["annotation_id"]])
        payload["evidence_snapshot"] = supplied
        assert_rejected(world.prepare(payload), "Caller-supplied evidence_snapshot")
    for missing_field in decision([])["framework_analysis"]:
        payload = decision([c["annotation_id"]])
        del payload["framework_analysis"][missing_field]
        assert_rejected(world.prepare(payload), f"Missing framework field {missing_field}")


def publish_fail_closed(world):
    c = chain(world, "publication")
    valid = decision([c["annotation_id"]])
    assert_rejected(world.prepare(valid, outage=True), "DB outage during prepare")
    bad = decision([7_000_000 + world.seed])
    for payload, outage in ((bad, False), (valid, True)):
        response = world.call("write", {"topic": "decision_packet", "payload": payload}, outage=outage)
        assert response.get("ok") is True, f"Publication harness failed: {response}"
        result = response["result"]
        assert not result.get("published"), "Rejected publication still reached the Kafka boundary"
        assert result.get("exit_code") or result.get("error") or "error" in result.get("stdout", "").lower(), (
            f"Rejected publication must report a diagnostic: {result}"
        )
    supplied = clone(valid)
    supplied["evidence_snapshot"] = chain_snapshot(c)
    response = world.call("write", {"topic": "decision_packet", "payload": supplied})
    assert response.get("ok") is True, f"Publication harness failed: {response}"
    result = response["result"]
    assert not result.get("published"), "Supplied snapshot was published before rejection"
    assert result.get("exit_code") or result.get("error") or "error" in result.get("stdout", "").lower(), (
        f"Supplied snapshot must be rejected with a diagnostic: {result}"
    )
    assert world.sql("SELECT COUNT(*) FROM decision_packets")[0][0] == 0


def api_validation(world):
    c = chain(world, "api")
    base = decision([c["annotation_id"]])
    legacy_id = world.insert("decision_packets", base)
    legacy = evidence(world, legacy_id)
    assert legacy.get("status") == "legacy_unverified"
    assert legacy.get("evidence_snapshot") is None
    assert legacy.get("raw_messages") == [], "Legacy decisions must not reconstruct evidence from current rows"
    assert legacy.get("errors"), "Legacy status must explain missing historical evidence"
    assert world.api(f"/api/decisions/{6_000_000 + world.seed}/evidence")["status_code"] == 404
    good = chain_snapshot(c)
    malformed = [None, [], {}, {"version": 1, "annotations": [], "tickets": [], "raw_refs": []}]
    for field in ("annotations", "tickets", "raw_refs"):
        bad = clone(good)
        bad[field] = []
        malformed.append(bad)
    bad = clone(good)
    bad["annotations"][0]["sha256"] = "0" * 64
    malformed.append(bad)
    bad = clone(good)
    bad["annotations"][0]["payload"]["attention_ticket_ids"] = [5_000_000 + world.seed]
    bad["annotations"][0]["sha256"] = digest(bad["annotations"][0]["payload"])
    malformed.append(bad)
    bad = clone(good)
    bad["tickets"][0]["raw_sha256"] = "1" * 64
    malformed.append(bad)
    bad = clone(good)
    bad["tickets"][0]["source"] = bad["raw_refs"][0]["source"] = "wrong-source"
    malformed.append(bad)
    bad = clone(good)
    bad["tickets"][0]["raw_message_id"] = bad["raw_refs"][0]["id"] = 4_000_000 + world.seed
    malformed.append(bad)
    bad = clone(good)
    bad["tickets"].append(clone(bad["tickets"][0]))
    malformed.append(bad)
    bad = clone(good)
    bad["version"] = 999
    malformed.append(bad)
    for index, snapshot in enumerate(malformed):
        payload = clone(base)
        payload["evidence_snapshot"] = snapshot
        row_id = world.insert("decision_packets", payload)
        body = evidence(world, row_id)
        assert body.get("status") == "broken", f"Malformed snapshot {index} must not verify: {body}"
        assert body.get("errors"), f"Malformed snapshot {index} needs diagnostic errors"
    disconnected = clone(base)
    disconnected["related_annotations"] = [3_000_000 + world.seed]
    disconnected["evidence_snapshot"] = good
    row_id = world.insert("decision_packets", disconnected)
    assert evidence(world, row_id).get("status") == "broken", "Decision references must match its snapshot graph"
    # Existing routes must still return the persisted objects through their old envelope.
    for path, expected_id in (("/api/decisions", legacy_id), ("/api/annotations", c["annotation_id"]),
                              ("/api/tickets", c["ticket_id"]), ("/api/feed", legacy_id)):
        response = world.api(path)
        assert response["status_code"] == 200, f"Existing route broke: {path}"
        body = response["body"]
        assert isinstance(body.get("items"), list) and body.get("count") == len(body["items"])
        assert any(item["id"] == expected_id for item in body["items"]), f"{path} lost persisted data"
    response = world.api(f"/api/tickets/{c['ticket_id']}")
    assert response["status_code"] == 200 and response["body"]["payload"] == c["ticket"]


def immutable_storage(world):
    c = chain(world, "immutable")
    row_id, published = world.publish(decision([c["annotation_id"]]))
    raw_before = world.sql("SELECT payload, raw FROM raw_messages WHERE id = %s", (c["raw_id"],))
    # Existing-volume migration and rerun must both preserve persisted evidence.
    world.reapply_migrations()
    assert stored_payload(world, "decision_packets", row_id) == published, (
        "Reapplying the additive migration changed an existing decision"
    )
    assert world.sql("SELECT payload, raw FROM raw_messages WHERE id = %s", (c["raw_id"],)) == raw_before, (
        "Reapplying the additive migration changed or removed an existing original"
    )
    statements = [
        ("UPDATE raw_messages SET raw = %s::jsonb WHERE id = %s", (json.dumps({"tampered": True}), c["raw_id"])),
        ("UPDATE raw_messages SET payload = payload WHERE id = %s", (c["raw_id"],)),
        ("DELETE FROM raw_messages WHERE id = %s", (c["raw_id"],)),
        ("UPDATE decision_packets SET payload = payload - 'evidence_snapshot' WHERE id = %s", (row_id,)),
        ("UPDATE decision_packets SET payload = jsonb_set(payload, '{evidence_snapshot}', 'null'::jsonb) WHERE id = %s", (row_id,)),
        ("UPDATE decision_packets SET payload = jsonb_set(payload, '{related_annotations}', '[]'::jsonb) WHERE id = %s", (row_id,)),
    ]
    for query, params in statements:
        rejected = False
        try:
            world.sql(query, params)
        except Exception:
            rejected = True
        assert rejected, f"Ordinary SQL mutation bypassed provenance immutability: {query}"
    assert stored_payload(world, "decision_packets", row_id) == published
    world.sql(
        "UPDATE decision_packets SET payload = jsonb_set(payload, '{reason}', %s::jsonb) WHERE id = %s",
        (json.dumps("Ordinary non-evidence fields remain editable."), row_id),
    )
    assert stored_payload(world, "decision_packets", row_id)["reason"] == "Ordinary non-evidence fields remain editable."
    assert_verified(world, row_id, chain_snapshot(c), {
        c["raw_id"]: (c["source"], c["source_id"], c["raw"]),
    })


def restart_persistence(world):
    c = chain(world, "restart")
    row_id, prepared = world.publish(decision([c["annotation_id"]]))
    expected = assert_verified(world, row_id, prepared["evidence_snapshot"], {
        c["raw_id"]: (c["source"], c["source_id"], c["raw"]),
    })
    # World.api starts a fresh application worker each time; no Python cache survives.
    for _ in range(3):
        assert evidence(world, row_id) == expected, "Evidence changed after application/connection restart"
    second_id, second = world.publish(decision([c["annotation_id"]]))
    assert second_id != row_id
    assert second["evidence_snapshot"] == prepared["evidence_snapshot"], "Stable evidence produced nondeterministic snapshots"


def consistent_snapshot(world):
    c = chain(world, "race")
    new_raw = {"text": "Atomically corrected source and annotation", "revision": world.seed + 1}
    new_raw_id = add_original(world, c["source"], c["source_id"], new_raw)
    new_ticket = clone(c["ticket"])
    new_ticket["raw"] = new_raw
    new_annotation = clone(c["annotation"])
    new_annotation["theme"] = "corrected in a concurrent committed transaction"
    new_annotation["facts"] = [{"text": "new version", "revision": world.seed + 1}]
    result = world.call("prepare", {
        "topic": "decision_packet", "payload": decision([c["annotation_id"]]),
    }, race={
        "annotation_id": c["annotation_id"], "annotation_payload": new_annotation,
        "ticket_id": c["ticket_id"], "ticket_payload": new_ticket,
    })
    assert result.get("ok") is True, f"A valid concurrent correction should yield a consistent snapshot: {result}"
    # A solution may lock the selected rows, preventing the fixture's concurrent
    # update until after preparation. Both a complete old snapshot and a complete
    # new snapshot satisfy the contract; a mixed snapshot does not.
    new_snapshot = expected_snapshot(
        {c["annotation_id"]: new_annotation},
        {c["ticket_id"]: (c["source"], c["source_id"], new_raw_id, new_raw)},
        {new_raw_id: (c["source"], c["source_id"], new_raw)},
    )
    snapshot = result["result"].get("evidence_snapshot")
    assert snapshot in (chain_snapshot(c), new_snapshot), (
        "Concurrent correction mixed an old annotation with new ticket evidence (or vice versa)"
    )


CASES = [
    ("original_preservation", 14, original_preservation),
    ("identity_replay", 12, identity_replay),
    ("historical_consistency", 12, historical_consistency),
    ("atomic_reference_updates", 10, atomic_reference_updates),
    ("invalid_evidence", 12, invalid_evidence),
    ("publish_fail_closed", 8, publish_fail_closed),
    ("api_validation", 12, api_validation),
    ("immutable_storage", 8, immutable_storage),
    ("restart_persistence", 6, restart_persistence),
    ("consistent_snapshot", 6, consistent_snapshot),
]

assert sum(weight for _, weight, _ in CASES) == 100
