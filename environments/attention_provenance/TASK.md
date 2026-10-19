# Attention Proxy: evidence that survives corrections

You are maintaining AI Attention Proxy, an information-consumption pipeline.
Its Fast agent routes attention without summarizing away the original; its Mid
agent curates annotations; its Strong agent makes decisions that must remain
traceable to original evidence. Implement a verifiable, historical evidence
snapshot for each newly published decision. Work in the existing Python skills,
PostgreSQL schema, and FastAPI service, preserving their existing interfaces.

## The incident

An analyst expands a decision after a Mid agent has corrected an annotation.
The dashboard now shows different evidence from what supported that decision.
Two feeds also reused the same source ID, and a replay delivered an old message
again. Ticket text is an enriched copy, so it cannot serve as the original.
The analyst needs the actual original JSON and the annotation as it existed at
publication, with an honest error when that evidence cannot be verified.

## Required behavior

1. Add `prepare_payload(topic, payload)` in `skills/db_write.py`. It returns a
   new payload ready for the existing Kafka producer; it must not mutate its
   argument. `main()` must call it before `produce_message`. Invalid evidence
   must raise an error and prevent publication, including when the DB is down.
   Preserve the existing framework-analysis validation for decisions.
   Preserve all caller payload fields and values, except deduplicating/sorting
   `related_annotations` and adding the generated snapshot.
2. For `decision_packet`, require a nonempty `related_annotations` list of
   positive integer IDs (booleans are not IDs). Each cited annotation must
   have a nonempty `attention_ticket_ids` list with the same ID rules. Resolve
   every reference in one consistent database snapshot. Duplicate references
   are accepted and deduplicated; order output arrays by ID.
3. Each ticket must contain nonempty `source` and `source_id` strings and a
   non-null `raw` JSON value. Match originals in `raw_messages` by all three:
   `payload.source`, `payload.source_id`, and the canonical digest of the
   row's `raw` column. The ticket's `data`, text, or summary is never an
   authoritative original. Do not substitute a different revision or another
   feed with the same ID. If identical originals were delivered repeatedly,
   select the smallest matching `raw_messages.id`. Reject unmatched evidence.
4. The prepared decision must contain the following generated snapshot.
   Reject a caller-supplied `evidence_snapshot`; do not trust one supplied by
   an agent. `payload` below is the complete annotation JSON as read then.

   ```json
   {
     "evidence_snapshot": {
       "version": 1,
       "annotations": [{"id": 1, "payload": {}, "sha256": "..."}],
       "tickets": [{"id": 2, "source": "twitter", "source_id": "x",
                    "raw_message_id": 3, "raw_sha256": "..."}],
       "raw_refs": [{"id": 3, "source": "twitter", "source_id": "x", "sha256": "..."}]
     }
   }
   ```

   A digest is SHA256 of UTF-8 encoded
   `json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)`.
   Preservation means JSON values, not wire-level whitespace/key ordering:
   this project already stores originals as PostgreSQL JSONB.
5. Add `GET /api/decisions/{decision_id}/evidence` to the existing FastAPI app.
   Missing decisions return 404. Existing decisions return:

   ```json
   {"decision_id": 1, "status": "verified", "evidence_snapshot": {},
    "raw_messages": [{"id": 3, "source": "twitter", "source_id": "x",
                      "raw": {}, "sha256": "..."}], "errors": []}
   ```

   `verified` means the stored snapshot is structurally consistent, the
   annotation hashes match, the decision/annotation/ticket/raw reference graph
   is complete, and original source identities and hashes still match. Return
   `broken` with nonempty diagnostic `errors` for malformed snapshots or
   missing/mismatched originals. Never return `verified` for an empty or
   disconnected evidence graph. For old decisions with no snapshot, return
   `legacy_unverified`, a null snapshot, no reconstructed originals, and an
   explanation. Do not invent historical truth by joining current annotations.
   Array order is ascending ID. Preserve existing feed/ticket/decision routes.
6. Later edits to annotations must not change the published snapshot. Fix
   `skills/db_update.py` so appending JSON objects (e.g. `facts`) deduplicates
   by JSON equality, and validate references in both `append` and `set`
   updates. Updates must be atomic: a failed validation changes nothing.
   Reject changes to a published decision's `related_annotations` or
   `evidence_snapshot` through that skill.
7. Supply additive, idempotent SQL under `postgres/init/002_*.sql` to reject
   UPDATE/DELETE of `raw_messages` and changes to a stored decision's
   `evidence_snapshot` or `related_annotations` once a snapshot is present.
   Existing rows must remain readable. This protects normal application
   writes, not a malicious DB superuser. Document how to migrate an existing
   database; init scripts alone run only for fresh volumes.
8. The same persisted decision must produce the same evidence JSON after
   reopening connections or restarting the application. Concurrent annotation
   corrections must not produce a mixed-version snapshot.

## Runtime and evaluation

- Python 3.12 and PostgreSQL with pgvector; local evaluation can use `pgserver`.
- All data are synthetic and fictional. No API keys, market feeds, trading,
  or paid model calls are needed.
- The grader calls the real write/update skill paths and FastAPI route, with
  real PostgreSQL persistence. The Kafka publication boundary is captured in
  process and persisted for replay; broker delivery/rebalances are out of scope.
- Evaluation varies identifiers, insertion orders, Unicode/nested originals,
  duplicate deliveries, conflicting revisions, corrections, and broken links.
  Hidden cases obey the requirements above; no hidden importance/profit rubric.
- Development feedback is allowed. Grading is controlled outside your solution
  workspace; editing tests, inventing output logs, or returning success without
  implementing the behavior does not satisfy the task.
- Suggested initial attempt budget: 90 minutes. Record actual elapsed time,
  model/version and tool/token costs if available. This budget is a starting
  protocol, not an empirically established difficulty rating.

## Acceptance

The externally run grader reports separate groups for original preservation,
identity/replay, historical consistency, invalid evidence, API compatibility,
and persistence. All critical provenance groups must pass for success. An
unimplemented endpoint or a no-op publisher cannot pass by avoiding decisions.
Natural-language reasoning quality, P&L, live Kafka reliability, and proof that
an LLM cognitively read an original are not measured by this task.
