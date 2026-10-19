import test from "node:test";
import assert from "node:assert/strict";
import { evidenceLabel, lastRevision, rawRecordId, recordKey, researchTitle, routingDisposition, routingGroups, safeSourceUrl, sourceText, sourceTimes, traceStages } from "./traceModel.js";

test("missing routing telemetry cannot imply rejection", () => {
  assert.equal(routingDisposition({}), "Not recorded");
  assert.equal(routingDisposition({ verdict: false }), "Filtered out");
  assert.equal(routingDisposition({ verdict: true }), "Passed");
  const stages = traceStages({ nodes: [{ kind: "raw" }] }, "research");
  assert.equal(stages[1].detail, "Telemetry not recorded");
  assert.equal(stages[5].detail, "Unscored research");
});

test("provenance verification does not become investment performance", () => {
  assert.equal(evidenceLabel("verified"), "Verified historical evidence");
  assert.equal(evidenceLabel("broken"), "Evidence verification failed");
  assert.equal(evidenceLabel("legacy_unverified"), "Historical evidence not recorded");
  assert.equal(traceStages({}, "decision", { status: "verified" })[5].detail, "Outcome not available");
});

test("source links reject executable and malformed URLs", () => {
  assert.equal(safeSourceUrl("javascript:alert(1)"), null);
  assert.equal(safeSourceUrl("data:text/html,test"), null);
  assert.equal(safeSourceUrl("not a URL"), null);
  assert.equal(safeSourceUrl("https://www.federalreserve.gov/test"), "https://www.federalreserve.gov/test");
});

test("source text prefers the original long post over its abbreviated envelope", () => {
  assert.equal(sourceText({ data: { text: "short" }, raw: { note_tweet: { note_tweet_results: { result: { text: "full original" } } } } }), "full original");
  assert.equal(sourceText({ raw: { title: "Headline only" } }), "Headline only");
  assert.equal(sourceText({}), null);
});

test("stable selection distinguishes agent thesis IDs and preserves revision order", () => {
  assert.notEqual(recordKey({ agent_id: "a", thesis_id: "x" }), recordKey({ agent_id: "b", thesis_id: "x" }));
  assert.equal(recordKey({ kind: "decision", id: 5 }), "decision:5");
  assert.deepEqual(lastRevision([]), {});
  assert.equal(lastRevision([{ revision_id: "r1" }, { revision_id: "r2" }]).revision_id, "r2");
  assert.equal(researchTitle({ thesis_id: "fed-enforcement-termination-2026-10-16" }), "Fed enforcement termination · 2026-10-16");
});

test("SEC and FRED observation timestamps are never labeled publication times", () => {
  for (const source of ["sec_edgar", "fred"]) {
    assert.deepEqual(sourceTimes({ source, timestamp: "2026-10-19", data: { published_at: null, observed_at: "2026-10-19" } }), { published: null, observed: "2026-10-19" });
  }
  assert.equal(sourceTimes({ source: "sec_edgar", data: { published_at: "2026-08-26", observed_at: "2026-10-19" } }).published, "2026-08-26");
  assert.equal(sourceTimes({ source: "twitter", timestamp: "observation fallback", raw: {} }).published, null);
  assert.equal(sourceTimes({ data: { feed_url: "https://example.com/rss" }, timestamp: "2026-10-16" }).published, "2026-10-16");
});

test("routing groups join identical source revisions but not edited originals", () => {
  const base = { source: "twitter", source_id: "twitter-123", raw_message_id: 7 };
  const grouped = routingGroups([{ ...base, id: 1, raw_sha256: "a", stage: "filter" }, { ...base, id: 2, raw_sha256: "a", stage: "fast" }, { ...base, id: 3, raw_sha256: "b" }]);
  assert.equal(grouped.length, 2);
  assert.equal(grouped[0].events.length, 2);
  assert.equal(grouped[1].events.length, 1);
});

test("conversation requests use only authoritative positive raw record IDs", () => {
  assert.equal(rawRecordId({ record_id: 3, id: "raw:3" }), 3);
  assert.equal(rawRecordId({ id: 12 }), 12);
  assert.equal(rawRecordId({ id: "raw:12" }), 12);
  assert.equal(rawRecordId({ id: "ticket:12" }), null);
  assert.equal(rawRecordId({ id: "twitter-2111978951438647414" }), null);
  assert.equal(rawRecordId({ id: -1 }), null);
  assert.equal(rawRecordId({ id: true }), null);
});
