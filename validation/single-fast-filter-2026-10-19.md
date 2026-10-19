# Single Fast filter acceptance

Issue: https://github.com/ischencheng/Attention-Agent/issues/13

The local deployment now uses the existing `local-ml-filter-v1` consumer group as
a zero-model relay into `fastagent_input`. Fast is the single model gate. The
actual gateway uses `gpt-6-luna`; Mid uses `gpt-6-luna`; Strong remains configured
for `gpt-6.1-sol`.

## Production verification

The accompanying JSON records a real collected source, original **#642**, with
relay event **931** and Fast event **1218**. This was existing pending production
work, not a synthetic input injected for validation.

- The relay's acknowledged output exactly matches Fast's input broker coordinates.
- A read-only Kafka lookup verified identical value bytes, key and headers before
  and after the relay. Stored original content and both routing hashes agree.
- The relay records no model and no usage. Fast rejected the observation with a
  recorded reason and an actual new successful Luna artifact.
- The artifact used the new single-filter prompt, reported one completed turn,
  and made no tool calls. Exactly one new artifact matched this specific short
  request; no matching failed attempt was found in the observed window.
- The API trace includes both events. The dashboard visibly distinguishes relay
  forwarding from the Fast verdict and displays the actual producing model.
- A real Mid Luna cycle reviewed tickets 75–94 and saved durable state. It published
  no annotation because the evidence did not meet the publication gate. The cycle
  start audit and actual run artifact agree on the model.

This verifies routing, actual model execution and saved state. It does not
establish that the underlying judgments are correct or that future returns improve.
Strong's existing state remains intact; this change does not claim a new Strong
model run or a scored production forecast.

## Cutover and regression evidence

Both old classifiers stopped gracefully. Before the policy change, each persisted
filter/Fast audit offset was below its group's next committed offset. There was no
persisted audit awaiting an input commit at that checkpoint. The additive stage
migration ran before the updated sink and relay, and the idle host supervisor
restarted with explicit role settings. Existing groups and topics remained intact;
committed offsets advanced without resets, and historical filter rows remain.

- **112 backend tests passed**, including real PostgreSQL migration, immutable
  audit replay, failed delivery, source context and scheduler model selection.
- **11 frontend semantic tests passed** and the production frontend build passed.
- Relay, Fast, sink and frontend Docker images built and were deployed locally.
- Independent code review found no blocking issue. This was agent review, not human
  review or a claim of external CI results.

The screenshot is stored locally at
`environments/attention_judgment/runs/local-pipeline/single-fast-trace.png`.
Runtime logs, raw model prompts/transcripts and credentials remain ignored.

## Measurement limits

The [historical baseline](single-fast-filter-baseline-2026-10-19.md) documents
separate filter and Fast invocations on matched requests. This live acceptance
sample establishes one Fast invocation and no relay model call for one cold short
request. It is not a controlled estimate of aggregate token or dollar savings.
Cached usage is historical; longer inputs can use several chunks; failed calls
can retry; and the new relay sends previously excluded input to Fast and embeddings.
Old filter rejections were not replayed.
