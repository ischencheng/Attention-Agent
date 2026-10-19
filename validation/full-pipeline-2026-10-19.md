# Live pipeline verification: October 19, 2026

The complete application was started locally and processed real source material.
The accompanying JSON file captures aggregate service, model, database and ledger
evidence at a specific time. It contains no credentials or full agent memory.

## Observed execution

- Sixteen Docker services were running, including Kafka, Flink, persistence,
  filtering, semantic embeddings, Fast, ingestion, API and dashboard.
- The macOS `local.attention-proxy` LaunchAgent supervised the host gateway,
  primary-source collector, Mid/Strong cycles and production evaluation.
- Real authenticated Codex calls completed Mid and Strong cycles. Their existing
  `claude-mid` and `claude-strong` database keys remain for compatibility; the
  model runtime is Codex, not Claude.
- Mid accumulated pending themes and published one factual annotation. Strong
  retained a research thesis with evidence gaps, invalidation conditions and a
  review date. It did not publish an investment decision.
- The production observer independently archived that actual Strong state.
  A repeated observation reused the same version.
- The active Flink job completed checkpoints. Its output is at-least-once:
  startup/recovery replays contribute to clean-delivery counts, so those counts
  must not be interpreted as unique sources.
- SEC filing metadata and company facts reached the real ingestion path. FRED
  reported a missing API key; authenticated FRED retrieval remains unverified.

## Acceptance evidence

Automated checks cover worker acknowledgement and failure handling, real
PostgreSQL provenance, agent state and scheduling, classifier/embedding contracts,
RSS and research-source normalization, research versioning and forecast scoring
boundaries. Suite counts appear in the JSON snapshot. The independent provenance
grader passed all ten cases; its Kafka boundary is captured, while live Kafka
delivery was checked separately through the application.

The frontend production build passed. Actual Codex execution and a real
768-dimensional semantic embedding were also checked outside unit tests.

## Limits

There are no production investment decisions or resolved market forecasts at this
capture. A research thesis and a justified decision to wait are application
outputs, not evidence of forecasting accuracy or investment returns. The evaluator
will score eligible, explicitly declared forecasts only when their observation
windows close.

The pipeline runs on this Mac and needs the host and Docker to remain available.
Research polling preserves observed versions, not intermediate edits overwritten
between polls. The local append-only ledger provides an audit trail, not protection
against an administrator replacing its files.
