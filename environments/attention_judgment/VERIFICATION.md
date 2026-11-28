# Verification — 2026-10-19

This report covers the historical Attention Judgment pilot. It summarizes model
runs, a synthetic replay, and software checks as of October 19, 2026.

## Forward pilot

Two completed Codex model cycles used the same 22 primary-feed observations.
They recorded two hypotheses and exercised recovery of prior state. A subsequent
cycle detected an unchanged feed, skipped inference, and made no duplicate
judgment. Neither cycle recorded a numeric forecast. All six price observations
were pending at the report cutoff, with zero scored predictions.

This pilot covered the recorded host cycles. Full Docker/Kafka/Flink execution,
containerized agents, and month-long follow-up were outside its scope.

## Synthetic longitudinal replay

The scenario staged four updates about a fictional semiconductor company.
The initial run passed 7 of 14 checks. Its validator required disjoint support
and counterevidence references, even when the same document contained facts on
both sides. After correcting that rule and adding a regression test, a second
run passed all 14 checks. It carried the thesis across three versions, skipped a
duplicate, and kept the cancellation evidence after a later correction.

The checks covered thesis continuity, mixed evidence, deduplication, and
time-gated observations. Comparisons against stateless or summary-only agents,
held-out forecasting, and training effects were outside the experiment's scope.

## Price and software checks

- A historical MSFT and SPY price check returned four completed trading
  sessions, skipping Thanksgiving and respecting the Friday early close. The
  two tickers were selected in advance.
- At the report date, 63 judgment, market, export, and replay tests passed,
  including an export against temporary PostgreSQL. A separate group of 15 Codex
  adapter and scheduler tests passed with a test CLI.
- See [the provenance report](../attention_provenance/VERIFICATION.md) for the
  repository-level grader and its PostgreSQL and Kafka boundary checks.
