# Verification — 2026-10-19

Compact evidence: [validation.json](validation.json). Full original feeds, model
prompts/results/events, ledgers, price responses and failed attempts remain in
Git-ignored `runs/` on this machine. No model output was rewritten after freezing.

## Actual Codex runs

Codex CLI **0.163.0**, requested model **gpt-6.1-sol**, Python 3.12.13. The CLI is
open source; the selected hosted model is separate. The earlier 0.137.0 CLI could
not consume the current account model catalog; rejected requests are saved and
were never counted as completed decisions. The isolated runtime did not change
the user's system CLI or authentication files.

Two successful forward model cycles consumed the same 22 primary-RSS observations
and produced two research theses: local agent hardware/software delivery and
Nordic AI infrastructure deployment versus planned capacity. The second cycle
also exercised restored prior state. A subsequent unchanged-feed daily cycle
skipped the model and created no duplicate judgment.

Neither thesis supplied a numeric price forecast. This is **zero predictive
coverage**, not a successful prediction. Both identified NVDA from source content;
it was not a required portfolio holding. Their day/week/month observation dates
are **2026-10-19 / 2026-10-23 / 2026-10-30**, with entry at the next open after
freezing. All six price observations remain **pending** at this report time.
The business hypotheses have 6–18 month horizons and remain unverified.

The source pilot covers Federal Reserve, NVIDIA, AMD and OpenAI RSS only. It is
not a full-article investigation or the original Twitter/Bluesky/Kafka stream.
The PostgreSQL export bridge was tested against real temporary PostgreSQL, not
against a running production source database.

## Actual longitudinal synthetic replay

Four fresh Codex processes received staged evidence about a fictitious unlisted
semiconductor company: initial capacity/contract claim, a syndicated duplicate,
a cancellation, and a correction to temporary acceptance delay.

| Attempt | Structural checks | Result |
|---|---:|---|
| `synthetic-versioned-01` | 7/14 | Failed; retained in full |
| `synthetic-versioned-02` | 14/14 | Passed under corrected validation |

The first attempt exposed a validator false rejection: references identify whole
documents, and one announcement can contain both support and limiting facts.
The original rule incorrectly required disjoint support/counterevidence IDs.
That rule was removed; source-event counting remains deduplicated, and a regression
test now checks mixed evidence. The scenario and structural grader were unchanged.
Failure of the first commit propagated into the duplicate step, which had no
persisted prior thesis. This was an evaluator defect, not a measured memory loss.

The successful attempt created versions 1, 2 and 3 of one thesis, made no revision
for the duplicate, retained the cancellation after the correction, and never used
future publication or late-arriving evidence. Agent-assisted prose review also
found the planned-versus-realized capacity, cancellation, 90-day delay, absence
of new prepayment and disclosure uncertainty handled consistently. A title's
wording about the contract being restored was slightly ambiguous; its body
correctly said the contract remained valid. This review is not human approval
or an independent financial truth oracle.

This is one disclosed synthetic trajectory. No-memory and summary-only model
comparisons, held-out generalization, financial forecasting advantage, or RL
training gains have **not** been measured. The 14 checks validate structural
behavior, not every sentence of the analysis.

## Real price integration

A real Yahoo chart download for MSFT and SPY over 2025-11-24 through 2025-11-28
returned four completed sessions, correctly omitting Thanksgiving and respecting
the Friday early close. Open-to-close price returns were **3.5811%** and
**3.1236%**, a **0.4574 percentage-point** difference. The MSFT entry-open/daily-close
path had **0.2105%** maximum drawdown. Raw responses and data-vintage hashes are
saved in `runs/market-smoke.json`.

This is a historical data/settlement integration check with a preselected pair,
not a Codex investment decision, backtested strategy or blind forecast. The data
was downloaded on 2026-10-19 and may contain vendor revisions. Price returns
exclude dividends; reported corporate actions leave a horizon unresolved pending
an auditable adjustment. Missing bars are never filled.

## Software and deployment checks

- **63** judgment/market/export/replay tests pass, including real PostgreSQL export.
- **15** Codex adapter/scheduler tests pass (fake CLI failure injection).
- Existing provenance reference **100/100**, baseline **0/100**, six seeded faulty
  implementations rejected; real PostgreSQL, captured Kafka boundary.
- Python compilation, Bash syntax, Compose YAML parsing and `git diff --check` pass.
- Docker images, full Kafka/Flink stack and actual container agent execution were
  **not** run: this host has no Docker binary.

A local weekday **16:35 America/New_York** heartbeat (`attention-proxy`) is active.
It runs the bounded daily command, continues settlement after a model error,
and reports material changes/results/failures. The computer and app must be
available. A configured schedule does not constitute a completed week or month
of observation, and long-running context compaction is not yet implemented.
