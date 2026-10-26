# Attention Judgment

A topic-led, continuing research environment for AI Attention Proxy. It discovers
questions from the original topic scope, keeps versioned evidence-backed theses,
and observes later outcomes. There is no fixed portfolio or mandatory trade.

The design draws on the local MrBrain material, Mechanize founders' environment
essays, and the pinned Agent Book. See [DESIGN.zh-CN.md](DESIGN.zh-CN.md) for
sources, [TASK.md](TASK.md) for the target behavior, and [VERIFICATION.md](VERIFICATION.md)
for what has actually run. The task contract is broader than this first runtime;
the limitations below are part of its reported status.

## Financial decision RL interface

For policy training and offline evaluation, start with the
[financial decision environment](../financial_decision/README.md). It provides
reset/step, explicit actions, a reward contract and a free synthetic episode.

The actual production pipeline runs through `scripts/local_pipeline.py`; see
[LOCAL_RUNTIME.md](../../LOCAL_RUNTIME.md). Its independent evaluator observes
actual Strong decision packets and research state. The RSS commands below are a
separate historical pilot and do not run the production pipeline.

## Legacy RSS pilot

From the repository root, with Python 3.12 and Codex CLI **0.163.0**:

```bash
uv venv --python 3.12
uv pip install --python .venv/bin/python -r environments/attention_judgment/requirements.txt
npm install --prefix environments/attention_judgment/runs/runtime --no-audit --no-fund @openai/codex@0.163.0
# Authenticate through the CLI if needed; never copy tokens into artifacts.
environments/attention_judgment/runs/runtime/node_modules/.bin/codex login
.venv/bin/python environments/attention_judgment/daily.py \
  --codex-bin "$PWD/environments/attention_judgment/runs/runtime/node_modules/.bin/codex" \
  --model <model-available-to-your-account> --timeout 240
```

The verified local account run used `gpt-6.1-sol`. Model availability belongs to
the account, not the repository. CLI 0.137.0 failed against this account's newer
model catalog; those failed attempts were retained and did not create judgments.
The open-source component is the Codex harness; the hosted model is separate.

`daily.py` locks concurrent daily runs, calls the model once, settles previous
judgments even if the model fails, and writes:

- `runs/paper/ledger.sqlite3`: hash-linked, append-only observations, judgments,
  frozen forecasts, market snapshots and settlements.
- `runs/paper/attempts/`: source snapshots, prompt/schema, Codex events, final JSON,
  CLI version, usage, errors and per-attempt summary.
- `runs/paper/REPORT.zh-CN.md`, `report.json`, `daily-*.json`: readable current
  state and audit reports. These local runtime artifacts are Git-ignored.

Separate commands are available:

```bash
.venv/bin/python environments/attention_judgment/run.py cycle --fetch --codex-bin <path> --model <model>
.venv/bin/python environments/attention_judgment/run.py settle
.venv/bin/python environments/attention_judgment/run.py report
.venv/bin/python environments/attention_judgment/daily.py --settle-only
```

The pilot requires an explicit invocation; it is separate from the production
supervisor and its heartbeat. Week/month resolution uses the same settlement
implementation. Holidays and early closes use XNYS.

## Original stream and bounded pilot

The original scopes are preserved in `scope.json`: AI, earnings, guidance, capex,
datacenters, storage, power/energy/nuclear, semiconductors, GPUs, inference/LLMs,
margins/valuation, CPI, Fed and yields; followed Twitter is a separate channel.
The full product still uses Kafka/Flink/PostgreSQL and its original collectors.

`--fetch` is a bounded **public-primary-RSS pilot** (Fed, NVIDIA, AMD and OpenAI)
for a host without the Docker stack. These publishers do not define a stock list.
It saves original RSS, declares excerpts/truncation and reports source failures.
It is not equivalent to monitoring all followed accounts, financial news, or
full articles. A model is permitted to conclude that more research is needed.

The PostgreSQL bridge exports `raw_messages.raw` with actual database arrival
timestamps (read-only, repeatable-read, no enriched-text fallback):

```bash
# Uses the project's existing DB_* environment settings.
.venv/bin/python skills/export_observations.py --since 2026-10-01T00:00:00Z \
  --output environments/attention_judgment/runs/originals.jsonl
.venv/bin/python environments/attention_judgment/run.py cycle \
  --input environments/attention_judgment/runs/originals.jsonl --codex-bin <path> --model <model>
```

The default export has no row cap. With `--limit`, continue using both returned
cursor fields (`--since` and `--after-row-id`) to preserve timestamp ties.
Exporter tests require the provenance environment's pgserver/psycopg2 dependencies;
the live RSS runner itself does not need PostgreSQL.

`--input path.jsonl` consumes originals from any configured collector. Each line:

```json
{"source":"publisher","source_id":"stable-url-or-id","published_at":"2026-10-19T14:00:00Z","available_at":"2026-10-19T14:03:00Z","event_id":"optional-shared-causal-id","raw":{"title":"Original title","text":"Original material"}}
```

The content hash distinguishes revisions. First-seen archived metadata stays
fixed on redelivery. Both timestamps must be visible at the cutoff. All inbound
originals are preserved, even when the bounded prompt cannot process them yet;
only a committed cycle advances reviewed IDs. Earlier referenced evidence stays
in context; unreviewed observations are processed oldest-first. Long-running
memory consolidation/context compaction is not implemented yet: monitor prompt
size and token usage rather than silently dropping old counterevidence.

Use `--as-of <timezone-aware-time> --ledger <separate-experiment.sqlite3>` for
historical replay. Replay cutoffs must advance, and replay memory/results never
become forward headline performance. Later-downloaded historical prices are not
historical data vintages; the model may already know old outcomes.

## What gets measured

Judgments preserve stable thesis IDs, revisions, supporting/contrary originals,
MrBrain dimensions, uncertainty, invalidation, intended horizon and research
questions. Research/watch/abstain are valid stances. A revision cannot erase prior
counterevidence or overwrite a forecast. Source-event IDs verify identity, not
semantic causal independence; reasoning about syndication remains auditable model
work. Schema checks do not prove the content of a thesis is true.

The verifier fixes entry at the **first US regular open strictly after the
judgment is frozen**. Day ends at that session's close; week/month end at the last
session of its calendar week/month. The agent cannot retrospectively choose dates
or winning ticker proxies. SPY is a market comparator, not a required holding.

For each discovered US symbol, save absolute open-to-close price return, SPY
return, excess return and maximum drawdown along entry-open/daily-close marks.
This is neither intraday drawdown, portfolio P&L, actual fills nor total return.
Missing bars are never filled. Reported splits/dividends conservatively leave the
horizon unresolved until a separate auditable adjustment is implemented. Future
horizons remain pending. Stored settlements are never recomputed in place when a
vendor changes history; a correction must be a separate explicitly linked record.

Optional forecasts predeclare the probability that the symbol outperforms SPY
at a specified horizon. Resolved forecasts get Brier error and the fixed 0.5
baseline (0.25 error). Passive observations receive **no prediction score**.
Day/week/month observations overlap and are correlated; a tiny selected sample
cannot demonstrate calibration or persistent excess returns.

Long-term business theses remain `pending_independent_evidence_review` until
frozen claims/invalidation can be checked independently. The runtime queues
research and due reviews, but its controlled candidate cannot browse extra
material. The heartbeat can inspect saved/new evidence and surface unresolved
research needs. There is no automated fundamental truth oracle or claim that the
framework itself learns weights. The [offline RL interface](../financial_decision/README.md)
provides environment transitions and rewards; a policy trainer owns weight updates.

## Tests and controlled replay

```bash
.venv/bin/python -m unittest discover -s environments/attention_judgment/tests -v
.venv/bin/python -m unittest discover -s agent/tests -v
```

```bash
.venv/bin/python environments/attention_judgment/replay.py \
  --run-root environments/attention_judgment/runs/my-synthetic-run \
  --memory versioned --codex-bin <path> --model <model> --timeout 240
```

The synthetic replay uses invented company events (initial claim, duplicate,
contradiction, correction) and separate model processes to exercise continuity.
It does not assert a single correct bullish/bearish opinion, and synthetic scores
are not investment performance. The no-memory mode is a separately runnable
control; see the replay CLI help and verification record for measured runs.

The earlier [provenance environment](../attention_provenance/README.md) remains a
separate reproducible coding task against real PostgreSQL. Together these layers
distinguish evidence integrity, continuing judgment, and delayed observed results.
