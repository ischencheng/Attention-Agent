# AI Attention Proxy: Financial Decision RL Environment

Train and evaluate an agent's ability to discover financial research questions,
inspect evidence, revise a thesis and commit forecasts before outcomes arrive.
The task is long-term judgment over an evolving information stream, with the
original source and decision path available for inspection.

The [financial decision environment](environments/financial_decision/README.md)
provides an offline `reset`/`step` interface, read/judge/forecast/wait actions,
time-gated evidence, preserved thesis history, delayed forecast rewards and an
optional Gymnasium adapter. Run the free synthetic example:

```bash
python -m pip install -e .
python -m environments.financial_decision demo --output /tmp/attention-rl-demo --seed 7
```

The demo uses fictional data and a hand-written policy. It demonstrates episode
execution and traceability; model weight training and investment performance
require separate experiments.

The environment builds on Attention Proxy's
[delegated information consumption](PRODUCT.md) pipeline:

- **Fast:** allocate attention to potentially useful originals.
- **Mid:** accumulate facts, source relationships and unresolved questions.
- **Strong:** maintain a judgment, preserve counterevidence and decide whether a
  testable forecast is warranted.

Entities emerge from the configured topics. Research, abstention and waiting are
valid actions; there is no required portfolio or trade.

## Production pipeline

The existing stack connects multi-source ingestion, Kafka/Flink, LLM workers,
PostgreSQL, FastAPI and a React dashboard. See [CLAUDE.md](CLAUDE.md) for the
architecture and development commands.

For the complete local deployment, use [the runtime guide](LOCAL_RUNTIME.md).
`scripts/local_pipeline.py start` starts the actual streaming services and a
persistent host supervisor for the Fast classifier, semantic embeddings,
Mid/Strong and production-result evaluation. The dashboard exposes each stage
and the agents' last saved state. Merely starting idle agent containers is not
an agent schedule.

The dashboard's **Decision trace** connects original sources, recorded routing
reasons, Mid annotations and Strong research. Inspect current judgments, revision
history, independently frozen versions and recorded agent cycles separately.
[Trace design and book references](TRACE_DESIGN.md) explain the evidence contract,
deployment and the specific ideas adopted from AI Agent Book.

## Continuing judgment and delayed evaluation

The [Attention Judgment environment](environments/attention_judgment/README.md)
uses the original topic scope to discover research questions, retain/revise
MrBrain-informed theses, and freeze predictions before daily, weekly and monthly
market observations. It includes a Codex runner, durable audit ledger, real market
calendar, price/benchmark evaluator, source coverage and a bounded daily loop.
It does not require a fixed portfolio, and no real orders are placed.

Mid and Strong now run through the open-source Codex CLI 0.163.0. Their existing
state IDs remain compatible. The local deployment uses one Codex Fast filter,
with a zero-model delivery relay and a real local embedding model. Fast and Mid
default to `gpt-6-luna`; Strong defaults to `gpt-6.1-sol`. Role-specific model
settings override the shared fallback. Gemini remains an optional Fast provider.
See [agent/AGENTS.md](agent/AGENTS.md), `.env.example`, and the environment README
for authentication, operation and measured limitations. The hosted model itself
is not claimed to be open source.

`pipeline.py` observes real `decision_packets`, independently verifies provenance,
and freezes them in `runs/production/`. It also versions Strong's saved research
state independently, including cycles that produce no decision. Research snapshots
preserve the original reasoning and update/freeze times; they remain unscored and
do not create market predictions. Reports expose counts and lineage, while the
complete state stays in the local ledger. Observer polling cannot recover changes
that were overwritten between polls.

The earlier `run.py`/`daily.py` RSS pilot
is a separate experiment and is not evidence that the production pipeline ran.

## Evidence provenance and software engineering evaluation

The [Attention Provenance environment](environments/attention_provenance/README.md)
turns a real gap in this pipeline into a reproducible coding task: preserve a
decision's historical evidence through corrections, replay, and source-ID
collisions. It includes a frozen unsolved checkout, explicit task contract,
reference repair, real-PostgreSQL grader, failure traces, and grader self-checks.

The repair adds immutable original records, decision evidence snapshots, and
`GET /api/decisions/{id}/evidence`. It reports legacy or broken provenance
explicitly rather than reconstructing an unsupported history.

Read the [Chinese design explanation](environments/attention_provenance/DESIGN.zh-CN.md)
for the connection to the original project and Mechanize's task-design essays.
This is an engineering evaluation prototype; model difficulty and RL training
benefits have not been measured.
