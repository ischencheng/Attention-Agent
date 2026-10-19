# AI Attention Proxy

Infrastructure for delegating information consumption to AI: filter noise,
accumulate useful evidence across time and sources, and free human attention.
Downstream agents consume the resulting context and can inspect the originals
behind it. Investment research is the current downstream application.

- **Fast:** route potentially valuable information without premature summaries.
- **Mid:** organize facts and relationships beside the original material.
- **Strong:** use the original evidence when making a decision.

The product contract is [delegated information consumption](PRODUCT.md): preserve
source context, reduce repeated reading, and let sufficient evidence determine
whether a judgment is warranted.

The existing stack connects multi-source ingestion, Kafka/Flink, LLM workers,
PostgreSQL, FastAPI and a React dashboard. See [CLAUDE.md](CLAUDE.md) for the
architecture and development commands.

For the complete local deployment, use [the runtime guide](LOCAL_RUNTIME.zh-CN.md).
`scripts/local_pipeline.py start` starts the actual streaming services and a
persistent host supervisor for Codex filtering/Fast, semantic embeddings,
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
state IDs remain compatible. The local deployment also uses Codex for filtering
and Fast, with a real local embedding model; Gemini remains an optional provider.
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
