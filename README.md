# AI Attention Proxy

An information-consumption pipeline that preserves original evidence while
delegating attention routing, factual curation, and goal-specific decisions to
different agents.

- **Fast:** route potentially valuable information without premature summaries.
- **Mid:** organize facts and relationships beside the original material.
- **Strong:** use the original evidence when making a decision.

The existing stack connects multi-source ingestion, Kafka/Flink, LLM workers,
PostgreSQL, FastAPI and a React dashboard. See [CLAUDE.md](CLAUDE.md) for the
architecture and development commands.

## Continuing judgment and delayed evaluation

The [Attention Judgment environment](environments/attention_judgment/README.md)
uses the original topic scope to discover research questions, retain/revise
MrBrain-informed theses, and freeze predictions before daily, weekly and monthly
market observations. It includes a Codex runner, durable audit ledger, real market
calendar, price/benchmark evaluator, source coverage and a bounded daily loop.
It does not require a fixed portfolio, and no real orders are placed.

Mid and Strong now run through the open-source Codex CLI 0.163.0. Their existing
state IDs remain compatible. The Gemini attention filters remain lightweight
classifiers; the replacement concerns the stateful research/decision agents.
See [agent/AGENTS.md](agent/AGENTS.md), `.env.example`, and the environment README
for authentication, operation and measured limitations. The hosted model itself
is not claimed to be open source.

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
