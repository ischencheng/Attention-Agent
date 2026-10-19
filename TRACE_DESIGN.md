# Trace and decision-path design

The explorer follows the application's recorded evidence from an original source
through routing, factual annotation and continuing judgment. It explains what the
system recorded, why it advanced or withheld an item, and what remains unresolved.
It does not reconstruct private model reasoning.

This serves the [product's information-consumption contract](PRODUCT.md): let a
person or downstream agent trace useful context back to its sources. Investment
research supplies the current examples and outcome evaluator. Runtime events
and infrastructure health remain separate from the business evidence path.

## Design references and project choices

The relevant sections of [AI Agent Book](https://github.com/bojieli/ai-agent-book)
were checked at commit `dbc046eb896ac4e39aa19c7774c8bf49583b89a6`. This was also
the current upstream commit when rechecked on October 19, 2026. The earlier
judgment design already cited chapters 3, 6, 7 and 9; the trace work additionally
uses the observability and multi-agent communication sections. This is a focused
reading, not a claim to have read or implemented the entire book.

| Source | Application to Attention Proxy |
| --- | --- |
| [Chapter 3: memory hierarchy](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter3.md#记忆的层次结构) | Present execution events, durable thesis versions and current research status separately. Editing current memory must not replace an earlier audit record. |
| [Chapter 7: observability](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter7.md#L698-L714) | Expose recorded model outcomes, input/output references, timing, usage and errors. Preserve explicit gaps where historical telemetry was not collected. |
| [Chapter 6: event handling](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter6.md#L113-L167) | Record each agent attempt, its selected input versions, trigger, start and outcome. A later successful retry must not erase the previous failed attempt. |
| [Chapter 10: agent communication](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter10.md#agent-间的通信与控制) | Join agent handoffs using actual source identities, content hashes and referenced record IDs. Nearby timestamps alone never establish causality. |
| [Chapter 9: learning signals](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter9.md#L19-L47) | Separate successful execution, evidence validity and eventual forecast outcome. A completed research cycle can correctly produce no scored forecast. |
| [Chapter 9: consolidation](https://github.com/bojieli/ai-agent-book/blob/dbc046eb896ac4e39aa19c7774c8bf49583b89a6/book/chapter9.md#L75-L89) | Keep original observations and thesis revisions available when showing compressed current research, including counterevidence, invalidation conditions and next review. |

The stage names, data contracts and financial evaluation rules are this project's
design choices. The book does not prescribe this pipeline or establish that its
investment judgments are correct.

## User-facing evidence contract

The logical path is original observation → recorded routing outcome → attention
ticket → factual annotation → Strong judgment → review or independent outcome.
Each edge needs a recorded reference. A missing ticket can mean pending work,
failure or an older unaudited rejection; it is not itself a rejection verdict.

Published decisions use their frozen, independently checked evidence snapshots.
Continuing research uses saved application state and separately archived versions,
clearly labeled unscored. Current annotation contents cannot silently substitute
for the historical evidence of a published decision.

Source publication time, ingestion time, agent execution time and observer capture
time remain distinct. Routing records preserve the actual concise reason and
coverage metadata when available. Older missing reasons are not backfilled with
new explanations. No synthetic success label, market prediction or trade is
created to complete an otherwise empty stage.

The upstream `source_clean` delivery hop records `stage=relay`: it forwards the
original envelope without a model call. Fast is the single model filter in the
default local deployment. Older `stage=filter` events keep their original model
metadata and reasons. Distinct stage identities allow a pending old input to be
delivered under the new relay policy without rewriting its earlier audit.

Cached verdicts retain the producing model and its historical usage. Displaying
that usage does not establish new token consumption. Long-source chunking,
retries and policy bypasses mean one logical filter is not a promise of exactly
one model invocation for every observation.

The source-context view is a separate, read-only lookup anchored to an original
record. It follows recorded native Twitter reply ancestors within the same source
namespace and validates the native identity of each stored candidate. Returned
records are locally collected context; missing parents, multiple stored versions,
cycles and retrieval limits remain explicit. It does not infer a thread from
author names or adjacent timestamps. Native event time comes from the provider's
original fields, never from an ingestion-time fallback.

Context retrieved now is not added to an earlier decision's frozen evidence and
does not establish which messages the deciding agent read. The current adapter
covers stored Twitter reply ancestors. Complete thread crawling and bounded
chat-history retrieval require separate source support and are not implemented.

Only structured application events, summaries and source records enter the
explorer. Gateway tokens, API keys and private raw runtime transcripts are outside
the API's mounted data directories.

## Local deployment

The API and dashboard bind to localhost. Their runtime mounts are read-only and
limited to the production audit projection, cycle summaries and optional sanitized
historical summaries. The supervisor refreshes a consistent SQLite backup for the
API after production evaluation; its capture time is separate from thesis update
and observation times. The API does not open the actively written WAL ledger as
an immutable database or silently ignore pending WAL commits.

For an existing deployment, apply the additive routing migration before updating
the sink and classifiers:

```sh
docker compose exec -T postgres psql -U bluesky -d bluesky -v ON_ERROR_STOP=1 < postgres/init/003_routing_events.sql
docker compose exec -T postgres psql -U bluesky -d bluesky -v ON_ERROR_STOP=1 < postgres/init/004_relay_routing_stage.sql
```

Fresh PostgreSQL volumes apply this file during initialization. Neither upgrade
path requires resetting Kafka offsets or deleting application state. Rebuild the
changed services, then run `scripts/local_pipeline.py evaluate` with the local
Python environment to refresh the research projection. Running supervisors need
a restart to load changes to the host scheduler; let active agent cycles finish
before restarting them.
