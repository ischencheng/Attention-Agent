# Local pipeline

The default information path is:

`Sources → source_raw → Flink → source_clean → zero-model relay → Fast → attention tickets → Mid → annotations → Strong`

The relay preserves the existing Kafka topics and consumer groups. Its historical
service name is `local-ml-filter`, but its default `relay` provider makes no model
request. Fast checks both topic relevance and concrete useful information. Mid
accumulates evidence across observations; Strong owns continuing judgment.
Embedding independently consumes the relayed stream using the local
`all-mpnet-base-v2` model. Raw sources remain available even when Fast rejects them.

## Setup and model selection

Docker Desktop, Python 3.12, Node.js and an existing Codex login are required.
The supervisor uses that login without reading or copying credential files.

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r scripts/requirements-local.txt
npm install --prefix environments/attention_judgment/runs/runtime @openai/codex@0.163.0
docker compose build
.venv/bin/python scripts/local_pipeline.py start
```

Copy the relevant settings from `.env.example` into the ignored `.env`:

| Setting | Default role |
| --- | --- |
| `CODEX_GATEWAY_MODEL=gpt-6-luna` | Actual host classifier model used by Fast |
| `CLASSIFICATION_PROVIDER=codex` | Fast provider |
| `CLASSIFICATION_MODEL=gpt-6-luna` | Worker configuration; the gateway response records the actual model |
| `MID_CODEX_MODEL=gpt-6-luna` | Mid aggregation |
| `STRONG_CODEX_MODEL=gpt-6.1-sol` | Strong judgment |

For Mid and Strong, a nonempty role setting overrides `CODEX_MODEL`; without
either setting, Mid uses Luna and Strong uses Sol. The gateway model is separate
from the shared Mid/Strong fallback. Empty ordinary cycles skip the model.
Changing a host model requires restarting the gateway or allowing a new agent
cycle to start. Let in-flight work finish before restarting services.

The default relay does not need model credentials. Its legacy model providers
remain available for explicit standalone use; opting into them reintroduces an
upstream classifier. Fast's legacy `local` scoring provider depends on the old
filter score and is not compatible with the byte-preserving relay.

Codex child processes use explicit environment profiles: Fast receives the
`model` profile, and Mid/Strong receive the `research` profile required by their
current tools. Run metadata records profile and variable names without values.
This limits implicit inheritance; it does not isolate the research agents from
host files or privileged storage services. See the implemented controls and
remaining work in [agent execution boundaries](SECURITY_BOUNDARIES.md).

## Upgrade the routing audit

Before rebuilding the sink and relay on an existing database:

```sh
docker compose exec -T postgres psql -U bluesky -d bluesky -v ON_ERROR_STOP=1 < postgres/init/003_routing_events.sql
docker compose exec -T postgres psql -U bluesky -d bluesky -v ON_ERROR_STOP=1 < postgres/init/004_relay_routing_stage.sql
```

Fresh database volumes apply both migrations automatically. Preserve the current
volumes, topics and group IDs. Gracefully stop the old filter, record committed
offsets, then rebuild and restart the affected services. Do not reset offsets.
Old filter rejections remain committed; the upgrade does not replay them.

The new `relay` audit stage coexists with historical `filter` records, including
an audited record whose input commit previously failed. Forwarding requires
output acknowledgment, then audit acknowledgment, before committing the input.
Delivery remains at-least-once, so downstream consumers must retain deduplication.

## Operation and evidence

```sh
.venv/bin/python scripts/local_pipeline.py status
.venv/bin/python scripts/local_pipeline.py cycle mid
.venv/bin/python scripts/local_pipeline.py cycle strong
.venv/bin/python scripts/local_pipeline.py evaluate
```

The dashboard is at <http://localhost:5173>, API at <http://localhost:8000>, and
Flink at <http://localhost:8081>. Mid checks pending inputs every 30 minutes;
Strong checks every 40 minutes and reviews continuing research daily. Evaluation
polls every five minutes. Schedules require this computer and Docker to be awake.
The optional `scripts/install_local_service.py` installs the macOS LaunchAgent.

Runtime data, cycle audits and model artifacts live under the ignored
`environments/attention_judgment/runs/` directory. Verify actual source hashes,
broker handoffs, model artifacts and saved agent state. A running container alone
does not establish successful processing or correct judgment.

One logical Fast gate can use multiple calls for long sources or retries; a cache
hit uses no new model call. Cached usage describes the original execution. Count
distinct model artifacts and include failed attempts when measuring invocation
cost. Removing the first gate also admits more records to Fast and embeddings,
so total token savings require a comparable workload rather than a fixed claimed
percentage.

Only eligible explicit forecasts receive delayed daily, weekly and monthly market
scores after their windows close. Research without a prediction remains unscored;
there are no real orders. See [trace design](TRACE_DESIGN.md) and the validation
records for source provenance and independent evidence freezing.
