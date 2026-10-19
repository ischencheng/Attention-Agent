# CLAUDE.md

Development guidance for this repository. Runtime Mid/Strong instructions are in `agent/AGENTS.md`.

## Overview

AI Attention Proxy — a multi-source information consumption pipeline built on a 3-node KRaft Kafka cluster. It ingests real-time data from Bluesky, Twitter, Finnhub (stock prices), and Polygon.io (financial news), runs them through a multi-stage LLM processing chain, and surfaces investment-relevant signals in a React frontend. Mid/strong agent work is delegated to Codex CLI agents via Python skills.

## Key Commands

### Start / Stop the Full Stack

```bash
# First-time setup: copy .env.example and fill in API keys
cp .env.example .env

# Build and start all services
docker compose up -d --build

# Replay mode (injects golden dataset instead of live feed)
docker compose -f docker-compose.yaml -f docker-compose.replay.yaml up -d --build

# Tear down (preserves bind-mounted data in kafka1/data, kafka2/data, kafka3/data, postgres/data)
docker compose down

# Full reset (destroys all persisted data)
docker compose down --remove-orphans
Remove-Item -Recurse -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data
New-Item -ItemType Directory -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data | Out-Null
docker compose up -d --build
```

### Syntax / Config Checks

```bash
# Python syntax check
python -m py_compile llm-worker/llm_worker.py sink-consumer/kafka_to_postgres_sink.py

# Validate compose config
docker compose config -q
```

### Database Inspection

```bash
# Query any core table (from PowerShell)
docker compose exec -T postgres psql -U bluesky -d bluesky -c "SELECT * FROM <table> ORDER BY created_at DESC LIMIT 20;"
```

Core tables: `raw_messages`, `source_clean`, `vector_embeddings`, `attention_tickets`, `annotations`, `decision_packets`

### Historical Evidence and the Coding Environment

New decisions published through `skills/db_write.py` require valid annotation
and ticket references. The write skill generates `evidence_snapshot` in one
consistent database transaction; callers must not supply it. The snapshot
preserves the annotation at publication and identifies authoritative originals
by source, source ID, and content digest. Later annotation corrections do not
change that history.

`GET /api/decisions/{decision_id}/evidence` returns `verified`, `broken`, or
`legacy_unverified`. Legacy decisions are not retrospectively assigned evidence.
Verification checks provenance integrity, not investment merit or whether a
model understood the source material.

Apply the additive migration to an existing database before relying on raw
immutability (init scripts run automatically only for fresh data volumes):

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U bluesky -d bluesky \
  < postgres/init/002_evidence.sql
docker compose up -d --build backend-fastapi
```

The API image includes `skills/evidence.py` at `/app/evidence.py`. Its Docker
build context is the repository root; for a standalone build, use
`docker build -f backend-fastapi/Dockerfile .` from that root.
The migration blocks ordinary UPDATE/DELETE of raw originals and changes to
published decision snapshots/references; it does not constrain a DB superuser.

The runnable repository-level coding task is documented in
[`environments/attention_provenance/README.md`](environments/attention_provenance/README.md),
with its full behavioral contract in
[`TASK.md`](environments/attention_provenance/TASK.md).
Use its exported unsolved baseline for an agent attempt: the current checkout
contains the reference repair. Its grader uses real PostgreSQL and FastAPI with
captured Kafka publication, and makes no claim about live broker delivery,
LLM quality, or trading returns.

## Service Endpoints

| Service     | URL                       |
|-------------|---------------------------|
| Kafka UI    | http://localhost:8080     |
| Flink UI    | http://localhost:8081     |
| FastAPI     | http://localhost:8000     |
| Frontend    | http://localhost:5173     |
| Prometheus  | http://localhost:9090     |
| Grafana     | http://localhost:3000 (admin/admin) |
| Jaeger      | http://localhost:16686    |

## Architecture

### Data Flow (Kafka Topics)

```
Multi-Source Ingestors
  bluesky-ingestor   (WebSocket push)      ─┐
  finnhub-ingestor   (WebSocket push)       ├──> source_raw
  polygon-news-ingestor (poll every 60s)    │
  twitter-ingestor   (poll every 20m)      ─┘
    -> Flink job (PyFlink)        -> source_clean
    -> local-ml-filter (Python)   -> fastagent_input   [Gemini pre-filter, threshold 0.55]
    -> embed-worker (Python)      -> vector_embedding  [from fastagent_input, Gemini embedding-001, 768-dim]
    -> fast-agent (Python)        -> attention_ticket  [Gemini flash-lite, threshold 0.60]
    -> external agents (skills)   -> annotations + decision_packet
    -> sink-consumer (Python)     -> all topics -> PostgreSQL
    -> backend-fastapi (Python)   -> React frontend via REST + WebSocket
```

### Content Types

All messages use a unified envelope: `{source, source_id, content_type, timestamp, data, raw}`

| content_type   | source    | ingestor               |
|----------------|-----------|------------------------|
| social_post    | bluesky   | bluesky-ingestor       |
| social_post    | twitter   | twitter-ingestor       |
| price_tick     | finnhub   | finnhub-ingestor       |
| news_article   | polygon   | polygon-news-ingestor  |

### Component Roles

**Ingestors** (`ingestors/` + `bluesky-demo/`):
- **bluesky-demo/bluesky_ingestor.py** — WebSocket client for Bluesky Jetstream; produces to `source_raw`; metrics on `:8020`
- **ingestors/finnhub_ws.py** — WebSocket client for Finnhub real-time stock trades; produces `price_tick` to `source_raw`; metrics on `:8021`
- **ingestors/polygon_news_poller.py** — Polls Polygon.io REST API for financial news; produces `news_article` to `source_raw`; metrics on `:8022`
- **ingestors/twitter_poller.py** — Polls Twitter via Caravo API (twitter241/search-v3); produces `social_post` to `source_raw`; metrics on `:8023`
- **ingestors/base.py** — Shared base: Kafka producer setup, topic creation, envelope helpers

**Processing pipeline**:
- **flink/bluesky_flink_job.py** — PyFlink streaming job; normalizes messages, keyword-filters for investment relevance, passes `price_tick` through unconditionally; fans out to `source_clean`
- **llm-worker/local_ml_filter.py** — Pre-filter; reads `source_clean`, scores with Gemini, outputs to `fastagent_input`; metrics on `:8031`
- **llm-worker/embed_worker.py** — Reads `fastagent_input`, calls Gemini embedding API, produces to `vector_embedding`; metrics on `:8034`
- **llm-worker/llm_worker.py** — Fast-agent; reads `fastagent_input`, calls Gemini for attention scoring, produces `attention_ticket` if score >= threshold; metrics on `:8030`

**Sink + API**:
- **sink-consumer/kafka_to_postgres_sink.py** — Consumes 6 topics and writes to corresponding PostgreSQL tables; idempotent via `UNIQUE(kafka_topic, kafka_partition, kafka_offset)`; periodic re-subscribe for late-created topics; metrics on `:8040`
- **backend-fastapi/app.py** — FastAPI service; REST endpoints (`/api/overview`, `/api/feed`, `/api/decisions`, `/api/annotations`, `/api/tickets`) + WebSocket push; Prometheus metrics on `/metrics`
- **frontend-react/** — Vite/React SPA; multi-source dashboard with color-coded feed cards, source badges, urgency badges

**Skills** (`skills/`):
- **skills/db_query.py** — Read from any table or run arbitrary SQL
- **skills/db_write.py** — Write to Kafka topics (annotations, decision_packet) via skill → Kafka → sink → DB
- **skills/db_update.py** — Update existing rows (e.g., append tickets to annotations)
- **skills/search_vectors.py** — Semantic search over vector embeddings
- **skills/mrbrain_invest.py** — Evaluate investments using MrBrain's 10-layer framework (value investing, risk management, macro cycles, competition theory, psychology). Source: 43 videos from 脑总MrBrain. Reference: `MrBrain-invest-principle/`

**Agent** (`agent/`):
- **agent/AGENTS.md** — Workspace instructions for mid/strong agents running inside Docker
- **agent/Dockerfile** — Container that runs Codex CLI 0.163.0 with mounted Python skills
- Mid/strong agent services in docker-compose mount `skills/`; `agent/AGENTS.md` and the Codex runner are copied into the image
- **trigger_agents.sh** — Scheduling & manual trigger script:
  - Loop mode: mid-agent every 30m, strong-agent every 40m (independent timers)
  - Manual: `./trigger_agents.sh mid|strong|both` for one-shot trigger
  - `--mid-interval` / `--strong-interval` flags to override defaults
- **Discovery scope** (`RESEARCH_CONTEXT`): original investment topics; entities emerge from evidence with no fixed portfolio or ticker restriction

### Infrastructure

- **Kafka**: 3-node KRaft cluster (no ZooKeeper); each node is both broker and controller; replication factor 3, min ISR 2; log retention 10 minutes (`LOG_RETENTION_MS: 600000`)
- **PostgreSQL**: pgvector-enabled (pg16); schema initialized from `postgres/init/001_schema.sql`; all tables: `id SERIAL, kafka_topic, kafka_partition, kafka_offset, payload JSONB, raw JSONB, created_at TIMESTAMPTZ`
- **Observability**: OpenTelemetry Collector -> Jaeger (traces); Prometheus (metrics from all services + kafka-exporter + postgres-exporter); Grafana dashboards in `observability/grafana/dashboards/`
- **Kafka listener layout**: Internal docker network uses `kafka{1,2,3}:19092`; host access uses `localhost:9092/9093/9094`

### Environment

Required secrets (set in `.env`):

| Variable          | Required | Used By                |
|-------------------|----------|------------------------|
| `GEMINI_API_KEY`  | Yes      | ML workers, fast-agent, agents |
| `FINNHUB_API_KEY` | Optional | finnhub-ingestor       |
| `POLYGON_API_KEY` | Optional | polygon-news-ingestor  |
| `CARAVO_API_KEY`  | Optional | twitter-ingestor       |
| `CODEX_API_KEY` | Optional | Mid/Strong hosted model auth; alternatively log in through each container CLI |
| `TWITTER_FOLLOW_USER` | Optional | twitter-ingestor (default: genhcnehcsi) |
| `RESEARCH_CONTEXT` | Optional | Original topic interests, not a ticker allowlist |
| `CODEX_MODEL` / role overrides | Optional | Account-available model selection |
