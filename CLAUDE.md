# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is an AI-powered information consumption pipeline built on a 3-node KRaft Kafka cluster. It ingests real-time posts from Bluesky's Jetstream, runs them through a multi-stage LLM processing chain, and surfaces investment-relevant signals in a React frontend.

## Key Commands

### Start / Stop the Full Stack

```bash
# First-time setup: copy .env.example and add GEMINI_API_KEY
cp .env.example .env

# Build and start all services
docker compose up -d --build

# Replay mode (injects golden dataset instead of live Bluesky feed)
docker compose -f docker-compose.yaml -f docker-compose.replay.yaml up -d --build

# Tear down (preserves bind-mounted data in kafka1/data, kafka2/data, kafka3/data, postgres/data)
docker compose down

# Full reset (destroys all persisted data)
docker compose down --remove-orphans
Remove-Item -Recurse -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data
New-Item -ItemType Directory -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data | Out-Null
docker compose up -d --build
```

### TypeScript Agents (ts-agents)

```bash
cd ts-agents
npm install
npm run build          # tsc compile
npm run mid            # run mid-agent locally (node dist/mid-agent/index.js)
npm run strong         # run strong-agent locally
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

Core tables: `raw_messages`, `source_clean`, `embedding_requests`, `vector_embeddings`, `attention_tickets`, `annotations`, `decision_packets`

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
Bluesky Jetstream (WebSocket)
  -> bluesky-ingestor           -> source_raw
  -> Flink job (PyFlink)        -> source_clean  +  embedding_requests
  -> local-ml-filter (Python)   -> fastagent_input   [keyword/ML pre-filter, threshold 0.55]
  -> embed-worker (Python)      -> vector_embedding  [Gemini embedding-001, 768-dim]
  -> fast-agent (Python)        -> attention_ticket  [Gemini flash-lite, threshold 0.60]
  -> mid-agent (TypeScript)     -> annotations       [Gemini flash-lite, pi-agent-core]
  -> strong-agent (TypeScript)  -> decision_packet   [Gemini flash-lite, pi-agent-core]
  -> sink-consumer (Python)     -> all topics -> PostgreSQL
  -> backend-fastapi (Python)   -> React frontend via REST + WebSocket
```

### Component Roles

- **bluesky-demo/bluesky_ingestor.py** — WebSocket client for Bluesky Jetstream; produces raw events to `source_raw`; metrics on `:8020`
- **flink/bluesky_flink_job.py** — PyFlink streaming job; normalizes messages, keyword-filters for investment relevance, fans out to `source_clean` and `embedding_requests`; runs as a submitted job on flink-jobmanager/taskmanager
- **llm-worker/local_ml_filter.py** — Pre-filter before fast-agent; reads `source_clean`, scores with Gemini, outputs to `fastagent_input`; metrics on `:8031`
- **llm-worker/embed_worker.py** — Reads `embedding_requests`, calls Gemini embedding API, produces to `vector_embedding`; metrics on `:8034`
- **llm-worker/llm_worker.py** — Fast-agent; reads `fastagent_input`, calls Gemini for attention scoring, produces `attention_ticket` if score >= threshold; metrics on `:8030`
- **ts-agents/src/mid-agent/** — TypeScript agent using `@mariozechner/pi-agent-core`; reads `attention_ticket`, enriches with analysis, produces `annotations`; metrics on `:8032`
- **ts-agents/src/strong-agent/** — TypeScript agent; reads `annotations`, makes final investment signal decision, produces `decision_packet`; metrics on `:8033`
- **sink-consumer/kafka_to_postgres_sink.py** — Consumes all 7 topics and writes to corresponding PostgreSQL tables; idempotent via `UNIQUE(kafka_topic, kafka_partition, kafka_offset)`; metrics on `:8040`
- **backend-fastapi/app.py** — FastAPI service; serves decision_packets/annotations via REST and WebSocket push to frontend; also exposes Prometheus metrics
- **frontend-react/** — Vite/React SPA; proxies API calls to backend-fastapi

### Infrastructure

- **Kafka**: 3-node KRaft cluster (no ZooKeeper); each node is both broker and controller; replication factor 3, min ISR 2; log retention 10 minutes (`LOG_RETENTION_MS: 600000`)
- **PostgreSQL**: Single instance; schema initialized from `postgres/init/001_schema.sql`; all tables share the same JSONB `payload` + `raw` structure
- **Observability**: OpenTelemetry Collector -> Jaeger (traces); Prometheus (metrics from all services + kafka-exporter + postgres-exporter); Grafana dashboards in `observability/grafana/dashboards/`
- **Kafka listener layout**: Internal docker network uses `kafka{1,2,3}:19092`; host access uses `localhost:9092/9093/9094`

### TypeScript Agents

Built with `@mariozechner/pi-agent-core` and `kafkajs`. Source under `ts-agents/src/`:
- `core/` — shared Kafka consumer/producer wrappers
- `adapters/` — Kafka and DB adapters
- `tools/` — shared LLM tool definitions
- `mid-agent/` and `strong-agent/` — individual agent entrypoints

### Environment

The only required secret is `GEMINI_API_KEY` (set in `.env`). All services receive it via `${GEMINI_API_KEY:-}` in docker-compose.yaml.
