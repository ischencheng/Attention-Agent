# Kafka Pipeline Observability Guide

This guide defines the observability model for the Bluesky -> Kafka -> Flink -> Fast Agent -> Mid Agent -> Strong Agent -> Postgres pipeline.

It covers three layers:

1. Service metrics (Prometheus + Grafana)
2. End-to-end metrics and SLOs (Prometheus recording/alert rules)
3. Recorded execution audit traces (OpenTelemetry + Jaeger)

The metrics sections are a configuration reference. Metric availability depends
on the enabled services and successful Prometheus scrapes. Verifying recorded
audit traces does not validate this metrics catalog or its SLO queries.

## 1. Current stack and files

- Prometheus scrape config: `observability/prometheus.yml`
- Prometheus rules: `observability/prometheus-rules.pipeline.yml`
- OTel collector config: `observability/otel-collector-config.yaml`
- Grafana datasources: `observability/grafana/provisioning/datasources/datasource.yml`
- Grafana dashboards:
  - `observability/grafana/dashboards/kafka-classic-overview.json`
  - `observability/grafana/dashboards/pipeline-e2e-observability.json`
  - `observability/grafana/dashboards/pipeline-observability-complete.json`
  - `observability/grafana/dashboards/pipeline-data-explorer.json`

## 2. Service metrics catalog

### bluesky-ingestor

- `bluesky_ingestor_events_received_total`: Jetstream events received.
- `bluesky_ingestor_events_filtered_total{reason}`: events dropped by filter reason.
- `bluesky_ingestor_raw_publish_success_total`: successful writes to `source_raw`.
- `bluesky_ingestor_raw_publish_failed_total{error_type}`: failed writes to `source_raw`.
- `bluesky_ingestor_reconnect_total`: reconnect attempts to Jetstream.
- `bluesky_ingestor_last_event_unixtime`: timestamp of latest source event.

Useful PromQL:

- ingest throughput: `rate(bluesky_ingestor_raw_publish_success_total[5m])`
- filter ratio: `sum(rate(bluesky_ingestor_events_filtered_total[5m])) / clamp_min(sum(rate(bluesky_ingestor_events_received_total[5m])), 0.000001)`

### flink-jobmanager / flink-taskmanager

Flink runtime metrics are exposed through Prometheus reporter ports (`9249`, `9250`), including:

- task throughput and backpressure
- checkpoint duration/failures
- job/task health

Useful PromQL patterns:

- checkpoint failures (example): `sum(rate(flink_taskmanager_job_task_numRecordsInErrors[5m]))`
- busy ratio (depends on exposed metric names): query `flink_*busy*` series in Prometheus explore first.

### fast-agent / mid-agent / strong-agent

- `local_ml_eval_total{result}`: local ML classifier pass/filter counters.
- `local_ml_score`: local ML classifier relevance-score histogram.
- `narrative_clue_out_total{status}`: fast-agent clue output counters.
- `event_packet_out_total{status}`: mid-agent event packet output counters.
- `strong_agent_requests_total{status}`: strong-agent external request counters.
- `strong_agent_latency_seconds`: strong-agent request latency histogram.
- `decision_packet_out_total{status}`: strong-agent decision packet output counters.

Useful PromQL:

- local ML pass ratio: `sum(rate(local_ml_eval_total{result="passed"}[5m])) / clamp_min(sum(rate(local_ml_eval_total[5m])), 0.000001)`
- strong agent P95: `histogram_quantile(0.95, sum(rate(strong_agent_latency_seconds_bucket[5m])) by (le))`
- strong agent error ratio: `sum(rate(strong_agent_requests_total{status="error"}[5m])) / clamp_min(sum(rate(strong_agent_requests_total[5m])), 0.000001)`

### sink-consumer

- `sink_consumer_messages_total{topic,status}`: message handling results.
- `sink_consumer_db_writes_total{topic,status}`: DB write success/error by topic.
- `sink_consumer_db_write_latency_seconds{topic}`: DB write latency histogram.
- `sink_consumer_commit_total{status}`: Kafka commit success/error.
- `sink_consumer_last_successful_write_unixtime`: freshness anchor for sink writes.

Useful PromQL:

- DB write P95: `histogram_quantile(0.95, sum(rate(sink_consumer_db_write_latency_seconds_bucket[5m])) by (le, topic))`
- sink freshness: `time() - sink_consumer_last_successful_write_unixtime`

### backend-fastapi

- `fastapi_requests_total{endpoint}`: API endpoint request counts.
- `fastapi_db_query_latency_seconds{query_name}`: DB query latency histogram.
- `ws_signals_pushed_total`: websocket push count.
- `signal_end_to_end_latency_seconds`: end-to-end latency histogram (event timestamp -> websocket visible).
- `frontend_web_vitals_value{metric_name,rating}`: browser web-vitals values.
- `frontend_web_vitals_total{metric_name,status}`: browser web-vitals report count.

Useful PromQL:

- E2E P95: `histogram_quantile(0.95, sum(rate(signal_end_to_end_latency_seconds_bucket[5m])) by (le))`
- API request rate: `sum by (endpoint) (rate(fastapi_requests_total[5m]))`

### postgres (via postgres-exporter)

Exporter endpoint is `postgres-exporter:9187`.

Typical metrics:

- transaction rates (read/write)
- active/idle connections
- deadlocks and lock waits
- cache hit ratio
- sequential/index scans

Note: exact metric names depend on exporter version and collector set. Confirm in Prometheus Explore with `pg_*` and `postgres_*`.

## 3. End-to-end metrics and SLOs

The file `observability/prometheus-rules.pipeline.yml` defines recording rules:

- `pipeline_e2e_latency_seconds:p50`
- `pipeline_e2e_latency_seconds:p95`
- `pipeline_e2e_success_ratio:5m`
- `pipeline_e2e_error_ratio:5m`
- `pipeline_freshness_seconds`

Suggested SLO baseline:

- latency: `pipeline_e2e_latency_seconds:p95 < 10s` (warning at `>15s`)
- success ratio: `pipeline_e2e_success_ratio:5m > 0.9`
- error ratio: `pipeline_e2e_error_ratio:5m < 0.05`
- freshness: `pipeline_freshness_seconds < 120`

## 4. Recorded execution traces in Jaeger

The local supervisor runs `scripts/observability_exporter.py` independently of
model scheduling. It reads routing audits and cycle summaries, sends bounded
OTLP/HTTP batches to the Collector, and records collector acceptance in a
separate SQLite receipt database. It does not write application evidence,
acknowledge Kafka inputs, execute models, or change evaluation results.

| Service | Operation | Actual timing coverage |
| --- | --- | --- |
| `attention-routing` | `relay.recorded_handling` | Recorded handling through broker acknowledgment when forwarded, before audit publication and offset commit |
| `attention-routing` | `filter.recorded_handling`, `fast.recorded_handling` | Recorded classification start to completion, before output delivery, audit publication and offset commit |
| `attention-mid` | `agent.recorded_cycle` | Recorded Mid attempt, including failed attempts |
| `attention-strong` | `agent.recorded_cycle` | Recorded Strong attempt, including failed attempts |

Each span is an independent projection with `attention.trace_origin=recorded_audit`.
Its stable ID identifies the exact audit record. These IDs are not propagated
live contexts and do not establish parent/child relationships between stages.
The dashboard's evidence graph separately displays verified source hashes,
Kafka coordinates and explicit decision references.

No spans are fabricated for source polling, Flink transformations, database
operations, individual tools, or failed Fast requests without routing records.
Tool metadata without timestamps remains untimed. Cached usage describes the
historical cached result, not new tokens consumed by the displayed route.
Attributes exclude prompts, article text, rationales, commands, tool outputs,
URLs and credentials. A rejected routing decision is a valid outcome, not a
runtime error; failed agent cycles are marked as errors.

The dashboard's Observability view displays exporter freshness, collector
acceptance counts and Jaeger availability. Direct links are present only for
records with export receipts. Acceptance is not proof of retention or immediate
query availability; Jaeger ingestion can lag. Pending counts describe the bounded
scan window, and a truncated scan means additional records may remain.

Jaeger uses a named Badger volume with seven-day span retention. The volume
initializer creates only the writable storage directory for the image's UID.
Application originals, failed-cycle summaries and research archives have their
own durable storage and outlive Jaeger's display retention.

```sh
# One bounded read/export pass; no model calls.
.venv/bin/python scripts/observability_exporter.py --once --batch-size 500

# Explicit recovery after trace-storage loss: at most 500 recent records per kind.
.venv/bin/python scripts/observability_exporter.py --replay 500
```

Stop the supervised exporter before an explicit replay; its instance lock avoids
concurrent receipt writers. Network and retryable HTTP failures remain pending.
Invalid records and OTLP partial rejections are visible as blocked; partial
rejections require explicit replay after correction because OTLP does not identify
which records were rejected. Do not reset application state or Kafka offsets.

Search Jaeger by the services above and use a lookback that includes the record's
original execution time. Backfilling an old audit preserves its original time;
it does not create a new operation at the export time. Use the direct record
links when available.

The previous "trace completeness" ratio divided optional decision publications
by Bluesky ingestion. It was not tracing coverage and penalized valid filtering
and abstention. Its recording rule, alert and dashboard panel have been removed.
This does not change financial evaluation or any historical decision.

Protocol and storage contracts: [OTLP JSON encoding](https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding)
and [Jaeger 2.5.0 Badger configuration](https://github.com/jaegertracing/jaeger/blob/v2.5.0/cmd/jaeger/config-badger.yaml).

## 5. Grafana PostgreSQL datasource and SQL panels

`observability/grafana/provisioning/datasources/datasource.yml` now includes:

- Prometheus datasource (`uid: prometheus`)
- Jaeger datasource (`uid: jaeger`)
- PostgreSQL datasource (`uid: postgres`)

### Can Grafana show actual row-level business data?

Yes. Use PostgreSQL datasource and SQL panels.

Prometheus panels show time-series metrics, not row payloads.  
SQL panels can show concrete records from `raw_messages`, `narrative_clues`, `event_packets`, `decision_packets`.

The dashboard `pipeline-data-explorer.json` provides:

- latest signals table
- latest LLM insights table
- per-minute signal counts

## 6. Validation checklist

After `docker compose up -d --build`:

1. Prometheus targets should be `UP` for:
   - `bluesky_ingestor`
   - `flink_jobmanager`
   - `flink_taskmanager`
   - `local_ml_filter`
   - `fast_agent`
   - `mid_agent`
   - `strong_agent`
   - `sink_consumer`
   - `backend_fastapi`
   - `postgres_exporter`
2. Grafana should show:
   - metrics dashboards populated
   - `Pipeline Data Explorer (PostgreSQL)` tables populated
3. Jaeger should show recorded routing/cycle spans at their actual timestamps.
   Verify a real relay, Fast rejection, successful cycle and failed cycle by
   their exact audit IDs. Confirm those IDs still resolve after a Jaeger restart.

## 7. Recommended next hardening steps

- Add Alertmanager routing and notification channels.
- Add exemplar correlation from metrics to trace IDs.
- Add `trace_id`/`event_id` columns into sinked DB tables for SQL -> trace pivoting.
- Add frontend route/action spans with OTel Web SDK if deeper browser tracing is required.
