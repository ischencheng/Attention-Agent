# Kafka Pipeline Observability Guide

This guide defines the observability model for the Bluesky -> Kafka -> Flink -> Fast Agent -> Mid Agent -> Strong Agent -> Postgres pipeline.

It covers three layers:

1. Service metrics (Prometheus + Grafana)
2. End-to-end metrics and SLOs (Prometheus recording/alert rules)
3. End-to-end trace pipeline (OpenTelemetry + Jaeger)

## 1. Current stack and files

- Prometheus scrape config: `kafka-cluster/observability/prometheus.yml`
- Prometheus rules: `kafka-cluster/observability/prometheus-rules.pipeline.yml`
- OTel collector config: `kafka-cluster/observability/otel-collector-config.yaml`
- Grafana datasources: `kafka-cluster/observability/grafana/provisioning/datasources/datasource.yml`
- Grafana dashboards:
  - `kafka-cluster/observability/grafana/dashboards/kafka-classic-overview.json`
  - `kafka-cluster/observability/grafana/dashboards/pipeline-e2e-observability.json`
  - `kafka-cluster/observability/grafana/dashboards/pipeline-observability-complete.json`
  - `kafka-cluster/observability/grafana/dashboards/pipeline-data-explorer.json`

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

The file `kafka-cluster/observability/prometheus-rules.pipeline.yml` defines recording rules:

- `pipeline_e2e_latency_seconds:p50`
- `pipeline_e2e_latency_seconds:p95`
- `pipeline_e2e_success_ratio:5m`
- `pipeline_e2e_error_ratio:5m`
- `pipeline_trace_completeness_ratio:5m`
- `pipeline_freshness_seconds`

Suggested SLO baseline:

- latency: `pipeline_e2e_latency_seconds:p95 < 10s` (warning at `>15s`)
- success ratio: `pipeline_e2e_success_ratio:5m > 0.9`
- error ratio: `pipeline_e2e_error_ratio:5m < 0.05`
- freshness: `pipeline_freshness_seconds < 120`
- trace completeness: `pipeline_trace_completeness_ratio:5m > 0.7`

## 4. End-to-end trace pipeline (OTel + Jaeger)

### Trace hierarchy

- `ingest.receive`
  - `kafka.produce`
- `flink.transform.normalize`
- `fast_agent.triage`
- `mid_agent.consume_clue`
- `mid_agent.aggregate`
- `strong_agent.decision`
  - `kafka.produce`
- `sink_consume_span`
  - `sink_db_write_span`
  - `sink_commit_span`
- `api.ws.push`
- API query spans:
  - `api.signals.query`
  - `api.insights.query`
  - `api.overview.query`
  - `api.frontend.vitals`

### Context propagation

Across Kafka boundaries, context is propagated via:

- Kafka headers (`traceparent` from OTel propagator)
- payload field `traceparent` (used by Flink / agent paths)

This allows one business event to be traced across ingestor -> flink -> fast-agent -> mid-agent -> strong-agent -> sink.

### Jaeger usage

- Search by service: `bluesky-ingestor`, `flink-normalizer`, `fast-agent`, `mid-agent`, `strong-agent`, `sink-consumer`
- Filter by operation names above
- Compare slow traces to normal traces using operation duration breakdown

## 5. Grafana PostgreSQL datasource and SQL panels

`kafka-cluster/observability/grafana/provisioning/datasources/datasource.yml` now includes:

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
3. Jaeger should show cross-service traces with hierarchical spans.

## 7. Recommended next hardening steps

- Add Alertmanager routing and notification channels.
- Add exemplar correlation from metrics to trace IDs.
- Add `trace_id`/`event_id` columns into sinked DB tables for SQL -> trace pivoting.
- Add frontend route/action spans with OTel Web SDK if deeper browser tracing is required.
