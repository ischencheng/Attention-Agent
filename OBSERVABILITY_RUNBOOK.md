# Wikimedia Kafka Observability Runbook

## Scope

This runbook explains the Wikimedia-based idempotent demo:
- `wikimedia_producer.py` ingests Wikimedia EventStreams and produces to Kafka.
- `wikimedia_consumer.py` consumes the demo topic and tracks duplicate source IDs.
- Prometheus scrapes producer/consumer and Kafka exporter.
- Grafana dashboards: `Kafka Classic Overview` and `Wikimedia Idempotent Overview`.

## Start full stack

```bash
docker compose up -d --build
```

Check:
- `http://localhost:9090/targets`
- `http://localhost:3000` (admin/admin)

## Start/stop producer and consumer independently

Producer only:

```bash
docker compose up -d wikimedia-producer
docker compose stop wikimedia-producer
```

Consumer only:

```bash
docker compose up -d wikimedia-consumer
docker compose stop wikimedia-consumer
```

## Optional: run non-idempotent producer for contrast

```bash
docker compose run --rm wikimedia-producer python wikimedia_producer.py --bootstrap kafka1:19092,kafka2:19092,kafka3:19092 --topic wikimedia-idempotent --idempotent false --metrics-port 8010
```

## Metrics to watch

Producer:
- `wikimedia_producer_source_events_total`
- `wikimedia_producer_publish_success_total`
- `wikimedia_producer_publish_failed_total`
- `wikimedia_producer_buffer_size`

Consumer:
- `wikimedia_consumer_messages_total`
- `wikimedia_consumer_unique_events_total`
- `wikimedia_consumer_duplicate_events_total`
- `wikimedia_consumer_parse_errors_total`

Kafka:
- `kafka_brokers`
- `kafka_topic_partitions`
- `kafka_consumergroup_lag`

## Acceptance checks

- Prometheus targets `UP`:
  - `kafka_exporter`
  - `wikimedia_producer`
  - `wikimedia_consumer`
- Grafana:
  - `Wikimedia Idempotent Overview` has producer/consumer data
  - `Kafka Classic Overview` has broker/topic/lag data
