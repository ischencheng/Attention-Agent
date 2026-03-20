#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from psycopg2.extras import Json


LOGGER = logging.getLogger("kafka-postgres-sink")
TRACER = trace.get_tracer("sink.consumer")

MESSAGES_TOTAL = Counter(
    "sink_consumer_messages_total",
    "Total consumed messages handled by sink consumer.",
    ("topic", "status"),
)
DB_WRITES_TOTAL = Counter(
    "sink_consumer_db_writes_total",
    "Total db write attempts by topic and status.",
    ("topic", "status"),
)
DB_WRITE_LATENCY_SECONDS = Histogram(
    "sink_consumer_db_write_latency_seconds",
    "DB write latency in seconds for sink consumer.",
    ("topic",),
)
COMMIT_TOTAL = Counter(
    "sink_consumer_commit_total",
    "Total Kafka offset commit attempts in sink consumer.",
    ("status",),
)
LAST_SUCCESSFUL_WRITE_UNIXTIME = Gauge(
    "sink_consumer_last_successful_write_unixtime",
    "Unix timestamp of the latest successful DB write.",
)


def init_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/") + "/v1/traces"
    )
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    LOGGER.info("otel tracing enabled endpoint=%s service=%s", endpoint, service_name)


def kafka_headers_to_carrier(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    carrier: dict[str, str] = {}
    for key, value in headers or []:
        if value is None:
            continue
        try:
            carrier[key] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return carrier


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka topics and persist records into PostgreSQL.")
    parser.add_argument(
        "--bootstrap",
        default="kafka1:19092,kafka2:19092,kafka3:19092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topics",
        default="bluesky_raw,bluesky_enriched,strategy_signal_candidate",
        help="Comma separated Kafka topics to consume.",
    )
    parser.add_argument(
        "--group-id",
        default="sink-postgres-v1",
        help="Kafka consumer group id.",
    )
    parser.add_argument("--db-host", default="postgres", help="PostgreSQL host.")
    parser.add_argument("--db-port", type=int, default=5432, help="PostgreSQL port.")
    parser.add_argument("--db-name", default="bluesky", help="PostgreSQL database name.")
    parser.add_argument("--db-user", default="bluesky", help="PostgreSQL username.")
    parser.add_argument("--db-password", default="bluesky", help="PostgreSQL password.")
    parser.add_argument(
        "--poll-timeout-seconds",
        type=float,
        default=1.0,
        help="Kafka consumer poll timeout.",
    )
    parser.add_argument(
        "--db-connect-max-attempts",
        type=int,
        default=0,
        help="Max attempts to connect to PostgreSQL. 0 means retry forever.",
    )
    parser.add_argument(
        "--db-connect-backoff-seconds",
        type=float,
        default=2.0,
        help="Initial backoff seconds when retrying DB connection.",
    )
    parser.add_argument("--metrics-port", type=int, default=8040, help="Prometheus metrics port.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def parse_topics(topics_csv: str) -> list[str]:
    topics = [token.strip() for token in topics_csv.split(",") if token.strip()]
    if not topics:
        raise ValueError("at least one topic is required")
    return topics


def epoch_ms_to_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return datetime.fromtimestamp(numeric / 1000.0, tz=timezone.utc)


def decode_message_key(msg: Message) -> str | None:
    key = msg.key()
    if key is None:
        return None
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def decode_json_payload(msg: Message) -> dict[str, Any] | None:
    raw_value = msg.value()
    if raw_value is None:
        return None
    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def insert_raw_post(cursor: Any, msg: Message, payload: dict[str, Any]) -> None:
    record = payload.get("record") or {}
    cursor.execute(
        """
        INSERT INTO raw_posts (
            kafka_topic, kafka_partition, kafka_offset, message_key,
            source, author_did, uri, cid, operation, collection,
            post_text, event_ts, received_at, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            decode_message_key(msg),
            payload.get("source"),
            payload.get("did"),
            payload.get("uri"),
            payload.get("cid"),
            payload.get("operation"),
            payload.get("collection"),
            record.get("text"),
            epoch_ms_to_utc(payload.get("time_us", 0) // 1000 if isinstance(payload.get("time_us"), int) else None),
            epoch_ms_to_utc(payload.get("received_at_ms")),
            Json(payload),
        ),
    )


def insert_enriched_post(cursor: Any, msg: Message, payload: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO enriched_posts (
            kafka_topic, kafka_partition, kafka_offset, message_key,
            pipeline_version, source, author_did, uri, cid,
            langs, post_text, features, candidate, event_ts, received_at, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            decode_message_key(msg),
            payload.get("pipeline_version"),
            payload.get("source"),
            payload.get("author_did"),
            payload.get("uri"),
            payload.get("cid"),
            Json(payload.get("langs") if isinstance(payload.get("langs"), list) else []),
            payload.get("text"),
            Json(payload.get("features") if isinstance(payload.get("features"), dict) else {}),
            Json(payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}),
            epoch_ms_to_utc(payload.get("event_ts_ms")),
            epoch_ms_to_utc(payload.get("received_at_ms")),
            Json(payload),
        ),
    )


def insert_strategy_signal(cursor: Any, msg: Message, payload: dict[str, Any]) -> None:
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    signal_type = "final" if msg.topic() == "strategy_signal_final" else "candidate"

    cursor.execute(
        """
        INSERT INTO strategy_signals (
            kafka_topic, kafka_partition, kafka_offset, message_key,
            signal_type, pipeline_version, source, author_did, uri, cid,
            score, is_candidate, reasons, tickers, hashtags, keyword_hits,
            sentiment_score, author_posts_5m, event_ts, received_at, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            decode_message_key(msg),
            signal_type,
            payload.get("pipeline_version"),
            payload.get("source"),
            payload.get("author_did"),
            payload.get("uri"),
            payload.get("cid"),
            int(candidate.get("score")) if isinstance(candidate.get("score"), int) else None,
            bool(candidate.get("is_candidate")) if "is_candidate" in candidate else None,
            Json(candidate.get("reasons") if isinstance(candidate.get("reasons"), list) else []),
            Json(features.get("tickers") if isinstance(features.get("tickers"), list) else []),
            Json(features.get("hashtags") if isinstance(features.get("hashtags"), list) else []),
            Json(features.get("keyword_hits") if isinstance(features.get("keyword_hits"), list) else []),
            int(features.get("sentiment_score")) if isinstance(features.get("sentiment_score"), int) else None,
            int(features.get("author_posts_5m")) if isinstance(features.get("author_posts_5m"), int) else None,
            epoch_ms_to_utc(payload.get("event_ts_ms")),
            epoch_ms_to_utc(payload.get("received_at_ms")),
            Json(payload),
        ),
    )


def insert_llm_insight(cursor: Any, payload: dict[str, Any]) -> None:
    llm = payload.get("llm_insight") if isinstance(payload.get("llm_insight"), dict) else {}
    if not llm:
        return

    cursor.execute(
        """
        INSERT INTO llm_insights (
            strategy_signal_id, model_name, summary, risk_tags, recommendation, confidence, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            None,
            llm.get("model_name"),
            llm.get("summary"),
            Json(llm.get("risk_tags") if isinstance(llm.get("risk_tags"), list) else []),
            llm.get("recommendation"),
            llm.get("confidence"),
            Json(llm),
        ),
    )


def route_and_insert(cursor: Any, msg: Message, payload: dict[str, Any]) -> bool:
    topic = msg.topic()
    if topic == "bluesky_raw":
        insert_raw_post(cursor, msg, payload)
        return True
    if topic == "bluesky_enriched":
        insert_enriched_post(cursor, msg, payload)
        return True
    if topic in {"strategy_signal_candidate", "strategy_signal_final"}:
        insert_strategy_signal(cursor, msg, payload)
        if topic == "strategy_signal_final":
            insert_llm_insight(cursor, payload)
        return True
    return False


def wait_for_db(args: argparse.Namespace):
    attempt = 0
    backoff = max(0.2, args.db_connect_backoff_seconds)
    while True:
        attempt += 1
        try:
            conn = psycopg2.connect(
                host=args.db_host,
                port=args.db_port,
                dbname=args.db_name,
                user=args.db_user,
                password=args.db_password,
            )
            conn.autocommit = False
            LOGGER.info("postgres connected host=%s port=%s db=%s", args.db_host, args.db_port, args.db_name)
            return conn
        except Exception as exc:
            LOGGER.warning("postgres unavailable attempt=%s error=%s", attempt, exc)
            if args.db_connect_max_attempts > 0 and attempt >= args.db_connect_max_attempts:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 15.0)


def build_consumer(args: argparse.Namespace, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe(topics)
    return consumer


def commit_with_retry(
    consumer: Consumer, msg: Message, attempts: int = 3, backoff_seconds: float = 0.5
) -> bool:
    current_backoff = max(0.1, backoff_seconds)
    for attempt in range(1, max(1, attempts) + 1):
        try:
            consumer.commit(message=msg, asynchronous=False)
            COMMIT_TOTAL.labels("success").inc()
            return True
        except KafkaException as exc:
            COMMIT_TOTAL.labels("error").inc()
            LOGGER.warning(
                "kafka commit failed attempt=%s topic=%s partition=%s offset=%s error=%s",
                attempt,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                exc,
            )
            time.sleep(current_backoff)
            current_backoff = min(current_backoff * 2, 5.0)
    return False


def run() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_tracing(os.getenv("OTEL_SERVICE_NAME", "sink-consumer"))
    start_http_server(args.metrics_port)
    topics = parse_topics(args.topics)
    conn = wait_for_db(args)
    consumer = build_consumer(args, topics)

    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOGGER.info(
        "sink started topics=%s group_id=%s metrics=0.0.0.0:%s/metrics",
        ",".join(topics),
        args.group_id,
        args.metrics_port,
    )
    unknown_topic_code = getattr(KafkaError, "UNKNOWN_TOPIC_OR_PART", 3)

    try:
        while running:
            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                error = msg.error()
                code = error.code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                # Topics like bluesky_enriched may not exist until Flink starts and creates them.
                if code == unknown_topic_code:
                    LOGGER.warning("topic not ready yet: %s", error)
                    time.sleep(2.0)
                    continue
                raise KafkaException(error)

            topic = msg.topic()
            incoming_carrier = kafka_headers_to_carrier(msg.headers())
            payload = decode_json_payload(msg)
            if payload and isinstance(payload.get("traceparent"), str):
                incoming_carrier.setdefault("traceparent", payload.get("traceparent", ""))
            parent_context = propagate.extract(incoming_carrier)

            with TRACER.start_as_current_span("sink_consume_span", context=parent_context) as consume_span:
                consume_span.set_attribute("messaging.system", "kafka")
                consume_span.set_attribute("messaging.operation", "process")
                consume_span.set_attribute("messaging.destination.name", topic)
                consume_span.set_attribute("messaging.kafka.partition", int(msg.partition()))
                consume_span.set_attribute("messaging.kafka.offset", int(msg.offset()))
                if payload is None:
                    MESSAGES_TOTAL.labels(topic=topic, status="invalid_payload").inc()
                    LOGGER.warning(
                        "skip invalid payload topic=%s partition=%s offset=%s",
                        topic,
                        msg.partition(),
                        msg.offset(),
                    )
                    commit_with_retry(consumer, msg)
                    continue

                try:
                    with TRACER.start_as_current_span("sink_db_write_span"):
                        with DB_WRITE_LATENCY_SECONDS.labels(topic=topic).time():
                            with conn.cursor() as cursor:
                                inserted = route_and_insert(cursor, msg, payload)
                                if not inserted:
                                    LOGGER.warning("skip unsupported topic topic=%s", topic)
                                    MESSAGES_TOTAL.labels(topic=topic, status="unsupported_topic").inc()
                                    conn.commit()
                                    commit_with_retry(consumer, msg)
                                    continue
                            conn.commit()
                    DB_WRITES_TOTAL.labels(topic=topic, status="success").inc()
                    LAST_SUCCESSFUL_WRITE_UNIXTIME.set(time.time())
                    MESSAGES_TOTAL.labels(topic=topic, status="processed").inc()
                    with TRACER.start_as_current_span("sink_commit_span"):
                        if not commit_with_retry(consumer, msg):
                            LOGGER.error(
                                "give up committing offset topic=%s partition=%s offset=%s",
                                topic,
                                msg.partition(),
                                msg.offset(),
                            )
                except Exception as exc:
                    conn.rollback()
                    DB_WRITES_TOTAL.labels(topic=topic, status="error").inc()
                    MESSAGES_TOTAL.labels(topic=topic, status="db_error").inc()
                    LOGGER.error(
                        "db write failed topic=%s partition=%s offset=%s error=%s",
                        topic,
                        msg.partition(),
                        msg.offset(),
                        exc,
                    )
                    time.sleep(1.0)
    finally:
        consumer.close()
        conn.close()
        LOGGER.info("sink stopped")


if __name__ == "__main__":
    run()
