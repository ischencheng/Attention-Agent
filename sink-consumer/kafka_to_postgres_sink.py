#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from typing import Any

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from psycopg2.extras import Json


LOGGER = logging.getLogger("kafka-postgres-sink")

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
LAST_SUCCESSFUL_WRITE_UNIXTIME = Gauge("sink_consumer_last_successful_write_unixtime", "Unix timestamp of the latest successful DB write.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka topics and persist records into PostgreSQL.")
    parser.add_argument(
        "--bootstrap",
        default="kafka1:19092,kafka2:19092,kafka3:19092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topics",
        default="source_raw,source_clean,vector_embedding,attention_ticket,annotations,decision_packet",
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


def extract_raw(payload: dict[str, Any]) -> Any:
    if "raw" in payload:
        return payload["raw"]
    return payload

def extract_embedding_literal(payload: dict[str, Any]) -> str | None:
    embedding = payload.get("embedding")
    if not isinstance(embedding, list):
        return None
    values: list[str] = []
    for value in embedding:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        values.append(f"{number:.10f}")
    if not values:
        return None
    return "[" + ",".join(values) + "]"


TOPIC_TABLE_MAP: dict[str, str] = {
    "source_raw": "raw_messages",
    "source_clean": "source_clean",
    "vector_embedding": "vector_embeddings",
    "attention_ticket": "attention_tickets",
    "annotations": "annotations",
    "decision_packet": "decision_packets",
}

def insert_topic_payload(cursor: Any, table_name: str, msg: Message, payload: dict[str, Any]) -> None:
    cursor.execute(
        f"""
        INSERT INTO {table_name} (kafka_topic, kafka_partition, kafka_offset, payload, raw)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            Json(payload),
            Json(extract_raw(payload)),
        ),
    )

def insert_vector_embedding_payload(cursor: Any, msg: Message, payload: dict[str, Any]) -> None:
    embedding_literal = extract_embedding_literal(payload)
    if embedding_literal is None:
        cursor.execute(
            """
            INSERT INTO vector_embeddings (kafka_topic, kafka_partition, kafka_offset, payload, raw)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            """,
            (
                msg.topic(),
                msg.partition(),
                msg.offset(),
                Json(payload),
                Json(extract_raw(payload)),
            ),
        )
        return

    cursor.execute(
        """
        INSERT INTO vector_embeddings (kafka_topic, kafka_partition, kafka_offset, payload, embedding_vec, raw)
        VALUES (%s, %s, %s, %s, %s::vector, %s)
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (
            msg.topic(),
            msg.partition(),
            msg.offset(),
            Json(payload),
            embedding_literal,
            Json(extract_raw(payload)),
        ),
    )


def route_and_insert(cursor: Any, msg: Message, payload: dict[str, Any]) -> bool:
    topic = msg.topic()
    if topic == "vector_embedding":
        insert_vector_embedding_payload(cursor, msg, payload)
        return True
    table_name = TOPIC_TABLE_MAP.get(topic)
    if table_name is not None:
        insert_topic_payload(cursor, table_name, msg, payload)
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
    last_resubscribe = time.monotonic()
    RESUBSCRIBE_INTERVAL = 30.0  # re-subscribe every 30s to pick up late-created topics

    try:
        while running:
            # Periodically re-subscribe so the consumer discovers topics
            # that were created after the initial subscribe() call.
            now = time.monotonic()
            if now - last_resubscribe >= RESUBSCRIBE_INTERVAL:
                consumer.subscribe(topics)
                last_resubscribe = now

            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                error = msg.error()
                code = error.code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                # Topics may not exist until upstream services start and create them.
                if code == unknown_topic_code:
                    LOGGER.warning("topic not ready yet: %s", error)
                    # Force a re-subscribe to pick up newly created topics
                    consumer.subscribe(topics)
                    last_resubscribe = time.monotonic()
                    time.sleep(2.0)
                    continue
                raise KafkaException(error)

            topic = msg.topic()
            payload = decode_json_payload(msg)
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
                committed = commit_with_retry(consumer, msg)
                if not committed:
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
