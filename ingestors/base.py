#!/usr/bin/env python3
"""Shared utilities for all ingestors."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


LOGGER = logging.getLogger("ingestor-base")


def ensure_topic(
    bootstrap: str,
    topic: str,
    partitions: int = 6,
    replication_factor: int = 3,
    max_attempts: int = 0,
    backoff_seconds: float = 2.0,
) -> None:
    attempt = 0
    backoff = max(0.1, backoff_seconds)
    while True:
        attempt += 1
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap})
            metadata = admin.list_topics(timeout=15)
            if topic in set(metadata.topics.keys()):
                return
            futures = admin.create_topics(
                [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)],
                request_timeout=20,
            )
            futures[topic].result()
            LOGGER.info("created topic=%s", topic)
            return
        except Exception as ex:
            if "TOPIC_ALREADY_EXISTS" in str(ex):
                return
            if max_attempts > 0 and attempt >= max_attempts:
                raise
            wait = min(backoff, 15.0)
            LOGGER.warning("kafka not ready attempt=%s error=%s retry_in=%.1fs", attempt, ex, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 15.0)


def create_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 1_000_000,
            "retry.backoff.ms": 100,
            "delivery.timeout.ms": 120_000,
            "request.timeout.ms": 30_000,
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )


def make_envelope(
    source: str,
    source_id: str,
    content_type: str,
    timestamp: str,
    data: dict[str, Any],
    raw: Any,
) -> bytes:
    envelope = {
        "source": source,
        "source_id": source_id,
        "content_type": content_type,
        "timestamp": timestamp,
        "data": data,
        "raw": raw,
    }
    return json.dumps(envelope, ensure_ascii=False).encode("utf-8")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def produce_with_flush(producer: Producer, topic: str, key: str, value: bytes) -> None:
    producer.produce(topic=topic, key=key.encode("utf-8"), value=value)
    producer.poll(0)
