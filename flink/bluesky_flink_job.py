#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict, Optional
from collections.abc import Iterable

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


LOGGER = logging.getLogger("flink-normalizer")

DEFAULT_KEYWORDS = {"gpu", "agent", "finance", "picture", "ai", "earnings", "guidance", "capex"}


def load_keywords() -> set[str]:
    raw = os.getenv("INVESTMENT_KEYWORDS", "")
    if raw:
        return {token.strip().lower() for token in raw.split(",") if token.strip()}
    return set(DEFAULT_KEYWORDS)


KEYWORDS = load_keywords()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Keep only keyword-matched Bluesky posts into source_clean.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--raw-topic", default="source_raw")
    parser.add_argument("--clean-topic", default="source_clean")
    parser.add_argument("--group-id", default="flink-source-normalizer-v1")
    parser.add_argument("--startup-mode", choices=("committed", "earliest", "latest"), default="committed")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--self-test", action="store_true", help="Run single-file parser self test and exit.")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def loads_maybe_json(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
            return payload if isinstance(payload, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def process_raw(payload: Dict[str, Any], keywords: set[str]) -> Optional[Dict[str, Any]]:
    """Process unified envelope from source_raw. Returns envelope unchanged if it passes filter."""
    content_type = payload.get("content_type")
    source_id = payload.get("source_id")
    if not source_id or not content_type:
        return None

    # Price ticks always pass through (pre-filtered by ticker subscription)
    if content_type == "price_tick":
        return payload

    # Twitter source passes through without keyword filter (pre-filtered by following list)
    source = payload.get("source", "")
    if source == "twitter":
        return payload

    # For other social_post and news_article, apply keyword filter
    data = payload.get("data") or {}
    text_parts = [
        data.get("text") or "",
        data.get("title") or "",
        data.get("snippet") or "",
    ]
    text = " ".join(text_parts).strip()
    if not text:
        return None

    haystack = text.lower()
    for kw in keywords:
        token = kw.strip().lower()
        if token and token in haystack:
            return payload  # pass unified envelope through unchanged
    return None


def flink_map_fn(raw_message: str) -> Iterable[str]:
    payload = loads_maybe_json(raw_message)
    if payload is None:
        return []
    output = process_raw(payload, KEYWORDS)
    if output is None:
        return []
    return [json.dumps(output, ensure_ascii=False)]


def ensure_topics(bootstrap: str, topics: list[str]) -> None:
    """Provision stream endpoints before KafkaSource enumerates partitions.

    Fresh clusters otherwise enter a restart loop until an ingestor happens to
    create source_raw. Match the ingestors' six-partition, three-replica policy.
    Concurrent ingestor creation is harmless; other errors reach the supervisor.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=6, replication_factor=3) for topic in dict.fromkeys(topics)],
        request_timeout=20,
    )
    for topic, future in futures.items():
        try:
            future.result()
            LOGGER.info("created Kafka topic %s", topic)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def run_flink_job(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    ensure_topics(args.bootstrap, [args.raw_topic, args.clean_topic])
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(max(1, args.parallelism))
    if args.checkpoint_interval_ms <= 0:
        raise ValueError("checkpoint-interval-ms must be positive")
    env.enable_checkpointing(args.checkpoint_interval_ms)

    # Checkpoints commit source offsets only after the at-least-once sink has
    # flushed. A new job resumes those offsets; explicit earliest/latest remain
    # available for deliberate replays. The final partial checkpoint may replay,
    # so downstream consumers still deduplicate source identities.
    if args.startup_mode == "committed":
        startup = KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
    elif args.startup_mode == "earliest":
        startup = KafkaOffsetsInitializer.earliest()
    else:
        startup = KafkaOffsetsInitializer.latest()
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_topics(args.raw_topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(startup)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    normalized_stream = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="source_raw_consumer",
    ).flat_map(flink_map_fn, output_type=Types.STRING())

    clean_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.clean_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    normalized_stream.sink_to(clean_sink).name("source_clean_sink")
    LOGGER.info(
        "starting flink normalizer raw=%s clean=%s",
        args.raw_topic,
        args.clean_topic,
    )
    env.execute("source-raw-to-clean-and-embedding")


def run_single_file_test() -> None:
    raw_str = r'''
    {
      "source": "bluesky",
      "source_id": "bafyreigsrkuihkw3evywnsj3y7z3drf56wa6ed4ki7dsz7h3ybhvqq5giy",
      "content_type": "social_post",
      "timestamp": "2026-03-21T19:08:05.388Z",
      "data": {
        "text": "AI picture pipeline update with better GPU utilization.",
        "author": "did:plc:trwim3dlnep55nelnthy3r5g",
        "lang": "en"
      },
      "raw": {}
    }
    '''
    payload = loads_maybe_json(raw_str)
    keywords = {"ai", "gpu"}
    if payload is None:
        print("bad payload")
        return
    msg = process_raw(payload, keywords)
    if msg is None:
        print("no hit")
        return
    print(json.dumps(msg, ensure_ascii=False, indent=2))


def main() -> None:
    args = parse_args()
    if args.self_test:
        run_single_file_test()
        return
    run_flink_job(args)


if __name__ == "__main__":
    main()
