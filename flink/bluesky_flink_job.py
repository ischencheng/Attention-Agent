#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict, Optional
from collections.abc import Iterable

from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
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
    parser.add_argument("--startup-mode", choices=("earliest", "latest"), default="earliest")
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
    commit = payload.get("commit")
    if not isinstance(commit, dict):
        return None
    if commit.get("collection") != "app.bsky.feed.post":
        return None

    cid = commit.get("cid")
    if not isinstance(cid, str) or not cid:
        return None

    record = commit.get("record")
    if not isinstance(record, dict):
        return None
    text = record.get("text")
    if not isinstance(text, str):
        return None

    haystack = text.lower()
    for kw in keywords:
        token = kw.strip().lower()
        if token and token in haystack:
            return {"cid": cid, "raw": payload}
    return None


def flink_map_fn(raw_message: str) -> Iterable[str]:
    payload = loads_maybe_json(raw_message)
    if payload is None:
        return []
    output = process_raw(payload, KEYWORDS)
    if output is None:
        return []
    return [json.dumps(output, ensure_ascii=False)]


def run_flink_job(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(max(1, args.parallelism))

    startup = (
        KafkaOffsetsInitializer.earliest()
        if args.startup_mode == "earliest"
        else KafkaOffsetsInitializer.latest()
    )
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
      "did": "did:plc:trwim3dlnep55nelnthy3r5g",
      "kind": "commit",
      "commit": {
        "cid": "bafyreigsrkuihkw3evywnsj3y7z3drf56wa6ed4ki7dsz7h3ybhvqq5giy",
        "rev": "3mhlq4rsfjv22",
        "rkey": "3mhlq4roxrs2y",
        "record": {
          "$type": "app.bsky.feed.post",
          "text": "AI picture pipeline update with better GPU utilization.",
          "langs": ["en"],
          "createdAt": "2026-03-21T19:08:05.388Z"
        },
        "operation": "create",
        "collection": "app.bsky.feed.post"
      },
      "time_us": 1774120086929886
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
