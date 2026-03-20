#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
from collections.abc import Iterable
from typing import Any

from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


LOGGER = logging.getLogger("bluesky-flink-job")
TRACER = trace.get_tracer("flink.job")

TICKER_PATTERN = re.compile(r"\$[A-Z]{2,10}\b")
HASHTAG_PATTERN = re.compile(r"#([A-Za-z0-9_]{2,40})")

KEYWORD_WEIGHTS: dict[str, int] = {
    "buy": 2,
    "bull": 2,
    "long": 2,
    "breakout": 2,
    "sell": 2,
    "short": 2,
    "bear": 2,
    "dump": 2,
    "signal": 1,
    "entry": 1,
    "target": 1,
    "stop": 1,
    "support": 1,
    "resistance": 1,
    "momentum": 1,
}

POSITIVE_WORDS = {"bull", "long", "breakout", "pump", "win", "strong", "up"}
NEGATIVE_WORDS = {"bear", "short", "dump", "risk", "weak", "down", "loss"}


def init_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/") + "/v1/traces"
    )
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    LOGGER.info("otel tracing enabled endpoint=%s service=%s", endpoint, service_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flink stream job for bluesky cleansing, enrichment and candidate generation."
    )
    parser.add_argument(
        "--bootstrap",
        default="kafka1:19092,kafka2:19092,kafka3:19092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument("--raw-topic", default="bluesky_raw", help="Input raw topic.")
    parser.add_argument("--enriched-topic", default="bluesky_enriched", help="Output enriched topic.")
    parser.add_argument(
        "--candidate-topic",
        default="strategy_signal_candidate",
        help="Output strategy candidate topic.",
    )
    parser.add_argument(
        "--group-id",
        default="flink-bluesky-enrich-v1",
        help="Kafka source consumer group id.",
    )
    parser.add_argument(
        "--startup-mode",
        choices=("earliest", "latest"),
        default="earliest",
        help="Kafka startup mode for the source.",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Flink job parallelism.",
    )
    parser.add_argument(
        "--checkpoint-interval-ms",
        type=int,
        default=10_000,
        help="Flink checkpoint interval in milliseconds.",
    )
    parser.add_argument(
        "--dedup-window-ms",
        type=int,
        default=15 * 60 * 1000,
        help="Dedup window based on uri+cid in milliseconds.",
    )
    parser.add_argument(
        "--activity-window-ms",
        type=int,
        default=5 * 60 * 1000,
        help="Author activity rolling window in milliseconds.",
    )
    parser.add_argument(
        "--candidate-threshold",
        type=int,
        default=3,
        help="Candidate score threshold.",
    )
    parser.add_argument(
        "--topic-partitions",
        type=int,
        default=6,
        help="Partition count for auto-created output topics.",
    )
    parser.add_argument(
        "--topic-replication-factor",
        type=int,
        default=3,
        help="Replication factor for auto-created output topics.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def ensure_topics(args: argparse.Namespace) -> None:
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    metadata = admin.list_topics(timeout=15)
    existing = set(metadata.topics.keys())
    to_create: list[NewTopic] = []
    for topic_name in (args.enriched_topic, args.candidate_topic):
        if topic_name in existing:
            continue
        to_create.append(
            NewTopic(
                topic_name,
                num_partitions=max(1, args.topic_partitions),
                replication_factor=max(1, args.topic_replication_factor),
            )
        )
    if not to_create:
        return

    futures = admin.create_topics(to_create, request_timeout=20)
    for topic_name, future in futures.items():
        try:
            future.result()
            LOGGER.info("created topic=%s", topic_name)
        except Exception as exc:  # pragma: no cover - operational safeguard
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                LOGGER.info("topic already exists topic=%s", topic_name)
                continue
            raise


def extract_record_fields(raw_event: dict[str, Any]) -> tuple[str, list[str]]:
    record = raw_event.get("record") or {}
    text = str(record.get("text") or "").strip()

    raw_langs = record.get("langs", [])
    if isinstance(raw_langs, str):
        raw_langs = [raw_langs]
    langs = [str(lang).strip().lower() for lang in raw_langs if str(lang).strip()]
    single_lang = str(record.get("lang") or "").strip().lower()
    if single_lang and single_lang not in langs:
        langs.append(single_lang)
    return text, langs


def compute_features(text: str) -> dict[str, Any]:
    lower_text = text.lower()
    words = [token.strip(".,!?;:()[]{}\"'") for token in lower_text.split() if token]

    ticker_hits = sorted(set(match[1:] for match in TICKER_PATTERN.findall(text)))
    hashtag_hits = sorted(set(HASHTAG_PATTERN.findall(text)))
    keyword_hits = sorted({keyword for keyword in KEYWORD_WEIGHTS if keyword in lower_text})
    keyword_score = sum(KEYWORD_WEIGHTS[keyword] for keyword in keyword_hits)
    sentiment_score = sum((1 if token in POSITIVE_WORDS else -1 if token in NEGATIVE_WORDS else 0) for token in words)

    return {
        "text_length": len(text),
        "tickers": ticker_hits,
        "hashtags": hashtag_hits,
        "keyword_hits": keyword_hits,
        "keyword_score": keyword_score,
        "sentiment_score": sentiment_score,
        "word_count": len(words),
    }


def normalize_message(message: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None

    if str(payload.get("operation") or "").lower() != "create":
        return None

    if str(payload.get("collection") or "") != "app.bsky.feed.post":
        return None

    text, langs = extract_record_fields(payload)
    if not text:
        return None

    did = str(payload.get("did") or "").strip()
    uri = str(payload.get("uri") or "").strip()
    cid = str(payload.get("cid") or "").strip()
    time_us = payload.get("time_us")
    received_at_ms = int(payload.get("received_at_ms") or 0)
    event_ts_ms = int(time_us // 1000) if isinstance(time_us, int) else received_at_ms

    features = compute_features(text)
    base_score = features["keyword_score"] + max(0, features["sentiment_score"]) + (2 * len(features["tickers"]))
    dedup_key = f"{uri}:{cid}" if (uri and cid) else hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()

    return {
        "pipeline_version": "m2-v1",
        "source": payload.get("source", "bluesky_jetstream"),
        "author_did": did,
        "uri": uri,
        "cid": cid,
        "langs": langs,
        "text": text,
        "event_ts_ms": event_ts_ms,
        "received_at_ms": received_at_ms,
        "dedup_key": dedup_key,
        "features": features,
        "candidate": {
            "base_score": base_score,
            "score": base_score,
            "is_candidate": False,
            "reasons": [],
        },
    }


def parse_and_normalize(message: str) -> Iterable[str]:
    incoming_carrier: dict[str, str] = {}
    try:
        parsed = json.loads(message)
        if isinstance(parsed, dict):
            traceparent = str(parsed.get("traceparent") or "").strip()
            if traceparent:
                incoming_carrier["traceparent"] = traceparent
    except json.JSONDecodeError:
        parsed = None
    parent_context = propagate.extract(incoming_carrier)

    with TRACER.start_as_current_span("flink.transform.normalize", context=parent_context) as span:
        record = normalize_message(message)
        if record is None:
            return []
        outgoing_carrier: dict[str, str] = {}
        propagate.inject(outgoing_carrier)
        if outgoing_carrier.get("traceparent"):
            record["traceparent"] = outgoing_carrier["traceparent"]
        span.set_attribute("flink.stage", "normalize")
        span.set_attribute("flink.author_did", str(record.get("author_did") or ""))
        return [json.dumps(record, ensure_ascii=False)]


class DeduplicateByKeyProcessFunction(KeyedProcessFunction):
    def __init__(self, dedup_window_ms: int) -> None:
        self.dedup_window_ms = dedup_window_ms
        self.last_seen_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("last_seen_ts_ms", Types.LONG())
        self.last_seen_state = runtime_context.get_state(descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        record = json.loads(value)
        event_ts_ms = int(record.get("event_ts_ms") or 0)
        if event_ts_ms <= 0:
            event_ts_ms = int(record.get("received_at_ms") or 0)

        last_seen = self.last_seen_state.value()
        if last_seen is not None and (event_ts_ms - last_seen) <= self.dedup_window_ms:
            return

        self.last_seen_state.update(event_ts_ms)
        yield value


class AuthorActivityProcessFunction(KeyedProcessFunction):
    def __init__(self, activity_window_ms: int, candidate_threshold: int) -> None:
        self.activity_window_ms = activity_window_ms
        self.candidate_threshold = candidate_threshold
        self.activity_state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ListStateDescriptor("author_activity_timestamps", Types.LONG())
        self.activity_state = runtime_context.get_list_state(descriptor)

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        record = json.loads(value)
        event_ts_ms = int(record.get("event_ts_ms") or 0)
        if event_ts_ms <= 0:
            event_ts_ms = int(record.get("received_at_ms") or 0)

        timestamps = list(self.activity_state.get())
        min_ts = event_ts_ms - self.activity_window_ms
        pruned = [ts for ts in timestamps if ts >= min_ts]
        pruned.append(event_ts_ms)
        self.activity_state.update(pruned)

        features = record.get("features") or {}
        features["author_posts_5m"] = len(pruned)
        record["features"] = features

        candidate = record.get("candidate") or {}
        dynamic_boost = min(3, len(pruned) // 3)
        score = int(candidate.get("base_score") or 0) + dynamic_boost

        reasons = []
        if features.get("tickers"):
            reasons.append("ticker_hit")
        if int(features.get("keyword_score") or 0) >= 2:
            reasons.append("keyword_weight")
        if len(pruned) >= 5:
            reasons.append("author_activity")

        candidate["score"] = score
        candidate["is_candidate"] = score >= self.candidate_threshold and len(reasons) > 0
        candidate["reasons"] = reasons
        record["candidate"] = candidate

        yield json.dumps(record, ensure_ascii=False)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_tracing(os.getenv("OTEL_SERVICE_NAME", "flink-job"))

    ensure_topics(args)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(max(1, args.parallelism))
    env.enable_checkpointing(args.checkpoint_interval_ms)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

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

    normalized = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="bluesky_raw_source",
    ).flat_map(parse_and_normalize, output_type=Types.STRING())

    deduplicated = normalized.key_by(
        lambda payload: json.loads(payload).get("dedup_key", ""),
        key_type=Types.STRING(),
    ).process(
        DeduplicateByKeyProcessFunction(args.dedup_window_ms),
        output_type=Types.STRING(),
    )

    enriched_stream = deduplicated.key_by(
        lambda payload: json.loads(payload).get("author_did", ""),
        key_type=Types.STRING(),
    ).process(
        AuthorActivityProcessFunction(
            activity_window_ms=args.activity_window_ms,
            candidate_threshold=args.candidate_threshold,
        ),
        output_type=Types.STRING(),
    )

    candidate_stream = enriched_stream.filter(
        lambda payload: bool(json.loads(payload).get("candidate", {}).get("is_candidate", False))
    )

    enriched_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.enriched_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    candidate_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(args.candidate_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    enriched_stream.sink_to(enriched_sink).name("kafka_enriched_sink")
    candidate_stream.sink_to(candidate_sink).name("kafka_candidate_sink")

    LOGGER.info(
        "starting flink job raw=%s enriched=%s candidate=%s",
        args.raw_topic,
        args.enriched_topic,
        args.candidate_topic,
    )
    env.execute("bluesky-flink-enrich-and-candidate")


if __name__ == "__main__":
    main()
