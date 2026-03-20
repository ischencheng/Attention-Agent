#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
import threading
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Gauge, start_http_server


LOGGER = logging.getLogger("bluesky-ingestor")


def init_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/") + "/v1/traces"
    )
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    LOGGER.info("otel tracing enabled endpoint=%s service=%s", endpoint, service_name)


def parse_csv(value: str) -> set[str]:
    return {token.strip().lower() for token in value.split(",") if token.strip()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bluesky Jetstream to Kafka ingestor.")
    parser.add_argument(
        "--bootstrap",
        default="kafka1:19092,kafka2:19092,kafka3:19092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument("--topic", default="bluesky_raw", help="Kafka raw topic.")
    parser.add_argument("--dlq-topic", default="bluesky_raw_dlq", help="Kafka DLQ topic.")
    parser.add_argument(
        "--jetstream-url",
        default="wss://jetstream2.us-east.bsky.network/subscribe",
        help="Jetstream WebSocket URL.",
    )
    parser.add_argument(
        "--keywords",
        default="",
        help="Comma separated keyword filter (case-insensitive substring match).",
    )
    parser.add_argument("--author-dids", default="", help="Comma separated DID allow list.")
    parser.add_argument(
        "--languages",
        default="",
        help="Comma separated language allow list (matched from record.lang/langs).",
    )
    parser.add_argument("--metrics-port", type=int, default=8020, help="Prometheus metrics port.")
    parser.add_argument(
        "--max-produce-retries",
        type=int,
        default=5,
        help="Retry attempts for Kafka produce before DLQ.",
    )
    parser.add_argument(
        "--produce-retry-backoff-seconds",
        type=float,
        default=0.5,
        help="Initial retry backoff for Kafka produce.",
    )
    parser.add_argument(
        "--delivery-timeout-ms",
        type=int,
        default=120_000,
        help="Kafka delivery timeout in milliseconds.",
    )
    parser.add_argument(
        "--reconnect-max-backoff-seconds",
        type=float,
        default=30.0,
        help="Max backoff when reconnecting to Jetstream.",
    )
    parser.add_argument(
        "--kafka-startup-max-attempts",
        type=int,
        default=0,
        help="Max retries to contact Kafka during startup topic checks. 0 means retry forever.",
    )
    parser.add_argument(
        "--kafka-startup-backoff-seconds",
        type=float,
        default=2.0,
        help="Initial backoff for startup Kafka topic checks.",
    )
    parser.add_argument(
        "--topic-partitions",
        type=int,
        default=6,
        help="Partition count used when auto-creating raw and DLQ topics.",
    )
    parser.add_argument(
        "--topic-replication-factor",
        type=int,
        default=3,
        help="Replication factor used when auto-creating raw and DLQ topics.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def build_jetstream_url(base_url: str) -> str:
    split = urlsplit(base_url)
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    query.setdefault("compress", "false")
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def extract_text(event: dict[str, Any]) -> str:
    commit = event.get("commit") or {}
    record = commit.get("record") or {}
    text = record.get("text")
    return str(text) if text else ""


def extract_languages(event: dict[str, Any]) -> set[str]:
    commit = event.get("commit") or {}
    record = commit.get("record") or {}
    raw_langs = record.get("langs", [])
    if isinstance(raw_langs, str):
        raw_langs = [raw_langs]
    langs: set[str] = {str(lang).strip().lower() for lang in raw_langs if str(lang).strip()}
    single_lang = record.get("lang")
    if single_lang:
        langs.add(str(single_lang).strip().lower())
    return langs


class IngestMetrics:
    def __init__(self) -> None:
        self.events_received = Counter(
            "bluesky_ingestor_events_received_total",
            "Total events received from Jetstream.",
        )
        self.events_filtered = Counter(
            "bluesky_ingestor_events_filtered_total",
            "Total events filtered out.",
            ("reason",),
        )
        self.raw_publish_success = Counter(
            "bluesky_ingestor_raw_publish_success_total",
            "Total successful publish operations to raw topic.",
        )
        self.raw_publish_failed = Counter(
            "bluesky_ingestor_raw_publish_failed_total",
            "Total failed publish operations to raw topic.",
            ("error_type",),
        )
        self.dlq_publish_success = Counter(
            "bluesky_ingestor_dlq_publish_success_total",
            "Total successful DLQ writes.",
        )
        self.dlq_publish_failed = Counter(
            "bluesky_ingestor_dlq_publish_failed_total",
            "Total failed DLQ write attempts.",
            ("error_type",),
        )
        self.reconnects = Counter(
            "bluesky_ingestor_reconnect_total",
            "Total Jetstream reconnect attempts.",
        )
        self.last_event_unixtime = Gauge(
            "bluesky_ingestor_last_event_unixtime",
            "Unix timestamp of the latest received Jetstream event.",
        )


class BlueskyIngestor:
    def __init__(self, args: argparse.Namespace, metrics: IngestMetrics) -> None:
        self.args = args
        self.metrics = metrics
        self.topic = args.topic
        self.dlq_topic = args.dlq_topic
        self.jetstream_url = build_jetstream_url(args.jetstream_url)
        self.author_dids = parse_csv(args.author_dids)
        self.keywords = parse_csv(args.keywords)
        self.languages = parse_csv(args.languages)
        self.running = True
        self.reconnect_backoff = 1.0
        self.tracer = trace.get_tracer("bluesky.ingestor")
        self.producer = Producer(
            {
                "bootstrap.servers": args.bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "retries": 1_000_000,
                "retry.backoff.ms": 100,
                "delivery.timeout.ms": args.delivery_timeout_ms,
                "request.timeout.ms": 30_000,
                "compression.type": "zstd",
                "linger.ms": 20,
            }
        )

    def _ensure_topics_once(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.args.bootstrap})
        metadata = admin.list_topics(timeout=15)
        existing = set(metadata.topics.keys())
        to_create: list[NewTopic] = []
        for topic_name in (self.topic, self.dlq_topic):
            if topic_name in existing:
                continue
            to_create.append(
                NewTopic(
                    topic_name,
                    num_partitions=max(1, self.args.topic_partitions),
                    replication_factor=max(1, self.args.topic_replication_factor),
                )
            )

        if not to_create:
            return

        created = admin.create_topics(to_create, request_timeout=20)
        for topic_name, future in created.items():
            try:
                future.result()
                LOGGER.info("created topic=%s", topic_name)
            except Exception as ex:
                # Topic may already exist due to races with another client.
                if "TOPIC_ALREADY_EXISTS" in str(ex):
                    LOGGER.info("topic already exists topic=%s", topic_name)
                    continue
                raise

    def ensure_topics(self) -> None:
        max_attempts = self.args.kafka_startup_max_attempts
        attempt = 0
        backoff = max(0.1, self.args.kafka_startup_backoff_seconds)

        while self.running:
            attempt += 1
            try:
                self._ensure_topics_once()
                return
            except Exception as ex:
                if max_attempts > 0 and attempt >= max_attempts:
                    raise
                wait_seconds = min(backoff, 15.0)
                LOGGER.warning(
                    "kafka not ready for topic ensure attempt=%s error=%s retry_in=%.1fs",
                    attempt,
                    ex,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                backoff = min(backoff * 2, 15.0)

    def should_keep_event(self, event: dict[str, Any]) -> tuple[bool, str]:
        did = str(event.get("did", "")).strip().lower()
        if self.author_dids and did not in self.author_dids:
            return False, "author_did"

        text = extract_text(event).lower()
        if self.keywords and not any(keyword in text for keyword in self.keywords):
            return False, "keyword"

        langs = extract_languages(event)
        if self.languages and not (langs & self.languages):
            return False, "language"

        return True, ""

    def build_key(self, event: dict[str, Any]) -> str:
        did = str(event.get("did", "")).strip()
        if did:
            return did
        commit = event.get("commit") or {}
        uri = str(commit.get("uri", "")).strip()
        if uri:
            return hashlib.sha256(uri.encode("utf-8")).hexdigest()
        return hashlib.sha256(json.dumps(event, sort_keys=True).encode("utf-8")).hexdigest()

    def build_normalized_payload(self, event: dict[str, Any]) -> dict[str, Any]:
        commit = event.get("commit") or {}
        return {
            "source": "bluesky_jetstream",
            "received_at_ms": int(time.time() * 1000),
            "did": event.get("did"),
            "kind": event.get("kind"),
            "time_us": event.get("time_us"),
            "collection": commit.get("collection"),
            "operation": commit.get("operation"),
            "uri": commit.get("uri"),
            "cid": commit.get("cid"),
            "record": commit.get("record"),
            "raw": event,
        }

    def _produce_and_wait(
        self,
        topic: str,
        key: str,
        payload: bytes,
        headers: list[tuple[str, bytes | None]] | None = None,
    ) -> None:
        done = threading.Event()
        result: dict[str, Exception | None] = {"error": None}

        def on_delivery(err: Any, _msg: Any) -> None:
            result["error"] = err
            done.set()

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload,
            headers=headers,
            on_delivery=on_delivery,
        )

        deadline = time.time() + max(self.args.delivery_timeout_ms / 1000, 1)
        while not done.is_set():
            self.producer.poll(0.1)
            if time.time() > deadline:
                raise TimeoutError(f"delivery timeout topic={topic}")

        error = result["error"]
        if error is not None:
            raise RuntimeError(str(error))

    async def produce_with_retry(
        self,
        topic: str,
        key: str,
        payload: bytes,
        max_retries: int,
        backoff_seconds: float,
        trace_headers: list[tuple[str, bytes | None]] | None = None,
    ) -> tuple[bool, Exception | None]:
        attempt = 0
        current_backoff = max(backoff_seconds, 0.05)
        last_error: Exception | None = None

        while attempt < max_retries and self.running:
            attempt += 1
            try:
                await asyncio.to_thread(
                    self._produce_and_wait,
                    topic=topic,
                    key=key,
                    payload=payload,
                    headers=[("retry-attempt", str(attempt).encode("utf-8"))]
                    + (trace_headers or []),
                )
                return True, None
            except Exception as ex:
                last_error = ex
                self.producer.poll(0)
                await asyncio.sleep(current_backoff)
                current_backoff = min(current_backoff * 2, 5.0)

        return False, last_error or RuntimeError("unknown error")

    async def publish_to_dlq(
        self, event: dict[str, Any], key: str, reason: str, error: str, attempts: int
    ) -> None:
        dlq_record = {
            "source": "bluesky_jetstream",
            "failed_at_ms": int(time.time() * 1000),
            "target_topic": self.topic,
            "reason": reason,
            "error": error,
            "attempts": attempts,
            "raw_event": event,
        }
        ok, dlq_error = await self.produce_with_retry(
            topic=self.dlq_topic,
            key=key,
            payload=json.dumps(dlq_record, ensure_ascii=False).encode("utf-8"),
            max_retries=max(1, self.args.max_produce_retries),
            backoff_seconds=max(0.1, self.args.produce_retry_backoff_seconds),
        )
        if ok:
            self.metrics.dlq_publish_success.inc()
            return

        err_type = type(dlq_error).__name__ if dlq_error else "UnknownError"
        self.metrics.dlq_publish_failed.labels(err_type).inc()
        LOGGER.error("dlq publish failed key=%s error=%s", key, dlq_error)

    async def run_forever(self) -> None:
        while self.running:
            try:
                LOGGER.info("connecting jetstream=%s", self.jetstream_url)
                async with websockets.connect(
                    self.jetstream_url,
                    ping_interval=30,
                    ping_timeout=60,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    LOGGER.info("jetstream connected")
                    self.reconnect_backoff = 1.0
                    await self.consume_stream(ws)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.metrics.reconnects.inc()
                LOGGER.warning("jetstream error=%s reconnect_in=%.1fs", ex, self.reconnect_backoff)
                await asyncio.sleep(self.reconnect_backoff)
                self.reconnect_backoff = min(
                    self.reconnect_backoff * 2,
                    max(1.0, self.args.reconnect_max_backoff_seconds),
                )

    async def consume_stream(self, ws: Any) -> None:
        async for message in ws:
            if not self.running:
                break
            self.metrics.events_received.inc()
            self.metrics.last_event_unixtime.set(time.time())

            try:
                event = json.loads(message)
            except json.JSONDecodeError:
                self.metrics.events_filtered.labels("invalid_json").inc()
                continue

            keep, reason = self.should_keep_event(event)
            if not keep:
                self.metrics.events_filtered.labels(reason).inc()
                continue

            normalized = self.build_normalized_payload(event)
            key = self.build_key(event)
            payload = json.dumps(normalized, ensure_ascii=False).encode("utf-8")
            with self.tracer.start_as_current_span("ingest.receive") as span:
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination.name", self.topic)
                span.set_attribute("bluesky.did", str(normalized.get("did") or ""))
                span.set_attribute("bluesky.uri", str(normalized.get("uri") or ""))
                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                if carrier.get("traceparent"):
                    normalized["traceparent"] = carrier["traceparent"]
                trace_headers = [(name, value.encode("utf-8")) for name, value in carrier.items()]
                with self.tracer.start_as_current_span("kafka.produce"):
                    ok, error = await self.produce_with_retry(
                        topic=self.topic,
                        key=key,
                        payload=payload,
                        max_retries=max(1, self.args.max_produce_retries),
                        backoff_seconds=max(0.1, self.args.produce_retry_backoff_seconds),
                        trace_headers=trace_headers,
                    )
            if ok:
                self.metrics.raw_publish_success.inc()
                continue

            err_type = type(error).__name__ if error else "UnknownError"
            self.metrics.raw_publish_failed.labels(err_type).inc()
            await self.publish_to_dlq(
                event=event,
                key=key,
                reason="kafka_publish_failed",
                error=str(error) if error else "unknown",
                attempts=max(1, self.args.max_produce_retries),
            )

    def stop(self) -> None:
        self.running = False
        LOGGER.info("stopping ingestor")
        self.producer.flush(10)


async def async_main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_tracing(os.getenv("OTEL_SERVICE_NAME", "bluesky-ingestor"))

    start_http_server(args.metrics_port)
    metrics = IngestMetrics()
    ingestor = BlueskyIngestor(args, metrics)
    ingestor.ensure_topics()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        ingestor.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: _handle_signal())

    LOGGER.info(
        "ingestor started topic=%s dlq_topic=%s metrics=0.0.0.0:%s/metrics",
        args.topic,
        args.dlq_topic,
        args.metrics_port,
    )
    run_task = asyncio.create_task(ingestor.run_forever())
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

    for task in pending:
        task.cancel()
    for task in done:
        if task is run_task and run_task.exception():
            raise run_task.exception()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        LOGGER.info("keyboard interrupt received")


if __name__ == "__main__":
    main()
