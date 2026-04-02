#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import threading
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, start_http_server


LOGGER = logging.getLogger("bluesky-ingestor")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bluesky Jetstream to Kafka raw passthrough.")
    parser.add_argument(
        "--bootstrap",
        default="kafka1:19092,kafka2:19092,kafka3:19092",
        help="Kafka bootstrap servers.",
    )
    parser.add_argument("--topic", default="source_raw", help="Kafka raw topic.")
    parser.add_argument(
        "--jetstream-url",
        default="wss://jetstream2.us-east.bsky.network/subscribe",
        help="Jetstream WebSocket URL.",
    )
    parser.add_argument("--metrics-port", type=int, default=8020, help="Prometheus metrics port.")
    parser.add_argument(
        "--max-produce-retries",
        type=int,
        default=5,
        help="Retry attempts for Kafka produce.",
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
        help="Partition count used when auto-creating topics.",
    )
    parser.add_argument(
        "--topic-replication-factor",
        type=int,
        default=3,
        help="Replication factor used when auto-creating topics.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def build_jetstream_url(base_url: str) -> str:
    split = urlsplit(base_url)
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    query.setdefault("compress", "false")
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


class IngestMetrics:
    def __init__(self) -> None:
        self.events_received = Counter(
            "bluesky_ingestor_events_received_total",
            "Total events received from Jetstream.",
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
        self.jetstream_url = build_jetstream_url(args.jetstream_url)
        self.running = True
        self.reconnect_backoff = 1.0
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
        if self.topic in existing:
            return

        created = admin.create_topics(
            [
                NewTopic(
                    self.topic,
                    num_partitions=max(1, self.args.topic_partitions),
                    replication_factor=max(1, self.args.topic_replication_factor),
                )
            ],
            request_timeout=20,
        )
        future = created[self.topic]
        try:
            future.result()
            LOGGER.info("created topic=%s", self.topic)
        except Exception as ex:
            if "TOPIC_ALREADY_EXISTS" in str(ex):
                LOGGER.info("topic already exists topic=%s", self.topic)
                return
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

    def _produce_and_wait(self, topic: str, payload: bytes) -> None:
        done = threading.Event()
        result: dict[str, Exception | None] = {"error": None}

        def on_delivery(err: Any, _msg: Any) -> None:
            result["error"] = err
            done.set()

        self.producer.produce(topic=topic, value=payload, on_delivery=on_delivery)

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
        payload: bytes,
        max_retries: int,
        backoff_seconds: float,
    ) -> tuple[bool, Exception | None]:
        attempt = 0
        current_backoff = max(backoff_seconds, 0.05)
        last_error: Exception | None = None

        while attempt < max_retries and self.running:
            attempt += 1
            try:
                await asyncio.to_thread(self._produce_and_wait, topic=topic, payload=payload)
                return True, None
            except Exception as ex:
                last_error = ex
                self.producer.poll(0)
                await asyncio.sleep(current_backoff)
                current_backoff = min(current_backoff * 2, 5.0)

        return False, last_error or RuntimeError("unknown error")

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

    @staticmethod
    def transform_to_unified(raw_bytes: bytes) -> bytes | None:
        """Transform raw Bluesky Jetstream event into unified envelope format."""
        try:
            event = json.loads(raw_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if not isinstance(event, dict):
            return None

        commit = event.get("commit")
        if not isinstance(commit, dict):
            return None
        if commit.get("collection") != "app.bsky.feed.post":
            return None
        if commit.get("operation") != "create":
            return None

        record = commit.get("record", {})
        text = record.get("text", "")
        if not text:
            return None

        cid = commit.get("cid", "")
        langs = record.get("langs", [])

        envelope = {
            "source": "bluesky",
            "source_id": cid,
            "content_type": "social_post",
            "timestamp": record.get("createdAt", ""),
            "data": {
                "text": text,
                "author": event.get("did", ""),
                "lang": langs[0] if langs else "",
            },
            "raw": event,
        }
        return json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    async def consume_stream(self, ws: Any) -> None:
        async for message in ws:
            if not self.running:
                break

            self.metrics.events_received.inc()
            self.metrics.last_event_unixtime.set(time.time())

            raw_bytes = message if isinstance(message, bytes) else str(message).encode("utf-8")
            unified = self.transform_to_unified(raw_bytes)
            if unified is None:
                continue  # skip non-post events

            ok, error = await self.produce_with_retry(
                topic=self.topic,
                payload=unified,
                max_retries=max(1, self.args.max_produce_retries),
                backoff_seconds=max(0.1, self.args.produce_retry_backoff_seconds),
            )

            if ok:
                self.metrics.raw_publish_success.inc()
                continue

            err_type = type(error).__name__ if error else "UnknownError"
            self.metrics.raw_publish_failed.labels(err_type).inc()
            LOGGER.error("raw publish failed error=%s", str(error) if error else "unknown")

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
        "ingestor started topic=%s metrics=0.0.0.0:%s/metrics",
        args.topic,
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
