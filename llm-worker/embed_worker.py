#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from typing import Any

import requests
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from prometheus_client import Counter, Histogram, start_http_server


LOGGER = logging.getLogger("embed-worker")
EMBED_TOTAL = Counter("embed_worker_total", "Embedding records", ["status"])
EMBED_LATENCY = Histogram("embed_worker_latency_seconds", "Embedding generation latency")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate embeddings for embedding requests.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--input-topic", default="source_clean")
    parser.add_argument("--output-topic", default="vector_embedding")
    parser.add_argument("--group-id", default="embed-worker-v1")
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--metrics-port", type=int, default=8034)
    parser.add_argument("--embedding-provider", choices=("gemini", "local"), default="gemini")
    parser.add_argument("--embedding-model", default=os.getenv("EMBEDDING_MODEL", "gemini-embedding-001"))
    parser.add_argument("--embedding-dim", type=int, default=768)
    parser.add_argument("--embedding-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--embedding-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def decode_payload(raw: bytes | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def extract_cid(payload: dict[str, Any]) -> str | None:
    cid = payload.get("cid")
    if isinstance(cid, str) and cid:
        return cid
    raw = payload.get("raw")
    if isinstance(raw, dict):
        commit = raw.get("commit")
        if isinstance(commit, dict):
            commit_cid = commit.get("cid")
            if isinstance(commit_cid, str) and commit_cid:
                return commit_cid
    return None


def local_fallback_embedding(text: str, dim: int = 64) -> list[float]:
    # Deterministic tiny fallback to avoid dropping data when API is unavailable.
    vec = [0.0] * dim
    for token in text.lower().split():
        idx = hash(token) % dim
        vec[idx] += 1.0
    norm_sq = sum(v * v for v in vec)
    if norm_sq <= 0:
        return vec
    norm = norm_sq ** 0.5
    return [round(v / norm, 6) for v in vec]


def get_gemini_embedding(
    text: str,
    model: str,
    api_key: str,
    output_dim: int,
    timeout_seconds: float,
) -> list[float] | None:
    if not api_key:
        return None
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent"
    response = requests.post(
        endpoint,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        json={
            "model": f"models/{model}",
            "content": {"parts": [{"text": text[:8000]}]},
            "outputDimensionality": max(128, min(3072, int(output_dim))),
        },
        timeout=max(2.0, timeout_seconds),
    )
    response.raise_for_status()
    payload = response.json()
    embedding = payload.get("embedding") if isinstance(payload, dict) else {}
    values = embedding.get("values") if isinstance(embedding, dict) else None
    if not isinstance(values, list):
        return None
    return [float(v) for v in values]


def extract_text(payload: dict[str, Any]) -> str:
    raw = payload.get("raw")
    if isinstance(raw, dict):
        commit = raw.get("commit")
        if isinstance(commit, dict):
            record = commit.get("record")
            if isinstance(record, dict):
                text = record.get("text")
                if isinstance(text, str):
                    return text
    return ""


def commit_with_retry(consumer: Consumer, msg: Any, attempts: int = 3, backoff_seconds: float = 0.5) -> bool:
    current_backoff = max(0.1, backoff_seconds)
    for attempt in range(1, max(1, attempts) + 1):
        try:
            consumer.commit(message=msg, asynchronous=False)
            return True
        except KafkaException as exc:
            LOGGER.warning(
                "embed-worker commit failed attempt=%s topic=%s partition=%s offset=%s error=%s",
                attempt,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                exc,
            )
            time.sleep(current_backoff)
            current_backoff = min(current_backoff * 2, 5.0)
    return False


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(args.metrics_port)
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    producer = Producer({"bootstrap.servers": args.bootstrap})
    consumer.subscribe([args.input_topic])
    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    LOGGER.info("embed-worker started input=%s output=%s", args.input_topic, args.output_topic)
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
                if code == unknown_topic_code:
                    LOGGER.warning("embed-worker topic not ready yet: %s", error)
                    time.sleep(2.0)
                    continue
                raise KafkaException(error)

            payload = decode_payload(msg.value())
            if payload is None:
                EMBED_TOTAL.labels("invalid_payload").inc()
                commit_with_retry(consumer, msg)
                continue
            cid = extract_cid(payload)
            if not cid:
                EMBED_TOTAL.labels("invalid_payload").inc()
                commit_with_retry(consumer, msg)
                continue
            with EMBED_LATENCY.time():
                text = extract_text(payload)
                embedding: list[float]
                if args.embedding_provider == "gemini":
                    remote = get_gemini_embedding(
                        text=text,
                        model=args.embedding_model,
                        api_key=args.embedding_api_key,
                        output_dim=args.embedding_dim,
                        timeout_seconds=args.embedding_timeout_seconds,
                    )
                    embedding = remote if remote else local_fallback_embedding(text)
                else:
                    embedding = local_fallback_embedding(text)

                output = {
                    "cid": cid,
                    "raw": payload,
                    "embedding": embedding,
                }
                producer.produce(
                    topic=args.output_topic,
                    key=cid.encode("utf-8"),
                    value=json.dumps(output, ensure_ascii=False).encode("utf-8"),
                    headers=msg.headers(),
                )
                producer.poll(0)
                EMBED_TOTAL.labels("success").inc()
            commit_with_retry(consumer, msg)
    finally:
        producer.flush(10)
        consumer.close()
        LOGGER.info("embed-worker stopped")


if __name__ == "__main__":
    main()
