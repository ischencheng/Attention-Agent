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
from worker_runtime import consumer_config, producer_config, produce_confirmed, retry_record, validate_embedding, gateway_headers


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
    parser.add_argument("--embedding-provider", choices=("gemini", "http"), default="gemini")
    parser.add_argument("--embedding-model", default=os.getenv("EMBEDDING_MODEL", "gemini-embedding-001"))
    parser.add_argument("--embedding-dim", type=int, default=768)
    parser.add_argument("--embedding-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--embedding-timeout-seconds", type=float, default=180.0)
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


def extract_source_id(payload: dict[str, Any]) -> str | None:
    source_id = payload.get("source_id")
    if isinstance(source_id, str) and source_id:
        return source_id
    return None


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
    max_retries = 3
    backoff = 2.0
    for attempt in range(1, max_retries + 1):
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
        if response.status_code == 429:
            if attempt < max_retries:
                LOGGER.warning("gemini embedding 429, retry %d/%d backoff=%.1fs", attempt, max_retries, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            response.raise_for_status()
        response.raise_for_status()
        break
    payload = response.json()
    embedding = payload.get("embedding") if isinstance(payload, dict) else {}
    values = embedding.get("values") if isinstance(embedding, dict) else None
    if not isinstance(values, list):
        return None
    return validate_embedding(values, output_dim)


def extract_text(payload: dict[str, Any]) -> str:
    """Extract text from unified envelope format."""
    data = payload.get("data") or {}
    ct = payload.get("content_type", "")
    if ct == "social_post":
        return (data.get("text") or "").strip()
    if ct == "news_article":
        return f"{data.get('title', '')} {data.get('text') or data.get('snippet', '')}".strip()
    if ct == "price_tick":
        ticker = data.get("ticker", "")
        return f"{ticker} price:{data.get('price', '')} change:{data.get('change_pct', '')}%"
    return json.dumps(data, ensure_ascii=False)[:2000]


def get_http_embedding(text: str, output_dim: int, timeout_seconds: float) -> tuple[list[float], str]:
    endpoint = os.getenv("EMBEDDING_GATEWAY_URL", "http://host.docker.internal:8765/embed")
    response = requests.post(endpoint, headers=gateway_headers(), json={"text": text, "dimensions": output_dim}, timeout=max(2.0, timeout_seconds))
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or not isinstance(result.get("model"), str) or not result["model"].strip():
        raise ValueError("Embedding gateway must return its model identity")
    return validate_embedding(result.get("embedding"), output_dim), result["model"]


def process_record(msg: Any, payload: dict[str, Any], args: argparse.Namespace,
                   producer: Producer, consumer: Consumer) -> None:
    source_id = extract_source_id(payload)
    if not source_id:
        consumer.commit(message=msg, asynchronous=False)
        return
    with EMBED_LATENCY.time():
        text = extract_text(payload)
        if not text:
            consumer.commit(message=msg, asynchronous=False)
            EMBED_TOTAL.labels("empty_text").inc()
            return
        if args.embedding_provider == "http":
            embedding, model_used = get_http_embedding(text, args.embedding_dim, args.embedding_timeout_seconds)
        else:
            embedding = get_gemini_embedding(text, args.embedding_model, args.embedding_api_key,
                                              args.embedding_dim, args.embedding_timeout_seconds)
            if embedding is None:
                raise ValueError("Gemini embedding unavailable; no synthetic fallback")
            model_used = args.embedding_model
        output = dict(payload)
        output["embedding"] = embedding
        output["embedding_model"] = model_used
        produce_confirmed(producer, topic=args.output_topic, key=source_id.encode("utf-8"),
                          value=json.dumps(output, ensure_ascii=False).encode("utf-8"), headers=msg.headers())
        consumer.commit(message=msg, asynchronous=False)
        EMBED_TOTAL.labels("success").inc()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start_http_server(args.metrics_port)
    consumer = Consumer(consumer_config(args.bootstrap, args.group_id))
    producer = Producer(producer_config(args.bootstrap))
    consumer.subscribe([args.input_topic])
    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    LOGGER.info("embed-worker started input=%s output=%s provider=%s", args.input_topic, args.output_topic, args.embedding_provider)
    try:
        while running:
            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF, getattr(KafkaError, "UNKNOWN_TOPIC_OR_PART", 3)):
                    time.sleep(2)
                    continue
                raise KafkaException(msg.error())
            payload = decode_payload(msg.value())
            if payload is None:
                EMBED_TOTAL.labels("invalid_payload").inc()
                operation = lambda: consumer.commit(message=msg, asynchronous=False)
            else:
                operation = lambda: process_record(msg, payload, args, producer, consumer)
            if not retry_record(operation, lambda: running):
                break
    finally:
        producer.flush(10)
        consumer.close()
        LOGGER.info("embed-worker stopped")


if __name__ == "__main__":
    main()
