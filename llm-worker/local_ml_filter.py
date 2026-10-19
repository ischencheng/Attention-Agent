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
from confluent_kafka import Consumer, KafkaException, Producer
from prometheus_client import Counter, Histogram, start_http_server
from worker_runtime import classify_with_codex, consumer_config, producer_config, produce_confirmed, retry_record


LOGGER = logging.getLogger("local-ml-filter")
FILTERED_TOTAL = Counter("local_ml_filter_total", "Local ML filter decisions", ["decision"])
FILTER_LATENCY = Histogram("local_ml_filter_latency_seconds", "Local ML filter latency")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local relevance filter before Fast Agent.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--input-topic", default="source_clean")
    parser.add_argument("--output-topic", default="fastagent_input")
    parser.add_argument("--group-id", default="local-ml-filter-v1")
    parser.add_argument("--threshold", type=float, default=0.55)
    parser.add_argument("--metrics-port", type=int, default=8031)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--llm-provider", choices=("gemini", "local", "codex"), default="gemini")
    parser.add_argument("--llm-model", default=os.getenv("LOCAL_FILTER_MODEL", "gemini-2.5-flash-lite"))
    parser.add_argument("--llm-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--llm-timeout-seconds", type=float, default=float(os.getenv("LLM_TIMEOUT_SECONDS", "180")))
    parser.add_argument("--llm-batch-window-seconds", type=float, default=10.0, help="Legacy compatibility flag; records now process sequentially")
    parser.add_argument("--llm-batch-max-size", type=int, default=32, help="Legacy compatibility flag; records now process sequentially")
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


def derive_message_key(payload: dict[str, Any], incoming_key: bytes | str | None) -> str:
    if incoming_key is not None:
        if isinstance(incoming_key, bytes):
            return incoming_key.decode("utf-8", errors="replace")
        return str(incoming_key)
    source_id = payload.get("source_id")
    if isinstance(source_id, str) and source_id:
        return source_id
    return "unknown"


def get_relevance_score(payload: dict[str, Any]) -> float:
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    value = features.get("relevance_local_score")
    try:
        score = float(value)
    except (TypeError, ValueError):
        score = 0.0
    return max(0.0, min(1.0, score))


def build_message_text(payload: dict[str, Any]) -> str:
    """Extract text from unified envelope format."""
    data = payload.get("data") or {}
    ct = payload.get("content_type", "")
    if ct == "social_post":
        return (data.get("text") or "").strip()
    if ct == "news_article":
        return f"{data.get('title', '')} {data.get('text') or data.get('snippet', '')}".strip()
    if ct == "price_tick":
        return ""  # price ticks should be auto-forwarded, not LLM-filtered
    return json.dumps(data, ensure_ascii=False)[:2000]


def parse_yes_no(text: str) -> bool | None:
    normalized = text.strip().lower()
    if normalized == "yes":
        return True
    if normalized == "no":
        return False
    first_token = normalized.split()[0] if normalized else ""
    if first_token in {"yes", "no"}:
        return first_token == "yes"
    return None


def get_gemini_yes_no(payload: dict[str, Any], model: str, api_key: str, timeout_seconds: float) -> bool | None:
    if not api_key:
        return None
    message_text = build_message_text(payload)
    source = payload.get("source", "unknown")
    prompt = (
        "You are a pre-filter for an investment research pipeline.\n"
        "Determine if this message MIGHT contain information relevant to "
        "financial markets, investing, company performance, economic indicators, "
        "or market sentiment.\n"
        "Be INCLUSIVE - when in doubt, say YES. This is a broad net.\n"
        "Reply with only YES or NO.\n"
        f"[{source}] {message_text}"
    )
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    request_body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.0,
        },
    }
    request_headers = {"Content-Type": "application/json", "x-goog-api-key": api_key}
    effective_timeout = max(5.0, timeout_seconds)

    # Retry once on timeout
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            response = requests.post(
                endpoint,
                headers=request_headers,
                json=request_body,
                timeout=effective_timeout,
            )
            response.raise_for_status()
            break
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            if attempt == 0:
                LOGGER.info("gemini timeout on attempt 1, retrying with longer timeout...")
                effective_timeout = effective_timeout * 2
                continue
            raise
        except requests.exceptions.HTTPError as exc:
            # Retry on 503 Service Unavailable or 429 Rate Limit
            if attempt == 0 and hasattr(exc, 'response') and exc.response is not None and exc.response.status_code in (429, 503):
                LOGGER.info("gemini %d on attempt 1, retrying...", exc.response.status_code)
                time.sleep(2)
                continue
            raise
    else:
        if last_exc:
            raise last_exc

    data = response.json()
    candidates = data.get("candidates") if isinstance(data, dict) else []
    if not isinstance(candidates, list) or not candidates:
        return None
    parts = candidates[0].get("content", {}).get("parts", [])
    if not isinstance(parts, list) or not parts:
        return None
    output_text = str(parts[0].get("text") or "")
    return parse_yes_no(output_text)


def process_record(msg: Any, payload: dict[str, Any], args: argparse.Namespace,
                   producer: Producer, consumer: Consumer) -> None:
    with FILTER_LATENCY.time():
        model_used = "local_heuristic"
        classification_metadata = {}
        if payload.get("content_type") == "price_tick":
            is_yes, model_used = True, "price_tick_passthrough"
        elif args.llm_provider == "codex":
            is_yes, model_used, classification_metadata = classify_with_codex(payload, "filter", build_message_text(payload), args.llm_timeout_seconds)
        elif args.llm_provider == "gemini":
            is_yes = get_gemini_yes_no(payload, args.llm_model, args.llm_api_key, args.llm_timeout_seconds)
            model_used = args.llm_model
            if is_yes is None:
                raise ValueError("Gemini classification unavailable or invalid; no heuristic fallback")
        else:
            is_yes = get_relevance_score(payload) >= args.threshold
        if is_yes:
            output = dict(payload)
            output["local_ml_filter"] = {"verdict": True, "score": 1.0, "provider": args.llm_provider, "model": model_used, **classification_metadata}
            produce_confirmed(producer, topic=args.output_topic,
                              key=derive_message_key(payload, msg.key()).encode("utf-8"),
                              value=json.dumps(output, ensure_ascii=False).encode("utf-8"), headers=msg.headers())
        consumer.commit(message=msg, asynchronous=False)
        FILTERED_TOTAL.labels("yes" if is_yes else "no").inc()


def process_pending_batch(pending: list[tuple[Any, dict[str, Any]]], args: argparse.Namespace,
                          producer: Producer, consumer: Consumer) -> None:
    # Kept as a small callable for offline replay. Exceptions stop the batch before later offsets.
    for msg, payload in pending:
        process_record(msg, payload, args, producer, consumer)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start_http_server(args.metrics_port)
    consumer = Consumer(consumer_config(args.bootstrap, args.group_id))
    producer = Producer(producer_config(args.bootstrap))
    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    consumer.subscribe([args.input_topic])
    LOGGER.info("local-ml-filter started input=%s output=%s provider=%s", args.input_topic, args.output_topic, args.llm_provider)
    try:
        while running:
            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                if "UNKNOWN_TOPIC" in msg.error().name():
                    time.sleep(2)
                    continue
                raise KafkaException(msg.error())
            payload = decode_payload(msg.value())
            if payload is None:
                # Invalid inputs are explicit drops; a commit failure is still retried in order.
                LOGGER.warning("invalid JSON payload at %s/%s/%s", msg.topic(), msg.partition(), msg.offset())
                operation = lambda: consumer.commit(message=msg, asynchronous=False)
            else:
                operation = lambda: process_record(msg, payload, args, producer, consumer)
            if not retry_record(operation, lambda: running):
                break
    finally:
        producer.flush(10)
        consumer.close()
        LOGGER.info("local-ml-filter stopped")


if __name__ == "__main__":
    main()
