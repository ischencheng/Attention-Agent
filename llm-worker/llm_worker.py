#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import logging
import os
import re
import signal
import time
from typing import Any

import requests
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram, start_http_server


LOGGER = logging.getLogger("llm-worker")
TRACER = trace.get_tracer("llm.worker")

REQUESTS_TOTAL = Counter("llm_requests_total", "Total llm requests", ["status"])
ERRORS_TOTAL = Counter("llm_errors_total", "Total llm errors")
LATENCY_SECONDS = Histogram("llm_latency_seconds", "LLM request latency seconds")
COST_USD_TOTAL = Counter("llm_cost_usd_total", "Estimated llm cost in usd")
SIGNALS_CANDIDATE_TOTAL = Counter("signals_candidate_total", "Candidate signals consumed")
SIGNALS_FINAL_TOTAL = Counter("signals_final_total", "Final signals produced")
SIGNALS_DROP_TOTAL = Counter("signals_drop_total", "Signals dropped due to invalid payload")


def init_tracing(service_name: str) -> None:
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318").rstrip("/") + "/v1/traces"
    )
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    RequestsInstrumentor().instrument()
    LOGGER.info("otel tracing enabled endpoint=%s service=%s", endpoint, service_name)


def kafka_headers_to_carrier(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    carrier: dict[str, str] = {}
    for key, value in headers or []:
        if value is None:
            continue
        try:
            carrier[key] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return carrier


def build_output_headers(existing_headers: list[tuple[str, bytes | None]] | None) -> list[tuple[str, bytes | None]]:
    output_headers: list[tuple[str, bytes | None]] = list(existing_headers or [])
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    for key, value in carrier.items():
        output_headers.append((key, value.encode("utf-8")))
    return output_headers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LLM worker for strategy signal enrichment.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--input-topic", default="strategy_signal_candidate")
    parser.add_argument("--output-topic", default="strategy_signal_final")
    parser.add_argument("--group-id", default="llm-worker-v1")
    parser.add_argument("--metrics-port", type=int, default=8030)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=0.5)
    parser.add_argument("--llm-api-url", default="")
    parser.add_argument("--llm-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--llm-model", default="gemini-3-flash-preview")
    parser.add_argument("--llm-provider", choices=("gemini", "generic"), default="gemini")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--batch-max-wait-seconds", type=float, default=600)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def ensure_output_topic(args: argparse.Namespace) -> None:
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    metadata = admin.list_topics(timeout=10)
    if args.output_topic in metadata.topics:
        return
    futures = admin.create_topics([NewTopic(args.output_topic, num_partitions=6, replication_factor=3)], request_timeout=20)
    for topic_name, future in futures.items():
        try:
            future.result()
            LOGGER.info("created output topic=%s", topic_name)
        except Exception as exc:
            # Topic may have been created by another service in parallel.
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                LOGGER.info("output topic already exists topic=%s", topic_name)
                continue
            raise


def decode_message_value(raw: bytes | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def build_prompt(payload: dict[str, Any]) -> str:
    text = str(payload.get("text") or "")
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    tickers = ",".join(features.get("tickers", [])) if isinstance(features.get("tickers"), list) else ""
    reasons = ",".join(candidate.get("reasons", [])) if isinstance(candidate.get("reasons"), list) else ""
    return (
        "You are a financial strategy research assistant.\n"
        "Given this social post and lightweight features, produce:\n"
        "1) short summary\n2) risk tags list\n3) recommendation text\n4) confidence [0,1].\n"
        "Return strict JSON only with keys: summary, risk_tags, recommendation, confidence.\n\n"
        f"Text: {text}\nTickers: {tickers}\nReasons: {reasons}\n"
    )

def heuristic_insight(payload: dict[str, Any], model_name: str) -> dict[str, Any]:
    features = payload.get("features") if isinstance(payload.get("features"), dict) else {}
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    text = str(payload.get("text") or "")
    tickers = features.get("tickers", []) if isinstance(features.get("tickers"), list) else []

    risk_tags: list[str] = []
    if int(features.get("sentiment_score") or 0) < 0:
        risk_tags.append("negative_sentiment")
    if len(text) < 25:
        risk_tags.append("low_context")
    if int(candidate.get("score") or 0) >= 6:
        risk_tags.append("high_volatility_signal")
    if not tickers:
        risk_tags.append("no_ticker")

    recommendation = "watchlist_only"
    if tickers and "negative_sentiment" not in risk_tags and int(candidate.get("score") or 0) >= 4:
        recommendation = "needs_human_review_for_entry"

    confidence = max(0.25, min(0.9, 0.35 + (int(candidate.get("score") or 0) * 0.07)))
    summary = text[:140] + ("..." if len(text) > 140 else "")
    return {
        "model_name": model_name,
        "summary": summary or "No text content",
        "risk_tags": risk_tags,
        "recommendation": recommendation,
        "confidence": round(confidence, 2),
    }


def extract_json_value_from_text(text: str) -> Any:
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        if "\n" in candidate:
            candidate = candidate.split("\n", 1)[1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\[.*\]", candidate, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    match = re.search(r"\{.*\}", candidate, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def extract_json_from_text(text: str) -> dict[str, Any] | None:
    data = extract_json_value_from_text(text)
    return data if isinstance(data, dict) else None


def normalize_insight(raw: dict[str, Any], model_name: str) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "summary": str(raw.get("summary") or ""),
        "risk_tags": raw.get("risk_tags") if isinstance(raw.get("risk_tags"), list) else [],
        "recommendation": str(raw.get("recommendation") or ""),
        "confidence": float(raw.get("confidence") or 0.5),
    }


def call_cloud_llm(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not args.llm_api_key:
        return None
    if args.llm_provider == "gemini":
        return call_gemini_llm(args, payload)
    return call_generic_llm(args, payload)


def call_cloud_llm_batch(args: argparse.Namespace, payloads: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    if not payloads:
        return []
    if len(payloads) == 1:
        return [call_cloud_llm(args, payloads[0])]
    if not args.llm_api_key:
        return [None] * len(payloads)
    if args.llm_provider == "gemini":
        return call_gemini_llm_batch(args, payloads)
    return call_generic_llm_batch(args, payloads)


def build_batch_prompt(payloads: list[dict[str, Any]]) -> str:
    lines = [
        "You are a financial strategy research assistant.",
        "For each item below, output one object with keys: summary, risk_tags, recommendation, confidence.",
        "Return strict JSON array only.",
        f"The array length must be exactly {len(payloads)}.",
        "Output order must match input order.",
        "",
    ]
    for idx, payload in enumerate(payloads):
        lines.append(f"ITEM {idx}")
        lines.append(build_prompt(payload))
        lines.append("")
    return "\n".join(lines)


def call_gemini_llm(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any] | None:
    base_url = args.llm_api_url or "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    api_url = base_url.format(model=args.llm_model)
    prompt = build_prompt(payload)
    started = time.monotonic()
    try:
        response = requests.post(
            api_url,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": args.llm_api_key,
            },
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2},
            },
            timeout=15,
        )
        response.raise_for_status()
        body = response.json()
        content = (
            body.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        parsed = extract_json_from_text(content) if isinstance(content, str) else None
        if not isinstance(parsed, dict):
            raise ValueError("gemini response does not contain JSON object")
        REQUESTS_TOTAL.labels("success").inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        COST_USD_TOTAL.inc(0.001)
        return normalize_insight(parsed, args.llm_model)
    except Exception as exc:
        REQUESTS_TOTAL.labels("error").inc()
        ERRORS_TOTAL.inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        LOGGER.warning("gemini call failed, fallback enabled: %s", exc)
        return None


def call_gemini_llm_batch(args: argparse.Namespace, payloads: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    base_url = args.llm_api_url or "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    api_url = base_url.format(model=args.llm_model)
    prompt = build_batch_prompt(payloads)
    started = time.monotonic()
    try:
        response = requests.post(
            api_url,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": args.llm_api_key,
            },
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2},
            },
            timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        content = (
            body.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        parsed = extract_json_value_from_text(content) if isinstance(content, str) else None
        if not isinstance(parsed, list) or len(parsed) != len(payloads):
            raise ValueError("gemini batch response does not match expected array size")

        output: list[dict[str, Any] | None] = []
        for item in parsed:
            output.append(normalize_insight(item, args.llm_model) if isinstance(item, dict) else None)
        REQUESTS_TOTAL.labels("success").inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        COST_USD_TOTAL.inc(0.001 * max(1, len(payloads)))
        return output
    except Exception as exc:
        REQUESTS_TOTAL.labels("error").inc()
        ERRORS_TOTAL.inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        LOGGER.warning("gemini batch call failed, fallback enabled: %s", exc)
        return [None] * len(payloads)


def call_generic_llm(args: argparse.Namespace, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not args.llm_api_url:
        return None
    prompt = build_prompt(payload)
    started = time.monotonic()
    try:
        response = requests.post(
            args.llm_api_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {args.llm_api_key}" if args.llm_api_key else "",
            },
            json={
                "model": args.llm_model,
                "messages": [
                    {"role": "system", "content": "Return compact JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
            },
            timeout=15,
        )
        response.raise_for_status()
        body = response.json()
        content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = json.loads(content) if isinstance(content, str) else None
        if not isinstance(parsed, dict):
            raise ValueError("llm response is not JSON object")
        REQUESTS_TOTAL.labels("success").inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        COST_USD_TOTAL.inc(0.001)
        return normalize_insight(parsed, args.llm_model)
    except Exception as exc:
        REQUESTS_TOTAL.labels("error").inc()
        ERRORS_TOTAL.inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        LOGGER.warning("cloud llm call failed, fallback enabled: %s", exc)
        return None


def call_generic_llm_batch(args: argparse.Namespace, payloads: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    if not args.llm_api_url:
        return [None] * len(payloads)
    prompt = build_batch_prompt(payloads)
    started = time.monotonic()
    try:
        response = requests.post(
            args.llm_api_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {args.llm_api_key}" if args.llm_api_key else "",
            },
            json={
                "model": args.llm_model,
                "messages": [
                    {"role": "system", "content": "Return compact JSON array only."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
            },
            timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        content = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = extract_json_value_from_text(content) if isinstance(content, str) else None
        if not isinstance(parsed, list) or len(parsed) != len(payloads):
            raise ValueError("generic batch response does not match expected array size")

        output: list[dict[str, Any] | None] = []
        for item in parsed:
            output.append(normalize_insight(item, args.llm_model) if isinstance(item, dict) else None)
        REQUESTS_TOTAL.labels("success").inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        COST_USD_TOTAL.inc(0.001 * max(1, len(payloads)))
        return output
    except Exception as exc:
        REQUESTS_TOTAL.labels("error").inc()
        ERRORS_TOTAL.inc()
        LATENCY_SECONDS.observe(time.monotonic() - started)
        LOGGER.warning("generic batch call failed, fallback enabled: %s", exc)
        return [None] * len(payloads)


@dataclass
class PendingRecord:
    msg: Any
    payload: dict[str, Any]
    parent_context: Any
    content_hash: str


def produce_with_retry(
    producer: Producer,
    topic: str,
    key: str | None,
    value: dict[str, Any],
    max_retries: int,
    backoff_seconds: float,
    headers: list[tuple[str, bytes | None]] | None = None,
) -> None:
    serialized = json.dumps(value, ensure_ascii=False).encode("utf-8")
    retries = max(1, max_retries)
    for attempt in range(1, retries + 1):
        try:
            producer.produce(topic=topic, key=key, value=serialized, headers=headers)
            producer.poll(0)
            producer.flush(10)
            return
        except BufferError as exc:
            LOGGER.warning("producer buffer error attempt=%s err=%s", attempt, exc)
            time.sleep(backoff_seconds * attempt)
    raise RuntimeError("failed to produce final signal after retries")


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_tracing(os.getenv("OTEL_SERVICE_NAME", "llm-worker"))
    start_http_server(args.metrics_port)
    ensure_output_topic(args)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
        }
    )
    consumer.subscribe([args.input_topic])

    cache: dict[str, dict[str, Any]] = {}
    pending_records: list[PendingRecord] = []
    batch_size = max(1, int(args.batch_size))
    batch_max_wait = max(0.1, float(args.batch_max_wait_seconds))
    first_pending_at = 0.0
    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    def process_batch(records: list[PendingRecord]) -> None:
        pre_cached_hashes = {record.content_hash for record in records if record.content_hash in cache}
        unique_payloads: list[dict[str, Any]] = []
        unique_hashes: list[str] = []
        for record in records:
            if record.content_hash in cache or record.content_hash in unique_hashes:
                continue
            unique_hashes.append(record.content_hash)
            unique_payloads.append(record.payload)

        if unique_payloads:
            with TRACER.start_as_current_span("llm.invoke.batch"):
                batch_insights = call_cloud_llm_batch(args, unique_payloads)
            for index, content_hash in enumerate(unique_hashes):
                llm_result = batch_insights[index] if index < len(batch_insights) else None
                insight = llm_result if isinstance(llm_result, dict) else heuristic_insight(
                    unique_payloads[index], args.llm_model
                )
                cache[content_hash] = insight

        batch_seen_hashes: set[str] = set()
        for record in records:
            with TRACER.start_as_current_span("llm.enrich", context=record.parent_context) as span:
                SIGNALS_CANDIDATE_TOTAL.inc()
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination.name", args.output_topic)
                span.set_attribute("candidate.uri", str(record.payload.get("uri") or ""))

                insight = cache.get(record.content_hash)
                if insight is None:
                    insight = heuristic_insight(record.payload, args.llm_model)
                    cache[record.content_hash] = insight

                enriched_payload = dict(record.payload)
                enriched_payload["pipeline_version"] = "m4-v1"
                enriched_payload["llm_insight"] = insight
                enriched_payload["llm_enriched_at_ms"] = int(time.time() * 1000)
                enriched_payload["llm_cache_hit"] = (
                    record.content_hash in pre_cached_hashes or record.content_hash in batch_seen_hashes
                )

                key = record.msg.key().decode("utf-8") if record.msg.key() else record.payload.get("author_did")
                with TRACER.start_as_current_span("kafka.produce"):
                    produce_with_retry(
                        producer=producer,
                        topic=args.output_topic,
                        key=str(key) if key is not None else None,
                        value=enriched_payload,
                        max_retries=args.max_retries,
                        backoff_seconds=max(0.2, args.retry_backoff_seconds),
                        headers=build_output_headers(record.msg.headers()),
                    )
                SIGNALS_FINAL_TOTAL.inc()
                consumer.commit(message=record.msg, asynchronous=False)
                batch_seen_hashes.add(record.content_hash)

    LOGGER.info(
        "llm worker started input=%s output=%s batch_size=%s batch_max_wait=%ss",
        args.input_topic,
        args.output_topic,
        batch_size,
        batch_max_wait,
    )
    unknown_topic_code = 3

    try:
        while running:
            poll_timeout = args.poll_timeout_seconds
            if pending_records:
                elapsed = time.monotonic() - first_pending_at
                remaining = max(0.0, batch_max_wait - elapsed)
                poll_timeout = min(poll_timeout, remaining)

            msg = consumer.poll(poll_timeout)
            if msg is None:
                if pending_records and (time.monotonic() - first_pending_at >= batch_max_wait):
                    process_batch(pending_records)
                    pending_records = []
                    first_pending_at = 0.0
                continue
            if msg.error():
                error = msg.error()
                code = error.code()
                # Candidate topic may not exist until Flink starts creating it.
                if code == unknown_topic_code:
                    LOGGER.warning("input topic not ready yet: %s", error)
                    time.sleep(2.0)
                    continue
                raise KafkaException(error)

            payload = decode_message_value(msg.value())
            if payload is None:
                SIGNALS_DROP_TOTAL.inc()
                consumer.commit(message=msg, asynchronous=False)
                continue
            incoming_carrier = kafka_headers_to_carrier(msg.headers())
            payload_traceparent = str(payload.get("traceparent") or "").strip()
            if payload_traceparent and "traceparent" not in incoming_carrier:
                incoming_carrier["traceparent"] = payload_traceparent
            parent_context = propagate.extract(incoming_carrier)
            text = str(payload.get("text") or "")
            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            pending_records.append(PendingRecord(msg=msg, payload=payload, parent_context=parent_context, content_hash=content_hash))
            if len(pending_records) == 1:
                first_pending_at = time.monotonic()

            if len(pending_records) >= batch_size or (time.monotonic() - first_pending_at) >= batch_max_wait:
                process_batch(pending_records)
                pending_records = []
                first_pending_at = 0.0
    finally:
        if pending_records:
            process_batch(pending_records)
        consumer.close()
        producer.flush(10)
        LOGGER.info("llm worker stopped")


if __name__ == "__main__":
    main()
