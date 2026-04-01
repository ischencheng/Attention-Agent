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
from prometheus_client import Counter, start_http_server


LOGGER = logging.getLogger("fast-agent")

FAST_AGENT_INPUT_TOTAL = Counter("fast_agent_input_total", "Input records for fast agent")
FAST_AGENT_OUTPUT_TOTAL = Counter("attention_ticket_out_total", "Output attention tickets produced", ["status"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast agent: fastagent_input -> attention_ticket.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--input-topic", default="fastagent_input")
    parser.add_argument("--output-topic", default="attention_ticket")
    parser.add_argument("--group-id", default="fast-agent-v1")
    parser.add_argument("--metrics-port", type=int, default=8030)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--attention-threshold", type=float, default=0.55)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--llm-provider", choices=("gemini", "local"), default="gemini")
    parser.add_argument("--llm-model", default="gemini-2.5-flash-lite")
    parser.add_argument("--llm-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--llm-timeout-seconds", type=float, default=12.0)
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


def local_score(payload: dict[str, Any]) -> float:
    local_filter = payload.get("local_ml_filter")
    if isinstance(local_filter, dict):
        return float(local_filter.get("score") or 0.0)
    return 0.0


def build_message_text(payload: dict[str, Any]) -> str:
    raw = payload.get("raw")
    if isinstance(raw, dict):
        commit = raw.get("commit")
        if isinstance(commit, dict):
            record = commit.get("record")
            if isinstance(record, dict):
                record_text = record.get("text")
                if isinstance(record_text, str) and record_text.strip():
                    return record_text.strip()
    return json.dumps(payload, ensure_ascii=False)[:2000]


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
    prompt = (
        "Please use common sense to judge whether this message is worth attention.\n"
        "Reply with only YES or NO.\n"
        f"Message:\n{message_text}"
    )
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    response = requests.post(
        endpoint,
        headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.0,
            },
        },
        timeout=max(2.0, timeout_seconds),
    )
    response.raise_for_status()
    data = response.json()
    candidates = data.get("candidates") if isinstance(data, dict) else []
    if not isinstance(candidates, list) or not candidates:
        return None
    parts = candidates[0].get("content", {}).get("parts", [])
    if not isinstance(parts, list) or not parts:
        return None
    output_text = str(parts[0].get("text") or "")
    return parse_yes_no(output_text)


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
    LOGGER.info(
        "fast-agent started input=%s output=%s llm_provider=%s llm_model=%s llm_timeout_seconds=%.1f",
        args.input_topic,
        args.output_topic,
        args.llm_provider,
        args.llm_model,
        args.llm_timeout_seconds,
    )
    unknown_topic_code = getattr(KafkaError, "UNKNOWN_TOPIC_OR_PART", 3)

    try:
        while running:
            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is None:
                continue
            if msg.error():
                error = msg.error()
                code = error.code()
                if code == unknown_topic_code:
                    LOGGER.warning("fast-agent topic not ready yet: %s", error)
                    time.sleep(2.0)
                    continue
                raise KafkaException(msg.error())
            FAST_AGENT_INPUT_TOTAL.inc()
            payload = decode_payload(msg.value())
            if payload is None:
                FAST_AGENT_OUTPUT_TOTAL.labels("invalid_payload").inc()
                consumer.commit(message=msg, asynchronous=False)
                continue

            cid = extract_cid(payload)
            if not cid:
                FAST_AGENT_OUTPUT_TOTAL.labels("invalid_payload").inc()
                consumer.commit(message=msg, asynchronous=False)
                continue

            is_yes: bool | None = None
            if args.llm_provider == "gemini":
                try:
                    is_yes = get_gemini_yes_no(
                        payload=payload,
                        model=args.llm_model,
                        api_key=args.llm_api_key,
                        timeout_seconds=args.llm_timeout_seconds,
                    )
                except Exception as exc:
                    LOGGER.warning("gemini classify failed, fallback local score: %s", exc)

            if is_yes is None:
                is_yes = local_score(payload) >= args.attention_threshold

            if not is_yes:
                FAST_AGENT_OUTPUT_TOTAL.labels("filtered").inc()
                consumer.commit(message=msg, asynchronous=False)
                continue

            output = {"cid": cid, "raw": payload}
            producer.produce(
                topic=args.output_topic,
                key=cid.encode("utf-8"),
                value=json.dumps(output, ensure_ascii=False).encode("utf-8"),
                headers=msg.headers(),
            )
            producer.poll(0)
            FAST_AGENT_OUTPUT_TOTAL.labels("success").inc()
            consumer.commit(message=msg, asynchronous=False)
    finally:
        producer.flush(10)
        consumer.close()
        LOGGER.info("fast-agent stopped")


if __name__ == "__main__":
    main()
