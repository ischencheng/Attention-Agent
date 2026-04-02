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


LOGGER = logging.getLogger("local-ml-filter")
FILTERED_TOTAL = Counter("local_ml_filter_total", "Local ML filter decisions", ["decision"])
FILTER_LATENCY = Histogram("local_ml_filter_latency_seconds", "Local ML filter latency")
DEBUG_LOG_PATH = "debug-c6c356.log"
DEBUG_SESSION_ID = "c6c356"


def debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": DEBUG_SESSION_ID,
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local relevance filter before Fast Agent.")
    parser.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    parser.add_argument("--input-topic", default="source_clean")
    parser.add_argument("--output-topic", default="fastagent_input")
    parser.add_argument("--group-id", default="local-ml-filter-v1")
    parser.add_argument("--threshold", type=float, default=0.55)
    parser.add_argument("--metrics-port", type=int, default=8031)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--llm-provider", choices=("gemini", "local"), default="gemini")
    parser.add_argument("--llm-model", default=os.getenv("LOCAL_FILTER_MODEL", "gemini-2.5-flash-lite"))
    parser.add_argument("--llm-api-key", default=os.getenv("GEMINI_API_KEY", ""))
    parser.add_argument("--llm-timeout-seconds", type=float, default=12.0)
    parser.add_argument("--llm-batch-window-seconds", type=float, default=10.0)
    parser.add_argument("--llm-batch-max-size", type=int, default=32)
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
        return f"{data.get('title', '')} {data.get('snippet', '')}".strip()
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
    # region agent log
    debug_log(
        run_id="pre-fix",
        hypothesis_id="H1",
        location="local_ml_filter.py:get_gemini_yes_no",
        message="gemini_input",
        data={"model": model, "message_preview": message_text[:500]},
    )
    # endregion
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
    # region agent log
    debug_log(
        run_id="pre-fix",
        hypothesis_id="H2",
        location="local_ml_filter.py:get_gemini_yes_no",
        message="gemini_output",
        data={"model": model, "output": output_text[:500]},
    )
    # endregion
    return parse_yes_no(output_text)


def process_pending_batch(
    pending: list[tuple[Any, dict[str, Any]]],
    args: argparse.Namespace,
    producer: Producer,
    consumer: Consumer,
) -> None:
    if not pending:
        return

    for msg, payload in pending:
        with FILTER_LATENCY.time():
            is_yes: bool | None = None
            model_used = "local_heuristic"
            if args.llm_provider == "gemini":
                try:
                    is_yes = get_gemini_yes_no(
                        payload=payload,
                        model=args.llm_model,
                        api_key=args.llm_api_key,
                        timeout_seconds=args.llm_timeout_seconds,
                    )
                    if is_yes is not None:
                        model_used = args.llm_model
                except Exception as exc:
                    LOGGER.warning("gemini classify failed, fallback local score: %s", exc)
            if is_yes is None:
                is_yes = get_relevance_score(payload) >= args.threshold

            # region agent log
            debug_log(
                run_id="pre-fix",
                hypothesis_id="H3",
                location="local_ml_filter.py:process_pending_batch",
                message="decision_eval",
                data={
                    "topic": msg.topic(),
                    "partition": int(msg.partition()),
                    "offset": int(msg.offset()),
                    "decision": "yes" if is_yes else "no",
                    "model_used": model_used,
                },
            )
            # endregion

            if is_yes:
                message_key = derive_message_key(payload, msg.key())
                producer.produce(
                    topic=args.output_topic,
                    key=message_key.encode("utf-8"),
                    value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    headers=msg.headers(),
                )
                producer.poll(0)
                FILTERED_TOTAL.labels("yes").inc()
            else:
                FILTERED_TOTAL.labels("no").inc()
        consumer.commit(message=msg, asynchronous=False)


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
    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Wait for input topic to exist (handles startup race with Flink)
    MAX_TOPIC_WAIT = 120  # seconds
    topic_ready = False
    wait_start = time.monotonic()
    while running and not topic_ready:
        try:
            consumer.subscribe([args.input_topic])
            # Do a short poll to check for UNKNOWN_TOPIC errors
            test_msg = consumer.poll(2.0)
            if test_msg is not None and test_msg.error():
                err = test_msg.error()
                if err.code() in (KafkaException(err).args[0].code() if False else -1,):
                    pass  # won't reach
                code_name = err.name()
                if "UNKNOWN_TOPIC" in code_name:
                    elapsed = time.monotonic() - wait_start
                    if elapsed > MAX_TOPIC_WAIT:
                        LOGGER.error("topic %s not available after %ds, giving up", args.input_topic, MAX_TOPIC_WAIT)
                        raise KafkaException(err)
                    LOGGER.info("waiting for topic %s ... (%.0fs)", args.input_topic, elapsed)
                    consumer.unsubscribe()
                    time.sleep(5)
                    continue
                else:
                    raise KafkaException(err)
            topic_ready = True
        except KafkaException as exc:
            err_str = str(exc)
            if "UNKNOWN_TOPIC" in err_str:
                elapsed = time.monotonic() - wait_start
                if elapsed > MAX_TOPIC_WAIT:
                    raise
                LOGGER.info("waiting for topic %s ... (%.0fs)", args.input_topic, elapsed)
                try:
                    consumer.unsubscribe()
                except Exception:
                    pass
                time.sleep(5)
            else:
                raise

    LOGGER.info(
        "local-ml-filter started input=%s output=%s threshold=%.2f llm_window_s=%.1f llm_batch_max=%s",
        args.input_topic,
        args.output_topic,
        args.threshold,
        args.llm_batch_window_seconds,
        args.llm_batch_max_size,
    )
    pending: list[tuple[Any, dict[str, Any]]] = []
    next_flush = time.monotonic() + max(0.5, args.llm_batch_window_seconds)

    try:
        while running:
            msg = consumer.poll(args.poll_timeout_seconds)
            if msg is not None:
                if msg.error():
                    err = msg.error()
                    if "UNKNOWN_TOPIC" in err.name():
                        LOGGER.warning("topic temporarily unavailable, retrying...")
                        time.sleep(2)
                        continue
                    raise KafkaException(err)

                payload = decode_payload(msg.value())
                if payload is None:
                    # region agent log
                    debug_log(
                        run_id="pre-fix",
                        hypothesis_id="H2",
                        location="local_ml_filter.py:main",
                        message="invalid_payload",
                        data={"topic": msg.topic(), "partition": int(msg.partition()), "offset": int(msg.offset())},
                    )
                    # endregion
                    FILTERED_TOTAL.labels("invalid_payload").inc()
                    consumer.commit(message=msg, asynchronous=False)
                else:
                    pending.append((msg, payload))

            now = time.monotonic()
            batch_max = max(1, int(args.llm_batch_max_size))
            should_flush = bool(pending) and (now >= next_flush or len(pending) >= batch_max)
            if should_flush:
                process_pending_batch(pending, args, producer, consumer)
                pending.clear()
                next_flush = now + max(0.5, args.llm_batch_window_seconds)
    finally:
        if pending:
            process_pending_batch(pending, args, producer, consumer)
        producer.flush(10)
        consumer.close()
        LOGGER.info("local-ml-filter stopped")


if __name__ == "__main__":
    main()
