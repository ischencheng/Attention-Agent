"""Shared worker delivery rules: retries are ordered and Kafka output is acknowledged."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import re
import time
from typing import Any, Callable

import requests
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

LOGGER = logging.getLogger("worker-runtime")


def gateway_headers() -> dict[str, str]:
    token = os.getenv("ATTENTION_GATEWAY_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def classify_with_codex(payload: dict[str, Any], role: str, text: str, timeout: float) -> tuple[bool, str, dict[str, Any]]:
    if len(text) > 128000:
        return True, "routing-policy", {"classified_by_model": False, "full_review_required": True,
                                       "classification_mode": "oversize_full_review", "reason": None,
                                       "reason_status": "not_recorded"}
    endpoint = os.getenv("CODEX_GATEWAY_URL", "http://host.docker.internal:8765/classify")
    response = requests.post(
        endpoint,
        headers=gateway_headers(),
        json={"role": role, "message": text, "source": payload.get("source", "unknown"),
              "source_id": payload.get("source_id"), "content_type": payload.get("content_type", "unknown")},
        timeout=max(2.0, timeout),
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or type(result.get("verdict")) is not bool:
        raise ValueError("Codex gateway must return a boolean verdict")
    model = result.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ValueError("Codex gateway must identify its model")
    metadata = {"classified_by_model": result.get("classified_by_model", True),
                "full_review_required": result.get("full_review_required", False),
                "classification_mode": result.get("classification_mode", "model")}
    if type(metadata["classified_by_model"]) is not bool or type(metadata["full_review_required"]) is not bool:
        raise ValueError("Invalid classification coverage metadata")
    for key in ("evaluated_chunks", "total_chunks"):
        if key in result:
            metadata[key] = result[key]
    reason = result.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise ValueError("Codex gateway reason must be text or null")
    metadata["reason"] = reason if reason and reason.strip() else None
    metadata["reason_status"] = "recorded" if metadata["reason"] is not None else "not_recorded"
    for key in ("cached", "cache_key", "usage"):
        if key in result:
            metadata[key] = result[key]
    return result["verdict"], model, metadata


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def digest_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def trace_context(msg: Any) -> dict[str, str] | None:
    """Retain a valid propagated W3C header; this does not claim a worker span."""
    values = []
    for name, value in msg.headers() or []:
        if name.lower() != "traceparent":
            continue
        try:
            value = value.decode("ascii") if isinstance(value, bytes) else value
        except UnicodeDecodeError:
            return None
        if not isinstance(value, str) or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", value):
            return None
        values.append(value)
    if len(values) != 1:
        return None
    _, trace_id, parent_id, _ = values[0].split("-")
    if int(trace_id, 16) == 0 or int(parent_id, 16) == 0:
        return None
    return {"traceparent": values[0], "trace_id": trace_id, "parent_span_id": parent_id,
            "status": "propagated_header_only"}


def routing_event(msg: Any, payload: dict[str, Any] | None, *, stage: str, verdict: bool,
                  classifier: dict[str, Any], started_at: str, completed_at: str,
                  output_topic: str | None = None, output: dict[str, Any] | None = None) -> dict[str, Any]:
    """Describe the observed routing attempt without manufacturing model reasons."""
    coordinates = {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}
    if (stage not in {"filter", "fast"} or type(verdict) is not bool
            or not isinstance(coordinates["topic"], str)
            or any(type(coordinates[key]) is not int or coordinates[key] < 0 for key in ("partition", "offset"))):
        raise ValueError("Routing events require a valid stage, verdict and Kafka input coordinates")
    envelope = payload or {}
    raw = envelope.get("raw")
    reason = classifier.get("reason")
    reason = reason if isinstance(reason, str) and reason.strip() else None
    return {"schema_version": 1, "event_id": digest_json({"stage": stage, "input": coordinates}),
            "stage": stage, "outcome": "pass" if verdict else "reject", "verdict": verdict,
            "input": coordinates, "output_topic": output_topic if verdict else None,
            "output": output if verdict else None,
            "output_acknowledged": bool(verdict and output_topic),
            "source": envelope.get("source"), "source_id": envelope.get("source_id"),
            "content_type": envelope.get("content_type"),
            "raw_sha256": digest_json(raw) if raw is not None else None,
            "original_hash_status": "recorded" if raw is not None else "not_recorded",
            "input_payload_sha256": digest_json(payload) if payload is not None else hashlib.sha256(msg.value() or b"").hexdigest(),
            "input_hash_kind": "canonical_json" if payload is not None else "kafka_value_bytes",
            "reason": reason, "reason_status": "recorded" if reason else "not_recorded",
            "trace": trace_context(msg),
            "classifier": dict(classifier), "started_at": started_at, "completed_at": completed_at,
            "recorded_at": utcnow()}


def publish_routing_event(producer: Any, event: dict[str, Any], topic: str) -> None:
    produce_confirmed(producer, topic=topic, key=event["event_id"].encode("utf-8"),
                      value=json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8"))


def reject_invalid_record(msg: Any, producer: Any, consumer: Any, *, stage: str, topic: str) -> None:
    at = utcnow()
    event = routing_event(msg, None, stage=stage, verdict=False,
                          classifier={"provider": "routing-policy", "model": None,
                                      "classified_by_model": False, "classification_mode": "invalid_json",
                                      "reason": None, "reason_status": "not_recorded"},
                          started_at=at, completed_at=at)
    publish_routing_event(producer, event, topic)
    consumer.commit(message=msg, asynchronous=False)


def ensure_routing_topic(bootstrap: str, topic: str) -> None:
    """Create the audit topic without changing existing records or offsets."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, num_partitions=6, replication_factor=3)])
    try:
        futures[topic].result(timeout=30)
    except Exception as exc:
        error = exc.args[0] if exc.args else None
        if not hasattr(error, "code") or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def validate_embedding(values: Any, dimensions: int) -> list[float]:
    if not isinstance(values, list) or len(values) != dimensions:
        raise ValueError(f"Expected an embedding with {dimensions} dimensions")
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
        raise ValueError("Embedding values must be finite numbers")
    if not any(values):
        raise ValueError("A zero vector is not a semantic embedding")
    return [float(value) for value in values]


def produce_confirmed(producer: Any, *, timeout_seconds: float = 60.0, **record: Any) -> dict[str, Any] | None:
    """Return only after the broker acknowledges this output; at-least-once delivery."""
    delivered: list[Any] = []
    acknowledgments: list[Any] = []

    def on_delivery(error: Any, _message: Any) -> None:
        delivered.append(error)
        acknowledgments.append(_message)

    producer.produce(**record, on_delivery=on_delivery)
    remaining = producer.flush(timeout_seconds)
    if remaining or not delivered:
        raise TimeoutError("Kafka output was not acknowledged before the delivery deadline")
    if delivered[0] is not None:
        raise RuntimeError(f"Kafka output delivery failed: {delivered[0]}")
    message = acknowledgments[0]
    if message is not None:
        return {"topic": message.topic(), "partition": message.partition(), "offset": message.offset()}
    return None


def retry_record(operation: Callable[[], None], should_continue: Callable[[], bool], *,
                 retry_seconds: float = 5.0, sleep: Callable[[float], None] = time.sleep,
                 max_retry_seconds: float = 900.0) -> bool:
    """Do not poll/commit a later input while the current record has failed."""
    deadline = time.monotonic() + max_retry_seconds
    while should_continue():
        try:
            operation()
            return True
        except Exception as exc:
            # Exception strings from HTTP libraries can include credential-bearing URLs.
            LOGGER.warning("record processing failed (%s); input remains uncommitted, retrying", type(exc).__name__)
            if time.monotonic() >= deadline:
                # Exit before max.poll.interval so the service restart can rejoin its group.
                raise RuntimeError("Retry deadline reached; restart worker without committing the input") from exc
            sleep(retry_seconds)
    return False


def consumer_config(bootstrap: str, group_id: str) -> dict[str, Any]:
    return {"bootstrap.servers": bootstrap, "group.id": group_id,
            "auto.offset.reset": "earliest", "enable.auto.commit": False,
            "enable.auto.offset.store": False, "allow.auto.create.topics": False,
            "max.poll.interval.ms": 1800000}


def producer_config(bootstrap: str) -> dict[str, Any]:
    return {"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"}
