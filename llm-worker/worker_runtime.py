"""Shared worker delivery rules: retries are ordered and Kafka output is acknowledged."""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Callable

import requests

LOGGER = logging.getLogger("worker-runtime")


def gateway_headers() -> dict[str, str]:
    token = os.getenv("ATTENTION_GATEWAY_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}


def classify_with_codex(payload: dict[str, Any], role: str, text: str, timeout: float) -> tuple[bool, str]:
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
    return result["verdict"], model


def validate_embedding(values: Any, dimensions: int) -> list[float]:
    if not isinstance(values, list) or len(values) != dimensions:
        raise ValueError(f"Expected an embedding with {dimensions} dimensions")
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
        raise ValueError("Embedding values must be finite numbers")
    if not any(values):
        raise ValueError("A zero vector is not a semantic embedding")
    return [float(value) for value in values]


def produce_confirmed(producer: Any, *, timeout_seconds: float = 60.0, **record: Any) -> None:
    """Return only after the broker acknowledges this output; at-least-once delivery."""
    delivered: list[Any] = []

    def on_delivery(error: Any, _message: Any) -> None:
        delivered.append(error)

    producer.produce(**record, on_delivery=on_delivery)
    remaining = producer.flush(timeout_seconds)
    if remaining or not delivered:
        raise TimeoutError("Kafka output was not acknowledged before the delivery deadline")
    if delivered[0] is not None:
        raise RuntimeError(f"Kafka output delivery failed: {delivered[0]}")


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
