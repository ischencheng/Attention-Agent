"""Stable identities for spans projected from recorded audits, not live contexts."""
from __future__ import annotations

import hashlib
import json

TRACE_NAMESPACE = "attention-agent.recorded-audit.v1"


def trace_identity(kind: str, key: str) -> dict[str, str]:
    """Return the same export identity without claiming propagated trace context."""
    if kind not in {"routing", "cycle"}:
        raise ValueError("unsupported audit kind")
    if not isinstance(key, str) or not key or len(key) > 512 or any(ord(c) < 32 for c in key):
        raise ValueError("invalid audit key")
    encoded = json.dumps([TRACE_NAMESPACE, kind, key], ensure_ascii=False, separators=(",", ":")).encode()
    trace_id = hashlib.sha256(b"trace:" + encoded).hexdigest()[:32]
    span_id = hashlib.sha256(b"span:" + encoded).hexdigest()[:16]
    return {"trace_id": trace_id if int(trace_id, 16) else "0" * 31 + "1",
            "span_id": span_id if int(span_id, 16) else "0" * 15 + "1"}
