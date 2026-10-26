"""Validate operator-owned, offline, point-in-time episode manifests.

A manifest contains private resolution data. Give policies environment observations,
not the manifest. This is a trusted-process research API, not a security sandbox.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from ..attention_judgment.ledger import canonical, digest

SCHEMA_VERSION = 1
OBSERVATION_FIELDS = frozenset({"id", "source", "source_id", "published_at", "available_at", "event_id", "raw", "raw_sha256"})
PUBLIC_QUESTION_FIELDS = ("question_id", "title", "criterion", "available_at", "deadline", "resolve_at")
MANIFEST_FIELDS = frozenset({"schema_version", "episode_id", "mode", "timeline", "observations", "questions"})
_SCOPE = json.loads(Path(__file__).parents[1].joinpath("attention_judgment", "scope.json").read_text())


def utc_time(value):
    """Require explicit timezone awareness; return an absolute UTC datetime."""
    if not isinstance(value, str):
        raise ValueError("timestamps must be timezone-aware ISO strings")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("timestamps must be timezone-aware ISO strings") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware ISO strings")
    return result.astimezone(timezone.utc)


def _fields(value, expected, label):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError(f"invalid {label} fields")


def _text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} requires nonempty text")


def _identifier(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise ValueError(f"invalid {label}")


def _ids(values, existing, label):
    if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
        raise ValueError(f"invalid {label}")
    if len(set(values)) != len(values) or not set(values) <= existing:
        raise ValueError(f"invalid {label}")


def load_manifest(manifest):
    """Copy and validate a manifest; no live sources, models, or databases are used."""
    if isinstance(manifest, (str, Path)):
        manifest = json.loads(Path(manifest).read_text())
    manifest = deepcopy(manifest)
    try:
        # Also rejects NaN/Infinity at any depth and unsupported Python objects.
        canonical(manifest)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("manifest must contain finite JSON data") from None
    _fields(manifest, MANIFEST_FIELDS, "manifest")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported manifest schema version")
    _identifier(manifest["episode_id"], "episode_id")
    if manifest["mode"] not in ("synthetic", "historical_replay"):
        raise ValueError("unsupported episode mode")
    timeline = manifest["timeline"]
    if not isinstance(timeline, list) or not timeline:
        raise ValueError("timeline requires at least one UTC cutoff")
    times = [utc_time(t) for t in timeline]
    if any(datetime.fromisoformat(t.replace("Z", "+00:00")).utcoffset().total_seconds() != 0 for t in timeline):
        raise ValueError("timeline cutoffs must use UTC")
    if any(a >= b for a, b in zip(times, times[1:])):
        raise ValueError("timeline cutoffs must be strictly increasing")
    if not isinstance(manifest["observations"], list):
        raise ValueError("observations must be a list")
    originals = {}
    for item in manifest["observations"]:
        _fields(item, OBSERVATION_FIELDS, "observation")
        for field in ("source", "source_id", "event_id"):
            _text(item[field], field)
        if not isinstance(item["raw"], dict):
            raise ValueError("observation raw must be an object")
        utc_time(item["published_at"])
        utc_time(item["available_at"])
        raw_hash = digest(item["raw"])
        if item["raw_sha256"] != raw_hash or item["id"] != digest([item["source"], item["source_id"], raw_hash]):
            raise ValueError("observation identity/content hash mismatch")
        if item["id"] in originals:
            raise ValueError("duplicate observation ID")
        originals[item["id"]] = item
    if not isinstance(manifest["questions"], list):
        raise ValueError("questions must be a list")
    question_ids = set()
    for item in manifest["questions"]:
        _fields(item, (*PUBLIC_QUESTION_FIELDS, "resolution"), "question")
        _identifier(item["question_id"], "question_id")
        if item["question_id"] in question_ids:
            raise ValueError("duplicate question ID")
        question_ids.add(item["question_id"])
        _text(item["title"], "question title")
        criterion = item["criterion"]
        _fields(criterion, ("metric", "operator", "target", "unit", "source"), "criterion")
        for field in ("metric", "unit", "source"):
            _text(criterion[field], f"criterion {field}")
        if criterion["operator"] not in (">", ">=", "<", "<=", "==", "!="):
            raise ValueError("unsupported criterion operator")
        if type(criterion["target"]) not in (int, float, str) or criterion["target"] == "":
            raise ValueError("criterion target must be a finite number or nonempty string")
        released, deadline, resolve_at = (utc_time(item[k]) for k in ("available_at", "deadline", "resolve_at"))
        if not released < deadline <= resolve_at:
            raise ValueError("question requires release < deadline <= resolve_at")
        resolution = item["resolution"]
        _fields(resolution, ("status", "value", "available_at", "evidence_ids", "reason"), "resolution")
        _text(resolution["reason"], "resolution reason")
        _ids(resolution["evidence_ids"], originals.keys(), "resolution evidence")
        if resolution["status"] not in ("resolved", "unresolved"):
            raise ValueError("invalid resolution status")
        if resolution["status"] == "resolved":
            if type(resolution["value"]) is not int or resolution["value"] not in (0, 1):
                raise ValueError("resolved outcome must be 0 or 1")
            if resolution["available_at"] is None or not resolution["evidence_ids"]:
                raise ValueError("resolved outcome requires availability and original evidence")
        elif resolution["value"] is not None:
            raise ValueError("unresolved outcome must be null")
        if resolution["available_at"] is None:
            if resolution["evidence_ids"]:
                raise ValueError("resolution evidence requires an availability timestamp")
            continue
        available = utc_time(resolution["available_at"])
        if available < resolve_at:
            raise ValueError("resolution cannot be available before resolve_at")
        if any(utc_time(originals[r][k]) > available for r in resolution["evidence_ids"] for k in ("published_at", "available_at")):
            raise ValueError("resolution evidence was unavailable at resolution time")
    return manifest


def public_question(question):
    """Return only the operator's public, predeclared forecast specification."""
    return deepcopy({key: question[key] for key in PUBLIC_QUESTION_FIELDS})


def topic_scope():
    return deepcopy({key: _SCOPE[key] for key in ("keywords", "followed_twitter_bypasses_keyword_filter", "asset_universe")})
