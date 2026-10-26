"""Validated, immutable inputs for topic-led research and paper-account replay."""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamps must be ISO strings with a timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc
    if result.tzinfo is None:
        raise ValueError("timestamps require a timezone")
    return result.astimezone(timezone.utc)


def json_copy(value: Any) -> Any:
    """Reject non-JSON objects and NaN instead of silently coercing inputs."""
    try:
        encoded = json.dumps(value, allow_nan=False)
        result = json.loads(encoded)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("input must be finite JSON") from exc
    if result != value:
        raise ValueError("input must use JSON string keys and array values")
    return copy.deepcopy(result)


def text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def string_list(value: Any, name: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise ValueError(f"{name} must be an array{' with at least one item' if nonempty else ''}")
    for item in value:
        text(item, name)
    if len(value) != len(set(value)):
        raise ValueError(f"{name} must not contain duplicates")
    return value


def load_manifest(value: dict | str | Path) -> dict:
    if isinstance(value, (str, Path)):
        value = json.loads(Path(value).read_text())
    data = json_copy(value)
    if not isinstance(data, dict):
        raise ValueError("manifest must be an object")
    required = {"schema_version", "episode_id", "mode", "price_basis", "timeline",
                "scope", "portfolio", "assets", "bars", "corporate_actions", "sources"}
    if set(data) != required:
        raise ValueError(f"manifest fields must be exactly {sorted(required)}")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported manifest schema_version")
    text(data["episode_id"], "episode_id")
    if not isinstance(data["mode"], str) or data["mode"] not in {"synthetic", "historical_replay"}:
        raise ValueError("mode must be synthetic or historical_replay")
    if data["price_basis"] != "raw":
        raise ValueError("only raw prices with explicit corporate actions are supported")
    times = string_list(data["timeline"], "timeline", nonempty=True)
    if len(times) < 2:
        raise ValueError("timeline needs an initial cutoff and a terminal cutoff")
    parsed = [timestamp(item) for item in times]
    if any(a >= b for a, b in zip(parsed, parsed[1:])):
        raise ValueError("timeline must be strictly increasing")
    string_list(data["scope"], "scope", nonempty=True)
    if not isinstance(data["portfolio"], dict):
        raise ValueError("portfolio must be an object")
    for collection in ("assets", "bars", "corporate_actions", "sources"):
        if not isinstance(data[collection], list):
            raise ValueError(f"{collection} must be an array")
    asset_ids = set()
    for asset in data["assets"]:
        if not isinstance(asset, dict) or set(asset) != {"asset_id", "symbol", "name", "available_at", "terms"}:
            raise ValueError("asset fields must be asset_id, symbol, name, available_at, terms")
        for key in ("asset_id", "symbol", "name"):
            text(asset[key], key)
        if asset["asset_id"] in asset_ids:
            raise ValueError("asset IDs must be unique")
        asset_ids.add(asset["asset_id"])
        timestamp(asset["available_at"])
        string_list(asset["terms"], "asset terms")
    source_ids = set()
    for source in data["sources"]:
        expected = {"id", "source", "event_id", "available_at", "published_at", "title", "body", "terms", "authority"}
        if not isinstance(source, dict) or set(source) != expected:
            raise ValueError(f"source fields must be exactly {sorted(expected)}")
        for key in ("id", "source", "event_id", "title", "body"):
            text(source[key], key)
        if source["id"] in source_ids:
            raise ValueError("source IDs must be unique")
        source_ids.add(source["id"])
        timestamp(source["published_at"])
        timestamp(source["available_at"])
        string_list(source["terms"], "source terms")
        if type(source["authority"]) is not bool:
            raise ValueError("source authority must be boolean")
    # Market owns price, position, execution and corporate-action validation.
    return data
