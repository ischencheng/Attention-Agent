"""Admit captured source versions and a declared market vintage for offline replay.

Hash checks establish artifact integrity, not independent truth of a publisher's
claims. Historical price reconstruction is explicitly weaker than captured
point-in-time prices. Model publication precedes the episode; an unknown training
cutoff is never replaced with a guessed date.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

from .market import Market
from .scenario import json_copy, load_manifest, string_list, text, timestamp


FORMAT = "attention-research-archive-v1"
ROLES = ("fast", "mid", "strong")
FILES = ("episode.json", "sources.json", "market.json", "policy-manifest.json")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _read(value):
    return json_copy(json.loads(Path(value).read_text()) if isinstance(value, (str, Path)) else value)


def _url(value, name):
    text(value, name)
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError(f"{name} must be a public HTTPS evidence URL")


def _hash(value, name):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{name} must be a SHA256 hex digest")


def validate_policy_manifest(value, episode_start):
    """Validate declared identities; the inference adapter must verify served files."""
    policy = _read(value)
    if not isinstance(policy, dict) or policy.get("schema_version") != 1:
        raise ValueError("policy manifest requires schema_version 1")
    if not isinstance(policy.get("roles"), dict) or set(policy["roles"]) != set(ROLES):
        raise ValueError("policy manifest must identify fast, mid and strong")
    start = timestamp(episode_start)
    for role, identity in policy["roles"].items():
        if not isinstance(identity, dict):
            raise ValueError(f"{role} identity must be an object")
        text(identity.get("model_id"), f"{role}.model_id")
        if not re.fullmatch(r"[0-9a-f]{40}", str(identity.get("revision", ""))):
            raise ValueError(f"{role} requires a full immutable 40-character model revision")
        if timestamp(identity.get("released_at")) >= start:
            raise ValueError(f"{role} checkpoint must be released strictly before the episode")
        _url(identity.get("release_evidence_url"), f"{role}.release_evidence_url")
        _url(identity.get("revision_evidence_url"), f"{role}.revision_evidence_url")
        cutoff = identity.get("knowledge_cutoff")
        if cutoff is not None:
            if timestamp(cutoff) >= start:
                raise ValueError(f"{role} training cutoff must precede the episode")
            _url(identity.get("knowledge_cutoff_evidence_url"), f"{role}.knowledge_cutoff_evidence_url")
        if identity.get("temporal_basis") != "checkpoint_published_before_episode":
            raise ValueError(f"{role} must declare the checkpoint publication boundary")
        files = identity.get("artifact_sha256")
        if not isinstance(files, dict) or not files:
            raise ValueError(f"{role} must declare model artifact hashes")
        for name, value in files.items():
            path = Path(text(name, "model artifact path"))
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("model artifact paths must be relative")
            _hash(value, "model artifact hash")
    return policy


def validate_sources(value):
    bundle = _read(value)
    if not isinstance(bundle, dict) or bundle.get("schema_version") != 1:
        raise ValueError("source archive requires schema_version 1")
    collected = timestamp(bundle.get("collected_at"))
    rows = bundle.get("records")
    if not isinstance(rows, list) or not rows:
        raise ValueError("source archive needs captured originals")
    if bundle.get("rejected_records"):
        raise ValueError("partial source exports with rejected records cannot be admitted")
    selection = bundle.get("selection")
    if (not isinstance(selection, dict) or type(selection.get("received_row_count")) is not int
            or selection["received_row_count"] != len(rows)):
        raise ValueError("source archive needs a complete captured receipt interval")
    lower, upper = timestamp(selection.get("start_at")), timestamp(selection.get("end_at"))
    if lower >= upper or upper > collected:
        raise ValueError("source selection interval must precede collection")
    identities, raw_ids = set(), set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("source record must be an object")
        for field in ("id", "source", "source_id", "event_id", "title", "body"):
            text(row.get(field), f"source.{field}")
        if row["id"] in identities:
            raise ValueError("duplicate source version ID")
        identities.add(row["id"])
        if "raw" not in row or digest(row["raw"]) != row.get("raw_sha256"):
            raise ValueError(f"source {row['id']} raw hash mismatch")
        if digest(row["body"]) != row.get("body_sha256"):
            raise ValueError(f"source {row['id']} body hash mismatch")
        published, available = timestamp(row.get("published_at")), timestamp(row.get("available_at"))
        if published > available or available > collected:
            raise ValueError("source dates must satisfy publication <= availability <= collection")
        receipt = row.get("receipt")
        if not isinstance(receipt, dict) or receipt.get("kind") != "postgres_raw_messages":
            raise ValueError("source requires a captured raw_messages receipt")
        if type(receipt.get("raw_id")) is not int or receipt["raw_id"] <= 0:
            raise ValueError("source receipt requires a positive raw_id")
        if receipt["raw_id"] in raw_ids:
            raise ValueError("database receipt IDs must be unique")
        raw_ids.add(receipt["raw_id"])
        if timestamp(receipt.get("created_at")) != available:
            raise ValueError("availability must equal the actual database receipt time")
        if not lower <= available <= upper:
            raise ValueError("source receipt falls outside the captured interval")
        _hash(receipt.get("payload_sha256"), "source receipt payload hash")
        payload = row.get("payload")
        if not isinstance(payload, dict) or digest(payload) != receipt["payload_sha256"]:
            raise ValueError("source original envelope hash mismatch")
        # Replay the collector's projection from the preserved envelope/original,
        # rather than trusting a separately editable body and publication date.
        from .collector import source_record
        projected = source_record({"id": receipt["raw_id"], "created_at": available,
                                   "payload": payload, "raw": row["raw"]})
        for field in ("id", "source", "source_id", "event_id", "published_at", "title", "body", "authority", "terms"):
            if projected[field] != row.get(field):
                raise ValueError(f"source {field} differs from its captured original projection")
        if type(row.get("authority")) is not bool:
            raise ValueError("source authority must be a boolean")
        string_list(row.get("terms"), "source terms")
    return bundle


def validate_market(value):
    bundle = _read(value)
    if not isinstance(bundle, dict) or bundle.get("schema_version") != 1:
        raise ValueError("market archive requires schema_version 1")
    if bundle.get("price_basis") != "raw":
        raise ValueError("market archive must use raw prices")
    if bundle.get("price_vintage") != "historical_reconstruction":
        raise ValueError("this collector supports explicitly labeled historical reconstruction only")
    collected = timestamp(bundle.get("collected_at"))
    assets = bundle.get("assets")
    if not isinstance(assets, list) or not assets:
        raise ValueError("market archive needs an explicit data-coverage universe")
    ids = [asset.get("asset_id") for asset in assets if isinstance(asset, dict)]
    if len(ids) != len(assets) or len(set(ids)) != len(ids):
        raise ValueError("market assets must have unique IDs")
    receipts = bundle.get("receipts")
    if (not isinstance(receipts, list) or any(not isinstance(r, dict) for r in receipts)
            or {r.get("asset_id") for r in receipts} != set(ids) or len(receipts) != len(ids)):
        raise ValueError("each market asset requires exactly one provider receipt")
    if (not isinstance(bundle.get("bars"), list) or any(not isinstance(row, dict) for row in bundle["bars"])
            or not isinstance(bundle.get("corporate_actions"), list)
            or any(not isinstance(row, dict) for row in bundle["corporate_actions"])):
        raise ValueError("market bars and corporate actions must be lists of objects")
    for receipt in receipts:
        asset = receipt["asset_id"]
        text(receipt.get("provider"), "market provider")
        _url(receipt.get("request_url"), "market request URL")
        fetched = timestamp(receipt.get("fetched_at"))
        if fetched > collected:
            raise ValueError("market receipt cannot be fetched after archive collection")
        if "raw_response" not in receipt or digest(receipt["raw_response"]) != receipt.get("raw_response_sha256"):
            raise ValueError("market raw response hash mismatch")
        bars = [bar for bar in bundle.get("bars", []) if bar.get("asset_id") == asset]
        actions = [action for action in bundle.get("corporate_actions", []) if action.get("asset_id") == asset]
        if not bars or digest(bars) != receipt.get("bars_sha256") or digest(actions) != receipt.get("corporate_actions_sha256"):
            raise ValueError("market projection hash mismatch")
        if any(timestamp(bar["close_at"]) > fetched for bar in bars):
            raise ValueError("market archive contains a bar not yet complete at collection")
        _verify_price_projection(receipt, next(row for row in assets if row["asset_id"] == asset), bars, actions)
    return bundle


def _verify_price_projection(receipt, asset, bars, actions):
    """Bind evaluator prices to the original provider fields, not just new hashes."""
    url = urlparse(receipt["request_url"])
    if (receipt["provider"] != "Yahoo Finance chart API"
            or url.hostname not in {"query1.finance.yahoo.com", "query2.finance.yahoo.com"}
            or url.path != "/v8/finance/chart/" + quote(asset["symbol"].upper(), safe="")):
        raise ValueError("unsupported market provider; a verified projection adapter is required")
    raw = receipt["raw_response"]
    chart = raw.get("chart", {}) if isinstance(raw, dict) else {}
    results = chart.get("result") if isinstance(chart, dict) else None
    if (not isinstance(chart, dict) or chart.get("error") or not isinstance(results, list)
            or len(results) != 1 or not isinstance(results[0], dict)):
        raise ValueError("market original contains no unambiguous chart result")
    result = results[0]
    meta = result.get("meta", {})
    if (not isinstance(meta, dict) or meta.get("symbol") != asset["symbol"].upper()
            or meta.get("exchangeTimezoneName") not in {"America/New_York", "US/Eastern"}):
        raise ValueError("market original identity differs from the asset catalog")
    indicators = result.get("indicators", {})
    quotes = indicators.get("quote") if isinstance(indicators, dict) else None
    times = result.get("timestamp")
    if not isinstance(quotes, list) or len(quotes) != 1 or not isinstance(quotes[0], dict) or not isinstance(times, list):
        raise ValueError("market original is missing daily quote arrays")
    zone = ZoneInfo("America/New_York")
    def day(value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("market original contains an invalid timestamp")
        return datetime.fromtimestamp(value, timezone.utc).astimezone(zone).date()
    expected = {timestamp(bar["close_at"]).astimezone(zone).date(): bar for bar in bars}
    observed = set()
    for index, value in enumerate(times):
        session = day(value)
        if not min(expected) <= session <= max(expected):
            continue
        if session not in expected or session in observed:
            raise ValueError("market original and projected sessions differ")
        observed.add(session)
        for field in ("open", "close"):
            values = quotes[0].get(field)
            if (not isinstance(values, list) or index >= len(values)
                    or isinstance(values[index], bool) or not isinstance(values[index], (int, float))
                    or not math.isfinite(values[index]) or values[index] <= 0
                    or values[index] != expected[session][field]):
                raise ValueError("market price differs from its original provider projection")
    if observed != set(expected):
        raise ValueError("market projected session is missing from original response")
    events = result.get("events") or {}
    if not isinstance(events, dict) or actions:
        raise ValueError("corporate-action archive requires a separate verified projection adapter")
    for kind in ("splits", "dividends"):
        entries = events.get(kind) or {}
        if not isinstance(entries, dict):
            raise ValueError("market original corporate actions are malformed")
        for entry in entries.values():
            if not isinstance(entry, dict) or day(entry.get("date")) in expected:
                raise ValueError("reported corporate actions cannot be silently removed")


def _assemble(spec, source_bundle, market_bundle):
    start, end = timestamp(spec.get("start_at")), timestamp(spec.get("end_at"))
    if start >= end:
        raise ValueError("episode start must precede end")
    coverage = source_bundle["selection"]
    if timestamp(coverage["start_at"]) > start or timestamp(coverage["end_at"]) < end:
        raise ValueError("captured source interval must cover the entire episode")
    selected = [row for row in source_bundle["records"] if start <= timestamp(row["available_at"]) <= end]
    if not selected:
        raise ValueError("episode contains no captured source arrivals")
    selected.sort(key=lambda row: (timestamp(row["available_at"]), timestamp(row["published_at"]), row["id"]))
    source_fields = ("id", "source", "event_id", "available_at", "published_at", "title", "body", "terms", "authority")
    # Market opens are execution events, not implicit policy invocations. Arrival
    # times and closes provide deterministic policy cutoffs without wall-clock IO.
    cutoffs = {start, end}
    cutoffs.update(timestamp(row["available_at"]) for row in selected)
    cutoffs.update(timestamp(bar["close_at"]) for bar in market_bundle["bars"] if start < timestamp(bar["close_at"]) <= end)
    for value in spec.get("extra_cutoffs", []):
        stamp = timestamp(value)
        if not start <= stamp <= end:
            raise ValueError("extra cutoff lies outside the episode")
        cutoffs.add(stamp)
    bars = [bar for bar in market_bundle["bars"] if timestamp(bar["close_at"]) <= end]
    actions = [action for action in market_bundle["corporate_actions"]
               if timestamp(action.get("at", action.get("ex_at"))) <= end]
    manifest = {"schema_version": 1, "episode_id": spec.get("episode_id"), "mode": "historical_replay",
                "price_basis": "raw", "timeline": [stamp.isoformat() for stamp in sorted(cutoffs)],
                "scope": spec.get("scope"), "portfolio": spec.get("portfolio"),
                "assets": market_bundle["assets"], "bars": bars, "corporate_actions": actions,
                "sources": [{key: row[key] for key in source_fields} for row in selected]}
    manifest = load_manifest(manifest)
    Market(manifest)  # Reuse complete-grid, terminal close and accounting checks.
    return manifest


def replay_integrity(contract):
    """One displayable verdict shared by the runner and trace viewer."""
    return deepcopy(contract["replay_integrity"])


def build_archive(spec, sources, market, policies, output=None):
    spec = _read(spec)
    source_bundle, market_bundle = validate_sources(sources), validate_market(market)
    manifest = _assemble(spec, source_bundle, market_bundle)
    policy = validate_policy_manifest(policies, manifest["timeline"][0])
    known_cutoffs = all(identity.get("knowledge_cutoff") is not None for identity in policy["roles"].values())
    integrity = {
        "status": "admitted_with_limits", "benchmark_eligible": True,
        "benchmark_scope": "retrospective market-price reconstruction; captured source arrivals",
        "strict_point_in_time_eligible": False,
        "episode_start": manifest["timeline"][0], "episode_end": manifest["timeline"][-1],
        "source_receipt_policy": "actual PostgreSQL receipt; no publication-time backfill",
        "source_receipts_verified": True, "source_hashes_verified": True,
        "model_boundary_verified": True, "model_cutoff_verified": known_cutoffs,
        "training_cutoff": {role: identity.get("knowledge_cutoff") for role, identity in policy["roles"].items()},
        "model_files_verified": False,
        "price_provenance_verified": True, "price_vintage": market_bundle["price_vintage"],
        "reasons": ["Historical prices were downloaded later and may include provider revisions; original price vintages are not certified.",
                    "Model file hashes are declared here; the inference adapter must verify the actual served files."],
    }
    if not known_cutoffs:
        integrity["reasons"].append("Training cutoff is not established for every role; the verified temporal claim is checkpoint publication before the episode.")
    objects = dict(zip(FILES, (manifest, source_bundle, market_bundle, policy)))
    contract = {"format": FORMAT, "spec": spec, "artifact_hashes": {name: digest(value) for name, value in objects.items()},
                "policy_manifest": policy, "source_count": len(manifest["sources"]),
                "reward_contract": "terminal_net_portfolio_return_v1", "replay_integrity": integrity}
    result = {"manifest": manifest, "contract": contract, "sources": source_bundle,
              "market": market_bundle, "policy_manifest": policy}
    if output is not None:
        output = Path(output)
        if output.exists() and any(output.iterdir()):
            raise ValueError("archive output directory must be new or empty")
        output.mkdir(parents=True, exist_ok=True)
        for name, value in {**objects, "archive-contract.json": contract}.items():
            (output / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    return result


def load_archive(path, policy_manifest=None):
    root = Path(path)
    saved = _read(root / "archive-contract.json")
    if saved.get("format") != FORMAT:
        raise ValueError("unsupported archive format")
    objects = {name: _read(root / name) for name in FILES}
    if saved.get("artifact_hashes") != {name: digest(value) for name, value in objects.items()}:
        raise ValueError("archive artifact hashes do not match the frozen contract")
    rebuilt = build_archive(saved.get("spec"), objects["sources.json"], objects["market.json"], objects["policy-manifest.json"])
    if rebuilt["manifest"] != objects["episode.json"] or rebuilt["contract"] != saved:
        raise ValueError("archive derived manifest or admission contract was changed")
    if policy_manifest is not None and _read(policy_manifest) != rebuilt["policy_manifest"]:
        raise ValueError("selected policy identities differ from the admitted archive")
    return rebuilt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build")
    for key in ("spec", "sources", "market", "policies", "output"):
        build.add_argument("--" + key, required=True, type=Path)
    check = sub.add_parser("validate")
    check.add_argument("archive", type=Path)
    args = parser.parse_args(argv)
    try:
        result = (load_archive(args.archive) if args.command == "validate" else
                  build_archive(args.spec, args.sources, args.market, args.policies, args.output))
    except (ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(2, f"Archive rejected: {exc}\n")
    print(json.dumps(result["contract"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
