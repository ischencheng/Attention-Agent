#!/usr/bin/env python3
"""Freeze and evaluate decisions published by the actual multilevel pipeline.

This observer never calls an agent or creates a research conclusion. It reads
PostgreSQL decision_packets and verifies their historical evidence graph using
the same verifier as the API. It keeps its own append-only production ledger,
separate from the earlier direct-RSS experiment. Database insertion time is
recorded, but evaluation begins only after this observer freezes a version:
ordinary non-evidence decision fields can be edited in the application DB.
Strong agent_state is archived separately as unscored application research;
its contents are not independently verified evidence or published predictions.

From the repository root (DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD apply):
  .venv/bin/python environments/attention_judgment/pipeline.py sync
  .venv/bin/python environments/attention_judgment/pipeline.py settle
  .venv/bin/python environments/attention_judgment/pipeline.py report

Only explicit structured forecasts using symbol, horizon (day/week/month),
p_outperform_spy and reason are scored. Confidence, an investment action, or
bullish language is not a calibrated price forecast. Explicit tickers without
forecasts receive descriptive price observations only, never a success score.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "skills"))
from evidence import verify_evidence
from ledger import Ledger, digest, timestamp, utcnow
from market import (evaluate_horizon, fetch_prices, horizon_complete,
                    horizon_dates, market_contract)

DECISION_KIND = "production_decision"
SETTLEMENT_KIND = "production_settlement"
RESEARCH_KIND = "production_research_state"
SYMBOL = re.compile(r"[A-Z][A-Z0-9.\-^]{0,14}\Z")


def _iso(value):
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("database timestamp requires an explicit timezone")
        return value.astimezone(timezone.utc).isoformat()
    return timestamp(value).isoformat()


def _symbol(value):
    return isinstance(value, str) and bool(SYMBOL.fullmatch(value))


def evaluation_targets(payload):
    """Extract only explicit instrument fields; never guess tickers from prose.

    Related tickers, entities and action objects are optional application
    fields. A forecast is itself an explicit symbol declaration. Malformed
    forecasts stay visible as errors while the original decision is archived.
    """
    symbols = set()
    errors = []
    for field in ("ticker", "symbol"):
        value = payload.get(field)
        if _symbol(value):
            symbols.add(value)
    for field in ("related_tickers", "tickers"):
        values = payload.get(field, [])
        if isinstance(values, list):
            symbols.update(value for value in values if _symbol(value))
    for field in ("actions", "entities"):
        values = payload.get(field, [])
        if isinstance(values, list):
            for item in values:
                if isinstance(item, dict):
                    for name in ("ticker", "symbol"):
                        if _symbol(item.get(name)):
                            symbols.add(item[name])
    forecasts = payload.get("forecasts", [])
    if not isinstance(forecasts, list):
        return sorted(symbols), [], ["forecasts must be an array; no forecasts scored"]
    accepted = []
    keys = set()
    duplicates = set()
    for index, forecast in enumerate(forecasts):
        if not isinstance(forecast, dict):
            errors.append(f"forecast {index}: expected an object")
            continue
        symbol = forecast.get("symbol")
        horizon = forecast.get("horizon")
        probability = forecast.get("p_outperform_spy")
        reason = forecast.get("reason")
        valid = (set(forecast) == {"symbol", "horizon", "p_outperform_spy", "reason"}
                 and _symbol(symbol) and symbol != "SPY"
                 and horizon in ("day", "week", "month")
                 and type(probability) in (int, float) and math.isfinite(probability)
                 and 0 <= probability <= 1
                 and isinstance(reason, str) and bool(reason.strip()))
        if not valid:
            errors.append(f"forecast {index}: incomplete or invalid explicit probability contract")
            continue
        key = (symbol, horizon)
        if key in keys:
            duplicates.add(key)
            errors.append(f"forecast {index}: duplicate symbol/horizon; all duplicates excluded")
        keys.add(key)
        symbols.add(symbol)
        accepted.append(copy.deepcopy(forecast))
    accepted = [f for f in accepted if (f["symbol"], f["horizon"]) not in duplicates]
    return sorted(symbols), accepted, errors


def lineage_metadata(conn, snapshot):
    """Resolve durable DB/Kafka identities without substituting live annotations.

    Historical annotation content comes from the verified publication snapshot;
    a later Mid annotation edit must not replace it with today's interpretation.
    These rows provide insertion times and upstream Kafka offsets only.
    """
    result = {}
    for name, table in (("raw_refs", "raw_messages"),
                        ("tickets", "attention_tickets"),
                        ("annotations", "annotations")):
        ids = sorted(entry["id"] for entry in snapshot[name])
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, kafka_topic, kafka_partition, kafka_offset, created_at "
                f"FROM {table} WHERE id = ANY(%s) ORDER BY id", (ids,))
            rows = cur.fetchall()
        if {row[0] for row in rows} != set(ids):
            raise ValueError(f"missing {table} rows in pipeline lineage")
        result[table] = [
            {"id": row[0], "kafka_topic": row[1], "kafka_partition": row[2],
             "kafka_offset": row[3], "available_at": _iso(row[4])}
            for row in rows
        ]
    return result


def freeze_packet(conn, ledger, row, *, source_id="attention-agent"):
    """Verify and freeze one committed DB row in the caller's read transaction.

    Returns unchanged on an exact retry. Edits to a previously observed row
    create a new version with a later evaluation start and prior hash link.
    The latest row content cannot be treated as its original historical value.
    """
    if type(row.get("id")) is not int or row["id"] <= 0:
        raise ValueError("decision row needs a positive integer ID")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source_id identifies a stable pipeline deployment")
    payload = row["payload"]
    payload_hash = digest(payload)
    verification = verify_evidence(conn, row["id"], payload)
    if verification["status"] != "verified":
        raise ValueError("unverified production evidence: " + "; ".join(verification["errors"]))
    lineage = lineage_metadata(conn, verification["evidence_snapshot"])
    available_at = _iso(row["created_at"])
    identity = {"source_id": source_id, "table": "decision_packets", "id": row["id"],
                "kafka_topic": row["kafka_topic"],
                "kafka_partition": row["kafka_partition"],
                "kafka_offset": row["kafka_offset"]}
    symbols, forecasts, forecast_errors = evaluation_targets(payload)
    stable_id = f"{source_id}:decision_packets:{row['id']}"
    with ledger.transaction():
        # Timestamp after acquiring the append lock, so waiting for another
        # observer cannot accidentally borrow an earlier market opening.
        frozen_at = utcnow()
        freeze_time = timestamp(frozen_at)
        available_times = [available_at] + [
            item["available_at"] for rows in lineage.values() for item in rows
        ]
        if any(timestamp(value) > freeze_time for value in available_times):
            raise ValueError("pipeline row availability is after observer freeze time")
        prior = next((entry for entry in reversed(ledger.entries(DECISION_KIND))
                      if entry["payload"]["stable_id"] == stable_id), None)
        if prior and prior["payload"]["production_identity"] != identity:
            raise ValueError("decision ID was reused with different Kafka identity; choose a new deployment source_id")
        if prior and prior["payload"]["payload_sha256"] == payload_hash:
            return {"status": "unchanged", "key": prior["key"], "hash": prior["hash"],
                    "decision_id": row["id"], "version": prior["payload"]["version"]}
        version = prior["payload"]["version"] + 1 if prior else 1
        frozen = {
            "mode": "production_forward", "stable_id": stable_id,
            "production_identity": identity, "version": version,
            "previous_version_hash": prior["hash"] if prior else None,
            "application_thesis_id": payload.get("thesis_id"),
            "application_revision": payload.get("revision", payload.get("version")),
            "available_at": available_at, "frozen_at": frozen_at,
            "evaluation_starts_at": frozen_at,
            "timestamp_policy": "forward_from_observer_freeze; DB insertion and source claims are not execution times",
            "payload": copy.deepcopy(payload), "payload_sha256": payload_hash,
            "evidence": verification, "lineage": lineage,
            "symbols": symbols, "forecasts": forecasts,
            "forecast_errors": forecast_errors,
            "dates": horizon_dates(freeze_time), "market_contract": market_contract(),
            "thesis_outcome": "pending_independent_evidence_review",
        }
        key = f"{stable_id}:v{version}"
        value = ledger.append(DECISION_KIND, key, frozen)
    return {"status": "frozen", "key": key, "hash": value, "decision_id": row["id"],
            "version": version, "dates": frozen["dates"], "symbols": symbols,
            "forecasts": len(forecasts), "forecast_errors": forecast_errors}


def strong_state_ids():
    """Read only the known Strong keys and the explicitly configured alias."""
    keys = {"claude-strong", "codex-strong"}
    configured = os.environ.get("STRONG_AGENT_ID", "").strip()
    if configured:
        keys.add(configured)
    return sorted(keys)


def research_summary(frozen):
    """Counts and lineage only; report output never includes the archived state."""
    state = frozen["state"]
    def count(name):
        value = state.get(name)
        if name == "pending_themes" and isinstance(value, dict):
            return sum(len(item) if isinstance(item, list) else 1 for item in value.values())
        return len(value) if isinstance(value, (dict, list)) else 0
    return {"stable_id": frozen["stable_id"], "agent_id": frozen["agent_id"],
            "version": frozen["version"], "updated_at": frozen["updated_at"],
            "frozen_at": frozen["frozen_at"], "state_sha256": frozen["state_sha256"],
            "thesis_count": count("theses"), "pending_theme_count": count("pending_themes"),
            "evaluation_status": "unscored_application_research"}


def freeze_research_state(ledger, row, *, source_id="attention-agent"):
    """Archive actual Strong working memory without turning it into a decision.

    Whole state is retained in the local ledger for later comparison. Symbols,
    forecasts, market dates and outcomes are deliberately not derived from it.
    A later write of identical content does not create another research version.
    """
    agent_id = row["agent_id"]
    if agent_id not in strong_state_ids():
        raise ValueError("only configured Strong agent state can be archived as research")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source_id identifies a stable pipeline deployment")
    state = row["state"]
    if not isinstance(state, dict):
        raise ValueError("Strong research state must be a JSON object")
    state_hash = digest(state)
    updated_at = _iso(row["updated_at"])
    stable_id = f"{source_id}:agent_state:{agent_id}"
    with ledger.transaction():
        frozen_at = utcnow()
        if timestamp(updated_at) > timestamp(frozen_at):
            raise ValueError("Strong state update is after observer freeze time")
        prior = next((entry for entry in reversed(ledger.entries(RESEARCH_KIND))
                      if entry["payload"]["stable_id"] == stable_id), None)
        if prior and timestamp(updated_at) < timestamp(prior["payload"]["updated_at"]):
            raise ValueError("Strong snapshot is older than the latest archived state")
        if prior and prior["payload"]["state_sha256"] == state_hash:
            return {"status": "unchanged", "key": prior["key"], "hash": prior["hash"],
                    **research_summary(prior["payload"])}
        version = prior["payload"]["version"] + 1 if prior else 1
        frozen = {"mode": "unscored_application_research", "stable_id": stable_id,
                  "source_id": source_id, "table": "agent_state", "agent_id": agent_id,
                  "version": version, "previous_version_hash": prior["hash"] if prior else None,
                  "updated_at": updated_at, "frozen_at": frozen_at,
                  "state": copy.deepcopy(state), "state_sha256": state_hash,
                  "evidence_status": "application_state_not_independently_verified",
                  "evaluation_status": "unscored_application_research"}
        key = f"{stable_id}:v{version}"
        value = ledger.append(RESEARCH_KIND, key, frozen)
    return {"status": "frozen", "key": key, "hash": value, **research_summary(frozen)}


def sync(conn, ledger, *, source_id="attention-agent"):
    """Observe all rows, including edits to older IDs, within one MVCC snapshot.

    The caller owns connection lifetime. No DB writes or Kafka publishing occur.
    A failed evidence graph is reported separately and never enters evaluation.
    Serialize observers before PostgreSQL takes a snapshot, including unchanged
    content observations. Per-record append locks alone allow an older snapshot
    to finish after a newer one and make stale content look like a reversion.
    Like the local supervisor, this observer runs on POSIX (macOS/Linux).
    """
    lock_path = ledger.path.with_name(ledger.path.name + ".sync.lock")
    with lock_path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            return _sync_locked(conn, ledger, source_id=source_id)
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _sync_locked(conn, ledger, *, source_id):
    conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
    results, rejected = [], []
    research_results, research_rejected = [], []
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, payload, kafka_topic, kafka_partition, kafka_offset, created_at "
                        "FROM decision_packets ORDER BY id")
            names = ("id", "payload", "kafka_topic", "kafka_partition", "kafka_offset", "created_at")
            # fetchmany bounds transient Python row storage. The libpq result is
            # still buffered; larger deployments should page a change log.
            while rows := cur.fetchmany(100):
                for values in rows:
                    row = dict(zip(names, values))
                    try:
                        results.append(freeze_packet(conn, ledger, row, source_id=source_id))
                    except (ValueError, TypeError, KeyError) as exc:
                        rejected.append({"decision_id": row["id"], "error": str(exc)})
        with conn.cursor() as cur:
            cur.execute("SELECT agent_id, state, updated_at FROM agent_state "
                        "WHERE agent_id = ANY(%s) ORDER BY agent_id", (strong_state_ids(),))
            for agent_id, state, updated_at in cur.fetchall():
                try:
                    research_results.append(freeze_research_state(ledger, {
                        "agent_id": agent_id, "state": state, "updated_at": updated_at,
                    }, source_id=source_id))
                except (ValueError, TypeError, KeyError) as exc:
                    research_rejected.append({"agent_id": agent_id, "error": str(exc)})
    result = {"source_id": source_id, "checked_at": utcnow(),
              "frozen": [r for r in results if r["status"] == "frozen"],
              "unchanged": sum(r["status"] == "unchanged" for r in results), "rejected": rejected,
              "research_frozen": [r for r in research_results if r["status"] == "frozen"],
              "research_unchanged": sum(r["status"] == "unchanged" for r in research_results),
              "research_rejected": research_rejected}
    ledger.append("production_sync", result["checked_at"], result)
    return result


def settle(ledger, *, as_of=None):
    """Resolve only matured frozen production observations; never re-score edits."""
    now = timestamp(_iso(as_of)) if as_of else datetime.now(timezone.utc)
    if now > datetime.now(timezone.utc):
        raise ValueError("settlement time cannot be in the future")
    settled = {entry["key"] for entry in ledger.entries(SETTLEMENT_KIND)}
    results, cache = [], {}
    for entry in ledger.entries(DECISION_KIND):
        frozen = entry["payload"]
        if frozen["market_contract"] != market_contract():
            results.append({"decision_hash": entry["hash"], "status": "unresolved",
                            "error": "frozen market contract differs from evaluator; explicit migration required"})
            continue
        for symbol in frozen["symbols"]:
            for horizon in ("day", "week", "month"):
                key = f"{entry['hash']}:{symbol}:{horizon}"
                if key in settled:
                    continue
                start, end = frozen["dates"]["entry"], frozen["dates"][horizon]
                if not horizon_complete(end, now):
                    results.append({"key": key, "status": "pending", "due_session": end})
                    continue
                try:
                    snapshots = []
                    for ticker in (symbol, "SPY"):
                        cache_key = (ticker, start, end)
                        if cache_key not in cache:
                            snapshot = fetch_prices(ticker, start, end, now)
                            value = ledger.append("production_market_snapshot", digest(snapshot), snapshot)
                            cache[cache_key] = (snapshot, value)
                        snapshots.append(cache[cache_key])
                    outcome = evaluate_horizon(snapshots[0][0], snapshots[1][0], start, end)
                    payload = {"decision_hash": entry["hash"], "stable_id": frozen["stable_id"],
                               "version": frozen["version"], "symbol": symbol, "horizon": horizon,
                               "evaluated_at": now.isoformat(), "outcome": outcome,
                               "market_snapshot_hashes": [snapshot[1] for snapshot in snapshots],
                               "forecast_score": None}
                    if outcome.get("status") == "resolved":
                        forecast = next((f for f in frozen["forecasts"]
                                         if f["symbol"] == symbol and f["horizon"] == horizon), None)
                        if forecast:
                            event = outcome["excess_return"] > 0
                            probability = forecast["p_outperform_spy"]
                            payload["forecast_score"] = {
                                "event_outperform_spy": event, "probability": probability,
                                "brier": (probability - int(event)) ** 2,
                                "constant_0_5_brier": 0.25,
                            }
                        ledger.append(SETTLEMENT_KIND, key, payload)
                    results.append({"key": key, **payload})
                except Exception as exc:
                    results.append({"key": key, "status": "unresolved",
                                    "error": f"{type(exc).__name__}: {exc}"})
    ledger.append("production_evaluation_run", utcnow(), {"results": results})
    return results


def report(ledger):
    decisions = ledger.entries(DECISION_KIND)
    settlements = ledger.entries(SETTLEMENT_KIND)
    research = ledger.entries(RESEARCH_KIND)
    latest_research = {entry["payload"]["stable_id"]: entry for entry in research}
    scores = [e["payload"]["forecast_score"] for e in settlements
              if e["payload"].get("forecast_score") is not None]
    return {
        "mode": "production_forward", "ledger_hash": ledger.verify(),
        "decision_packets": len({e["payload"]["stable_id"] for e in decisions}),
        "frozen_versions": len(decisions),
        "research_state_agents": len(latest_research), "research_state_versions": len(research),
        "latest_research_states": [{"key": entry["key"], "hash": entry["hash"],
                                    **research_summary(entry["payload"])}
                                   for entry in latest_research.values()],
        "resolved_price_observations": len(settlements),
        "resolved_predeclared_forecasts": len(scores),
        "mean_brier": sum(s["brier"] for s in scores) / len(scores) if scores else None,
        "decisions": [{"key": e["key"], "hash": e["hash"],
                       "production_identity": e["payload"]["production_identity"],
                       "version": e["payload"]["version"],
                       "available_at": e["payload"]["available_at"],
                       "frozen_at": e["payload"]["frozen_at"],
                       "dates": e["payload"]["dates"], "symbols": e["payload"]["symbols"],
                       "forecasts": len(e["payload"]["forecasts"]),
                       "forecast_errors": e["payload"]["forecast_errors"]} for e in decisions],
        "fundamental_thesis_correctness": "not automatically graded; compare independent later evidence with the frozen thesis",
        "limitations": ["observer of pipeline publications; does not generate decisions",
                        "Strong working memory is archived as unscored research, not independently verified decisions",
                        "application thesis IDs are recorded only when explicitly supplied",
                        "no inferred probability from confidence, actions or prose",
                        "price observations are not trades, portfolio returns or proof of business-thesis correctness",
                        "first observer freeze can be later than application publication",
                        "edits occurring between observer cycles are not recoverable from the application DB"],
    }


def connect_database():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "bluesky"),
        user=os.environ.get("DB_USER", "bluesky"),
        password=os.environ.get("DB_PASSWORD", "bluesky"),
        connect_timeout=10,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("sync", "settle", "report"))
    parser.add_argument("--ledger", type=Path, default=HERE / "runs/production/ledger.sqlite3")
    parser.add_argument("--source-id", default="attention-agent",
                        help="stable deployment identity; use a new ID after rebuilding the database")
    args = parser.parse_args()
    ledger = Ledger(args.ledger)
    try:
        if args.command == "sync":
            conn = connect_database()
            try:
                result = sync(conn, ledger, source_id=args.source_id)
            finally:
                conn.close()
        elif args.command == "settle":
            result = settle(ledger)
        else:
            result = report(ledger)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if args.command == "sync" and (result["rejected"] or result["research_rejected"]):
            raise SystemExit(1)
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
