"""Collect immutable inputs outside policy execution, without changing production.

``export-db`` reads a bounded PostgreSQL snapshot. ``prices`` downloads later
historical quotes with an explicit reconstruction label. Neither command runs a
policy, moves production offsets, or writes application state.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
import os
from pathlib import Path
import re

from .archive import digest, validate_sources, validate_market
from .scenario import timestamp


PRIMARY_SOURCES = frozenset({"Federal Reserve releases", "NVIDIA official blog", "AMD investor releases",
                             "OpenAI news", "sec_edgar"})


def utcnow():
    return datetime.now(timezone.utc)


def _plain(value):
    return html.unescape(re.sub(r"<[^>]+>", " ", str(value))).strip()


def source_record(row):
    """Preserve the raw version and use the actual DB receipt as availability."""
    payload, raw = row["payload"], row["raw"]
    if not isinstance(payload, dict) or not isinstance(raw, dict):
        raise ValueError("captured source envelope and raw original must be objects")
    source, native_id = payload.get("source"), payload.get("source_id")
    if not isinstance(source, str) or not source or not isinstance(native_id, str) or not native_id:
        raise ValueError("captured source lacks source identity")
    created = row["created_at"]
    available = created.isoformat() if isinstance(created, datetime) else created
    available = timestamp(available).isoformat()
    published = timestamp(payload.get("timestamp")).isoformat()
    if timestamp(published) > timestamp(available):
        raise ValueError("publication occurs after the captured receipt")
    title = raw.get("title") or payload.get("data", {}).get("title") or native_id
    if source == "twitter":
        if native_id != "twitter-" + str(raw.get("rest_id")):
            raise ValueError("Twitter native and envelope identities disagree")
        note = raw.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {})
        body = note.get("text") or raw.get("details", {}).get("full_text")
        if not isinstance(body, str) or not body.strip():
            raise ValueError("Twitter original lacks native text")
    elif any(key in raw for key in ("content_html", "description_html", "description")):
        body = _plain(raw.get("content_html") or raw.get("description_html") or raw.get("description"))
    else:
        # SEC originals retain all period/unit/accession fields; never substitute
        # a generated ticket summary for raw evidence.
        body = json.dumps(raw, ensure_ascii=False, sort_keys=True)
    if not body.strip():
        raise ValueError("captured original has no usable source text")
    raw_hash = digest(raw)
    return {"id": f"raw-{row['id']}-{raw_hash[:12]}", "source": source, "source_id": native_id,
            "event_id": digest([source, native_id]),
            "published_at": published, "available_at": available,
            "title": str(title), "body": body, "body_sha256": digest(body), "terms": [],
            "authority": source in PRIMARY_SOURCES, "raw": raw, "raw_sha256": raw_hash,
            "payload": payload,
            "receipt": {"kind": "postgres_raw_messages", "raw_id": row["id"], "created_at": available,
                        "payload_sha256": digest(payload)},
            "projection_coverage": "Native source text or original JSON; full original retained. Official publisher identity does not certify a causal claim or grant an automatic investment gate."}


def export_rows(rows, start_at, end_at, *, collected_at=None, source_names=None):
    start, end = timestamp(start_at), timestamp(end_at)
    if start >= end:
        raise ValueError("source export start must precede end")
    collected = collected_at or utcnow().isoformat()
    records, rejected = [], []
    for row in rows:
        observed = row["created_at"].isoformat() if isinstance(row["created_at"], datetime) else row["created_at"]
        if not start <= timestamp(observed) <= end:
            raise ValueError("database export returned a row outside the requested interval")
        try:
            records.append(source_record(row))
        except ValueError as exc:
            rejected.append({"raw_id": row["id"], "reason": str(exc)})
    records.sort(key=lambda row: (timestamp(row["available_at"]), timestamp(row["published_at"]), row["id"]))
    result = {"schema_version": 1, "collector": "postgres_raw_messages_readonly_v1",
              "collected_at": timestamp(collected).isoformat(),
              "selection": {"start_at": start.isoformat(), "end_at": end.isoformat(),
                            "source_names": source_names or [], "method": "complete captured receipt interval; no outcome or price filtering",
                            "received_row_count": len(rows)},
              "records": records, "rejected_records": rejected,
              "event_identity_note": "Source-native IDs identify record events; distinct IDs do not certify independent causal evidence."}
    if records and not rejected:
        validate_sources(result)
    return result


def collect_database(start_at, end_at, *, source_names=None, connect=None):
    """Export a repeatable, read-only snapshot; credentials are never serialized."""
    if connect is None:
        import psycopg2
        connect = lambda: psycopg2.connect(
            host=os.getenv("DB_HOST", "127.0.0.1"), port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "bluesky"), user=os.getenv("DB_USER", "bluesky"),
            password=os.getenv("DB_PASSWORD", "bluesky"), connect_timeout=10)
    start, end = timestamp(start_at), timestamp(end_at)
    if start >= end:
        raise ValueError("source export start must precede end")
    conn = connect()
    try:
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with conn, conn.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='15000ms'")
            query = "SELECT id,payload,raw,created_at FROM raw_messages WHERE created_at >= %s AND created_at <= %s"
            params = [start, end]
            if source_names:
                query += " AND payload->>'source' = ANY(%s)"
                params.append(list(source_names))
            cursor.execute(query + " ORDER BY created_at,id", params)
            rows = [dict(zip(("id", "payload", "raw", "created_at"), row)) for row in cursor.fetchall()]
        return export_rows(rows, start_at, end_at, source_names=source_names)
    finally:
        conn.close()


def market_bundle(assets, snapshots, *, calendar_for=None, collected_at=None):
    """Convert frozen provider responses; reject dividends without payment dates.

    This is a historical reconstruction, not proof of what a provider returned at
    a past decision time. The evaluator hides future bars separately.
    """
    if calendar_for is None:
        import exchange_calendars
        calendar_for = lambda start, end: exchange_calendars.get_calendar("XNYS", start=start, end=end)
    assets = json.loads(json.dumps(assets, allow_nan=False))
    if len(assets) != len(snapshots):
        raise ValueError("each asset needs one market snapshot")
    bars, actions, receipts = [], [], []
    for asset, snapshot in zip(assets, snapshots):
        if snapshot.get("symbol") != asset["symbol"].upper():
            raise ValueError("market snapshot symbol mismatch")
        if snapshot.get("missing_sessions") or snapshot.get("invalid_sessions"):
            raise ValueError("missing or invalid market sessions are not imputed")
        if snapshot.get("corporate_actions"):
            # Daily chart dividends provide an ex-date but no actual pay date.
            # Split-adjustment conventions also require a separate audited feed;
            # importing those bars blindly could double-count a split.
            raise ValueError("corporate-action interval needs independently verified raw prices and action timing; this provider import cannot certify it")
        calendar = calendar_for(snapshot["start_date"], snapshot["end_date"])
        asset_bars = []
        for bar in snapshot["bars"]:
            session = bar["session"]
            opened = calendar.session_open(session).to_pydatetime().isoformat()
            closed = calendar.session_close(session).to_pydatetime().isoformat()
            asset_bars.append({"asset_id": asset["asset_id"], "open_at": opened,
                               "close_at": closed, "open": bar["open"], "close": bar["close"]})
        bars.extend(asset_bars)
        receipts.append({"asset_id": asset["asset_id"], "provider": snapshot["source"],
                         "request_url": snapshot["request_url"], "fetched_at": snapshot["fetched_at"],
                         "raw_response": snapshot["raw_response"], "raw_response_sha256": digest(snapshot["raw_response"]),
                         "bars_sha256": digest(asset_bars), "corporate_actions_sha256": digest([]),
                         "calendar": snapshot["calendar"], "calendar_version": snapshot["calendar_version"],
                         "source_price_basis": snapshot["price_basis"]})
    result = {"schema_version": 1, "price_basis": "raw", "price_vintage": "historical_reconstruction",
              "collected_at": collected_at or utcnow().isoformat(), "assets": assets,
              "bars": bars, "corporate_actions": actions, "receipts": receipts,
              "limitations": ["Historical quote snapshots can be revised; original point-in-time price vintages are not certified.",
                              "Collector rejects reported corporate actions rather than inventing missing payment dates or assuming price adjustment conventions."]}
    validate_market(result)
    return result


def collect_prices(assets, start_date, end_date):
    from environments.attention_judgment.market import fetch_prices
    snapshots = [fetch_prices(asset["symbol"], start_date, end_date, utcnow()) for asset in assets]
    return market_bundle(assets, snapshots)


def _save_new(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("export-db")
    source.add_argument("--start", required=True)
    source.add_argument("--end", required=True)
    source.add_argument("--source", action="append", help="Restrict by original publisher identity; omit for all captured sources")
    source.add_argument("--output", required=True, type=Path)
    prices = sub.add_parser("prices")
    prices.add_argument("--assets", required=True, type=Path, help="Explicit data-coverage catalog JSON; not prescribed holdings")
    prices.add_argument("--start", required=True)
    prices.add_argument("--end", required=True)
    prices.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        result = (collect_database(args.start, args.end, source_names=args.source) if args.command == "export-db" else
                  collect_prices(json.loads(args.assets.read_text()), args.start, args.end))
        _save_new(args.output, result)
    except Exception as exc:
        # Connector exceptions can contain URLs or connection settings. Keep the
        # public CLI failure concise; validation errors are controlled messages.
        message = str(exc) if isinstance(exc, (ValueError, FileExistsError)) else type(exc).__name__
        parser.exit(2, f"Collection failed: {message}\n")
    print(json.dumps({"artifact": str(args.output), "records": len(result.get("records", [])),
                      "bars": len(result.get("bars", [])), "rejected": result.get("rejected_records", [])}))


if __name__ == "__main__":
    main()
