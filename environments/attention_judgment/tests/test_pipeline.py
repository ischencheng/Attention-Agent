"""Production observer unit tests plus a temporary real-PostgreSQL integration.

No running agent, live feed, or Kafka broker is implied by these tests.
"""
from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import pipeline
from ledger import Ledger, digest, timestamp


NOW = "2025-06-03T18:00:00+00:00"
LATER = "2025-06-04T18:00:00+00:00"
INSERTED = datetime(2025, 6, 2, 14, tzinfo=timezone.utc)


def record(row_id, payload, topic, *, raw=None):
    return {"id": row_id, "payload": copy.deepcopy(payload), "kafka_topic": topic,
            "kafka_partition": 0, "kafka_offset": row_id + 100,
            "created_at": INSERTED, "raw": copy.deepcopy(raw)}


def fixture():
    raw = {"title": "Fictional capacity report", "fact": "Not an investment recommendation"}
    annotation = {"agent_id": "test-mid", "attention_ticket_ids": [20],
                  "summary": "Original interpretation", "facts": [{"text": "Capacity is unverified"}]}
    snapshot = {"version": 1,
                "annotations": [{"id": 30, "payload": annotation, "sha256": digest(annotation)}],
                "tickets": [{"id": 20, "source": "fictional-feed", "source_id": "release-1",
                             "raw_message_id": 10, "raw_sha256": digest(raw)}],
                "raw_refs": [{"id": 10, "source": "fictional-feed", "source_id": "release-1",
                              "sha256": digest(raw)}]}
    payload = {"agent_id": "test-strong", "title": "Needs further research", "confidence": 0.99,
               "thesis_id": "capacity", "revision": 3, "related_annotations": [30],
               "actions": [{"action": "watch", "ticker": "ABC"}], "evidence_snapshot": snapshot,
               "timestamp": "2000-01-01T00:00:00Z"}
    return {
        "raw_messages": {10: record(10, {"source": "fictional-feed", "source_id": "release-1"}, "source_raw", raw=raw)},
        "attention_tickets": {20: record(20, {"source": "fictional-feed", "source_id": "release-1", "raw": raw}, "attention_ticket", raw=raw)},
        "annotations": {30: record(30, annotation, "annotations")},
        "decision_packets": {40: record(40, payload, "decision_packet")},
    }


class Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = []
        self.index = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, args=()):
        self.conn.queries.append(sql)
        if sql.startswith("SELECT agent_id, state, updated_at FROM"):
            fields = ("agent_id", "state", "updated_at")
        elif sql.startswith("SELECT id, payload FROM"):
            fields = ("id", "payload")
        elif sql.startswith("SELECT id, payload, raw FROM"):
            fields = ("id", "payload", "raw")
        elif sql.startswith("SELECT id, kafka_topic"):
            fields = ("id", "kafka_topic", "kafka_partition", "kafka_offset", "created_at")
        elif sql.startswith("SELECT id, payload, kafka_topic"):
            fields = ("id", "payload", "kafka_topic", "kafka_partition", "kafka_offset", "created_at")
        else:
            raise AssertionError(f"unexpected query {sql}")
        table = sql.split(" FROM ")[1].split()[0]
        values = self.conn.tables[table]
        ids = sorted(args[0] if args else values)
        self.rows = [tuple(copy.deepcopy(values[key][field]) for field in fields)
                     for key in ids if key in values]
        self.index = 0

    def fetchall(self):
        return self.rows

    def fetchmany(self, count):
        result = self.rows[self.index:self.index + count]
        self.index += count
        return result


class Connection:
    def __init__(self):
        self.tables = fixture()
        self.tables["agent_state"] = {}
        self.queries = []
        self.session = None

    def cursor(self):
        return Cursor(self)

    def set_session(self, **kwargs):
        self.session = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "production.sqlite3"
        self.ledger = Ledger(self.path)
        self.conn = Connection()

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    @property
    def row(self):
        return self.conn.tables["decision_packets"][40]

    def freeze(self, now=NOW):
        with patch.object(pipeline, "utcnow", return_value=now):
            return pipeline.freeze_packet(self.conn, self.ledger, self.row)

    def frozen(self):
        return self.ledger.entries(pipeline.DECISION_KIND)[-1]["payload"]

    def test_full_lineage_and_original_payload_are_frozen(self):
        result = self.freeze()
        frozen = self.frozen()
        self.assertEqual(result["decision_id"], 40)
        self.assertEqual(frozen["payload"], self.row["payload"])
        self.assertEqual(frozen["payload_sha256"], digest(self.row["payload"]))
        self.assertEqual(frozen["evidence"]["status"], "verified")
        self.assertEqual(frozen["evidence"]["raw_messages"][0]["raw"],
                         self.conn.tables["raw_messages"][10]["raw"])
        self.assertEqual(frozen["lineage"]["annotations"][0]["id"], 30)
        self.assertEqual(frozen["lineage"]["attention_tickets"][0]["kafka_offset"], 120)
        self.assertEqual(frozen["production_identity"]["kafka_offset"], 140)
        self.assertEqual(frozen["application_thesis_id"], "capacity")
        self.assertEqual(frozen["application_revision"], 3)

    def test_insertion_or_claimed_source_time_never_backdates_entry(self):
        self.freeze()
        frozen = self.frozen()
        self.assertEqual(frozen["available_at"], INSERTED.isoformat())
        self.assertEqual(frozen["evaluation_starts_at"], NOW)
        self.assertEqual(frozen["dates"]["entry"], "2025-06-04")
        self.assertEqual(frozen["dates"]["day"], "2025-06-04")
        self.assertEqual(frozen["dates"]["week"], "2025-06-06")
        self.assertEqual(frozen["dates"]["month"], "2025-06-30")

    def test_latest_mutable_annotation_does_not_replace_published_snapshot(self):
        self.conn.tables["annotations"][30]["payload"]["summary"] = "Revised after publication"
        self.freeze()
        archived = self.frozen()["evidence"]["evidence_snapshot"]["annotations"][0]
        self.assertEqual(archived["payload"]["summary"], "Original interpretation")

    def test_retries_after_restart_keep_original_freeze_time(self):
        first = self.freeze()
        self.ledger.close()
        self.ledger = Ledger(self.path)
        second = self.freeze(LATER)
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(second["hash"], first["hash"])
        self.assertEqual(self.frozen()["frozen_at"], NOW)
        self.assertEqual(len(self.ledger.entries(pipeline.DECISION_KIND)), 1)

    def test_decision_edits_are_new_forward_versions_including_a_revert(self):
        original = self.row["payload"]["title"]
        first = self.freeze()
        self.row["payload"]["title"] = "New conclusion"
        second = self.freeze(LATER)
        self.assertEqual(second["version"], 2)
        self.assertEqual(self.frozen()["previous_version_hash"], first["hash"])
        self.assertEqual(self.frozen()["dates"]["entry"], "2025-06-05")
        self.row["payload"]["title"] = original
        third = self.freeze("2025-06-05T18:00:00+00:00")
        self.assertEqual(third["version"], 3)
        self.assertEqual(self.frozen()["previous_version_hash"], second["hash"])
        self.assertEqual(self.frozen()["dates"]["entry"], "2025-06-06")

    def test_reused_database_id_with_new_kafka_identity_is_rejected(self):
        self.freeze()
        self.row["kafka_offset"] += 1
        with self.assertRaisesRegex(ValueError, "reused"):
            self.freeze(LATER)
        self.assertEqual(len(self.ledger.entries(pipeline.DECISION_KIND)), 1)

    def test_changed_authoritative_raw_or_broken_snapshot_cannot_be_evaluated(self):
        self.conn.tables["raw_messages"][10]["raw"]["title"] = "Tampered original"
        with self.assertRaisesRegex(ValueError, "unverified"):
            self.freeze()
        self.assertEqual(self.ledger.entries(), [])
        self.conn = Connection()
        self.row["payload"]["evidence_snapshot"]["annotations"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            self.freeze()
        self.assertEqual(self.ledger.entries(), [])

    def test_legacy_unverified_decisions_are_rejected_without_fake_evidence(self):
        del self.row["payload"]["evidence_snapshot"]
        with self.assertRaisesRegex(ValueError, "No historical"):
            self.freeze()
        self.assertEqual(self.ledger.entries(), [])

    def test_future_database_availability_and_missing_lineage_are_rejected(self):
        self.row["created_at"] = timestamp("2025-06-04T18:00:00Z")
        with self.assertRaisesRegex(ValueError, "availability"):
            self.freeze()
        self.conn = Connection()
        del self.conn.tables["attention_tickets"][20]
        with self.assertRaisesRegex(ValueError, "missing attention_tickets"):
            self.freeze()
        self.assertEqual(self.ledger.entries(), [])

    def test_sync_uses_read_only_snapshot_and_reports_failed_graphs(self):
        invalid = copy.deepcopy(self.row)
        invalid["id"] = 41
        invalid["payload"].pop("evidence_snapshot")
        self.conn.tables["decision_packets"][41] = invalid
        with patch.object(pipeline, "utcnow", return_value=NOW):
            result = pipeline.sync(self.conn, self.ledger)
        self.assertEqual(self.conn.session, {"isolation_level": "REPEATABLE READ", "readonly": True})
        self.assertEqual(len(result["frozen"]), 1)
        self.assertEqual(result["rejected"][0]["decision_id"], 41)
        self.assertTrue(all(query.startswith("SELECT") for query in self.conn.queries))

    def test_research_without_instrument_or_probability_is_valid(self):
        self.row["payload"]["actions"] = ["Research NVDA bullish: very confident"]
        self.freeze()
        self.assertEqual(self.frozen()["symbols"], [])
        self.assertEqual(self.frozen()["forecasts"], [])
        summary = pipeline.report(self.ledger)
        self.assertEqual(summary["decision_packets"], 1)
        self.assertIsNone(summary["mean_brier"])

    def test_direct_rss_judgments_are_not_counted_as_pipeline_decisions(self):
        self.ledger.append("judgment", "prior-rss", {"mode": "forward", "symbol": "ABC"})
        self.assertEqual(pipeline.report(self.ledger)["decision_packets"], 0)
        self.assertEqual(pipeline.report(self.ledger)["resolved_predeclared_forecasts"], 0)

    def settle(self, outcome):
        def snapshot(symbol, start, end, as_of):
            return {"symbol": symbol, "start": start, "end": end, "as_of": as_of.isoformat(), "bars": []}
        with patch.object(pipeline, "fetch_prices", side_effect=snapshot), \
                patch.object(pipeline, "evaluate_horizon", return_value=outcome):
            return pipeline.settle(self.ledger, as_of="2025-07-01T18:00:00Z")

    def test_passive_prices_are_not_a_successful_forecast(self):
        self.freeze()
        self.settle({"status": "resolved", "excess_return": 0.2})
        summary = pipeline.report(self.ledger)
        self.assertEqual(summary["resolved_price_observations"], 3)
        self.assertEqual(summary["resolved_predeclared_forecasts"], 0)
        self.assertIsNone(summary["mean_brier"])

    def test_pending_horizons_never_fetch_prices(self):
        self.freeze()
        with patch.object(pipeline, "fetch_prices") as fetch:
            result = pipeline.settle(self.ledger, as_of=NOW)
        fetch.assert_not_called()
        self.assertEqual(len(result), 3)
        self.assertTrue(all(item["status"] == "pending" for item in result))

    def test_changed_evaluator_contract_cannot_silently_resolve_old_decisions(self):
        self.freeze()
        contract = pipeline.market_contract()
        contract["version"] += 1
        with patch.object(pipeline, "market_contract", return_value=contract), \
                patch.object(pipeline, "fetch_prices") as fetch:
            result = pipeline.settle(self.ledger, as_of="2025-07-01T18:00:00Z")
        fetch.assert_not_called()
        self.assertEqual(result[0]["status"], "unresolved")
        self.assertIn("explicit migration", result[0]["error"])
        self.assertEqual(self.ledger.entries(pipeline.SETTLEMENT_KIND), [])

    def test_missing_price_retries_then_first_settlement_is_frozen(self):
        self.row["payload"]["forecasts"] = [{"symbol": "ABC", "horizon": "week",
                                            "p_outperform_spy": 0.75, "reason": "Fictional forecast"}]
        self.freeze()
        self.settle({"status": "unresolved", "reason": "missing bars"})
        self.assertEqual(self.ledger.entries(pipeline.SETTLEMENT_KIND), [])
        self.settle({"status": "resolved", "excess_return": 0.1})
        initial = self.ledger.entries(pipeline.SETTLEMENT_KIND)
        self.assertEqual(len(initial), 3)
        self.ledger.close()
        self.ledger = Ledger(self.path)
        self.settle({"status": "resolved", "excess_return": -0.5})
        self.assertEqual(self.ledger.entries(pipeline.SETTLEMENT_KIND), initial)
        self.assertEqual(pipeline.report(self.ledger)["resolved_predeclared_forecasts"], 1)
        self.assertAlmostEqual(pipeline.report(self.ledger)["mean_brier"], 0.0625)


class ResearchStateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "research.sqlite3"
        self.ledger = Ledger(self.path)
        self.row = {"agent_id": "claude-strong", "updated_at": INSERTED,
                    "state": {"theses": {"capacity": {"status": "research_only", "claim": "Sensitive original research"}},
                              "pending_themes": ["needs more evidence"], "unknown_fields": {"preserve": [1, False, None]}}}

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def freeze(self, now=NOW):
        with patch.object(pipeline, "utcnow", return_value=now):
            return pipeline.freeze_research_state(self.ledger, self.row)

    def test_whole_state_is_frozen_but_report_returns_summary_only(self):
        result = self.freeze()
        entry = self.ledger.entries(pipeline.RESEARCH_KIND)[0]
        self.assertEqual(entry["payload"]["state"], self.row["state"])
        self.assertEqual(entry["payload"]["state_sha256"], digest(self.row["state"]))
        self.assertEqual(entry["payload"]["updated_at"], INSERTED.isoformat())
        self.assertEqual(entry["payload"]["frozen_at"], NOW)
        self.assertEqual(entry["payload"]["evaluation_status"], "unscored_application_research")
        self.assertEqual(result["thesis_count"], 1)
        report = pipeline.report(self.ledger)
        self.assertEqual(report["research_state_agents"], 1)
        self.assertEqual(report["research_state_versions"], 1)
        self.assertEqual(report["latest_research_states"][0]["pending_theme_count"], 1)
        self.assertNotIn("state", report["latest_research_states"][0])
        self.assertNotIn("Sensitive original research", str(report))
        self.assertEqual(self.ledger.verify(), entry["hash"])

    def test_identical_content_is_idempotent_after_restart_and_timestamp_update(self):
        first = self.freeze()
        self.ledger.close()
        self.ledger = Ledger(self.path)
        self.row["updated_at"] = timestamp("2025-06-03T17:00:00Z")
        second = self.freeze(LATER)
        self.assertEqual(second["status"], "unchanged")
        self.assertEqual(second["hash"], first["hash"])
        self.assertEqual(second["frozen_at"], NOW)
        self.assertEqual(len(self.ledger.entries(pipeline.RESEARCH_KIND)), 1)

    def test_mutation_and_reversion_append_versions_without_erasing_prior_reasoning(self):
        original = copy.deepcopy(self.row["state"])
        first = self.freeze()
        self.row["state"]["theses"]["capacity"]["status"] = "invalidated"
        second = self.freeze(LATER)
        self.row["state"] = original
        third = self.freeze("2025-06-05T18:00:00Z")
        entries = self.ledger.entries(pipeline.RESEARCH_KIND)
        self.assertEqual([e["payload"]["version"] for e in entries], [1, 2, 3])
        self.assertEqual(entries[0]["payload"]["state"]["theses"]["capacity"]["status"], "research_only")
        self.assertEqual(entries[1]["payload"]["previous_version_hash"], first["hash"])
        self.assertEqual(entries[2]["payload"]["previous_version_hash"], second["hash"])
        self.assertNotEqual(third["hash"], first["hash"])

    def test_research_symbols_and_probabilities_never_create_predictions_or_prices(self):
        self.row["state"].update({"symbol": "ABC", "forecasts": [
            {"symbol": "ABC", "horizon": "day", "p_outperform_spy": 0.99, "reason": "Unpublished state"}]})
        self.freeze()
        with patch.object(pipeline, "fetch_prices") as fetch:
            result = pipeline.settle(self.ledger, as_of="2025-07-01T18:00:00Z")
        fetch.assert_not_called()
        self.assertEqual(result, [])
        report = pipeline.report(self.ledger)
        self.assertEqual(report["decision_packets"], 0)
        self.assertEqual(report["resolved_price_observations"], 0)
        self.assertEqual(report["resolved_predeclared_forecasts"], 0)
        self.assertIsNone(report["mean_brier"])
        self.assertEqual(self.ledger.entries(pipeline.DECISION_KIND), [])

    def test_older_observer_finishing_later_cannot_replace_newer_research(self):
        older_snapshot = copy.deepcopy(self.row)
        self.row["state"]["theses"]["capacity"]["status"] = "invalidated"
        self.row["updated_at"] = timestamp("2025-06-04T17:00:00Z")
        newer = self.freeze(LATER)
        self.row = older_snapshot
        with self.assertRaisesRegex(ValueError, "older than the latest"):
            self.freeze("2025-06-05T18:00:00Z")
        entries = self.ledger.entries(pipeline.RESEARCH_KIND)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["payload"]["state"]["theses"]["capacity"]["status"], "invalidated")
        self.assertEqual(pipeline.report(self.ledger)["latest_research_states"][0]["hash"], newer["hash"])

    def test_unknown_mid_keys_future_times_and_nonobject_states_are_rejected(self):
        self.row["agent_id"] = "claude-mid"
        with self.assertRaisesRegex(ValueError, "Strong"):
            self.freeze()
        self.row["agent_id"] = "claude-strong"
        self.row["updated_at"] = timestamp("2025-06-05T18:00:00Z")
        with self.assertRaisesRegex(ValueError, "after observer"):
            self.freeze()
        self.row["updated_at"] = INSERTED
        self.row["state"] = ["not an object"]
        with self.assertRaisesRegex(ValueError, "JSON object"):
            self.freeze()
        self.assertEqual(self.ledger.entries(), [])

    def test_sync_only_reads_known_strong_keys_and_configured_alias(self):
        conn = Connection()
        conn.tables["decision_packets"] = {}
        for key in ("claude-strong", "codex-strong", "custom-researcher", "claude-mid", "unrelated-strong"):
            conn.tables["agent_state"][key] = {**copy.deepcopy(self.row), "agent_id": key}
        with patch.dict(pipeline.os.environ, {"STRONG_AGENT_ID": "custom-researcher"}), \
                patch.object(pipeline, "utcnow", return_value=NOW):
            result = pipeline.sync(conn, self.ledger)
        self.assertEqual({entry["agent_id"] for entry in result["research_frozen"]},
                         {"claude-strong", "codex-strong", "custom-researcher"})
        self.assertEqual(result["research_rejected"], [])
        self.assertEqual(result["frozen"], [])
        self.assertEqual(conn.session, {"isolation_level": "REPEATABLE READ", "readonly": True})


class TargetTests(unittest.TestCase):
    def test_multiple_explicit_instrument_formats_and_no_prose_inference(self):
        symbols, forecasts, errors = pipeline.evaluation_targets({
            "related_tickers": ["ABC", None, "invalid symbol"],
            "entities": [{"name": "NVDA in prose", "symbol": "DEF"}, {"symbol": None}],
            "actions": [{"ticker": "GHI"}, "BUY NVDA"], "confidence": 1.0,
        })
        self.assertEqual(symbols, ["ABC", "DEF", "GHI"])
        self.assertEqual(forecasts, [])
        self.assertEqual(errors, [])

    def test_invalid_and_ambiguous_probabilities_are_never_scored(self):
        valid = {"symbol": "ABC", "horizon": "day", "p_outperform_spy": 0.6, "reason": "Test"}
        for bad in (True, float("nan"), 1.1, -0.1, "0.9"):
            with self.subTest(probability=bad):
                _, forecasts, errors = pipeline.evaluation_targets({"forecasts": [{**valid, "p_outperform_spy": bad}]})
                self.assertEqual(forecasts, [])
                self.assertTrue(errors)
        _, forecasts, errors = pipeline.evaluation_targets({"forecasts": [{**valid, "benchmark": "QQQ"}]})
        self.assertEqual(forecasts, [])
        self.assertTrue(errors)
        _, forecasts, errors = pipeline.evaluation_targets({"forecasts": [valid, {**valid, "p_outperform_spy": 0.9}]})
        self.assertEqual(forecasts, [])
        self.assertTrue(errors)


class PostgresBridgeTests(unittest.TestCase):
    def test_concurrent_observers_lock_before_snapshot_even_when_content_reverts(self):
        import pgserver
        import psycopg2
        from psycopg2.extras import Json

        with tempfile.TemporaryDirectory(prefix="attention-observer-order-test-") as directory:
            server = pgserver.get_server(Path(directory) / "postgres", cleanup_mode="stop")
            ledger_path = Path(directory) / "production.sqlite3"
            ledger = Ledger(ledger_path)
            original = {"theses": {"capacity": {"status": "research_only"}}}
            changed = {"theses": {"capacity": {"status": "invalidated"}}}
            older_ready = threading.Event()
            release_older = threading.Event()
            newer_attempting = threading.Event()
            newer_snapshot = threading.Event()
            freeze = pipeline.freeze_research_state

            def pause_older(observer_ledger, row, **kwargs):
                if row["state"] == changed:
                    older_ready.set()
                    if not release_older.wait(10):
                        raise RuntimeError("older observer was not released")
                return freeze(observer_ledger, row, **kwargs)

            def observe(role):
                class ObservedCursor(psycopg2.extensions.cursor):
                    def execute(self, sql, *args, **kwargs):
                        result = super().execute(sql, *args, **kwargs)
                        if role == "newer" and sql.startswith("SELECT id, payload, kafka_topic"):
                            newer_snapshot.set()
                        return result

                observer_ledger = Ledger(ledger_path)
                conn = psycopg2.connect(server.get_uri(), cursor_factory=ObservedCursor)
                try:
                    if role == "newer":
                        newer_attempting.set()
                    return pipeline.sync(conn, observer_ledger)
                finally:
                    conn.close()
                    observer_ledger.close()

            def update(state, updated_at):
                with psycopg2.connect(server.get_uri()) as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE agent_state SET state=%s,updated_at=%s WHERE agent_id='claude-strong'",
                                    (Json(state), timestamp(updated_at)))

            try:
                with psycopg2.connect(server.get_uri()) as setup:
                    with setup.cursor() as cur:
                        cur.execute((pipeline.ROOT / "postgres/init/001_schema.sql").read_text())
                        cur.execute("INSERT INTO agent_state(agent_id,state,updated_at) VALUES(%s,%s,%s)",
                                    ("claude-strong", Json(original), INSERTED))
                observe("initial")
                update(changed, "2025-06-03T17:00:00Z")
                with patch.object(pipeline, "freeze_research_state", side_effect=pause_older), \
                        ThreadPoolExecutor(max_workers=2) as executor:
                    older = executor.submit(observe, "older")
                    try:
                        self.assertTrue(older_ready.wait(5), "older observer did not read its snapshot")
                        # A later A observation would deduplicate against the
                        # initial A if it could overtake the paused B observer.
                        update(original, "2025-06-04T17:00:00Z")
                        newer = executor.submit(observe, "newer")
                        self.assertTrue(newer_attempting.wait(5), "newer observer did not attempt sync")
                        self.assertFalse(newer_snapshot.wait(0.25),
                                         "newer observer acquired a snapshot before the older sync completed")
                    finally:
                        release_older.set()
                    self.assertEqual(older.result(timeout=5)["research_rejected"], [])
                    self.assertEqual(newer.result(timeout=5)["research_rejected"], [])
                self.assertTrue(newer_snapshot.is_set())
                entries = ledger.entries(pipeline.RESEARCH_KIND)
                self.assertEqual([e["payload"]["state"] for e in entries], [original, changed, original])
                self.assertEqual([e["payload"]["version"] for e in entries], [1, 2, 3])
                self.assertEqual(pipeline.report(ledger)["latest_research_states"][0]["updated_at"],
                                 "2025-06-04T17:00:00+00:00")
            finally:
                release_older.set()
                ledger.close()
                server.cleanup()

    def test_real_database_snapshot_and_restart_safe_versioning(self):
        import pgserver
        import psycopg2
        from psycopg2.extras import Json

        with tempfile.TemporaryDirectory(prefix="attention-pipeline-test-") as directory:
            server = pgserver.get_server(Path(directory) / "postgres", cleanup_mode="stop")
            ledger = Ledger(Path(directory) / "production.sqlite3")
            conn = None
            try:
                with psycopg2.connect(server.get_uri()) as setup:
                    with setup.cursor() as cur:
                        cur.execute((pipeline.ROOT / "postgres/init/001_schema.sql").read_text())
                        cur.execute((pipeline.ROOT / "postgres/init/002_evidence.sql").read_text())
                        for table, rows in fixture().items():
                            for row in rows.values():
                                cur.execute(
                                    f"INSERT INTO {table} (id, payload, raw, kafka_topic, kafka_partition, kafka_offset, created_at) "
                                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                                    (row["id"], Json(row["payload"]), Json(row["raw"]), row["kafka_topic"],
                                     row["kafka_partition"], row["kafka_offset"], row["created_at"]),
                                )
                conn = psycopg2.connect(server.get_uri())
                with patch.object(pipeline, "utcnow", return_value=NOW):
                    first = pipeline.sync(conn, ledger)
                self.assertEqual(first["rejected"], [])
                self.assertEqual(len(first["frozen"]), 1)
                with conn.cursor() as cur:
                    cur.execute("SHOW transaction_read_only")
                    self.assertEqual(cur.fetchone()[0], "on")
                conn.rollback()
                with psycopg2.connect(server.get_uri()) as editor:
                    with editor.cursor() as cur:
                        cur.execute("UPDATE annotations SET payload=jsonb_set(payload,'{summary}',%s) WHERE id=30",
                                    (Json("Today's different interpretation"),))
                        cur.execute("UPDATE decision_packets SET payload=jsonb_set(payload,'{title}',%s) WHERE id=40",
                                    (Json("Updated decision"),))
                ledger.close()
                ledger = Ledger(Path(directory) / "production.sqlite3")
                with patch.object(pipeline, "utcnow", return_value=LATER):
                    second = pipeline.sync(conn, ledger)
                self.assertEqual(second["rejected"], [])
                self.assertEqual(second["frozen"][0]["version"], 2)
                frozen = ledger.entries(pipeline.DECISION_KIND)
                self.assertEqual(frozen[1]["payload"]["previous_version_hash"], frozen[0]["hash"])
                self.assertEqual(frozen[1]["payload"]["evidence"]["evidence_snapshot"]["annotations"][0]["payload"]["summary"],
                                 "Original interpretation")
                self.assertEqual(frozen[0]["payload"]["dates"]["entry"], "2025-06-04")
                self.assertEqual(frozen[1]["payload"]["dates"]["entry"], "2025-06-05")
            finally:
                if conn is not None:
                    conn.close()
                ledger.close()
                server.cleanup()


    def test_real_strong_state_without_decisions_is_frozen_and_mutations_versioned(self):
        import pgserver
        import psycopg2
        from psycopg2.extras import Json

        with tempfile.TemporaryDirectory(prefix="attention-research-test-") as directory:
            server = pgserver.get_server(Path(directory) / "postgres", cleanup_mode="stop")
            ledger_path = Path(directory) / "production.sqlite3"
            ledger = Ledger(ledger_path)
            conn = None
            original = {"theses": {"capacity": {"status": "research_only", "claim": "Original reasoning"}}}
            changed = {"theses": {"capacity": {"status": "weakened", "claim": "New counterevidence"}}}
            try:
                with psycopg2.connect(server.get_uri()) as setup:
                    with setup.cursor() as cur:
                        cur.execute((pipeline.ROOT / "postgres/init/001_schema.sql").read_text())
                        for key in ("claude-mid", "claude-strong"):
                            cur.execute("INSERT INTO agent_state(agent_id,state,updated_at) VALUES(%s,%s,%s)",
                                        (key, Json(original), INSERTED))
                conn = psycopg2.connect(server.get_uri())
                with patch.object(pipeline, "utcnow", return_value=NOW):
                    first = pipeline.sync(conn, ledger)
                self.assertEqual(first["frozen"], [])
                self.assertEqual(len(first["research_frozen"]), 1)
                self.assertEqual(first["research_frozen"][0]["agent_id"], "claude-strong")
                ledger.close()
                ledger = Ledger(ledger_path)
                with patch.object(pipeline, "utcnow", return_value=LATER):
                    repeated = pipeline.sync(conn, ledger)
                self.assertEqual(repeated["research_frozen"], [])
                self.assertEqual(repeated["research_unchanged"], 1)
                with psycopg2.connect(server.get_uri()) as editor:
                    with editor.cursor() as cur:
                        cur.execute("UPDATE agent_state SET state=%s,updated_at=%s WHERE agent_id='claude-strong'",
                                    (Json(changed), timestamp("2025-06-04T20:00:00Z")))
                with patch.object(pipeline, "utcnow", return_value="2025-06-05T18:00:00Z"):
                    revised = pipeline.sync(conn, ledger)
                self.assertEqual(revised["research_frozen"][0]["version"], 2)
                entries = ledger.entries(pipeline.RESEARCH_KIND)
                self.assertEqual(entries[0]["payload"]["state"], original)
                self.assertEqual(entries[1]["payload"]["state"], changed)
                self.assertEqual(entries[1]["payload"]["previous_version_hash"], entries[0]["hash"])
                summary = pipeline.report(ledger)
                self.assertEqual(summary["research_state_versions"], 2)
                self.assertEqual(summary["latest_research_states"][0]["version"], 2)
                self.assertEqual(summary["decision_packets"], 0)
                self.assertEqual(summary["resolved_predeclared_forecasts"], 0)
                self.assertIsNone(summary["mean_brier"])
            finally:
                if conn is not None:
                    conn.close()
                ledger.close()
                server.cleanup()


if __name__ == "__main__":
    unittest.main()
