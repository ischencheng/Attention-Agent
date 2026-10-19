"""Recorded timings, safe OTLP projection and restart/retry receipt contracts."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "skills"))
import observability_exporter as exporter
from runtime_traces import trace_identity

START = "2026-10-19T10:00:00.123456+00:00"
END = "2026-10-19T10:00:03.123456+00:00"


def routing(number=1, **extra):
    return {"event_id": f"{number:064x}", "stage": "fast", "outcome": "reject",
            "started_at": START, "completed_at": END,
            "input": {"topic": "fastagent_input", "partition": 0, "offset": number},
            "classifier": {"model": "gpt-6-luna", "provider": "codex", "classified_by_model": True,
                           "cached": False, "usage": {"input_tokens": 8, "output_tokens": 2}}, **extra}


def cycle(key="20261019-mid-a", **extra):
    return {"cycle_id": key, "role": "mid", "event": "completed", "started_at": START,
            "completed_at": END, "model": "gpt-6-luna", "selected_input_ids": [3, 4],
            "reviewed_input_ids": [3], "state_verified": True, **extra}


def attributes(span):
    return {item["key"]: next(iter(item["value"].values()))
            for item in span["resource"]["scopeSpans"][0]["spans"][0]["attributes"]}


def response(code=200, body=None):
    value = Mock(status_code=code)
    value.__enter__ = Mock(return_value=value)
    value.__exit__ = Mock(return_value=False)
    value.iter_content.return_value = [json.dumps({} if body is None else body).encode()]
    return value


class ProjectionTests(unittest.TestCase):
    def test_identity_stable_separated_and_nonzero(self):
        identity = trace_identity("routing", "abc")
        self.assertEqual(identity, trace_identity("routing", "abc"))
        self.assertNotEqual(identity, trace_identity("cycle", "abc"))
        self.assertNotEqual(identity, trace_identity("routing", "def"))
        self.assertEqual(len(identity["trace_id"]), 32)
        self.assertEqual(len(identity["span_id"]), 16)
        self.assertGreater(int(identity["span_id"], 16), 0)
        for kind, key in (("unknown", "a"), ("routing", ""), ("cycle", "a\nb")):
            with self.assertRaises(ValueError):
                trace_identity(kind, key)

    def test_recorded_nanosecond_duration_and_rejection_is_not_execution_error(self):
        record = routing()
        item = exporter.audit_span("routing", record["event_id"], record)
        span = item["resource"]["scopeSpans"][0]["spans"][0]
        self.assertEqual(int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"]), 3_000_000_000)
        self.assertTrue(span["startTimeUnixNano"].endswith("123456000"))
        self.assertEqual(span["name"], "fast.recorded_handling")
        self.assertEqual(span["status"]["code"], 1)
        self.assertNotIn("parentSpanId", span)
        self.assertNotIn("events", span)
        self.assertEqual(item["resource"]["resource"]["attributes"][0]["value"]["stringValue"], "attention-routing")
        for stage in ("relay", "filter", "fast"):
            projected = exporter.audit_span("routing", record["event_id"], {**record, "stage": stage})
            expected = ("recorded_handling_through_delivery_before_audit_and_commit" if stage == "relay"
                        else "recorded_handling_before_delivery_and_commit")
            self.assertEqual(attributes(projected)["attention.timing_scope"], expected)

    def test_privacy_allowlist_and_cached_usage_is_historical(self):
        secret = "PRIVATE-CANARY token=secret sk-aaaaaaaaaaaaaaaaaaaa"
        record = routing(reason=secret, source_id=secret, raw={"text": secret}, trace={"traceparent": secret},
                         classifier={"model": "gpt-6-luna", "provider": secret, "reason": secret, "cached": True,
                                     "usage": {"input_tokens": 8, "output_tokens": -3, "total_tokens": True, "api_key": secret}})
        item = exporter.audit_span("routing", record["event_id"], record)
        attrs = attributes(item)
        self.assertNotIn("PRIVATE-CANARY", json.dumps(item))
        self.assertNotIn("sk-", json.dumps(item))
        self.assertEqual(attrs["attention.historical_usage.input_tokens"], "8")
        self.assertNotIn("attention.reported_usage.input_tokens", attrs)
        self.assertNotIn("attention.provider", attrs)

    def test_cycle_failures_keep_real_window_without_tool_timing(self):
        record = cycle(event="failed", failed_at=END, error_type="TimeoutError", summary="PRIVATE-CANARY",
                       tool_event_count=2, tool_events=[{"command": "PRIVATE-CANARY", "timestamp": None}],
                       selected_input_ids=[3, "PRIVATE-CANARY", True], usage={"output_tokens": 12})
        item = exporter.audit_span("cycle", record["cycle_id"], record)
        span = item["resource"]["scopeSpans"][0]["spans"][0]
        self.assertEqual(span["status"]["code"], 2)
        self.assertEqual(span["name"], "agent.recorded_cycle")
        self.assertEqual(attributes(item)["attention.selected_input_ids"], "3")
        self.assertEqual(attributes(item)["attention.tool_timing"], "not_recorded")
        self.assertNotIn("PRIVATE-CANARY", json.dumps(item))
        self.assertNotIn("events", span)

    def test_invalid_timing_is_never_replaced_by_export_time(self):
        for change in ({"started_at": None}, {"started_at": "2026-10-19T10:00:00"}, {"completed_at": "2026-10-18T10:00:00Z"}):
            record = routing(**change)
            with self.assertRaises(ValueError):
                exporter.audit_span("routing", record["event_id"], record)


class TransportTests(unittest.TestCase):
    def setUp(self):
        value = routing()
        self.item = exporter.audit_span("routing", value["event_id"], value)

    def test_json_otlp_post_uses_bounded_timeout_and_no_redirect(self):
        session = Mock()
        session.post.return_value = response()
        exporter.send_batch("http://localhost/v1/traces", [self.item], session)
        args = session.post.call_args.kwargs
        self.assertEqual(args["headers"]["Content-Type"], "application/json")
        self.assertFalse(args["allow_redirects"])
        self.assertEqual(args["timeout"], (3, 10))
        self.assertEqual(args["json"]["resourceSpans"][0], self.item["resource"])

    def test_partial_success_not_acknowledged_or_automatically_retried(self):
        session = Mock()
        session.post.return_value = response(body={"partialSuccess": {"rejectedSpans": "1", "errorMessage": "PRIVATE-CANARY"}})
        with self.assertRaises(exporter.ExportError) as caught:
            exporter.send_batch("http://localhost/v1/traces", [self.item], session)
        self.assertEqual(caught.exception.error_type, "RejectedSpans")
        self.assertFalse(caught.exception.retryable)
        self.assertNotIn("PRIVATE-CANARY", str(caught.exception))
        session.post.return_value = response(body={"partialSuccess": {"rejectedSpans": "0", "errorMessage": "PRIVATE-CANARY"}})
        exporter.send_batch("http://localhost/v1/traces", [self.item], session)

    def test_response_failure_categories_and_body_bound(self):
        for code, retryable in ((503, True), (429, True), (400, False), (302, False)):
            session = Mock()
            session.post.return_value = response(code)
            with self.assertRaises(exporter.ExportError) as caught:
                exporter.send_batch("http://localhost/v1/traces", [self.item], session)
            self.assertEqual(caught.exception.retryable, retryable)
        session.post.side_effect = requests.Timeout("PRIVATE-CANARY")
        with self.assertRaises(exporter.ExportError) as caught:
            exporter.send_batch("http://localhost/v1/traces", [self.item], session)
        self.assertEqual(str(caught.exception), "Timeout")
        session.post.side_effect = None
        value = response()
        value.iter_content.return_value = [b"x" * (exporter.MAX_RESPONSE + 1)]
        session.post.return_value = value
        with self.assertRaisesRegex(exporter.ExportError, "OversizedResponse"):
            exporter.send_batch("http://localhost/v1/traces", [self.item], session)


class ExporterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.rows = [routing(1), routing(2), routing(3)]
        self.session = Mock()
        self.session.post.side_effect = lambda *a, **k: response()
        self.instances = []

    def tearDown(self):
        for instance in self.instances:
            instance.close()
        self.tmp.cleanup()

    def reader(self, after, limit, *, replay=False):
        rows = [(index + 1, row["event_id"], row) for index, row in enumerate(self.rows) if replay or index + 1 > after]
        if replay:
            rows.reverse()
        return rows[:limit], len(rows) > limit

    def make(self, **kwargs):
        instance = exporter.Exporter(self.root / "exports", self.root / "cycles", "http://localhost/v1/traces",
                                     batch_size=kwargs.get("batch_size", 2), routing_reader=kwargs.get("reader", self.reader), session=self.session)
        self.instances.append(instance)
        return instance

    def write_cycle(self, key, event, **extra):
        folder = self.root / "cycles" / key
        folder.mkdir(parents=True, exist_ok=True)
        value = cycle(key, event=event, **extra)
        (folder / f"{event}.json").write_text(json.dumps(value))

    def test_bounded_progress_restart_dedup_and_readonly_receipt(self):
        instance = self.make()
        first = instance.run_once()
        self.assertEqual(first["exported_counts"], {"routing": 2, "cycle": 0})
        self.assertTrue(first["scan_truncated"]["routing"])
        second = self.make().run_once()
        self.assertEqual(second["exported_counts"]["routing"], 3)
        instance.run_once()  # Caught up: preserve the last immutable routing ID.
        instance.run_once()
        self.assertEqual(self.session.post.call_count, 2)
        self.assertEqual(instance.state("cursor.routing"), "3")
        self.rows.append(routing(4))
        self.assertEqual(instance.run_once()["exported_counts"]["routing"], 4)
        db = sqlite3.connect(f"file:{self.root / 'exports/exports.sqlite3'}?mode=ro", uri=True)
        try:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(db.execute("SELECT count(*) FROM exports").fetchone()[0], 4)
        finally:
            db.close()

    def test_transient_failure_preserves_unacknowledged_window_then_recovers(self):
        self.session.post.side_effect = requests.ConnectionError("PRIVATE-CANARY")
        instance = self.make()
        status = instance.run_once()
        self.assertEqual(status["status"], "degraded")
        self.assertEqual(status["exported_counts"]["routing"], 0)
        self.assertEqual(instance.state("cursor.routing", "0"), "0")
        self.assertNotIn("PRIVATE-CANARY", (self.root / "exports/status.json").read_text())
        self.session.post.side_effect = lambda *a, **k: response()
        status = self.make().run_once()
        self.assertEqual(status["exported_counts"]["routing"], 2)
        self.assertEqual(status["status"], "healthy")

    def test_permanent_rejection_requires_explicit_bounded_replay(self):
        self.session.post.side_effect = lambda *a, **k: response(body={"partialSuccess": {"rejectedSpans": 1}})
        instance = self.make()
        status = instance.run_once()
        self.assertEqual(status["exported_counts"]["routing"], 0)
        self.assertEqual(status["blocked_counts"]["routing"], 2)
        self.session.post.side_effect = lambda *a, **k: response()
        instance.run_once()  # Later work continues.
        instance.run_once()  # Wrap.
        instance.run_once()  # Blocked records are not automatically retried.
        self.assertEqual(self.session.post.call_count, 2)
        status = instance.run_once(replay=3)
        self.assertEqual(status["exported_counts"]["routing"], 3)
        self.assertEqual(status["blocked_counts"]["routing"], 0)
        self.assertEqual(status["status"], "healthy")
        self.assertEqual(len(self.session.post.call_args.kwargs["json"]["resourceSpans"]), 3)

    def test_missing_terminal_and_partial_file_revisited_after_cursor_wrap(self):
        self.rows = []
        key = "20261019-mid-a"
        self.write_cycle(key, "started")
        instance = self.make()
        first = instance.run_once()
        self.assertEqual(first["pending_counts"]["cycle"], 1)
        self.assertEqual(first["exported_counts"]["cycle"], 0)
        folder = self.root / "cycles" / key
        (folder / "completed.json").write_text('{"incomplete":')
        instance.run_once()  # Cursor reset.
        self.assertEqual(instance.run_once()["blocked_counts"]["cycle"], 0)
        self.write_cycle(key, "completed")
        instance.run_once()
        final = instance.run_once()
        self.assertEqual(final["exported_counts"]["cycle"], 1)
        self.assertEqual(final["recent_traces"][0]["key"], key)

    def test_empty_cycle_folder_and_late_older_attempt_are_discovered(self):
        self.rows = []
        key = "20261019-mid-b"
        (self.root / "cycles" / key).mkdir(parents=True)
        instance = self.make()
        self.assertEqual(instance.run_once()["blocked_counts"]["cycle"], 0)
        self.write_cycle(key, "started")
        self.write_cycle(key, "completed")
        self.assertEqual(instance.run_once()["exported_counts"]["cycle"], 1)
        # This older ID first becomes visible after the newer cycle exported.
        self.write_cycle("20261019-mid-a", "started")
        self.write_cycle("20261019-mid-a", "completed")
        self.assertEqual(instance.run_once()["exported_counts"]["cycle"], 2)

    def test_unfinished_cycles_and_new_cycles_both_progress_under_one_record_budget(self):
        self.rows = []
        key = "20261019-mid-a"
        self.write_cycle(key, "started")
        instance = self.make(batch_size=1)
        instance.run_once()
        self.write_cycle("20261019-mid-b", "started")
        self.write_cycle("20261019-mid-b", "completed")
        instance.run_once()  # Pending queue turn.
        self.assertEqual(instance.run_once()["exported_counts"]["cycle"], 1)
        self.write_cycle(key, "completed")
        self.assertEqual(instance.run_once()["exported_counts"]["cycle"], 2)

    def test_failure_and_retry_are_distinct_cycles_and_public_output_stays_bounded(self):
        self.rows = []
        self.write_cycle("20261019-mid-failed", "started")
        self.write_cycle("20261019-mid-failed", "failed", failed_at=END, error_type="TimeoutError")
        self.write_cycle("20261019-mid-retry", "started")
        self.write_cycle("20261019-mid-retry", "completed")
        status = self.make().run_once()
        self.assertEqual(status["exported_counts"]["cycle"], 2)
        self.assertEqual(len({row["trace_id"] for row in status["recent_traces"]}), 2)
        self.assertEqual(status["pending_scope"], "scanned_window")
        self.assertEqual(status["receipt_scope"], "collector_accepted")

    def test_postgres_failure_does_not_hide_independent_cycle_export(self):
        self.write_cycle("20261019-mid-a", "started")
        self.write_cycle("20261019-mid-a", "completed")
        status = self.make(reader=Mock(side_effect=RuntimeError("PRIVATE-CANARY"))).run_once()
        self.assertEqual(status["status"], "degraded")
        self.assertEqual(status["error_type"], "RuntimeError")
        self.assertEqual(status["exported_counts"]["cycle"], 1)
        self.assertNotIn("PRIVATE-CANARY", json.dumps(status))

    def test_single_instance_lock_rejects_concurrent_writer(self):
        with exporter.single_instance(self.root):
            with self.assertRaises(BlockingIOError):
                with exporter.single_instance(self.root):
                    self.fail("second instance acquired the lock")

    def test_business_database_connection_is_read_only(self):
        with patch.object(exporter.psycopg2, "connect") as connect:
            connection = exporter.connect_db()
        connection.set_session.assert_called_once_with(readonly=True)


if __name__ == "__main__":
    unittest.main()
