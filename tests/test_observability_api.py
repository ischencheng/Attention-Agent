"""Read-only observability projections: receipts, freshness and safe failures."""
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
from runtime_traces import trace_identity

spec = importlib.util.spec_from_file_location("observability_trace_api_test", ROOT / "backend-fastapi/trace_api.py")
trace = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trace)


class ObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.env = patch.dict(os.environ, ATTENTION_OBSERVABILITY_DIR=str(self.path),
                              ATTENTION_JAEGER_PUBLIC_URL="http://localhost:16686", ATTENTION_JAEGER_QUERY_URL="http://jaeger:16686")
        self.env.start()
        self.addCleanup(self.env.stop)
        self.now = datetime(2026, 10, 19, 20, 0, tzinfo=timezone.utc)

    def receipt(self, kind="routing", key="event-123", **overrides):
        identity = trace_identity(kind, key) | overrides
        with sqlite3.connect(self.path / "exports.sqlite3") as db:
            db.execute("CREATE TABLE IF NOT EXISTS exports (kind TEXT,record_key TEXT,trace_id TEXT,span_id TEXT,exported_at TEXT,PRIMARY KEY(kind,record_key))")
            db.execute("INSERT OR REPLACE INTO exports VALUES (?,?,?,?,?)", (kind, key, identity["trace_id"], identity["span_id"], self.now.isoformat()))
        return identity

    def status(self, **overrides):
        value = {"status": "healthy", "updated_at": self.now.isoformat(), "last_success_at": self.now.isoformat(),
                 "exported_counts": {"routing": 1, "cycle": 0}, "pending_counts": {"routing": 2, "cycle": 0},
                 "blocked_counts": {"routing": 0, "cycle": 0}, "pending_scope": "scanned_window",
                 "scan_truncated": {"routing": True, "cycle": False}, **overrides}
        (self.path / "status.json").write_text(json.dumps(value))

    def read(self):
        with patch.object(trace, "jaeger_services", return_value={"status": "available", "application_services": [], "search_links": []}):
            return trace.observability_status(self.now)

    def test_missing_artifacts_are_unavailable_and_not_created(self):
        self.assertEqual(self.read()["exporter"]["status"], "unavailable")
        self.assertEqual(trace.export_receipts("routing", ["event-123"]), {})
        self.assertEqual(list(self.path.iterdir()), [])

    def test_only_matching_receipts_provide_trace_links(self):
        identity = self.receipt()
        self.receipt(key="mismatched", trace_id="f" * 32)
        found = trace.export_receipts("routing", ["event-123", "unexported", "mismatched"])
        self.assertEqual(set(found), {"event-123"})
        self.assertEqual(found["event-123"]["audit_trace_url"], "http://localhost:16686/trace/" + identity["trace_id"])
        self.assertEqual(found["event-123"]["audit_trace_status"], "export_acknowledged")
        self.assertNotIn("runtime_trace_id", found["event-123"])
        with patch.dict(os.environ, ATTENTION_JAEGER_PUBLIC_URL="https://user:secret@example.org"):
            self.assertIsNone(trace.export_receipts("routing", ["event-123"])["event-123"]["audit_trace_url"])

    def test_receipt_read_does_not_modify_database(self):
        self.receipt()
        path = self.path / "exports.sqlite3"
        before = path.read_bytes()
        trace.export_receipts("routing", ["event-123"])
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(sorted(item.name for item in self.path.iterdir()), ["exports.sqlite3"])

    def test_status_freshness_blocked_and_scanned_window_are_explicit(self):
        self.status()
        result = self.read()
        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["exporter"]["pending_scope"], "scanned_window")
        self.assertTrue(result["exporter"]["scan_truncated"]["routing"])
        self.status(updated_at=(self.now - timedelta(seconds=181)).isoformat())
        self.assertEqual(self.read()["exporter"]["status"], "stale")
        self.status(status="blocked", error_type="RejectedSpans", updated_at=(self.now - timedelta(seconds=181)).isoformat())
        result = self.read()["exporter"]
        self.assertEqual(result["status"], "blocked")
        self.assertTrue(result["stale"])
        self.assertEqual(result["error_type"], "RejectedSpans")

    def test_invalid_status_fails_closed(self):
        for changes in ({"updated_at": "invalid"}, {"updated_at": "2026-10-19T20:00:00"},
                        {"updated_at": (self.now + timedelta(seconds=10)).isoformat()}, {"status": "invented"}):
            self.status(**changes)
            self.assertEqual(self.read()["exporter"]["status"], "unavailable")
        (self.path / "status.json").write_bytes(b"x" * (trace.MAX_STATUS_BYTES + 1))
        self.assertEqual(self.read()["exporter"]["status"], "unavailable")

    def test_recent_traces_require_receipts_and_sanitize_status(self):
        identity = self.receipt()
        recent = {"kind": "routing", "key": "event-123", "trace_id": identity["trace_id"], "name": "fast.recorded_handling",
                  "service": "attention-routing", "start": (self.now - timedelta(seconds=5)).isoformat(), "end": self.now.isoformat(),
                  "prompt": "private prompt", "token": "private token"}
        self.status(api_key="private key", error_type="Bearer private token", exported_counts={"routing": 1, "token": "private token"},
                    recent_traces=[recent, {**recent, "key": "unexported"}, {**recent, "trace_id": "f" * 32},
                                   {**recent, "name": "private prompt"}, {**recent, "service": "private service"},
                                   {**recent, "end": (self.now - timedelta(seconds=10)).isoformat()}])
        result = self.read()
        self.assertEqual(len(result["recent_traces"]), 1)
        self.assertEqual(result["recent_traces"][0]["service"], "attention-routing")
        self.assertEqual(result["exporter"]["error_type"], "ExportError")
        self.assertNotIn("private", json.dumps(result))
        self.assertNotIn("prompt", json.dumps(result))

    def test_jaeger_live_check_is_bounded_and_service_names_are_allowlisted(self):
        response = io.BytesIO(json.dumps({"data": ["jaeger", "attention-routing", "attention-mid", "secret-service"]}).encode())
        opener = Mock()
        opener.open.return_value = response
        with patch.object(trace, "build_opener", return_value=opener):
            result = trace.jaeger_services()
        opener.open.assert_called_once_with("http://jaeger:16686/api/services", timeout=2)
        self.assertEqual(result["status"], "available")
        self.assertEqual(result["application_services"], ["attention-mid", "attention-routing"])
        self.assertTrue(all("lookback=2d" in link["url"] for link in result["search_links"]))
        self.assertNotIn("secret", json.dumps(result))

    def test_jaeger_failures_do_not_expose_remote_errors_or_claim_healthy(self):
        self.status()
        opener = Mock()
        opener.open.side_effect = URLError("password=private-error")
        with patch.object(trace, "build_opener", return_value=opener):
            result = trace.observability_status(self.now)
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["jaeger"]["status"], "unavailable")
        self.assertNotIn("private-error", json.dumps(result))
        for payload in (b"not json", b"x" * (trace.MAX_STATUS_BYTES + 1), b'{"data":null}'):
            opener.open.side_effect = None
            opener.open.return_value = io.BytesIO(payload)
            with patch.object(trace, "build_opener", return_value=opener):
                self.assertEqual(trace.jaeger_services()["status"], "unavailable")

    def test_graph_and_list_attach_audit_links_without_replacing_propagated_identity(self):
        identity = self.receipt()
        raw = {"text": "test source"}
        original = {"id": 3, "payload": {"source": "twitter", "source_id": "twitter-3"}, "original_raw": raw}
        payload = {"event_id": "event-123", "source": "twitter", "source_id": "twitter-3", "raw_sha256": trace.digest(raw),
                   "stage": "fast", "trace": {"trace_id": "b" * 32}}
        event = {"id": 1, "created_at": self.now, "payload": payload}
        def query(conn, sql, args=()):
            if "to_regclass" in sql:
                return [{"present": "routing_events"}]
            if "FROM routing_events" in sql:
                return [event]
            if "FROM raw_messages" in sql:
                return [{"id": 3, "payload": original["payload"], "raw": raw}]
            raise AssertionError(sql)
        with patch.object(trace, "rows", side_effect=query), patch.object(trace, "reader", return_value=nullcontext(None)):
            graph = trace.Graph(None)
            graph.routing(original)
            listing = trace.routing_list(None)
        node = graph.nodes["routing:1"]
        self.assertEqual(node["audit_trace_id"], identity["trace_id"])
        self.assertEqual(node["runtime_trace_id"], "b" * 32)
        self.assertEqual(node["runtime_trace_status"], "propagated_header_only")
        self.assertEqual(listing["items"][0]["audit_trace_id"], identity["trace_id"])

    def test_cycles_link_only_receipted_terminal_attempts(self):
        self.receipt("cycle", "20261019-mid-123")
        for key in ("20261019-mid-123", "20261019-mid-456"):
            folder = self.path / "cycles" / key
            folder.mkdir(parents=True)
            (folder / "completed.json").write_text(json.dumps({"cycle_id": key, "event": "completed", "status": "completed", "role": "mid"}))
        with patch.dict(os.environ, ATTENTION_CYCLE_AUDIT_DIR=str(self.path / "cycles"), ATTENTION_LEGACY_CYCLE_DIR=str(self.path / "missing")):
            result = trace.cycle_list()
        by_id = {item["cycle_id"]: item for item in result["items"]}
        self.assertIn("audit_trace_url", by_id["20261019-mid-123"])
        self.assertNotIn("audit_trace_url", by_id["20261019-mid-456"])


if __name__ == "__main__":
    unittest.main()
