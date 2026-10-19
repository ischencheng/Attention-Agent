"""Trace joins, historical snapshots, redaction and durable attempt regressions."""
from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "environments/attention_judgment"))
from cycle_audit import terminal_summary, tool_events, write_event
from ledger import Ledger

spec = importlib.util.spec_from_file_location("trace_api_test", ROOT / "backend-fastapi/trace_api.py")
trace = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trace)


class CycleAuditTests(unittest.TestCase):
    def test_failed_attempt_survives_later_success_and_rejects_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            write_event(folder, "20260101-mid-a", "started", {"role": "mid", "started_at": "2026-01-01", "status": "running"})
            write_event(folder, "20260101-mid-a", "failed", {"role": "mid", "status": "failed", "error_type": "TimeoutError"})
            write_event(folder, "20260102-mid-b", "started", {"role": "mid", "started_at": "2026-01-02", "status": "running"})
            write_event(folder, "20260102-mid-b", "completed", {"role": "mid", "status": "completed"})
            with self.assertRaises(FileExistsError):
                write_event(folder, "20260101-mid-a", "failed", {"status": "completed"})
            with patch.dict(os.environ, ATTENTION_CYCLE_AUDIT_DIR=folder, ATTENTION_LEGACY_CYCLE_DIR=folder + "/absent"):
                found = trace.cycle_list()
            self.assertEqual([item["status"] for item in found["items"]], ["completed", "failed"])

    def test_unfinished_attempt_does_not_claim_running_or_success(self):
        with tempfile.TemporaryDirectory() as folder:
            write_event(folder, "20260101-strong-a", "started", {"role": "strong", "status": "running"})
            with patch.dict(os.environ, ATTENTION_CYCLE_AUDIT_DIR=folder, ATTENTION_LEGACY_CYCLE_DIR=folder + "/absent"):
                result = trace.cycle_list()
            self.assertEqual(result["items"][0]["status"], "running_or_interrupted")

    def test_tool_summary_excludes_command_outputs_and_agent_text(self):
        with tempfile.TemporaryDirectory() as folder:
            events = [{"type": "item.started", "item": {"id": "item_1", "type": "command_execution", "command": "python /app/skills/db_query.py 'secret SQL'", "status": "in_progress"}},
                      {"type": "item.completed", "item": {"id": "item_1", "type": "command_execution", "command": "python /app/skills/db_query.py 'secret SQL'", "aggregated_output": "private result", "exit_code": 0, "status": "completed"}},
                      {"type": "item.completed", "item": {"id": "item_2", "type": "agent_message", "text": "private model transcript"}}]
            (Path(folder) / "events.jsonl").write_text("\n".join(json.dumps(item) for item in events))
            result = tool_events(folder)
            self.assertEqual(result["tool_event_count"], 2)
            self.assertEqual(result["tool_events"][1]["tool_names"], ["db_query.py"])
            self.assertEqual(result["tool_events"][1]["exit_code"], 0)
            self.assertNotIn("secret SQL", json.dumps(result))
            self.assertNotIn("private", json.dumps(result))
            self.assertIsNone(result["tool_events"][1]["timestamp"])

    def test_terminal_summary_only_exposes_allowed_usage_and_redacts_summary(self):
        result = terminal_summary({"role": "mid", "status": "completed", "completed_at": "now",
                                   "usage": {"input_tokens": 10, "api_key": "dont-show"},
                                   "final": {"summary": "token=secret-value", "reviewed_ticket_ids": [5]}})
        self.assertEqual(result["usage"], {"input_tokens": 10})
        self.assertNotIn("secret-value", json.dumps(result))


class LedgerReadTests(unittest.TestCase):
    def test_read_only_reader_observes_live_wal_and_verifies_each_entry(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "ledger.sqlite3"
            ledger = Ledger(path)
            try:
                state = {"theses": {"example": {"claim": "pending"}}}
                payload = {"agent_id": "codex-strong", "state": state, "state_sha256": trace.digest(state), "version": 1}
                ledger.append("production_research_state", "one", payload)
                with patch.dict(os.environ, ATTENTION_PRODUCTION_LEDGER=str(path)):
                    first, _ = trace.archived_research()
                    ledger.append("production_research_state", "two", {**payload, "version": 2})
                    second, _ = trace.archived_research()
                self.assertEqual(len(first), 1)
                self.assertEqual(len(second), 2)
                self.assertTrue(all(item["entry_hash_verified"] for item in second))
                self.assertEqual(ledger.verify(), second[0]["hash"])
            finally:
                ledger.close()

    def test_missing_ledger_does_not_create_an_empty_database(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "missing.sqlite3"
            with patch.dict(os.environ, ATTENTION_PRODUCTION_LEDGER=str(path)):
                records, notes = trace.archived_research()
            self.assertEqual(records, [])
            self.assertTrue(notes)
            self.assertFalse(path.exists())


class GraphTests(unittest.TestCase):
    def row(self, kind, number, payload):
        result = {"id": number, "created_at": datetime.now(timezone.utc), "payload": payload,
                  "kafka_topic": kind, "kafka_partition": 0, "kafka_offset": number}
        if kind == "raw":
            result["original_raw"] = payload["raw"]
        return result

    def test_ticket_requires_same_identity_and_actual_original_hash(self):
        raw = {"text": "actual source"}
        ticket = self.row("ticket", 5, {"source": "publisher", "source_id": "story", "raw": raw})
        original = self.row("raw", 3, {"source": "publisher", "source_id": "story", "raw": raw})
        graph = trace.Graph(None)
        def query(_conn, sql, args=()):
            if "SELECT id,payload,raw" in sql:
                return [{"id": 2, "payload": original["payload"], "raw": {"text": "old version"}},
                        {"id": 3, "payload": original["payload"], "raw": raw}]
            if "to_regclass" in sql:
                return [{"present": None}]
            raise AssertionError(sql)
        with patch.object(graph, "fetch", side_effect=lambda kind, number: {("ticket", 5): ticket, ("raw", 3): original}.get((kind, number))), patch.object(trace, "rows", side_effect=query):
            graph.expand("ticket", 5)
        result = graph.result()
        self.assertIn("raw:3", [node["id"] for node in result["nodes"]])
        self.assertNotIn("raw:2", [node["id"] for node in result["nodes"]])
        self.assertEqual(result["edges"][0]["verification"], "source_identity_and_raw_sha256")

    def test_routing_to_ticket_requires_exact_broker_coordinates(self):
        raw = {"text": "source"}
        original = self.row("raw", 3, {"source": "p", "source_id": "x", "raw": raw})
        ticket = self.row("ticket", 5, {})
        graph = trace.Graph(None)
        event = {"id": 1, "created_at": "now", "payload": {"stage": "fast", "outcome": "pass", "output": {"topic": "ticket", "partition": 0, "offset": 4}}}
        def query(_conn, sql, args=()):
            return [{"present": "routing_events"}] if "to_regclass" in sql else [event]
        with patch.object(trace, "rows", side_effect=query):
            graph.routing(original, ticket)
            self.assertFalse(any(edge["relation"] == "published_ticket" for edge in graph.edges))
            event["payload"]["output"]["offset"] = 5
            graph.routing(original, ticket)
        self.assertTrue(any(edge["verification"] == "broker_coordinates" for edge in graph.edges))

    def test_decision_uses_frozen_annotation_without_reading_current_annotation(self):
        graph = trace.Graph(None)
        decision = self.row("decision", 8, {"related_annotations": [1]})
        verified = {"status": "verified", "errors": [], "evidence_snapshot": {
            "annotations": [{"id": 1, "payload": {"summary": "historical", "attention_ticket_ids": [5]}, "sha256": "frozen"}],
            "tickets": [{"id": 5, "raw_message_id": 3}], "raw_refs": [{"id": 3}]},
            "raw_messages": [{"id": 3, "raw": {"text": "source"}}]}
        with patch.object(graph, "fetch", side_effect=lambda kind, number: decision if kind == "decision" else None) as fetch, patch.object(trace, "verify_evidence", return_value=verified):
            graph.decision(8)
        self.assertEqual([call.args[0] for call in fetch.call_args_list], ["decision", "raw"])
        self.assertEqual(graph.nodes["annotation:1"]["payload"]["summary"], "historical")
        self.assertEqual(graph.nodes["annotation:1"]["status"], "verified_historical_snapshot")

    def test_display_redacts_private_fields_and_never_treats_boolean_as_id(self):
        result = trace.clean({"password": "value", "reasoning": "private", "raw": {"text": "Bearer credential-value"}})
        self.assertNotIn("credential-value", json.dumps(result))
        self.assertEqual(result["reasoning"], "[redacted]")
        self.assertEqual(trace.ids([True, 3, -1, "5", 3]), [3])

    def test_research_revision_projection_drops_internal_debug_content(self):
        thesis = {"revisions": [{"claim": "recorded finding", "command_output": "private", "debug_context": "hidden", "tool_results": "secret"}],
                  "source_identity": {"source": "publisher", "debug_context": "also hidden"}}
        public = trace.public_thesis(thesis)
        self.assertEqual(public["revisions"], [{"claim": "recorded finding"}])
        self.assertEqual(public["source_identity"], {"source": "publisher"})

    def test_filter_and_fast_join_only_on_actual_forwarded_coordinates(self):
        original = self.row("raw", 3, {"source": "publisher", "source_id": "same", "raw": {"text": "original"}})
        events = [{"id": 1, "created_at": "first", "payload": {"stage": "filter", "outcome": "pass", "output": {"topic": "fastagent_input", "partition": 0, "offset": 2}}},
                  {"id": 2, "created_at": "second", "payload": {"stage": "fast", "outcome": "reject", "input": {"topic": "fastagent_input", "partition": 0, "offset": 2}}}]
        graph = trace.Graph(None)
        with patch.object(trace, "rows", side_effect=lambda conn, sql, args=(): [{"present": "routing_events"}] if "to_regclass" in sql else events):
            graph.routing(original)
        edge = next(item for item in graph.edges if item["relation"] == "forwarded_input")
        self.assertEqual((edge["source"], edge["target"], edge["verification"]), ("routing:1", "routing:2", "broker_coordinates"))


if __name__ == "__main__":
    unittest.main()
