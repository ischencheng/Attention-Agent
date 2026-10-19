"""Sink failures must never let a later Kafka offset hide an unpersisted row."""
from contextlib import ExitStack
import copy
import importlib.util
import itertools
from pathlib import Path
import tempfile
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import pgserver
import psycopg2


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("attention_sink", ROOT / "sink-consumer/kafka_to_postgres_sink.py")
sink = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sink)
sys.path.insert(0, str(ROOT / "llm-worker"))
from worker_runtime import routing_event


def message(offset=1):
    result = MagicMock()
    result.topic.return_value = "source_raw"
    result.partition.return_value = 0
    result.offset.return_value = offset
    result.error.return_value = None
    result.value.return_value = b'{"source":"test","source_id":"one","raw":{"text":"original"}}'
    return result


class SinkControlFlowTests(unittest.TestCase):
    def run_patches(self, consumer, conn):
        stack = ExitStack()
        args = SimpleNamespace(log_level="ERROR", metrics_port=8040, topics="source_raw",
                               group_id="test", poll_timeout_seconds=0)
        for name, value in (("parse_args", args), ("wait_for_db", conn), ("build_consumer", consumer)):
            stack.enter_context(patch.object(sink, name, return_value=value))
        stack.enter_context(patch.object(sink, "start_http_server"))
        stack.enter_context(patch.object(sink.signal, "signal"))
        return stack

    def test_shutdown_during_poison_record_does_not_commit_or_poll_later(self):
        consumer, conn = MagicMock(), MagicMock()
        consumer.poll.side_effect = [message(1), message(2)]
        with self.run_patches(consumer, conn), patch.object(sink, "persist_with_retry", return_value=False):
            sink.run()
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    def test_disconnected_database_exits_before_later_record(self):
        consumer, conn = MagicMock(), MagicMock()
        conn.closed = False
        conn.cursor.side_effect = psycopg2.OperationalError("database restarted")
        consumer.poll.side_effect = [message(1), message(2)]
        with self.run_patches(consumer, conn), self.assertRaises(psycopg2.OperationalError):
            sink.run()
        self.assertEqual(consumer.poll.call_count, 1)
        consumer.commit.assert_not_called()

    def test_exhausted_offset_commit_exits_before_next_record(self):
        consumer, conn = MagicMock(), MagicMock()
        consumer.poll.side_effect = [message(1), message(2)]
        with self.run_patches(consumer, conn), \
                patch.object(sink, "persist_with_retry", return_value=True), \
                patch.object(sink, "commit_with_retry", return_value=False), \
                self.assertRaisesRegex(RuntimeError, "Cannot commit persisted offset"):
            sink.run()
        self.assertEqual(consumer.poll.call_count, 1)

    def test_poison_record_stays_pending_until_shutdown(self):
        conn = MagicMock()
        conn.closed = False
        running = [True]

        def poison(*args):
            running[0] = False
            raise ValueError("invalid dimensions")

        with patch.object(sink, "route_and_insert", side_effect=poison):
            self.assertFalse(sink.persist_with_retry(conn, message(), {}, lambda: running[0]))
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()


class SinkPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="attention-sink-tests-")
        cls.server = pgserver.get_server(Path(cls.directory.name) / "postgres", cleanup_mode="stop")

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()
        cls.directory.cleanup()

    def test_transaction_retry_and_duplicate_offset_are_persisted_once(self):
        with psycopg2.connect(self.server.get_uri()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""CREATE TEMP TABLE raw_messages (
                    kafka_topic text, kafka_partition integer, kafka_offset bigint,
                    payload jsonb, raw jsonb,
                    UNIQUE (kafka_topic, kafka_partition, kafka_offset))""")
            conn.commit()
            calls = []
            original = sink.route_and_insert

            def transient(cursor, msg, payload):
                calls.append(msg.offset())
                if len(calls) == 1:
                    cursor.execute("SELECT 1 / 0")
                return original(cursor, msg, payload)

            payload = {"source": "test", "source_id": "one", "raw": {"text": "original"}}
            with patch.object(sink, "route_and_insert", side_effect=transient), \
                    patch.object(sink.time, "monotonic", side_effect=itertools.count()):
                self.assertTrue(sink.persist_with_retry(conn, message(10), payload, lambda: True))
            self.assertEqual(calls, [10, 10])
            self.assertTrue(sink.persist_with_retry(conn, message(10), payload, lambda: True))
            with conn.cursor() as cursor:
                cursor.execute("SELECT kafka_offset, raw FROM raw_messages")
                self.assertEqual(cursor.fetchall(), [(10, {"text": "original"})])


class RoutingSinkPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="attention-routing-sink-tests-")
        cls.server = pgserver.get_server(Path(cls.directory.name) / "postgres", cleanup_mode="stop")

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()
        cls.directory.cleanup()

    def setUp(self):
        self.conn = psycopg2.connect(self.server.get_uri())
        with self.conn.cursor() as cursor:
            cursor.execute((ROOT / "postgres/init/003_routing_events.sql").read_text())
            cursor.execute("TRUNCATE routing_events")
            cursor.execute((ROOT / "postgres/init/004_relay_routing_stage.sql").read_text())
        self.conn.commit()
        self.input = message(42)
        self.input.topic.return_value = "fastagent_input"
        self.input.headers.return_value = None
        self.audit_message = message(100)
        self.audit_message.topic.return_value = "routing_events"
        self.payload = routing_event(
            self.input, {"source": "rss", "source_id": "original-1", "raw": {"text": "Original evidence"}},
            stage="fast", verdict=True,
            classifier={"provider": "codex", "model": "actual-model", "classified_by_model": True,
                        "classification_mode": "model", "cached": False, "usage": {"input_tokens": 100},
                        "reason": "Contains original earnings evidence.", "reason_status": "recorded"},
            started_at="2026-10-19T05:00:00Z", completed_at="2026-10-19T05:00:01Z",
            output_topic="attention_ticket", output={"topic": "attention_ticket", "partition": 1, "offset": 50})

    def tearDown(self):
        self.conn.close()

    def insert(self, payload, message=None):
        with self.conn.cursor() as cursor:
            self.assertTrue(sink.route_and_insert(cursor, message or self.audit_message, payload))
        self.conn.commit()

    def test_duplicate_attempts_keep_first_reason_and_broker_ack_identity(self):
        self.insert(self.payload)
        retry = copy.deepcopy(self.payload)
        retry.update({"started_at": "2026-10-19T05:02:00Z", "completed_at": "2026-10-19T05:02:01Z",
                      "recorded_at": "2026-10-19T05:02:02Z"})
        retry["output"]["offset"] = 51
        retry["classifier"].update({"cached": True, "usage": None})
        self.audit_message.offset.return_value = 101
        self.insert(retry)
        self.insert(retry)
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT event_id, reason, source, source_id, raw_sha256, kafka_offset, payload FROM routing_events")
            rows = cursor.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][:6], (self.payload["event_id"], self.payload["reason"], "rss", "original-1",
                                      self.payload["raw_sha256"], 100))
        self.assertEqual(rows[0][6], self.payload)

    def test_relay_migration_preserves_legacy_audit_and_accepts_distinct_stage(self):
        # Simulate an existing database before applying the additive migration.
        with self.conn.cursor() as cursor:
            cursor.execute("ALTER TABLE routing_events DROP CONSTRAINT routing_events_stage_check")
            cursor.execute("ALTER TABLE routing_events ADD CONSTRAINT routing_events_stage_check "
                           "CHECK (stage IN ('filter', 'fast'))")
        self.conn.commit()
        original = {"source": "rss", "source_id": "original-1", "raw": {"text": "Original evidence"}}
        old_filter = routing_event(self.input, original, stage="filter", verdict=False,
                                   classifier={"provider": "codex", "model": "old-model", "classified_by_model": True,
                                               "reason": "Prior filter verdict"},
                                   started_at="2026-10-19T05:00:00Z", completed_at="2026-10-19T05:00:01Z")
        self.insert(old_filter)
        with self.conn.cursor() as cursor:
            migration = (ROOT / "postgres/init/004_relay_routing_stage.sql").read_text()
            cursor.execute(migration)
            cursor.execute(migration)
        self.conn.commit()
        relay = routing_event(self.input, original, stage="relay", verdict=True,
                              classifier={"provider": "routing-policy", "model": None, "classified_by_model": False,
                                          "classification_mode": "relay", "usage": None,
                                          "reason": "Forwarded unchanged to Fast"},
                              started_at="2026-10-19T05:01:00Z", completed_at="2026-10-19T05:01:01Z",
                              output_topic="fastagent_input", output={"topic": "fastagent_input", "partition": 1, "offset": 51})
        self.audit_message.offset.return_value = 101
        self.insert(relay)
        self.audit_message.offset.return_value = 102
        self.insert(self.payload)
        self.audit_message.offset.return_value = 103
        self.insert(relay)
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT stage, payload FROM routing_events ORDER BY id")
            self.assertEqual(cursor.fetchall(), [("filter", old_filter), ("relay", relay), ("fast", self.payload)])
        changed = copy.deepcopy(relay)
        changed["classifier"]["reason"] = "Conflicting relay policy"
        changed["reason"] = "Conflicting relay policy"
        with self.assertRaisesRegex(ValueError, "Conflicting routing audit"):
            self.insert(changed)
        self.conn.rollback()

    def test_conflicting_verdict_or_original_hash_cannot_replace_first_event(self):
        self.insert(self.payload)
        changed_verdict = copy.deepcopy(self.payload)
        changed_verdict.update({"verdict": False, "outcome": "reject", "output_topic": None, "output": None})
        changed_hash = {**self.payload, "raw_sha256": "a" * 64}
        changed_reason = {**self.payload, "reason": "Different recorded reasoning"}
        for conflicting in (changed_verdict, changed_hash, changed_reason):
            with self.subTest(change=conflicting), self.assertRaisesRegex(ValueError, "Conflicting routing audit"):
                self.insert(conflicting)
            self.conn.rollback()
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT payload FROM routing_events")
            self.assertEqual(cursor.fetchall(), [(self.payload,)])

    def test_invalid_event_identity_and_fabricated_reason_status_are_rejected(self):
        for invalid in ({**self.payload, "event_id": "0" * 64},
                        {**self.payload, "reason": None, "reason_status": "recorded"}):
            with self.assertRaises(ValueError):
                self.insert(invalid)
            self.conn.rollback()
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM routing_events")
            self.assertEqual(cursor.fetchone()[0], 0)

    def test_reused_audit_offset_with_another_input_is_not_silently_dropped(self):
        self.insert(self.payload)
        self.input.offset.return_value = 43
        second = routing_event(self.input, {"source": "rss", "source_id": "other", "raw": {"text": "Other"}},
                               stage="fast", verdict=False, classifier={"reason": None},
                               started_at="2026-10-19T05:00:00Z", completed_at="2026-10-19T05:00:01Z")
        with self.assertRaisesRegex(ValueError, "Conflicting routing audit"):
            self.insert(second)
        self.conn.rollback()
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT event_id FROM routing_events")
            self.assertEqual(cursor.fetchall(), [(self.payload["event_id"],)])


if __name__ == "__main__":
    unittest.main()
