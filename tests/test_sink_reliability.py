"""Sink failures must never let a later Kafka offset hide an unpersisted row."""
from contextlib import ExitStack
import importlib.util
import itertools
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import pgserver
import psycopg2


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("attention_sink", ROOT / "sink-consumer/kafka_to_postgres_sink.py")
sink = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sink)


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


if __name__ == "__main__":
    unittest.main()
