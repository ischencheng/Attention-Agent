"""Annotation/decision publication requires a broker delivery receipt."""
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
spec = importlib.util.spec_from_file_location("attention_publisher", ROOT / "skills/db_write.py")
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class Producer:
    def __init__(self, *, error=None, delivered=True, remaining=0):
        self.error, self.delivered, self.remaining = error, delivered, remaining

    def produce(self, **record):
        self.record = record

    def flush(self, timeout):
        if self.delivered:
            receipt = MagicMock()
            receipt.topic.return_value = self.record["topic"]
            receipt.partition.return_value = 2
            receipt.offset.return_value = 17
            self.record["on_delivery"](self.error, receipt)
        return self.remaining


class PublicationTests(unittest.TestCase):
    def test_success_has_confirmed_kafka_identity(self):
        producer = Producer()
        with patch.object(publisher, "Producer", return_value=producer):
            result = publisher.produce_message("annotations", {"summary": "observed fact"})
        self.assertEqual(result, {"status": "ok", "topic": "annotations", "partition": 2, "offset": 17})

    def test_empty_flush_without_callback_is_not_success(self):
        with patch.object(publisher, "Producer", return_value=Producer(delivered=False)), \
                self.assertRaises(TimeoutError):
            publisher.produce_message("annotations", {"summary": "fact"})

    def test_unflushed_output_is_not_success(self):
        with patch.object(publisher, "Producer", return_value=Producer(delivered=False, remaining=1)), \
                self.assertRaises(TimeoutError):
            publisher.produce_message("decision_packet", {"summary": "judgment"})

    def test_delivery_error_fails_cli_with_nonzero_exit(self):
        output = io.StringIO()
        params = {"topic": "annotations", "payload": {"summary": "fact"}}
        with patch.object(publisher, "Producer", return_value=Producer(error="broker unavailable")), \
                patch.object(publisher, "prepare_payload", side_effect=lambda _topic, payload: payload), \
                patch.object(sys, "argv", ["db_write.py", json.dumps(params)]), \
                redirect_stdout(output), self.assertRaises(SystemExit) as failure:
            publisher.main()
        self.assertEqual(failure.exception.code, 1)
        self.assertIn("Kafka publication failed", json.loads(output.getvalue())["error"])

    def test_unsupported_topic_fails_before_creating_producer(self):
        with patch.object(publisher, "Producer") as producer, self.assertRaises(ValueError):
            publisher.produce_message("source_raw", {"summary": "not an agent output"})
        producer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
