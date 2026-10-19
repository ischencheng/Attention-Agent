from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm-worker"))
import worker_runtime as runtime
import local_ml_filter as filter_worker
import llm_worker as fast_worker
import embed_worker


class Producer:
    def __init__(self, events, error=None, remaining=0, callback=True):
        self.events, self.error, self.remaining, self.callback = events, error, remaining, callback
        self.record = None

    def produce(self, **record):
        self.events.append("produce")
        self.record = record

    def flush(self, timeout):
        self.events.append("ack")
        if self.callback:
            self.record["on_delivery"](self.error, None)
        return self.remaining


class Consumer:
    def __init__(self, events):
        self.events = events

    def commit(self, **kwargs):
        self.events.append("commit")


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.payload = {"source": "rss", "source_id": "item-1", "content_type": "news_article",
                        "data": {"title": "A company raises its capital expenditure forecast", "snippet": "Evidence"}}
        self.msg = Mock()
        self.msg.key.return_value = b"item-1"
        self.msg.headers.return_value = None
        self.args = argparse.Namespace(llm_provider="codex", llm_model="test-model", llm_api_key="", llm_timeout_seconds=180,
                                       attention_threshold=0.5, threshold=0.5, output_topic="output",
                                       embedding_provider="http", embedding_dim=768, embedding_timeout_seconds=180)

    def test_oversize_routes_for_full_review_without_claiming_a_model_verdict(self):
        self.payload["data"]["text"] = "x" * 128001
        for module in (filter_worker, fast_worker):
            with patch.object(runtime.requests, "post") as post:
                events = []
                producer = Producer(events)
                module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                output = json.loads(producer.record["value"])
                metadata = output["local_ml_filter" if module is filter_worker else "fast_agent"]
                self.assertFalse(metadata["classified_by_model"])
                self.assertTrue(metadata["full_review_required"])
                self.assertEqual(metadata["model"], "routing-policy")
                self.assertEqual(output["data"], self.payload["data"])
                post.assert_not_called()

    def test_rss_body_is_preserved_in_all_model_inputs(self):
        payload = {"content_type": "news_article", "data": {"title": "Headline", "text": "Full RSS excerpt", "snippet": "Short preview"}}
        for build in (filter_worker.build_message_text, fast_worker.build_message_text, embed_worker.extract_text):
            self.assertEqual(build(payload), "Headline Full RSS excerpt")

    def test_all_workers_ack_before_input_commit(self):
        for module, provider, result in ((filter_worker, "classify_with_codex", (True, "codex-test", {"classified_by_model": True})),
                                         (fast_worker, "classify_with_codex", (True, "codex-test", {"classified_by_model": True})),
                                         (embed_worker, "get_http_embedding", ([0.1] * 768, "semantic-test"))):
            with self.subTest(worker=module.__name__), patch.object(module, provider, return_value=result):
                events = []
                producer = Producer(events)
                module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                self.assertEqual(events, ["produce", "ack", "commit"])
                output = json.loads(producer.record["value"])
                self.assertEqual(output["data"], self.payload["data"])

    def test_delivery_errors_never_commit(self):
        for module, provider, result in ((filter_worker, "classify_with_codex", (True, "codex-test", {"classified_by_model": True})),
                                         (fast_worker, "classify_with_codex", (True, "codex-test", {"classified_by_model": True})),
                                         (embed_worker, "get_http_embedding", ([0.1] * 768, "semantic-test"))):
            for kwargs in ({"error": "broker rejected"}, {"remaining": 1, "callback": False}):
                with self.subTest(worker=module.__name__, failure=kwargs), patch.object(module, provider, return_value=result):
                    events = []
                    with self.assertRaises((RuntimeError, TimeoutError)):
                        module.process_record(self.msg, self.payload, self.args, Producer(events, **kwargs), Consumer(events))
                    self.assertNotIn("commit", events)

    def test_provider_errors_never_become_negative_decisions(self):
        for module, provider in ((filter_worker, "classify_with_codex"), (fast_worker, "classify_with_codex"),
                                 (embed_worker, "get_http_embedding")):
            with self.subTest(worker=module.__name__), patch.object(module, provider, side_effect=RuntimeError("offline")):
                events = []
                with self.assertRaises(RuntimeError):
                    module.process_record(self.msg, self.payload, self.args, Producer(events), Consumer(events))
                self.assertEqual(events, [])

    def test_missing_gemini_key_does_not_drop_input(self):
        self.args.llm_provider = "gemini"
        for module in (filter_worker, fast_worker):
            events = []
            with self.assertRaises(ValueError):
                module.process_record(self.msg, self.payload, self.args, Producer(events), Consumer(events))
            self.assertEqual(events, [])

    def test_valid_negative_verdict_commits_without_publishing(self):
        for module in (filter_worker, fast_worker):
            with patch.object(module, "classify_with_codex", return_value=(False, "test", {"classified_by_model": True})):
                events = []
                module.process_record(self.msg, self.payload, self.args, Producer(events), Consumer(events))
                self.assertEqual(events, ["commit"])

    def test_retry_holds_current_record_until_success(self):
        op = Mock(side_effect=[RuntimeError("first failure"), RuntimeError("second failure"), None])
        sleeper = Mock()
        self.assertTrue(runtime.retry_record(op, lambda: True, sleep=sleeper))
        self.assertEqual(op.call_count, 3)
        self.assertEqual(sleeper.call_count, 2)

    def test_retry_deadline_exits_for_service_restart(self):
        with patch.object(runtime.time, "monotonic", side_effect=[0, 901]):
            with self.assertRaisesRegex(RuntimeError, "Retry deadline"):
                runtime.retry_record(Mock(side_effect=ValueError("offline")), lambda: True, sleep=Mock())

    def test_filter_does_not_discard_first_polled_record(self):
        args = argparse.Namespace(**vars(self.args), bootstrap="kafka", group_id="test", log_level="INFO",
                                  metrics_port=9999, input_topic="input", poll_timeout_seconds=1)
        consumer, producer = Mock(), Mock()
        self.msg.error.return_value = None
        self.msg.value.return_value = json.dumps(self.payload).encode()
        consumer.poll.return_value = self.msg
        handlers = []
        with patch.object(filter_worker, "parse_args", return_value=args), \
             patch.object(filter_worker, "Consumer", return_value=consumer), \
             patch.object(filter_worker, "Producer", return_value=producer), \
             patch.object(filter_worker, "start_http_server"), \
             patch.object(filter_worker.signal, "signal", side_effect=lambda _, fn: handlers.append(fn)), \
             patch.object(filter_worker, "process_record", side_effect=lambda *args: handlers[0]()) as process:
            filter_worker.main()
            self.assertEqual(consumer.poll.call_count, 1)
            process.assert_called_once_with(self.msg, self.payload, args, producer, consumer)

    def test_shutdown_leaves_failed_record_uncommitted(self):
        op = Mock(side_effect=RuntimeError("failure"))
        keep_running = Mock(side_effect=[True, False])
        self.assertFalse(runtime.retry_record(op, keep_running, sleep=Mock()))
        self.assertEqual(op.call_count, 1)

    def test_gateway_rejects_truthy_nonboolean_and_missing_model(self):
        for result in ({"verdict": "false", "model": "test"}, {"verdict": 1, "model": "test"}, {"verdict": True}):
            with patch.object(runtime.requests, "post") as post:
                post.return_value.json.return_value = result
                with self.assertRaises(ValueError):
                    runtime.classify_with_codex(self.payload, "fast", "body", 180)

    def test_embedding_validation_rejects_wrong_dim_invalid_or_zero_values(self):
        for vector in ([0.1] * 64, [float("nan")] * 768, [float("inf")] * 768, [True] * 768, [0.0] * 768):
            with self.assertRaises(ValueError):
                runtime.validate_embedding(vector, 768)
        self.assertEqual(len(runtime.validate_embedding([0.1] * 768, 768)), 768)

    def test_gemini_success_uses_one_request(self):
        with patch.object(embed_worker.requests, "post") as post:
            post.return_value.status_code = 200
            post.return_value.json.return_value = {"embedding": {"values": [0.1] * 768}}
            self.assertEqual(len(embed_worker.get_gemini_embedding("body", "model", "test-key", 768, 10)), 768)
            self.assertEqual(post.call_count, 1)

    def test_filter_price_ticks_are_forwarded_without_llm_call(self):
        self.payload["content_type"] = "price_tick"
        with patch.object(filter_worker, "classify_with_codex") as classifier:
            events = []
            filter_worker.process_record(self.msg, self.payload, self.args, Producer(events), Consumer(events))
            classifier.assert_not_called()
            self.assertEqual(events, ["produce", "ack", "commit"])

    def test_batch_stops_before_later_offset_when_current_record_fails(self):
        later = Mock()
        with patch.object(filter_worker, "process_record", side_effect=RuntimeError("failed")) as process:
            with self.assertRaises(RuntimeError):
                filter_worker.process_pending_batch([(self.msg, self.payload), (later, self.payload)], self.args, Mock(), Mock())
            self.assertEqual(process.call_count, 1)


if __name__ == "__main__":
    unittest.main()
