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
    def __init__(self, events, error=None, remaining=0, callback=True, fail_topic=None):
        self.events, self.error, self.remaining, self.callback = events, error, remaining, callback
        self.record = None
        self.records = []
        self.fail_topic = fail_topic

    def produce(self, **record):
        self.events.append("produce")
        self.record = record
        self.records.append(record)

    def flush(self, timeout):
        self.events.append("ack")
        if self.callback:
            ack = Mock()
            ack.topic.return_value = self.record["topic"]
            ack.partition.return_value = 2
            ack.offset.return_value = 99
            error = self.error if self.fail_topic is None or self.record["topic"] == self.fail_topic else None
            self.record["on_delivery"](error, ack)
        return self.remaining


class Consumer:
    def __init__(self, events):
        self.events = events

    def commit(self, **kwargs):
        self.events.append("commit")


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.payload = {"source": "rss", "source_id": "item-1", "content_type": "news_article",
                        "data": {"title": "A company raises its capital expenditure forecast", "snippet": "Evidence"},
                        "raw": {"text": "Original publisher report"}}
        self.msg = Mock()
        self.msg.key.return_value = b"item-1"
        self.msg.headers.return_value = None
        self.msg.topic.return_value = "input"
        self.msg.partition.return_value = 1
        self.msg.offset.return_value = 42
        self.args = argparse.Namespace(llm_provider="codex", llm_model="test-model", llm_api_key="", llm_timeout_seconds=180,
                                       attention_threshold=0.5, threshold=0.5, output_topic="output", routing_topic="routing_events",
                                       embedding_provider="http", embedding_dim=768, embedding_timeout_seconds=180)

    def test_relay_is_default_and_legacy_filters_require_explicit_selection(self):
        with patch.object(sys, "argv", ["local_ml_filter.py"]):
            args = filter_worker.parse_args()
        self.assertEqual(args.llm_provider, "relay")
        self.assertEqual((args.input_topic, args.output_topic, args.group_id),
                         ("source_clean", "fastagent_input", "local-ml-filter-v1"))
        for provider in ("codex", "gemini", "local"):
            with patch.object(sys, "argv", ["local_ml_filter.py", "--llm-provider", provider]):
                self.assertEqual(filter_worker.parse_args().llm_provider, provider)

    def test_relay_preserves_input_bytes_key_headers_without_model_calls(self):
        self.args.llm_provider = "relay"
        # Noncanonical whitespace and a binary key must survive the relay too.
        value = json.dumps(self.payload, ensure_ascii=False, indent=2).encode() + b"\n"
        self.msg.value.return_value = value
        headers = [("custom", b"\xff"), ("optional", None),
                   ("traceparent", b"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")]
        self.msg.headers.return_value = headers
        for key in (b"\xff-key", None):
            with self.subTest(key=key), patch.object(runtime.requests, "post") as post, \
                    patch.object(filter_worker, "classify_with_codex") as codex, \
                    patch.object(filter_worker, "get_gemini_yes_no") as gemini:
                self.msg.key.return_value = key
                events = []
                producer = Producer(events)
                filter_worker.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                output, audit = producer.records
                self.assertEqual((output["value"], output["key"], output["headers"]), (value, key, headers))
                self.assertNotIn("local_ml_filter", json.loads(output["value"]))
                event = json.loads(audit["value"])
                self.assertEqual(event["stage"], "relay")
                self.assertEqual(event["outcome"], "pass")
                self.assertEqual(event["raw_sha256"], runtime.digest_json(self.payload["raw"]))
                self.assertEqual(event["output"], {"topic": "output", "partition": 2, "offset": 99})
                self.assertEqual(event["classifier"], {
                    "provider": "routing-policy", "model": None, "classified_by_model": False,
                    "classification_mode": "relay", "usage": None,
                    "reason": "Forwarded unchanged to Fast, the single model relevance gate.",
                    "reason_status": "recorded"})
                self.assertEqual(event["trace"]["traceparent"], headers[-1][1].decode())
                self.assertEqual(events, ["produce", "ack", "produce", "ack", "commit"])
                post.assert_not_called()
                codex.assert_not_called()
                gemini.assert_not_called()

    def test_relay_output_or_audit_failure_keeps_input_uncommitted(self):
        self.args.llm_provider = "relay"
        self.msg.value.return_value = json.dumps(self.payload).encode()
        for fail_topic in ("output", "routing_events"):
            with self.subTest(fail_topic=fail_topic):
                events = []
                producer = Producer(events, error="delivery failed", fail_topic=fail_topic)
                with self.assertRaisesRegex(RuntimeError, "delivery failed"):
                    filter_worker.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                self.assertNotIn("commit", events)
                self.assertEqual(producer.record["topic"], fail_topic)
        events = []
        with self.assertRaises(TimeoutError):
            filter_worker.process_record(self.msg, self.payload, self.args,
                                         Producer(events, callback=False, remaining=1), Consumer(events))
        self.assertNotIn("commit", events)

    def test_oversize_routes_for_full_review_without_claiming_a_model_verdict(self):
        self.payload["data"]["text"] = "x" * 128001
        for module in (filter_worker, fast_worker):
            with patch.object(runtime.requests, "post") as post:
                events = []
                producer = Producer(events)
                module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                output = json.loads(producer.records[0]["value"])
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
                self.assertEqual(events, (["produce", "ack"] if module is embed_worker else ["produce", "ack"] * 2) + ["commit"])
                output = json.loads(producer.records[0]["value"])
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

    def test_valid_negative_verdict_publishes_audit_before_committing(self):
        for module in (filter_worker, fast_worker):
            with patch.object(module, "classify_with_codex", return_value=(False, "test", {"classified_by_model": True})):
                events = []
                producer = Producer(events)
                module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                self.assertEqual(events, ["produce", "ack", "commit"])
                self.assertEqual(producer.record["topic"], "routing_events")
                self.assertEqual(json.loads(producer.record["value"])["outcome"], "reject")

    def test_gateway_preserves_reason_and_actual_classifier_metadata(self):
        response = {"verdict": False, "model": "actual-model", "reason": "No new business or market evidence.",
                    "cached": True, "cache_key": "key-1", "usage": {"input_tokens": 123},
                    "classified_by_model": True, "classification_mode": "model", "full_review_required": False}
        with patch.object(runtime.requests, "post") as post:
            post.return_value.json.return_value = response
            verdict, model, metadata = runtime.classify_with_codex(self.payload, "filter", "body", 180)
        self.assertFalse(verdict)
        self.assertEqual(model, "actual-model")
        for key in ("reason", "cached", "cache_key", "usage"):
            self.assertEqual(metadata[key], response[key])
        self.assertEqual(metadata["reason_status"], "recorded")

    def test_pass_and_reject_audits_preserve_reason_original_hash_and_input_identity(self):
        metadata = {"reason": "Observed source evidence.", "reason_status": "recorded", "classified_by_model": True,
                    "classification_mode": "model", "cached": False, "cache_key": "actual-cache-key"}
        self.payload["raw_sha256"] = "untrusted claim"
        for module, stage in ((filter_worker, "filter"), (fast_worker, "fast")):
            for verdict in (True, False):
                with self.subTest(stage=stage, verdict=verdict), \
                        patch.object(module, "classify_with_codex", return_value=(verdict, "test-model", metadata)):
                    events = []
                    producer = Producer(events)
                    module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                    event = json.loads(producer.records[-1]["value"])
                    self.assertEqual(event["event_id"], runtime.digest_json({"stage": stage, "input": {
                        "topic": "input", "partition": 1, "offset": 42}}))
                    self.assertEqual(event["raw_sha256"], runtime.digest_json(self.payload["raw"]))
                    self.assertEqual(event["input_payload_sha256"], runtime.digest_json(self.payload))
                    self.assertEqual(event["source_id"], "item-1")
                    self.assertEqual(event["reason"], metadata["reason"])
                    self.assertEqual(event["outcome"], "pass" if verdict else "reject")
                    self.assertEqual(event["output"], {"topic": "output", "partition": 2, "offset": 99} if verdict else None)
                    self.assertLessEqual(event["started_at"], event["completed_at"])
                    self.assertLessEqual(event["completed_at"], event["recorded_at"])
                    self.assertEqual(events[-3:], ["produce", "ack", "commit"])

    def test_failed_audit_ack_never_commits_positive_or_negative_input(self):
        for module in (filter_worker, fast_worker):
            for verdict in (True, False):
                with self.subTest(worker=module.__name__, verdict=verdict), \
                        patch.object(module, "classify_with_codex", return_value=(verdict, "test", {"reason": "Recorded reason"})):
                    events = []
                    producer = Producer(events, error="audit broker rejected", fail_topic="routing_events")
                    with self.assertRaisesRegex(RuntimeError, "audit broker rejected"):
                        module.process_record(self.msg, self.payload, self.args, producer, Consumer(events))
                    self.assertNotIn("commit", events)
                    self.assertEqual(producer.record["topic"], "routing_events")

    def test_heuristic_and_gemini_without_reasons_explicitly_record_absence(self):
        for module in (filter_worker, fast_worker):
            for provider in ("gemini", "local"):
                with self.subTest(worker=module.__name__, provider=provider), \
                        patch.object(module, "get_gemini_yes_no", return_value=False):
                    self.args.llm_provider = provider
                    producer = Producer([])
                    module.process_record(self.msg, self.payload, self.args, producer, Consumer([]))
                    event = json.loads(producer.record["value"])
                    self.assertIsNone(event["reason"])
                    self.assertEqual(event["reason_status"], "not_recorded")
                    self.assertEqual(event["classifier"]["classified_by_model"], provider == "gemini")

    def test_invalid_json_rejection_is_audited_before_commit(self):
        self.msg.value.return_value = b"not-json"
        for stage in ("relay", "filter", "fast"):
            with self.subTest(stage=stage):
                events = []
                producer = Producer(events)
                runtime.reject_invalid_record(self.msg, producer, Consumer(events), stage=stage, topic="routing_events")
                event = json.loads(producer.record["value"])
                self.assertEqual(event["stage"], stage)
                self.assertEqual(event["classifier"]["classification_mode"], "invalid_json")
                self.assertEqual(event["original_hash_status"], "not_recorded")
                self.assertIsNone(event["raw_sha256"])
                self.assertIsNone(event["classifier"].get("usage"))
                if stage != "relay":
                    self.assertNotIn("usage", event["classifier"])
                self.assertEqual(events, ["produce", "ack", "commit"])

    def test_relay_main_audits_malformed_input_under_relay_stage(self):
        self.args.llm_provider = "relay"
        args = argparse.Namespace(**vars(self.args), bootstrap="kafka", group_id="test", log_level="INFO",
                                  metrics_port=9999, input_topic="input", poll_timeout_seconds=1)
        consumer, producer = Mock(), Mock()
        self.msg.error.return_value = None
        self.msg.value.return_value = b"not-json"
        consumer.poll.return_value = self.msg
        handlers = []
        with patch.object(filter_worker, "parse_args", return_value=args), \
                patch.object(filter_worker, "Consumer", return_value=consumer), \
                patch.object(filter_worker, "Producer", return_value=producer), \
                patch.object(filter_worker, "ensure_routing_topic"), \
                patch.object(filter_worker, "start_http_server"), \
                patch.object(filter_worker.signal, "signal", side_effect=lambda _, fn: handlers.append(fn)), \
                patch.object(filter_worker, "reject_invalid_record", side_effect=lambda *a, **kw: handlers[0]()) as reject:
            filter_worker.main()
        self.assertEqual(consumer.poll.call_count, 1)
        reject.assert_called_once_with(self.msg, producer, consumer, stage="relay", topic="routing_events")

    def test_only_valid_actual_traceparent_is_retained(self):
        header = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
        self.msg.headers.return_value = [("traceparent", header.encode())]
        context = runtime.trace_context(self.msg)
        self.assertEqual(context["traceparent"], header)
        self.assertEqual(context["status"], "propagated_header_only")
        for value in (b"invalid", b"00-00000000000000000000000000000000-0123456789abcdef-01"):
            self.msg.headers.return_value = [("traceparent", value)]
            self.assertIsNone(runtime.trace_context(self.msg))

    def test_audit_topic_creation_is_idempotent_but_other_errors_propagate(self):
        with patch.object(runtime, "AdminClient") as admin:
            runtime.ensure_routing_topic("kafka", "routing_events")
            topic = admin.return_value.create_topics.call_args.args[0][0]
            self.assertEqual((topic.topic, topic.num_partitions, topic.replication_factor), ("routing_events", 6, 3))
            future = admin.return_value.create_topics.return_value["routing_events"]
            from confluent_kafka import KafkaException, KafkaError
            future.result.side_effect = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
            runtime.ensure_routing_topic("kafka", "routing_events")
            future.result.side_effect = RuntimeError("offline")
            with self.assertRaisesRegex(RuntimeError, "offline"):
                runtime.ensure_routing_topic("kafka", "routing_events")

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
             patch.object(filter_worker, "ensure_routing_topic"), \
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
            self.assertEqual(events, ["produce", "ack", "produce", "ack", "commit"])

    def test_batch_stops_before_later_offset_when_current_record_fails(self):
        later = Mock()
        with patch.object(filter_worker, "process_record", side_effect=RuntimeError("failed")) as process:
            with self.assertRaises(RuntimeError):
                filter_worker.process_pending_batch([(self.msg, self.payload), (later, self.payload)], self.args, Mock(), Mock())
            self.assertEqual(process.call_count, 1)


if __name__ == "__main__":
    unittest.main()
