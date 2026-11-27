import json
import sqlite3
import subprocess
import threading
import unittest
from unittest.mock import patch

from scripts import ingest_social as social


class CollectorTests(unittest.TestCase):
    def test_x_login_error_does_not_block_youtube(self):
        def reach(channel, action, *_):
            if channel == "twitter":
                raise social.SourceError("NEED_LOGIN")
            if action == "read":
                return [{"title": "Video", "published_at": "2026-11-24", "text": "Description",
                         "raw": {"id": "z7tcQiyLK8E"}}]
            return [{"url": "https://www.youtube.com/watch?v=z7tcQiyLK8E", "text": "Captions",
                     "raw": {"lang": "en"}}]

        paused = {}
        with patch.object(social, "reach", side_effect=reach), patch.object(
            social, "video_ids", return_value=["z7tcQiyLK8E"]
        ):
            records, coverage = social.collect(["nvidia", "openai"], ["@NVIDIA"], 1, "en", threading.Event(), paused, {})
        self.assertEqual([(row["source"], row["status"]) for row in coverage],
                         [("twitter", "failed"), ("youtube", "ok")])
        self.assertEqual(paused, {"twitter": "NEED_LOGIN"})
        record, = records.values()
        self.assertEqual(record["data"]["text"], "Description\n\nCaptions")
        self.assertEqual(record["data"]["published_at"], "2026-11-24")
        self.assertEqual(record["timestamp"], record["data"]["observed_at"])
        self.assertEqual(record["data"]["raw_sha256"], social.digest(record["raw"]))

    def test_recent_video_reuses_hydrated_caption_and_expires(self):
        cache = {}
        result = {"source": "youtube", "source_id": "youtube-z7tcQiyLK8E", "raw": {"id": "z7tcQiyLK8E"}}
        with patch.object(social, "youtube_original", return_value=(result, None)) as hydrate, patch.object(
            social, "video_ids", return_value=["z7tcQiyLK8E"]
        ), patch.object(social.time, "monotonic", return_value=0) as clock:
            for _ in range(2):
                social.collect([], ["@NVIDIA"], 1, "en", threading.Event(), {}, cache)
            self.assertEqual(hydrate.call_count, 1)
            clock.return_value = 21601
            social.collect([], ["@NVIDIA"], 1, "en", threading.Event(), {}, cache)
            self.assertEqual(hydrate.call_count, 2)

    def test_rate_limit_keeps_the_cache_for_unreached_channels(self):
        cache = {"z7tcQiyLK8E": ({"source_id": "youtube-z7tcQiyLK8E"}, None, 999)}
        with patch.object(social, "video_ids", side_effect=social.SourceError("RATE_LIMITED")):
            _, coverage = social.collect([], ["@First", "@Second"], 1, "en", threading.Event(), {}, cache)
        self.assertEqual(list(cache), ["z7tcQiyLK8E"])
        self.assertEqual([row["target"] for row in coverage if row["source"] == "youtube"], ["@First"])

    def test_reach_refuses_fallback_and_exposes_only_error_code(self):
        result = subprocess.CompletedProcess([], 1, stdout=json.dumps({
            "schema_version": 1, "channel": "twitter", "action": "tweets", "ok": False,
            "error": {"code": "NEED_LOGIN", "message": "private upstream log"},
        }))
        with patch.object(social, "run_command", return_value=result) as command:
            with self.assertRaisesRegex(social.SourceError, "^NEED_LOGIN$"):
                social.reach("twitter", "tweets", "test")
        self.assertIn("opencli", command.call_args.args[0])
        result.returncode = 0
        result.stdout = json.dumps({"schema_version": 1, "channel": "twitter", "action": "tweets", "ok": True,
                                    "backend": "public-web", "items": []})
        with patch.object(social, "run_command", return_value=result):
            with self.assertRaisesRegex(social.SourceError, "^UPSTREAM_BROKEN$"):
                social.reach("twitter", "tweets", "test")

    def test_caption_not_found_keeps_description_and_platform_block_stops(self):
        metadata = [{"title": "Video", "text": "Description", "raw": {"id": "z7tcQiyLK8E"}}]
        with patch.object(social, "reach", side_effect=[metadata, social.SourceError("NOT_FOUND")]):
            original, caption_error = social.youtube_original("z7tcQiyLK8E", "en")
        self.assertEqual(caption_error, "NOT_FOUND")
        self.assertEqual(original["data"]["coverage"], "metadata_only")
        self.assertEqual(original["data"]["text"], "Description")
        with patch.object(social, "reach", side_effect=[metadata, social.SourceError("PLATFORM_BLOCKED")]):
            with self.assertRaisesRegex(social.SourceError, "^PLATFORM_BLOCKED$"):
                social.youtube_original("z7tcQiyLK8E", "en")

    def test_acknowledgment_enables_dedupe_but_content_edit_is_new(self):
        class Message:
            def partition(self):
                return 0

            def offset(self):
                return 127

        class Producer:
            ok = False
            sends = 0
            error = None
            sync_error = None

            def produce(self, topic, *, key, value, on_delivery):
                self.sends += 1
                self.callback = on_delivery
                if self.sync_error:
                    error, self.sync_error = self.sync_error, None
                    raise social.KafkaException(error)

            def flush(self, timeout):
                error, self.error = self.error, None
                if error is None and not self.ok:
                    error = object()
                self.callback(error, Message())
                return 0

        state = sqlite3.connect(":memory:")
        self.addCleanup(state.close)
        state.execute("CREATE TABLE delivered (version, source_id, topic, partition, offset, acknowledged_at, PRIMARY KEY (version, topic))")
        producer = Producer()
        stop = threading.Event()
        raw = {"id": "123", "text": "Original", "author": "nvidia", "likes": 1,
               "created_at": "Tue Nov 24 12:34:56 +0000 2026"}

        def publish(topic="source_raw"):
            record = social.tweet_original({"raw": raw.copy()}, "2026-11-27T12:00:00+00:00")
            self.assertEqual(record["timestamp"], "2026-11-24T12:34:56+00:00")
            self.assertEqual(record["source_id"], "twitter-123")
            return social.publish(producer, state, topic, {social.version_key(record): record}, stop)

        self.assertEqual(publish()["failed"], 1)
        self.assertEqual(state.execute("SELECT count(*) FROM delivered").fetchone()[0], 0)
        producer.ok = True
        self.assertEqual(publish()["acknowledged"][0]["offset"], 127)
        raw["likes"] += 1
        self.assertEqual(publish()["already_delivered"], 1)
        raw["text"] = "Edited original"
        self.assertEqual(len(publish()["acknowledged"]), 1)
        self.assertEqual(len(publish("test_topic")["acknowledged"]), 1)
        batch = {}
        for native_id, content in (("124", "x" * social.MAX_RECORD_BYTES), ("125", "Next original")):
            record = social.tweet_original({"raw": {**raw, "id": native_id, "text": content}}, social.now())
            batch[social.version_key(record)] = record
        mixed = social.publish(producer, state, "source_raw", batch, stop)
        self.assertEqual(mixed["rejected"], [{"source_id": "twitter-124", "code": "RECORD_TOO_LARGE"}])
        self.assertEqual([record["source_id"] for record in mixed["acknowledged"]], ["twitter-125"])
        self.assertEqual(producer.sends, 5)

        size_error = social.KafkaError(social.KafkaError.MSG_SIZE_TOO_LARGE)
        raw["text"] = "Broker size callback"
        producer.error = size_error
        self.assertEqual(publish()["rejected"], [{"source_id": "twitter-123", "code": "RECORD_TOO_LARGE"}])
        producer.sync_error = size_error
        self.assertEqual(publish()["rejected"], [{"source_id": "twitter-123", "code": "RECORD_TOO_LARGE"}])
        producer.error = social.KafkaError(social.KafkaError._TRANSPORT)
        self.assertEqual(publish()["failed"], 1)
        self.assertEqual(state.execute("SELECT count(*) FROM delivered").fetchone()[0], 4)
        self.assertEqual(len(publish()["acknowledged"]), 1)


if __name__ == "__main__":
    unittest.main()
