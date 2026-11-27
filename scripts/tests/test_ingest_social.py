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
            records, coverage = social.collect(["nvidia", "openai"], ["@NVIDIA"], 1, "en", threading.Event(), paused)
        self.assertEqual([(row["source"], row["status"]) for row in coverage],
                         [("twitter", "failed"), ("youtube", "ok")])
        self.assertEqual(paused, {"twitter": "NEED_LOGIN"})
        record, = records.values()
        self.assertEqual(record["data"]["text"], "Description\n\nCaptions")
        self.assertEqual(record["data"]["published_at"], "2026-11-24")
        self.assertEqual(record["timestamp"], record["data"]["observed_at"])
        self.assertEqual(record["data"]["raw_sha256"], social.digest(record["raw"]))

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

            def produce(self, topic, *, key, value, on_delivery):
                self.sends += 1
                self.callback = on_delivery

            def flush(self, timeout):
                self.callback(None if self.ok else object(), Message())
                return 0

        state = sqlite3.connect(":memory:")
        self.addCleanup(state.close)
        state.execute("CREATE TABLE delivered (version TEXT PRIMARY KEY, source_id, topic, partition, offset, acknowledged_at)")
        producer = Producer()
        stop = threading.Event()
        raw = {"id": "123", "text": "Original", "author": "nvidia", "likes": 1,
               "created_at": "Tue Nov 24 12:34:56 +0000 2026"}

        def publish():
            record = social.tweet_original({"raw": raw.copy()}, "2026-11-27T12:00:00+00:00")
            self.assertEqual(record["timestamp"], "2026-11-24T12:34:56+00:00")
            self.assertEqual(record["source_id"], "twitter-123")
            return social.publish(producer, state, "source_raw", {social.version_key(record): record}, stop)

        self.assertEqual(publish()["failed"], 1)
        self.assertEqual(state.execute("SELECT count(*) FROM delivered").fetchone()[0], 0)
        producer.ok = True
        self.assertEqual(publish()["acknowledged"][0]["offset"], 127)
        raw["likes"] += 1
        self.assertEqual(publish()["already_delivered"], 1)
        raw["text"] = "Edited original"
        self.assertEqual(len(publish()["acknowledged"]), 1)
        self.assertEqual(producer.sends, 3)


if __name__ == "__main__":
    unittest.main()
