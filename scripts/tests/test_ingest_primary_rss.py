"""RSS producer durability and envelope tests without external services."""
from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("primary_rss", ROOT / "scripts/ingest_primary_rss.py")
rss = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rss)
from sources import observation


def article(index=1, text="Original <b>GPU</b> report"):
    return observation("fictional-publisher", f"https://example.test/article-{index}",
                       f"2025-06-{index:02}T12:00:00Z", "2025-06-10T12:00:00Z",
                       {"title": "GPU report", "url": f"https://example.test/article-{index}",
                        "content_html": text, "description_html": "Short excerpt", "feed_url": "https://example.test/rss",
                        "coverage": "publisher-supplied RSS; not full article"})


class Message:
    def __init__(self, topic, offset):
        self._topic, self._offset = topic, offset

    def topic(self):
        return self._topic

    def partition(self):
        return 2

    def offset(self):
        return self._offset


class Producer:
    def __init__(self, fail=False):
        self.fail = fail
        self.messages = []
        self.pending = None

    def produce(self, **kwargs):
        self.messages.append(kwargs)
        self.pending = kwargs

    def flush(self, timeout):
        pending, self.pending = self.pending, None
        if pending:
            pending["on_delivery"]("broker unavailable" if self.fail else None,
                                   Message(pending["topic"], len(self.messages)))
        return 0


class RssTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.state = rss.DeliveryState(self.root / "state.sqlite3")
        self.producer = Producer()

    def tearDown(self):
        self.state.close()
        self.directory.cleanup()

    def poll(self, items, **kwargs):
        return rss.poll_once(self.producer, self.state, self.root / "poll", fetcher=lambda *a, **k: (items, [{"status": "ok"}]), **kwargs)

    def test_original_rss_is_preserved_and_goes_only_to_source_raw(self):
        item = article()
        result = self.poll([item])
        message = self.producer.messages[0]
        value = json.loads(message["value"])
        self.assertEqual(value["raw"], item["raw"])
        self.assertEqual(value["source_id"], item["source_id"])
        self.assertEqual(value["timestamp"], item["published_at"])
        self.assertEqual(value["data"]["available_at"], item["available_at"])
        self.assertEqual(value["data"]["text"], "Original  GPU  report")
        self.assertEqual(value["content_type"], "news_article")
        self.assertEqual(message["topic"], "source_raw")
        self.assertEqual(result["published"][0]["offset"], 1)
        self.assertTrue(self.state.contains(item["id"]))

    def test_failed_delivery_is_not_consumed_and_can_be_retried(self):
        item = article()
        self.producer.fail = True
        failed = self.poll([item])
        self.assertEqual(failed["published"], [])
        self.assertTrue(failed["failed"])
        self.assertFalse(self.state.contains(item["id"]))
        self.producer.fail = False
        self.assertEqual(len(self.poll([item])["published"]), 1)
        self.assertTrue(self.state.contains(item["id"]))

    def test_restart_deduplicates_same_content_but_publishes_corrections(self):
        item = article()
        self.poll([item])
        self.state.close()
        self.state = rss.DeliveryState(self.root / "state.sqlite3")
        fetched_again = copy.deepcopy(item)
        fetched_again["available_at"] = "2025-06-11T12:00:00Z"
        self.assertEqual(self.poll([fetched_again])["already_delivered"], 1)
        self.assertEqual(len(self.producer.messages), 1)
        changed = article(text="Corrected GPU report")
        result = self.poll([changed])
        self.assertEqual(len(result["published"]), 1)
        self.assertEqual(changed["source_id"], item["source_id"])
        self.assertNotEqual(changed["id"], item["id"])

    def test_bounded_latest_first_batch_preserves_pending_backlog(self):
        items = [article(index) for index in range(1, 7)]
        result = self.poll(items, limit=2)
        self.assertEqual(len(result["published"]), 2)
        self.assertEqual(result["remaining_unseen"], 4)
        self.assertTrue(self.state.contains(items[-1]["id"]))
        self.assertTrue(self.state.contains(items[-2]["id"]))
        self.assertFalse(self.state.contains(items[0]["id"]))
        self.assertEqual(len(self.poll(items, limit=2)["published"]), 2)

    def test_corrupted_raw_never_crosses_publish_boundary(self):
        item = article()
        item["raw"]["title"] = "Changed after hashing"
        result = self.poll([item])
        self.assertTrue(result["failed"])
        self.assertEqual(self.producer.messages, [])
        self.assertFalse(self.state.contains(item["id"]))


if __name__ == "__main__":
    unittest.main()
