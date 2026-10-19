#!/usr/bin/env python3
"""Poll public primary RSS and publish original envelopes to Kafka source_raw.

RSS is a complementary input to the existing filters, Fast, Mid and Strong
agents. This collector never writes tickets, annotations, decisions or model
prompts. Publisher RSS excerpts are preserved as supplied; they are not claimed
to be full articles. Topic matching and feeds use the saved project scope.

Default: at most 24 unseen content versions per poll, every 1200 seconds. Local
SQLite dedup records are committed only AFTER Kafka acknowledges a message.
Delivery is at least once: a crash between broker acknowledgment and SQLite
commit can replay that original, identifiable by source/source_id/raw hash.
"""
from __future__ import annotations

import argparse
import fcntl
import html
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import threading
from datetime import datetime, timezone
import uuid

ROOT = Path(__file__).resolve().parents[1]
JUDGMENT = ROOT / "environments/attention_judgment"
sys.path.insert(0, str(JUDGMENT))
from ledger import digest
from sources import fetch_feeds

DEFAULT_ROOT = JUDGMENT / "runs/production/rss"


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def envelope(observation):
    raw = observation["raw"]
    text = html.unescape(re.sub(r"<[^>]+>", " ", raw.get("content_html", "")))
    return {
        "source": observation["source"], "source_id": observation["source_id"],
        "content_type": "news_article", "timestamp": observation["published_at"],
        "data": {"title": raw.get("title", ""), "text": text,
                 "url": raw.get("url", observation["source_id"]),
                 "publisher": observation["source"], "feed_url": raw.get("feed_url"),
                 "available_at": observation["available_at"],
                 "observation_id": observation["id"],
                 "raw_sha256": observation["raw_sha256"],
                 "coverage": raw.get("coverage")},
        "raw": raw,
    }


class DeliveryState:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS delivered (
            observation_id TEXT PRIMARY KEY, source TEXT NOT NULL,
            source_id TEXT NOT NULL, raw_sha256 TEXT NOT NULL,
            acknowledged_at TEXT NOT NULL, kafka_topic TEXT NOT NULL,
            kafka_partition INTEGER NOT NULL, kafka_offset INTEGER NOT NULL
        )""")
        self.db.commit()

    def close(self):
        self.db.close()

    def contains(self, observation_id):
        return self.db.execute("SELECT 1 FROM delivered WHERE observation_id=?", (observation_id,)).fetchone() is not None

    def acknowledged(self, observation, receipt):
        with self.db:
            self.db.execute(
                "INSERT INTO delivered VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(observation_id) DO NOTHING",
                (observation["id"], observation["source"], observation["source_id"],
                 observation["raw_sha256"], iso_now(), receipt["topic"],
                 receipt["partition"], receipt["offset"]),
            )


def publish_one(producer, topic, observation, timeout=35):
    delivered = {}

    def on_delivery(error, message):
        if error is not None:
            delivered["error"] = str(error)
        else:
            delivered["receipt"] = {"topic": message.topic(), "partition": message.partition(), "offset": message.offset()}

    producer.produce(
        topic=topic, key=f"{observation['source']}:{observation['source_id']}".encode(),
        value=json.dumps(envelope(observation), ensure_ascii=False, allow_nan=False).encode(),
        on_delivery=on_delivery,
    )
    remaining = producer.flush(timeout)
    if remaining or "receipt" not in delivered:
        raise RuntimeError(delivered.get("error", "Kafka did not acknowledge the original before timeout"))
    return delivered["receipt"]


def poll_once(producer, state, output_dir, *, topic="source_raw", limit=24, max_age_days=14, fetcher=None):
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
    if max_age_days < 1 or max_age_days > 90:
        raise ValueError("max_age_days must be between 1 and 90")
    fetcher = fetcher or fetch_feeds
    observations, coverage = fetcher(output_dir, max_age_days=max_age_days, limit_per_feed=limit)
    # Content hash distinguishes a publisher correction from an unchanged item.
    # Different fetch times for identical content do not create new messages.
    unique = {item["id"]: item for item in observations}
    pending = sorted((item for item in unique.values() if not state.contains(item["id"])),
                     key=lambda item: (item["published_at"], item["id"]), reverse=True)
    published, failed = [], []
    for observation in pending[:limit]:
        if digest(observation["raw"]) != observation["raw_sha256"]:
            failed.append({"observation_id": observation["id"], "error": "raw hash mismatch"})
            continue
        try:
            receipt = publish_one(producer, topic, observation)
            state.acknowledged(observation, receipt)
            published.append({"observation_id": observation["id"], "source": observation["source"],
                              "source_id": observation["source_id"], **receipt})
        except Exception as exc:
            failed.append({"observation_id": observation["id"], "error": f"{type(exc).__name__}: {exc}"})
            # Preserve the remainder for retry; do not spend a timeout per item
            # during a broker outage or mark any unacknowledged input consumed.
            break
    result = {"polled_at": iso_now(), "topic": topic, "coverage": coverage,
              "fetched_versions": len(unique), "already_delivered": len(unique) - len(pending),
              "published": published, "failed": failed,
              "remaining_unseen": len(pending) - len(published),
              "max_messages_per_poll": limit}
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    (Path(output_dir) / "publication.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="source_raw")
    parser.add_argument("--state", type=Path, default=DEFAULT_ROOT / "delivered.sqlite3")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT / "polls")
    parser.add_argument("--limit", type=int, default=24)
    parser.add_argument("--max-age-days", type=int, default=14)
    parser.add_argument("--poll-interval-seconds", type=float, default=1200)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.poll_interval_seconds < 60:
        parser.error("poll interval must be at least 60 seconds")
    if not 1 <= args.limit <= 100 or not 1 <= args.max_age_days <= 90:
        parser.error("limit must be 1..100 and max-age-days must be 1..90")
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all",
                         "enable.idempotence": True, "compression.type": "zstd",
                         "delivery.timeout.ms": 30000, "request.timeout.ms": 10000})
    args.state.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    # Prevent concurrent collectors using the same delivery diary.
    with args.state.with_suffix(args.state.suffix + ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another RSS collector owns this delivery state")
        state = DeliveryState(args.state)
        try:
            while not stop.is_set():
                poll_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
                result = poll_once(producer, state, args.output_root / poll_id,
                                   topic=args.topic, limit=args.limit, max_age_days=args.max_age_days)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                if args.once:
                    if result["failed"] or not any(c["status"] == "ok" for c in result["coverage"]):
                        raise SystemExit(1)
                    break
                stop.wait(args.poll_interval_seconds)
        finally:
            state.close()
            producer.flush(5)


if __name__ == "__main__":
    main()
