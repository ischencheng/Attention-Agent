#!/usr/bin/env python3
"""Collect configured X accounts and YouTube channels into source_raw."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time

from confluent_kafka import KafkaError, KafkaException, Producer

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "environments/attention_judgment/runs/production/social/delivered.sqlite3"
VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
ACCOUNT = re.compile(r"[A-Za-z0-9_]{1,15}")
CHANNEL = re.compile(r"(?:@[\w.-]{1,100}|UC[A-Za-z0-9_-]{22})")
TERMINAL = {"NEED_SETUP", "NEED_LOGIN", "PLATFORM_BLOCKED"}
ERROR_CODES = TERMINAL | {"RATE_LIMITED", "NOT_FOUND", "BAD_INPUT", "UPSTREAM_BROKEN", "BUSY", "TIMEOUT"}
# Leave headroom for Kafka record overhead under the broker's 1 MiB limit.
MAX_RECORD_BYTES = 900_000
COMMAND_ENV = ("HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR",
               "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy")


class SourceError(Exception):
    pass


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def run_command(argv, timeout=45):
    try:
        return subprocess.run(argv, capture_output=True, text=True, timeout=timeout,
                              env={name: os.environ[name] for name in COMMAND_ENV if name in os.environ})
    except FileNotFoundError:
        raise SourceError("NEED_SETUP") from None
    except subprocess.TimeoutExpired:
        raise SourceError("TIMEOUT") from None
    except (OSError, UnicodeError):
        raise SourceError("UPSTREAM_BROKEN") from None


def reach(channel, action, *args):
    executable = os.environ.get("ATTENTION_REACH_BIN") or shutil.which("agent-reach")
    executable = executable or str(Path.home() / ".local/bin/agent-reach")
    backend = "opencli" if channel == "twitter" else "yt-dlp"
    result = run_command([executable, channel, action, *args, "--backend", backend, "--timeout", "30"],
                         timeout=180 if channel == "twitter" else 45)
    try:
        payload = json.loads(result.stdout)
        if (payload.get("schema_version"), payload.get("channel"), payload.get("action")) != (1, channel, action):
            raise ValueError
        if payload.get("ok") is False:
            code = (payload.get("error") or {}).get("code")
            raise SourceError(code if code in ERROR_CODES else "UPSTREAM_BROKEN")
        if (result.returncode or payload.get("ok") is not True or payload.get("backend") != backend
                or not isinstance(payload.get("items"), list)):
            raise ValueError
        return payload["items"]
    except (ValueError, TypeError, AttributeError):
        raise SourceError("UPSTREAM_BROKEN") from None


def video_ids(channel, limit):
    path = channel if channel.startswith("@") else f"channel/{channel}"
    result = run_command([sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-warnings", "--no-progress",
                          "--flat-playlist", "--playlist-end", str(limit), "--socket-timeout", "10",
                          "--retries", "0", "--extractor-retries", "0", "--print", "%(id)s",
                          f"https://www.youtube.com/{path}/videos"])
    if result.returncode:
        error = result.stderr.lower()
        if "no module named yt_dlp" in error:
            raise SourceError("NEED_SETUP")
        if "not a bot" in error or "sign in to confirm" in error:
            raise SourceError("PLATFORM_BLOCKED")
        if "429" in error or "too many requests" in error:
            raise SourceError("RATE_LIMITED")
        raise SourceError("UPSTREAM_BROKEN")
    ids = result.stdout.splitlines()
    if any(not VIDEO_ID.fullmatch(value) for value in ids):
        raise SourceError("UPSTREAM_BROKEN")
    return list(dict.fromkeys(ids))[:limit]


def publication_time(value):
    if not isinstance(value, str) or not value:
        return None, None
    try:
        return date.fromisoformat(value).isoformat(), "day"
    except ValueError:
        pass
    try:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).isoformat(), "instant"
    except (TypeError, ValueError, OverflowError):
        pass
    return None, None


def original(source, native_id, content_type, data, raw, observed_at):
    published_at, precision = publication_time(data.get("published_at"))
    if ((precision == "instant" and published_at > observed_at)
            or (precision == "day" and published_at > observed_at[:10])):
        published_at, precision = None, None
    return {"source": source, "source_id": f"{source}-{native_id}", "content_type": content_type,
            "timestamp": published_at if precision == "instant" else observed_at,
            "data": {**data, "published_at": published_at, "publication_precision": precision,
                     "observed_at": observed_at, "raw_sha256": digest(raw)}, "raw": raw}


def tweet_original(item, observed_at):
    if not isinstance(item, dict) or not isinstance(item.get("raw"), dict):
        raise SourceError("INVALID_RECORD")
    raw = item["raw"]
    native_id, text = raw.get("id"), raw.get("text")
    if (not isinstance(native_id, str) or not re.fullmatch(r"[1-9][0-9]{0,31}", native_id)
            or not isinstance(text, str) or not text.strip()):
        raise SourceError("INVALID_RECORD")
    data = {"text": text, "url": f"https://x.com/i/status/{native_id}", "author": item.get("author"),
            "published_at": raw.get("created_at"), "coverage": "post", "backend": "opencli"}
    return original("twitter", native_id, "social_post", data, raw, observed_at)


def youtube_original(video_id, languages):
    url = f"https://www.youtube.com/watch?v={video_id}"
    items = reach("youtube", "read", url)
    info = items[0] if len(items) == 1 and isinstance(items[0], dict) else {}
    details = info.get("raw")
    if (not isinstance(details, dict) or details.get("id") != video_id
            or not isinstance(info.get("title"), str) or not info["title"].strip()):
        raise SourceError("INVALID_RECORD")
    transcript, language, caption_error = None, None, None
    try:
        items = reach("youtube", "subtitle", url, "--lang", languages)
        caption = items[0] if len(items) == 1 and isinstance(items[0], dict) else {}
        caption_raw = caption.get("raw")
        if (caption.get("url") != url or not isinstance(caption.get("text"), str) or not caption["text"].strip()
                or not isinstance(caption_raw, dict) or not isinstance(caption_raw.get("lang"), str)):
            raise SourceError("INVALID_CAPTION")
        transcript, language = caption["text"], caption_raw["lang"]
    except SourceError as exc:
        if str(exc) != "NOT_FOUND":
            raise
        caption_error = str(exc)
    # Keep the observed content. Playback formats and mutable engagement counts
    # do not belong in a content version.
    raw = {"id": video_id, "title": info["title"], "description": info.get("text"),
           "channel_id": details.get("channel_id"), "author": info.get("author"),
           "published_at": info.get("published_at"), "transcript": transcript, "caption_language": language}
    text = "\n\n".join(value for value in (raw["description"], transcript) if isinstance(value, str) and value.strip())
    data = {"url": url, "title": raw["title"], "text": text, "author": raw["author"],
            "published_at": raw["published_at"], "coverage": "subtitle" if transcript else "metadata_only",
            "caption_language": language, "backend": "yt-dlp"}
    return original("youtube", video_id, "news_article", data, raw, now()), caption_error


def version_key(record):
    raw = record["raw"]
    content = raw if record["source"] == "youtube" else {
        key: raw.get(key) for key in ("text", "author", "created_at", "is_retweet", "media_urls", "quoted_tweet")}
    return digest([record["source"], record["source_id"], content])


def collect(accounts, channels, limit, languages, stop, paused, video_cache):
    records, coverage = {}, []
    active_videos = set()
    for source, targets in (("twitter", accounts), ("youtube", channels)):
        if not targets:
            coverage.append({"source": source, "status": "not_configured"})
            continue
        for target in targets:
            if stop.is_set():
                return records, coverage
            if source in paused:
                coverage.append({"source": source, "status": "paused", "code": paused[source]})
                break
            status = {"source": source, "target": target, "status": "ok", "items": 0, "caption_status": {}}
            try:
                if source == "twitter":
                    account_items = reach("twitter", "tweets", target, "--limit", str(limit))
                    for item in account_items[:limit]:
                        try:
                            record = tweet_original(item, now())
                        except SourceError as exc:
                            status.update(status="partial", code=str(exc))
                            continue
                        records[version_key(record)] = record
                        status["items"] += 1
                else:
                    for video_id in video_ids(target, limit):
                        if stop.is_set():
                            break
                        active_videos.add(video_id)
                        try:
                            entry = video_cache.get(video_id)
                            if entry and time.monotonic() < entry[2]:
                                record, caption_error, _ = entry
                            else:
                                record, caption_error = youtube_original(video_id, languages)
                                ttl = 3600 if caption_error else 21600
                                video_cache[video_id] = (record, caption_error, time.monotonic() + ttl)
                        except SourceError as exc:
                            if str(exc) in TERMINAL | {"RATE_LIMITED"}:
                                raise
                            status.update(status="partial", code=str(exc))
                            continue
                        records[version_key(record)] = record
                        status["items"] += 1
                        label = caption_error or "ok"
                        status["caption_status"][label] = status["caption_status"].get(label, 0) + 1
            except SourceError as exc:
                status.update(status="partial" if status["items"] else "failed", code=str(exc))
                if str(exc) in TERMINAL:
                    paused[source] = str(exc)
            coverage.append(status)
            if status.get("code") in TERMINAL | {"RATE_LIMITED"}:
                break
    for video_id in video_cache.keys() - active_videos:
        del video_cache[video_id]
    return records, coverage


def publish(producer, state, topic, records, stop):
    result = {"acknowledged": [], "already_delivered": 0, "failed": 0, "rejected": []}
    for version, record in records.items():
        if stop.is_set():
            break
        if state.execute("SELECT 1 FROM delivered WHERE version=? AND topic=?", (version, topic)).fetchone():
            result["already_delivered"] += 1
            continue
        encoded = json.dumps(record, ensure_ascii=False, allow_nan=False).encode()
        if len(encoded) > MAX_RECORD_BYTES:
            result["rejected"].append({"source_id": record["source_id"], "code": "RECORD_TOO_LARGE"})
            continue
        receipt = {}

        def delivered(error, message):
            if error is None:
                receipt.update(partition=message.partition(), offset=message.offset())
            elif isinstance(error, KafkaError):
                receipt["error"] = error

        try:
            producer.produce(topic, key=f"{record['source']}:{record['source_id']}",
                             value=encoded, on_delivery=delivered)
            remaining = producer.flush(25)
            if "error" in receipt:
                raise KafkaException(receipt["error"])
            if remaining or "offset" not in receipt:
                raise RuntimeError("Kafka acknowledgment missing")
            with state:
                state.execute("INSERT INTO delivered VALUES (?, ?, ?, ?, ?, ?)",
                              (version, record["source_id"], topic, receipt["partition"], receipt["offset"], now()))
        except KafkaException as exc:
            if exc.args and isinstance(exc.args[0], KafkaError) and exc.args[0].code() == KafkaError.MSG_SIZE_TOO_LARGE:
                result["rejected"].append({"source_id": record["source_id"], "code": "RECORD_TOO_LARGE"})
                continue
            result["failed"] += 1
            break
        except (RuntimeError, BufferError, sqlite3.Error):
            result["failed"] += 1
            break
        result["acknowledged"].append({"source_id": record["source_id"], "topic": topic, **receipt})
    return result


def configured_targets(value, pattern, *, strip_at=False):
    values = [item.strip() for item in value.split(",") if item.strip()]
    if strip_at:
        values = [item.removeprefix("@").lower() for item in values]
    if len(values) > 20 or any(not pattern.fullmatch(item) for item in values):
        raise ValueError("Use up to 20 comma-separated handles or channel IDs")
    return list(dict.fromkeys(values))


def main():
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--x", default=os.getenv("ATTENTION_X_ACCOUNTS", ""))
    parser.add_argument("--youtube", default=os.getenv("ATTENTION_YOUTUBE_CHANNELS", ""))
    parser.add_argument("--languages", default=os.getenv("ATTENTION_YOUTUBE_LANGUAGES", "en-orig,en,zh-Hans,zh"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094"))
    parser.add_argument("--topic", default="source_raw")
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--interval", type=int, default=1200)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--preview", action="store_true", help="Fetch one batch without publishing or writing delivery state")
    args = parser.parse_args()
    try:
        accounts = configured_targets(args.x, ACCOUNT, strip_at=True)
        channels = configured_targets(args.youtube, CHANNEL)
    except ValueError as exc:
        parser.error(str(exc))
    if (not 1 <= args.limit <= 10 or args.interval < 60
            or not re.fullmatch(r"[\w-]{1,24}(?:,[\w-]{1,24}){0,7}", args.languages)):
        parser.error("Use --limit 1..10, --interval >=60, and comma-separated caption language codes")
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    if args.preview:
        records, coverage = collect(accounts, channels, args.limit, args.languages, stop, {}, {})
        items = [{"source_id": row["source_id"], "coverage": row["data"]["coverage"],
                  "published_at": row["data"]["published_at"], "text_chars": len(row["data"]["text"])}
                 for row in records.values()]
        print(json.dumps({"coverage": coverage, "items": items}, ensure_ascii=False))
        return int(not items or any(row["status"] in {"partial", "failed"} for row in coverage))

    if not accounts and not channels:
        print(json.dumps({"status": "not_configured"}), flush=True)
        return int(args.once)
    args.state.parent.mkdir(parents=True, exist_ok=True)
    with args.state.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Another social collector owns this delivery state") from None
        state = sqlite3.connect(args.state)
        try:
            state.execute("""CREATE TABLE IF NOT EXISTS delivered (
                version TEXT NOT NULL, source_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL,
                offset INTEGER NOT NULL, acknowledged_at TEXT NOT NULL, PRIMARY KEY (version, topic))""")
            producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all", "enable.idempotence": True,
                                 "delivery.timeout.ms": 20000, "log_level": 0})
            paused, video_cache = {}, {}
            while not stop.is_set():
                records, coverage = collect(accounts, channels, args.limit, args.languages, stop, paused, video_cache)
                delivery = publish(producer, state, args.topic, records, stop)
                print(json.dumps({"polled_at": now(), "coverage": coverage, **delivery}, ensure_ascii=False), flush=True)
                if args.once:
                    return int(bool(delivery["failed"] or delivery["rejected"])
                               or any(row["status"] in {"partial", "failed"} for row in coverage))
                stop.wait(args.interval)
        finally:
            state.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
