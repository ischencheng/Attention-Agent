#!/usr/bin/env python3
"""Polygon.io news poller — financial news articles -> source_raw."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import time
from typing import Any

import requests
from prometheus_client import Counter, Gauge, start_http_server

from base import create_producer, ensure_topic, iso_now, make_envelope, produce_with_flush

LOGGER = logging.getLogger("polygon-news-ingestor")

ARTICLES_FETCHED = Counter("polygon_articles_fetched_total", "Total articles fetched")
PUBLISH_SUCCESS = Counter("polygon_publish_success_total", "Successful publishes")
PUBLISH_FAILED = Counter("polygon_publish_failed_total", "Failed publishes")
POLLS_TOTAL = Counter("polygon_polls_total", "Total poll cycles")
LAST_POLL_TIME = Gauge("polygon_last_poll_unixtime", "Unix time of last poll")

POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Polygon.io news poller -> Kafka source_raw")
    p.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    p.add_argument("--topic", default="source_raw")
    p.add_argument("--api-key", default=os.getenv("POLYGON_API_KEY", ""))
    p.add_argument("--poll-interval-seconds", type=float, default=60.0)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--metrics-port", type=int, default=8022)
    p.add_argument("--request-timeout-seconds", type=float, default=15.0)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def article_id(article: dict[str, Any]) -> str:
    aid = article.get("id")
    if aid:
        return str(aid)
    url = article.get("article_url", "")
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def article_to_envelope(article: dict[str, Any]) -> bytes:
    aid = article_id(article)
    published = article.get("published_utc", iso_now())
    title = article.get("title", "")
    description = article.get("description", "")
    tickers = article.get("tickers", [])

    return make_envelope(
        source="polygon",
        source_id=f"polygon-news-{aid}",
        content_type="news_article",
        timestamp=published,
        data={
            "title": title,
            "snippet": description[:500] if description else "",
            "url": article.get("article_url", ""),
            "publisher": (article.get("publisher") or {}).get("name", ""),
            "tickers": tickers,
        },
        raw=article,
    )


def poll_once(args: argparse.Namespace, seen_ids: set[str]) -> list[bytes]:
    params: dict[str, Any] = {
        "apiKey": args.api_key,
        "limit": args.limit,
        "order": "desc",
        "sort": "published_utc",
    }

    resp = requests.get(POLYGON_NEWS_URL, params=params, timeout=args.request_timeout_seconds)
    resp.raise_for_status()
    body = resp.json()
    results = body.get("results", [])

    envelopes: list[bytes] = []
    for article in results:
        aid = article_id(article)
        if aid in seen_ids:
            continue
        seen_ids.add(aid)
        envelopes.append(article_to_envelope(article))
        ARTICLES_FETCHED.inc()
    return envelopes


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not args.api_key:
        LOGGER.error("POLYGON_API_KEY not set, exiting")
        return

    start_http_server(args.metrics_port)
    ensure_topic(args.bootstrap, args.topic)
    producer = create_producer(args.bootstrap)

    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOGGER.info(
        "polygon-news-ingestor started topic=%s poll_interval=%.0fs metrics=0.0.0.0:%s/metrics",
        args.topic, args.poll_interval_seconds, args.metrics_port,
    )

    seen_ids: set[str] = set()
    # Cap seen_ids to prevent memory leak
    MAX_SEEN = 5000

    while running:
        try:
            POLLS_TOTAL.inc()
            LAST_POLL_TIME.set(time.time())
            envelopes = poll_once(args, seen_ids)
            for env in envelopes:
                try:
                    payload = json.loads(env)
                    key = payload.get("source_id", "unknown")
                    produce_with_flush(producer, args.topic, key, env)
                    PUBLISH_SUCCESS.inc()
                except Exception as ex:
                    PUBLISH_FAILED.inc()
                    LOGGER.error("publish failed error=%s", ex)

            if len(seen_ids) > MAX_SEEN:
                # Keep only the most recent half
                excess = len(seen_ids) - MAX_SEEN // 2
                for _ in range(excess):
                    seen_ids.pop()

            LOGGER.info("poll done new_articles=%d seen_total=%d", len(envelopes), len(seen_ids))
        except Exception as ex:
            LOGGER.warning("poll failed error=%s", ex)

        # Sleep in small increments to allow signal handling
        deadline = time.time() + args.poll_interval_seconds
        while running and time.time() < deadline:
            time.sleep(min(1.0, max(0, deadline - time.time())))

    producer.flush(10)
    LOGGER.info("polygon-news-ingestor stopped")


if __name__ == "__main__":
    main()
