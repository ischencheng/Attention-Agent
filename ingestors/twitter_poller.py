#!/usr/bin/env python3
"""Twitter poller via Caravo — polls tweets from a user's following list -> source_raw.

Flow:
1. Resolve --twitter-username to a numeric user ID (twitter241/user)
2. Fetch the following list (twitter241/followings)
3. Every poll cycle: search for recent tweets from followed users using
   batched `from:user1 OR from:user2 ...` queries (twitter241/search-v3)
4. Refresh the following list periodically (--followings-refresh-seconds)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from prometheus_client import Counter, Gauge, start_http_server

from base import create_producer, ensure_topic, iso_now, make_envelope, produce_with_flush

LOGGER = logging.getLogger("twitter-ingestor")

TWEETS_FETCHED = Counter("twitter_tweets_fetched_total", "Total tweets fetched")
PUBLISH_SUCCESS = Counter("twitter_publish_success_total", "Successful publishes")
PUBLISH_FAILED = Counter("twitter_publish_failed_total", "Failed publishes")
POLLS_TOTAL = Counter("twitter_polls_total", "Total poll cycles")
LAST_POLL_TIME = Gauge("twitter_last_poll_unixtime", "Unix time of last poll")
FOLLOWING_COUNT = Gauge("twitter_following_count", "Number of users in following list")

CARAVO_API_URL = "https://www.caravo.ai/api/tools"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Twitter following-list poller via Caravo -> Kafka source_raw")
    p.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    p.add_argument("--topic", default="source_raw")
    p.add_argument("--caravo-api-key", default=os.getenv("CARAVO_API_KEY", ""))
    p.add_argument("--twitter-username", default=os.getenv("TWITTER_USERNAME", "genhcnehcsi"),
                    help="Twitter username whose following list to poll")
    p.add_argument("--poll-interval-seconds", type=float, default=300.0)
    p.add_argument("--followings-refresh-seconds", type=float, default=3600.0,
                    help="How often to refresh the following list (default: 1 hour)")
    p.add_argument("--search-batch-size", type=int, default=10,
                    help="Max users per search query (to stay within query length limits)")
    p.add_argument("--metrics-port", type=int, default=8023)
    p.add_argument("--request-timeout-seconds", type=float, default=30.0)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Caravo API helpers
# ---------------------------------------------------------------------------

def caravo_exec(tool_id: str, data: dict, api_key: str, timeout: float) -> dict:
    resp = requests.post(
        f"{CARAVO_API_URL}/{tool_id}/execute",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=data,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def resolve_user_id(username: str, api_key: str, timeout: float) -> str:
    """Resolve a Twitter username to a numeric user ID."""
    body = caravo_exec("twitter241/user", {"username": username}, api_key, timeout)
    output = body.get("output", body)
    # Navigate: output.json.result.data.user.result.rest_id
    json_data = output.get("json", output) if isinstance(output, dict) else output
    if not isinstance(json_data, dict):
        raise ValueError(f"Unexpected response for @{username}")

    # Search for rest_id in nested structure
    def find_rest_id(obj: Any, depth: int = 0) -> str | None:
        if depth > 8 or not isinstance(obj, dict):
            return None
        if "rest_id" in obj and obj.get("__typename") == "User":
            return str(obj["rest_id"])
        for v in obj.values():
            if isinstance(v, dict):
                found = find_rest_id(v, depth + 1)
                if found:
                    return found
        return None

    rest_id = find_rest_id(json_data)
    if rest_id:
        return rest_id
    raise ValueError(f"Could not resolve user ID for @{username}: {json.dumps(body)[:500]}")


def decode_graphql_id(gql_id: str) -> str:
    """Decode base64 GraphQL ID like 'VXNlcjozMDY0MjU5MzMy' -> '3064259332'."""
    import base64
    try:
        decoded = base64.b64decode(gql_id + "==").decode("utf-8", errors="ignore")
        # Format is "User:12345"
        if ":" in decoded:
            return decoded.split(":", 1)[1]
    except Exception:
        pass
    return ""


def extract_user_from_result(user: dict[str, Any]) -> dict[str, str] | None:
    """Extract {id, username} from a Twitter user result object."""
    if user.get("__typename") != "User":
        return None
    core = user.get("core", {})
    screen_name = core.get("screen_name") or core.get("name") or ""
    if not screen_name:
        return None
    # rest_id may or may not be present; fall back to decoding GraphQL id
    uid = user.get("rest_id") or decode_graphql_id(user.get("id", ""))
    if not uid:
        return None
    return {"id": str(uid), "username": screen_name}


def fetch_following_list(user_id: str, api_key: str, timeout: float) -> list[dict[str, str]]:
    """Fetch list of users that user_id is following. Returns list of {id, username}."""
    following: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    cursor = None
    max_pages = 10  # safety limit

    for _ in range(max_pages):
        params: dict[str, str] = {"user": user_id, "count": "100"}
        if cursor:
            params["cursor"] = cursor

        body = caravo_exec("twitter241/followings", params, api_key, timeout)
        output = body.get("output", body)
        json_data = output.get("json", output) if isinstance(output, dict) else output
        if not isinstance(json_data, dict):
            break

        # Get cursor for pagination
        cursor_data = json_data.get("cursor", {})
        next_cursor = cursor_data.get("bottom")

        result = json_data.get("result", json_data)
        timeline = result.get("timeline", result.get("timeline_response", {}))
        timeline_inner = timeline.get("timeline", timeline)
        instructions = timeline_inner.get("instructions", [])

        found_users = False
        for inst in instructions:
            entries = inst.get("entries", [])
            for entry in entries:
                content = entry.get("content", {})
                # Try both itemContent (followings API) and content (other APIs)
                item_content = content.get("itemContent", content.get("content", {}))
                user_results = item_content.get("user_results", {})
                user = user_results.get("result", {})
                info = extract_user_from_result(user)
                if info and info["id"] not in seen_ids:
                    seen_ids.add(info["id"])
                    following.append(info)
                    found_users = True

        if not found_users or not next_cursor:
            break
        cursor = next_cursor

    return following


# ---------------------------------------------------------------------------
# Tweet extraction (reused from search results)
# ---------------------------------------------------------------------------

def extract_tweet_results(body: dict[str, Any]) -> list[dict[str, Any]]:
    output = body.get("output", body)
    json_data = output.get("json", output) if isinstance(output, dict) else output
    if not isinstance(json_data, dict):
        return []

    result = json_data.get("result", json_data)
    timeline = result.get("timeline_response") or result.get("timeline") or {}
    timeline_inner = timeline.get("timeline", timeline)
    instructions = timeline_inner.get("instructions", [])

    tweets: list[dict[str, Any]] = []
    for inst in instructions:
        entries = inst.get("entries", [])
        for entry in entries:
            content = entry.get("content", {})
            inner = content.get("content", content)
            tweet_results = inner.get("tweet_results", {})
            tweet = tweet_results.get("result", {})
            if tweet and tweet.get("__typename") == "Tweet":
                tweets.append(tweet)
    return tweets


def tweet_id(tweet: dict[str, Any]) -> str:
    tid = tweet.get("rest_id")
    if tid:
        return str(tid)
    details = tweet.get("details", {})
    text = details.get("full_text", "")
    return hashlib.sha256(text.encode()).hexdigest()[:32]


def extract_tweet_text(tweet: dict[str, Any]) -> str:
    details = tweet.get("details", {})
    return details.get("full_text", "")


def extract_tweet_author(tweet: dict[str, Any]) -> str:
    core = tweet.get("core", {})
    user_results = core.get("user_results", {}).get("result", {})
    user_core = user_results.get("core", {})
    return user_core.get("screen_name") or user_core.get("name") or ""


def extract_tweet_timestamp(tweet: dict[str, Any]) -> str:
    details = tweet.get("details", {})
    created_at_ms = details.get("created_at_ms")
    if created_at_ms:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return iso_now()


def tweet_to_envelope(tweet: dict[str, Any]) -> bytes:
    tid = tweet_id(tweet)
    text = extract_tweet_text(tweet)
    author = extract_tweet_author(tweet)
    timestamp = extract_tweet_timestamp(tweet)

    return make_envelope(
        source="twitter",
        source_id=f"twitter-{tid}",
        content_type="social_post",
        timestamp=timestamp,
        data={
            "text": text,
            "author": author,
        },
        raw=tweet,
    )


# ---------------------------------------------------------------------------
# Poll logic
# ---------------------------------------------------------------------------

def search_twitter(query: str, api_key: str, timeout: float) -> list[dict[str, Any]]:
    resp = requests.post(
        f"{CARAVO_API_URL}/twitter241/search-v3/execute",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"query": query, "count": "20", "type": "Latest"},
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    return extract_tweet_results(body)


def _tweet_id_int(tid: str) -> int | None:
    """Try to parse a tweet ID as int for comparison."""
    try:
        return int(tid)
    except (ValueError, TypeError):
        return None


def poll_following_tweets(
    following: list[dict[str, str]],
    batch_size: int,
    api_key: str,
    timeout: float,
    seen_ids: set[str],
    max_seen_id: int,
    is_first_poll: bool,
) -> tuple[list[bytes], int]:
    """Search for recent tweets from followed users in batches.

    Returns (envelopes, new_max_seen_id).
    - First poll: adds `since:YYYY-MM-DD` for the past 24 hours.
    - Subsequent polls: adds `since_id:<max_seen_id>` to only fetch new tweets.
    """
    envelopes: list[bytes] = []
    new_max = max_seen_id

    # Build time filter suffix
    if is_first_poll:
        since_date = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d")
        time_filter = f" since:{since_date}"
    elif max_seen_id > 0:
        time_filter = f" since_id:{max_seen_id}"
    else:
        time_filter = ""

    # Batch usernames into search queries: "from:u1 OR from:u2 OR ..."
    for i in range(0, len(following), batch_size):
        batch = following[i:i + batch_size]
        query = " OR ".join(f"from:{u['username']}" for u in batch) + time_filter
        try:
            tweets = search_twitter(query, api_key, timeout)
            for tweet in tweets:
                tid = tweet_id(tweet)
                if tid in seen_ids:
                    continue
                seen_ids.add(tid)
                # Track max numeric tweet ID for since_id
                tid_int = _tweet_id_int(tid)
                if tid_int is not None and tid_int > new_max:
                    new_max = tid_int
                text = extract_tweet_text(tweet)
                if not text:
                    continue
                envelopes.append(tweet_to_envelope(tweet))
                TWEETS_FETCHED.inc()
        except Exception as ex:
            LOGGER.warning("twitter search batch failed error=%s", ex)

    return envelopes, new_max


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not args.caravo_api_key:
        LOGGER.error("CARAVO_API_KEY not set, exiting")
        return

    start_http_server(args.metrics_port)
    ensure_topic(args.bootstrap, args.topic)
    producer = create_producer(args.bootstrap)

    # Resolve username -> user ID
    LOGGER.info("resolving user ID for @%s ...", args.twitter_username)
    try:
        user_id = resolve_user_id(args.twitter_username, args.caravo_api_key, args.request_timeout_seconds)
    except Exception as ex:
        LOGGER.error("failed to resolve @%s: %s", args.twitter_username, ex)
        return
    LOGGER.info("resolved @%s -> user_id=%s", args.twitter_username, user_id)

    # Fetch initial following list
    LOGGER.info("fetching following list ...")
    following: list[dict[str, str]] = []
    try:
        following = fetch_following_list(user_id, args.caravo_api_key, args.request_timeout_seconds)
    except Exception as ex:
        LOGGER.error("failed to fetch following list: %s", ex)
    FOLLOWING_COUNT.set(len(following))
    LOGGER.info("following %d users", len(following))

    running = True

    def handle_signal(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    LOGGER.info(
        "twitter-ingestor started user=@%s following=%d topic=%s poll_interval=%.0fs",
        args.twitter_username, len(following), args.topic, args.poll_interval_seconds,
    )

    seen_ids: set[str] = set()
    MAX_SEEN = 10000
    max_seen_id: int = 0
    is_first_poll = True
    last_following_refresh = time.time()

    while running:
        # Refresh following list periodically
        if time.time() - last_following_refresh > args.followings_refresh_seconds:
            try:
                following = fetch_following_list(user_id, args.caravo_api_key, args.request_timeout_seconds)
                FOLLOWING_COUNT.set(len(following))
                last_following_refresh = time.time()
                LOGGER.info("refreshed following list: %d users", len(following))
            except Exception as ex:
                LOGGER.warning("failed to refresh following list: %s", ex)

        if not following:
            LOGGER.warning("no following list, sleeping")
            time.sleep(args.poll_interval_seconds)
            continue

        try:
            POLLS_TOTAL.inc()
            LAST_POLL_TIME.set(time.time())
            envelopes, max_seen_id = poll_following_tweets(
                following, args.search_batch_size,
                args.caravo_api_key, args.request_timeout_seconds,
                seen_ids,
                max_seen_id,
                is_first_poll,
            )
            is_first_poll = False
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
                excess = len(seen_ids) - MAX_SEEN // 2
                for _ in range(excess):
                    seen_ids.pop()

            LOGGER.info("poll done new_tweets=%d seen_total=%d max_id=%d", len(envelopes), len(seen_ids), max_seen_id)
        except Exception as ex:
            LOGGER.warning("poll cycle failed error=%s", ex)

        deadline = time.time() + args.poll_interval_seconds
        while running and time.time() < deadline:
            time.sleep(min(1.0, max(0, deadline - time.time())))

    producer.flush(10)
    LOGGER.info("twitter-ingestor stopped")


if __name__ == "__main__":
    main()
