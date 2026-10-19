"""Point-in-time observations from explicit JSONL input or bounded public RSS."""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
import html
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
from ledger import digest, timestamp

SCOPE = json.loads(Path(__file__).with_name("scope.json").read_text())


def relevant(text, source=""):
    # Preserve original followed-Twitter exception. Imported source must reflect
    # the upstream followed-account collector, not arbitrary public Twitter.
    return source == "twitter" or any(k in text.lower() for k in SCOPE["keywords"])


def observation(source, source_id, published_at, available_at, raw, event_id=None):
    timestamp(published_at)
    timestamp(available_at)
    content_hash = digest(raw)
    return {"id": digest([source, source_id, content_hash]), "source": source,
            "source_id": source_id, "published_at": published_at,
            "available_at": available_at, "event_id": event_id or digest([source, source_id]),
            "raw": raw, "raw_sha256": content_hash}


def load_jsonl(path):
    result = []
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        result.append(observation(**{k: item[k] for k in ("source", "source_id", "published_at", "available_at", "raw")}, event_id=item.get("event_id")))
    return result


def fetch_feeds(output_dir, max_age_days=14, limit_per_feed=8):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    observations, coverage = [], []
    for index, feed in enumerate(SCOPE["pilot_feeds"]):
        try:
            req = Request(feed["url"], headers={"User-Agent": "AttentionProxyResearch/0.1 (public RSS reader)"})
            with urlopen(req, timeout=25) as response:
                data = response.read(4_000_001)
            if len(data) > 4_000_000:
                raise ValueError("feed exceeds bounded size")
            available = datetime.now(timezone.utc)
            (output_dir / f"feed-{index}.xml").write_bytes(data)
            items = ET.fromstring(data).findall(".//item")
            accepted = []
            for item in items:
                date = item.findtext("pubDate") or ""
                try:
                    published = parsedate_to_datetime(date).astimezone(timezone.utc)
                except (ValueError, TypeError):
                    continue
                if published > available or published < available - timedelta(days=max_age_days):
                    continue
                title = item.findtext("title") or ""
                description = item.findtext("description") or ""
                content = item.findtext("{http://purl.org/rss/1.0/modules/content/}encoded") or description
                if not relevant(title + " " + content):
                    continue
                url = item.findtext("link") or item.findtext("guid")
                if not url:
                    continue
                raw = {"title": title, "url": url, "description_html": description,
                       "content_html": content, "feed_url": feed["url"], "coverage": "publisher-supplied RSS content; full article may require further research"}
                accepted.append(observation(feed["name"], url, published.isoformat(), available.isoformat(), raw))
            accepted.sort(key=lambda x: x["published_at"], reverse=True)
            observations.extend(accepted[:limit_per_feed])
            coverage.append({**feed, "status": "ok", "available_at": available.isoformat(), "selected": len(accepted[:limit_per_feed]), "feed_sha256": digest(data.hex())})
        except Exception as exc:
            coverage.append({**feed, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
    (output_dir / "coverage.json").write_text(json.dumps(coverage, indent=2))
    return observations, coverage


def prompt_observation(item):
    raw = item["raw"]
    # This is a bounded view of the stored original, never represented as full
    # source coverage. Authoritative raw content and its hash remain in ledger.
    text = json.dumps(raw, ensure_ascii=False)
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    return {**{k: item[k] for k in ("id", "source", "source_id", "event_id", "published_at", "available_at", "raw_sha256")}, "content": text[:14000], "truncated": len(text) > 14000}
