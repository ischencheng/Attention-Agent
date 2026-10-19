#!/usr/bin/env python3
"""Read discovery-triggered SEC/FRED originals; --publish explicitly enters source_raw.

Examples (configuration comes only from the environment):
  python skills/research_sources.py '{"source":"sec_filings","symbol":"NVDA","limit":3}'
  python skills/research_sources.py '{"source":"sec_facts","symbol":"NVDA","concept":"Assets"}'
  python skills/research_sources.py '{"source":"fred","series_id":"DGS10","as_of":"2026-10-16"}'

SEC responses are current snapshots, not reconstructed historical vintages.
FRED observations explicitly pin both real-time dates; observation dates are
economic periods, not publication timestamps. This module never trades.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE = ROOT / "environments/attention_judgment/runs/research-sources"
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"
FRED_BASE = "https://api.stlouisfed.org/fred"
FIELDS = {"source", "symbol", "cik", "concept", "taxonomy", "series_id", "as_of", "start", "end", "limit", "forms"}


class SourceBlocked(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def utcnow():
    return datetime.now(timezone.utc)


def day(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{label} must be YYYY-MM-DD")
    return date.fromisoformat(value)


def token(value, label, pattern):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(f"Invalid {label}")
    return value


class Client:
    def __init__(self, store=DEFAULT_STORE, *, session=None, clock=utcnow):
        self.store = Path(store)
        self.session = session or requests.Session()
        self.clock = clock

    def fetch(self, url, *, params=None, headers=None):
        params = params or {}
        public_params = {key: value for key, value in params.items() if key != "api_key"}
        public_url = url + ("?" + urlencode(public_params) if public_params else "")
        self.store.mkdir(parents=True, exist_ok=True)
        # One local request gate is shared across agent processes. This is below
        # SEC's aggregate 10 requests/second ceiling; no aggressive retries.
        with (self.store / "request.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            lock.seek(0)
            previous = float(lock.read() or "0")
            time.sleep(max(0, 0.25 - (time.time() - previous)))
            try:
                response = self.session.get(url, params=params, headers=headers or {},
                                            timeout=30, allow_redirects=False)
                if response.status_code != 200:
                    raise SourceBlocked(f"Source HTTP {response.status_code}; no original accepted")
                value = response.json()
                if not isinstance(value, dict):
                    raise SourceBlocked("Source returned a non-object JSON document")
            except requests.RequestException as exc:
                # requests' normal exception string can expose FRED's API key.
                raise SourceBlocked(f"Source request failed ({type(exc).__name__})") from None
            except (json.JSONDecodeError, ValueError):
                raise SourceBlocked("Source returned invalid JSON") from None
            finally:
                lock.seek(0); lock.truncate(); lock.write(str(time.time())); lock.flush()
        observed = self.clock().isoformat()
        digest = sha(value)
        objects = self.store / "objects"
        objects.mkdir(exist_ok=True)
        path = objects / f"{digest}.json"
        if not path.exists():
            path.write_text(canonical(value) + "\n")
        return value, {"url": public_url, "observed_at": observed,
                       "response_sha256": digest, "snapshot_path": str(path.resolve())}

    def sec(self, params):
        user_agent = os.getenv("SEC_EDGAR_USER_AGENT", "").strip()
        if not re.search(r"[^\s@]+@[^\s@]+\.[^\s@]+", user_agent) or "@example." in user_agent.lower():
            raise SourceBlocked("Set SEC_EDGAR_USER_AGENT to the application's name and an authorized real contact address")
        if params.get("as_of") is not None:
            raise SourceBlocked("SEC lookup serves current snapshots; historical as_of reconstruction is not implemented")
        headers = {"User-Agent": user_agent, "Accept": "application/json"}
        snapshots = []
        symbol = params.get("symbol")
        cik = params.get("cik")
        if cik is None:
            symbol = token(symbol, "symbol", r"[A-Za-z0-9.\-]{1,20}").upper()
            mapping, receipt = self.fetch(SEC_TICKERS, headers=headers)
            snapshots.append(receipt)
            matches = [row for row in mapping.values() if isinstance(row, dict) and row.get("ticker", "").upper() == symbol]
            if not matches:
                raise SourceBlocked("Symbol is absent from the SEC ticker map; no filer inferred")
            cik = str(matches[0]["cik_str"])
        cik = token(str(cik), "CIK", r"\d{1,10}").zfill(10)
        source = params["source"]
        path = f"submissions/CIK{cik}.json" if source == "sec_filings" else f"api/xbrl/companyfacts/CIK{cik}.json"
        document, receipt = self.fetch("https://data.sec.gov/" + path, headers=headers)
        snapshots.append(receipt)
        records = []
        if source == "sec_filings":
            recent = document.get("filings", {}).get("recent", {})
            forms = params.get("forms", ["10-K", "10-Q", "8-K", "20-F", "6-K"])
            if not isinstance(forms, list) or not forms or any(not isinstance(form, str) for form in forms):
                raise ValueError("forms must be a nonempty list of filing form names")
            for index, accession in enumerate(recent.get("accessionNumber", [])):
                row = {name: values[index] for name, values in recent.items() if isinstance(values, list) and index < len(values)}
                if row.get("form") not in forms:
                    continue
                filing_date = row.get("filingDate")
                raw = {"kind": "sec_submission_metadata", "cik": cik, "record": row}
                record = self.record("sec_edgar", f"{cik}:{accession}", raw, receipt,
                                     title=f"SEC {symbol or cik} {row.get('form')} filing {filing_date}",
                                     published_at=row.get("acceptanceDateTime") or None,
                                     filing_date=filing_date)
                record["data"].update({"symbol": symbol, "cik": cik,
                    "document_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{row.get('primaryDocument', '')}",
                    "coverage": "SEC submission metadata; linked filing text was not downloaded"})
                records.append(record)
                if len(records) >= params["limit"]:
                    break
        else:
            taxonomy = token(params.get("taxonomy", "us-gaap"), "taxonomy", r"[A-Za-z0-9\-]{1,40}")
            concept = token(params.get("concept"), "concept", r"[A-Za-z][A-Za-z0-9]{0,160}")
            item = document.get("facts", {}).get(taxonomy, {}).get(concept)
            if not isinstance(item, dict):
                raise SourceBlocked("Requested SEC concept is absent; alternative tags are not silently substituted")
            points = [(unit, fact) for unit, facts in item.get("units", {}).items() for fact in facts if isinstance(fact, dict)]
            points.sort(key=lambda pair: (pair[1].get("filed", ""), pair[1].get("end", ""), pair[1].get("start", "")), reverse=True)
            for unit, fact in points[:params["limit"]]:
                raw = {"kind": "sec_company_fact", "cik": cik, "taxonomy": taxonomy,
                       "concept": concept, "label": item.get("label"), "description": item.get("description"),
                       "unit": unit, "record": fact}
                identity = f"{cik}:{taxonomy}:{concept}:{unit}:{fact.get('accn')}:{fact.get('start')}:{fact.get('end')}"
                record = self.record("sec_edgar", identity, raw, receipt,
                                     title=f"SEC {symbol or cik} financial fact {concept}", filing_date=fact.get("filed"))
                record["data"].update({"symbol": symbol, "cik": cik, "coverage": "Exact XBRL fact; periods and units are not aggregated"})
                records.append(record)
        return {"status": "ok" if records else "empty", "source": source,
                "symbol": symbol, "cik": cik, "records": records, "snapshots": snapshots,
                "latest_record_filing_date": max((r["data"]["filing_date"] or "" for r in records), default=None),
                "coverage": ("Current SEC submissions snapshot; recent filings only, not full history"
                             if source == "sec_filings" else
                             "Current companyfacts snapshot; a selected concept may be retired. Check each fact's filed date; no tags or periods are summed")}

    def record(self, source, identity, raw, receipt, *, title, published_at=None, filing_date=None, vintage=None):
        observed = receipt["observed_at"]
        return {"source": source, "source_id": identity, "content_type": "news_article",
                # Do not invent midnight publication from a date-only filing.
                "timestamp": published_at or observed,
                "data": {"title": title, "text": canonical(raw), "url": receipt["url"],
                         "observed_at": observed, "published_at": published_at,
                         "filing_date": filing_date,
                         "vintage": vintage or {"kind": "latest_snapshot", "observed_at": observed,
                                                "historical_availability_reconstructed": False}},
                "retrieval": receipt, "raw_sha256": sha(raw), "raw": raw}

    def fred(self, params):
        key = os.getenv("FRED_API_KEY", "").strip()
        if not key:
            raise SourceBlocked("FRED_API_KEY is not configured")
        series = token(params.get("series_id"), "FRED series_id", r"[A-Za-z0-9_\.]{1,80}").upper()
        today = self.clock().astimezone(ZoneInfo("America/Chicago")).date()
        as_of = day(params.get("as_of", today.isoformat()), "as_of")
        if as_of > today:
            raise ValueError("FRED as_of cannot be after the provider's current America/Chicago date")
        start = day(params.get("start", (as_of - timedelta(days=365)).isoformat()), "start")
        end = day(params.get("end", as_of.isoformat()), "end")
        if start > end or end > as_of:
            raise ValueError("Require start <= end <= as_of")
        query = {"series_id": series, "api_key": key, "file_type": "json",
                 "realtime_start": as_of.isoformat(), "realtime_end": as_of.isoformat()}
        metadata, metadata_receipt = self.fetch(FRED_BASE + "/series", params=query)
        rows = metadata.get("seriess", [])
        if len(rows) != 1 or rows[0].get("id") != series:
            raise SourceBlocked("FRED did not return the requested series metadata")
        query.update({"observation_start": start.isoformat(), "observation_end": end.isoformat(),
                      "sort_order": "desc", "limit": params["limit"]})
        document, receipt = self.fetch(FRED_BASE + "/series/observations", params=query)
        if (document.get("realtime_start") != as_of.isoformat()
                or document.get("realtime_end") != as_of.isoformat()):
            raise SourceBlocked("FRED returned a different real-time vintage than requested")
        records = []
        for observation in document.get("observations", []):
            if not (day(observation["realtime_start"], "realtime_start") <= as_of
                    <= day(observation["realtime_end"], "realtime_end")):
                raise SourceBlocked("FRED observation does not cover the requested vintage")
            raw = {"kind": "fred_observation", "series_id": series,
                   "series_metadata": rows[0], "observation": observation}
            vintage = {"kind": "fred_daily_point_in_time", "requested_as_of": as_of.isoformat(),
                       "realtime_start": observation["realtime_start"], "realtime_end": observation["realtime_end"],
                       "intraday_release_time_known": False}
            record = self.record("fred", f"{series}:{observation['date']}:{as_of}", raw, receipt,
                                 title=f"FRED macro {series}: {rows[0].get('title', series)}", vintage=vintage)
            record["data"].update({"observation_date": observation["date"],
                "coverage": "Vintage-pinned economic observation; date is the measured period, not release time"})
            records.append(record)
        return {"status": "ok" if records else "empty", "source": "fred", "series_id": series,
                "as_of": as_of.isoformat(), "records": records, "snapshots": [metadata_receipt, receipt],
                "coverage": "Bounded latest observations in the requested date range; no missing values imputed"}


def lookup(params, *, client=None):
    source = params.get("source") if isinstance(params, dict) else None
    try:
        if not isinstance(params, dict) or set(params) - FIELDS:
            raise ValueError("Expected JSON object with documented research-source parameters only")
        if source not in {"sec_filings", "sec_facts", "fred"}:
            raise ValueError("source must be sec_filings, sec_facts or fred")
        limit = params.get("limit", 5)
        if type(limit) is not int or not 1 <= limit <= 20:
            raise ValueError("limit must be an integer in 1..20")
        client = client or Client()
        params = {**params, "limit": limit}
        return client.fred(params) if source == "fred" else client.sec(params)
    except SourceBlocked as exc:
        return {"status": "blocked", "source": source, "reason": str(exc), "records": []}
    except (ValueError, KeyError, TypeError) as exc:
        return {"status": "invalid", "source": source, "reason": str(exc), "records": []}


def publish(result, store=DEFAULT_STORE):
    if result.get("status") not in {"ok", "empty"}:
        raise SourceBlocked("Blocked/invalid research cannot be published")
    sys.path.insert(0, str(ROOT / "llm-worker"))
    from worker_runtime import produce_confirmed, producer_config
    from confluent_kafka import Producer
    path = Path(store); path.mkdir(parents=True, exist_ok=True)
    with (path / "publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with sqlite3.connect(path / "published.sqlite3") as db:
            db.execute("CREATE TABLE IF NOT EXISTS published (key TEXT PRIMARY KEY, acknowledged_at TEXT NOT NULL)")
            producer = Producer(producer_config(os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")))
            receipts = []
            for record in result["records"]:
                identity = sha([record["source"], record["source_id"], record["raw_sha256"]])
                if db.execute("SELECT 1 FROM published WHERE key=?", (identity,)).fetchone():
                    receipts.append({"key": identity, "status": "already_acknowledged"}); continue
                value = canonical(record).encode()
                if len(value) > 900000:
                    raise SourceBlocked("Source record exceeds Kafka-safe publication limit; split the query")
                try:
                    produce_confirmed(producer, topic="source_raw", key=identity.encode(), value=value)
                    db.execute("INSERT INTO published VALUES (?,?)", (identity, utcnow().isoformat())); db.commit()
                    receipts.append({"key": identity, "status": "acknowledged", "topic": "source_raw"})
                except Exception as exc:
                    receipts.append({"key": identity, "status": "uncertain", "error_type": type(exc).__name__})
                    break
            return receipts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("params", help="JSON research request; credentials must be environment variables")
    parser.add_argument("--publish", action="store_true", help="Explicitly publish originals to source_raw with delivery acknowledgment")
    parser.add_argument("--output", type=Path, help="Save the complete research result locally")
    args = parser.parse_args()
    try:
        result = lookup(json.loads(args.params))
        if args.publish and result["status"] in {"ok", "empty"}:
            result["publication"] = publish(result)
            if any(item["status"] == "uncertain" for item in result["publication"]):
                result["status"] = "blocked"
    except Exception as exc:
        # Foreign library exceptions may contain credential-bearing request URLs.
        result = {"status": "blocked", "reason": f"Research operation failed ({type(exc).__name__})", "records": []}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in {"ok", "empty"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
