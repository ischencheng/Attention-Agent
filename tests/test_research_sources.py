"""Research originals, vintage pinning, credential scrubbing and acknowledged publication."""
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import requests

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("research_sources", ROOT / "skills/research_sources.py")
sources = importlib.util.module_from_spec(spec); spec.loader.exec_module(sources)
NOW = datetime(2026, 10, 19, 5, 30, tzinfo=timezone.utc)


class Response:
    def __init__(self, value, status=200): self.value, self.status_code = value, status
    def json(self): return self.value


class Session:
    def __init__(self, values): self.values, self.calls = iter(values), []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = next(self.values)
        if isinstance(result, Exception): raise result
        return result


class ResearchSourceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.patch = patch.dict(os.environ, {"SEC_EDGAR_USER_AGENT": "", "FRED_API_KEY": ""})
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.sleep = patch.object(sources.time, "sleep")
        self.sleep.start(); self.addCleanup(self.sleep.stop)

    def client(self, *values):
        session = Session(values)
        return sources.Client(self.directory.name, session=session, clock=lambda: NOW), session

    def test_missing_credentials_block_without_network(self):
        client, session = self.client()
        for params in ({"source": "sec_filings", "symbol": "NVDA"}, {"source": "fred", "series_id": "DGS10"}):
            result = sources.lookup(params, client=client)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["records"], [])
        self.assertEqual(session.calls, [])

    def test_sec_does_not_claim_historical_snapshot(self):
        os.environ["SEC_EDGAR_USER_AGENT"] = "Test tests@local.test"
        client, session = self.client()
        result = sources.lookup({"source": "sec_facts", "symbol": "NVDA", "concept": "Assets", "as_of": "2020-01-01"}, client=client)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(session.calls, [])

    def test_sec_original_fact_keeps_filing_period_unit_and_snapshot(self):
        os.environ["SEC_EDGAR_USER_AGENT"] = "Test tests@local.test"
        fact = {"start": "2025-01-01", "end": "2025-03-31", "filed": "2025-05-20",
                "val": 45, "accn": "0000000001-25-000001", "form": "10-Q"}
        client, _ = self.client(Response({"facts": {"us-gaap": {"Revenues": {"label": "Revenue", "units": {"USD": [fact]}}}}}))
        result = sources.lookup({"source": "sec_facts", "cik": "1", "concept": "Revenues"}, client=client)
        self.assertEqual(result["status"], "ok")
        record = result["records"][0]
        self.assertEqual(record["raw"]["record"], fact)
        self.assertEqual(record["raw"]["unit"], "USD")
        self.assertIsNone(record["data"]["published_at"])
        self.assertEqual(record["timestamp"], NOW.isoformat())
        self.assertEqual(record["data"]["filing_date"], "2025-05-20")
        self.assertFalse(record["data"]["vintage"]["historical_availability_reconstructed"])
        snapshot = result["snapshots"][0]
        self.assertEqual(sources.sha(json.loads(Path(snapshot["snapshot_path"]).read_text())), snapshot["response_sha256"])

    def test_fred_pins_vintage_and_preserves_missing_value(self):
        os.environ["FRED_API_KEY"] = "secret-fred-key"
        metadata = {"seriess": [{"id": "DGS10", "title": "Treasury Yield", "units": "Percent"}]}
        observation = {"date": "2026-10-15", "value": ".", "realtime_start": "2026-10-16", "realtime_end": "2026-10-16"}
        client, session = self.client(Response(metadata), Response({"realtime_start": "2026-10-16", "realtime_end": "2026-10-16", "observations": [observation]}))
        result = sources.lookup({"source": "fred", "series_id": "DGS10", "as_of": "2026-10-16"}, client=client)
        self.assertEqual(result["status"], "ok")
        self.assertNotIn("secret-fred-key", json.dumps(result))
        for _, call in session.calls:
            self.assertEqual(call["params"]["realtime_start"], "2026-10-16")
            self.assertEqual(call["params"]["realtime_end"], "2026-10-16")
            self.assertFalse(call["allow_redirects"])
        record = result["records"][0]
        self.assertEqual(record["raw"]["observation"]["value"], ".")
        self.assertIsNone(record["data"]["published_at"])
        self.assertFalse(record["data"]["vintage"]["intraday_release_time_known"])

    def test_fred_rejects_wrong_returned_vintage(self):
        os.environ["FRED_API_KEY"] = "key"
        client, _ = self.client(Response({"seriess": [{"id": "DGS10"}]}),
                                Response({"realtime_start": "2026-10-19", "realtime_end": "2026-10-19", "observations": []}))
        result = sources.lookup({"source": "fred", "series_id": "DGS10", "as_of": "2026-10-16"}, client=client)
        self.assertEqual(result["status"], "blocked")

    def test_provider_errors_never_echo_secret_url(self):
        os.environ["FRED_API_KEY"] = "secret-fred-key"
        client, _ = self.client(requests.ConnectionError("bad https://example/?api_key=secret-fred-key"))
        result = sources.lookup({"source": "fred", "series_id": "DGS10"}, client=client)
        self.assertEqual(result["status"], "blocked")
        self.assertNotIn("secret-fred-key", json.dumps(result))

    def test_future_and_invalid_identifiers_do_not_fetch(self):
        os.environ["FRED_API_KEY"] = "key"
        client, session = self.client()
        for params in ({"source": "fred", "series_id": "https://evil"},
                       {"source": "fred", "series_id": "DGS10", "as_of": "2026-10-20"},
                       {"source": "fred", "series_id": "DGS10", "limit": True}):
            self.assertEqual(sources.lookup(params, client=client)["status"], "invalid")
        self.assertEqual(session.calls, [])

    def test_publish_dedups_only_after_broker_ack(self):
        raw = {"kind": "fact", "record": {"val": 42}}
        record = {"source": "sec_edgar", "source_id": "filing-1", "raw": raw, "raw_sha256": sources.sha(raw)}
        result = {"status": "ok", "records": [record]}
        sys.path.insert(0, str(ROOT / "llm-worker"))
        with patch("confluent_kafka.Producer"), patch("worker_runtime.produce_confirmed") as send:
            self.assertEqual(sources.publish(result, self.directory.name)[0]["status"], "acknowledged")
            self.assertEqual(sources.publish(result, self.directory.name)[0]["status"], "already_acknowledged")
            send.assert_called_once()

    def test_failed_publication_remains_retryable(self):
        raw = {"kind": "fact"}
        result = {"status": "ok", "records": [{"source": "sec_edgar", "source_id": "one", "raw": raw, "raw_sha256": sources.sha(raw)}]}
        sys.path.insert(0, str(ROOT / "llm-worker"))
        with patch("confluent_kafka.Producer"), patch("worker_runtime.produce_confirmed", side_effect=TimeoutError):
            self.assertEqual(sources.publish(result, self.directory.name)[0]["status"], "uncertain")
        with patch("confluent_kafka.Producer"), patch("worker_runtime.produce_confirmed") as send:
            self.assertEqual(sources.publish(result, self.directory.name)[0]["status"], "acknowledged")
            send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
