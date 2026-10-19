"""Real PostgreSQL original-export tests; no production database or model calls."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pgserver
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("export_observations", ROOT / "skills/export_observations.py")
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)
JUDGMENT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JUDGMENT))
from sources import load_jsonl


ARRIVAL = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


class ExportPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="attention-export-tests-")
        cls.server = pgserver.get_server(Path(cls.directory.name) / "postgres", cleanup_mode="stop")
        cls.counter = 0

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()
        cls.directory.cleanup()

    def setUp(self):
        type(self).counter += 1
        name = f"export_case_{self.counter}"
        connection = psycopg2.connect(self.server.get_uri())
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        finally:
            connection.close()
        self.settings = parse_dsn(self.server.get_uri(name))
        with psycopg2.connect(**self.settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute((ROOT / "postgres/init/001_schema.sql").read_text())
        self.output = Path(self.directory.name) / f"{name}.jsonl"
        self.offset = 0

    def insert(self, raw, *, source="twitter", source_id="post-1", published="2026-10-19T06:00:00-04:00", arrival=ARRIVAL):
        self.offset += 1
        payload = {
            "source": source, "source_id": source_id, "timestamp": published,
            "data": {"text": "ENRICHED COPY: do not export me as original"},
            "raw": {"not": "the authoritative raw column"},
        }
        with psycopg2.connect(**self.settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO raw_messages (kafka_topic,kafka_partition,kafka_offset,payload,raw,created_at) "
                    "VALUES ('source_raw',0,%s,%s,%s,%s) RETURNING id",
                    (self.offset, Json(payload), Json(raw), arrival),
                )
                return cursor.fetchone()[0]

    def export(self, **kwargs):
        report = exporter.export_observations(self.output, settings=self.settings, **kwargs)
        return report, [json.loads(line) for line in self.output.read_text().splitlines()]

    def test_authoritative_original_and_database_availability(self):
        raw = {"原文": "完整原文 🛰️", "nested": [False, 0, None, {"quoted": "é ≠ é"}]}
        row_id = self.insert(raw)
        report, rows = self.export()
        self.assertEqual(report["count"], 1)
        self.assertEqual(rows[0]["raw"], raw)
        self.assertEqual(rows[0]["database_row_id"], row_id)
        self.assertEqual(rows[0]["published_at"], "2026-10-19T10:00:00+00:00")
        self.assertEqual(rows[0]["available_at"], ARRIVAL.isoformat())
        self.assertEqual(rows[0]["envelope_timestamp"], "2026-10-19T06:00:00-04:00")
        self.assertEqual(rows[0]["published_at_origin"], "envelope.timestamp")
        loaded = load_jsonl(self.output)
        self.assertEqual(loaded[0]["id"], rows[0]["id"])
        self.assertEqual(loaded[0]["raw_sha256"], rows[0]["raw_sha256"])

    def test_no_last_forty_cap_or_price_filter(self):
        ids = []
        for index in range(75):
            ids.append(self.insert({"p": index, "s": "SYNTHETIC"}, source="finnhub",
                                   source_id=f"tick-{index}", arrival=ARRIVAL - timedelta(minutes=index)))
        report, rows = self.export()
        self.assertEqual(report["count"], 75)
        self.assertFalse(report["has_more"])
        self.assertEqual(report["source_counts"], {"finnhub": 75})
        self.assertEqual([row["database_row_id"] for row in rows], list(reversed(ids)))
        self.assertEqual([row["raw"]["p"] for row in rows], list(reversed(range(75))))

    def test_timestamp_fallback_is_explicit_and_future_time_not_clamped(self):
        for index, value in enumerate((None, "bad-date", "2026-10-18T06:00:00", "2030-01-01T00:00:00Z")):
            self.insert({"source_statement": index}, source_id=f"dated-{index}", published=value)
        report, rows = self.export()
        self.assertEqual(report["timestamp_fallbacks"], 3)
        for row in rows[:3]:
            self.assertEqual(row["published_at"], ARRIVAL.isoformat())
            self.assertEqual(row["published_at_origin"], "raw_messages.created_at")
        self.assertEqual(rows[3]["published_at"], "2030-01-01T00:00:00+00:00")
        self.assertEqual(rows[3]["available_at"], ARRIVAL.isoformat())

    def test_namespace_revision_and_replay_are_preserved(self):
        first = self.insert({"text": "v1"})
        second = self.insert({"text": "v1"}, arrival=ARRIVAL + timedelta(seconds=1))
        third = self.insert({"text": "corrected v2"}, arrival=ARRIVAL + timedelta(seconds=2))
        fourth = self.insert({"text": "v1"}, source="polygon", arrival=ARRIVAL + timedelta(seconds=3))
        _, rows = self.export()
        self.assertEqual([r["database_row_id"] for r in rows], [first, second, third, fourth])
        self.assertEqual(rows[0]["id"], rows[1]["id"])
        self.assertNotEqual(rows[0]["id"], rows[2]["id"])
        self.assertEqual(rows[0]["event_id"], rows[2]["event_id"])
        self.assertNotEqual(rows[0]["event_id"], rows[3]["event_id"])
        self.assertEqual(rows[1]["available_at"], (ARRIVAL + timedelta(seconds=1)).isoformat())

    def test_limit_cursor_does_not_drop_timestamp_ties(self):
        self.insert({"old": True}, source_id="before-cutoff", arrival=ARRIVAL - timedelta(seconds=1))
        expected = [self.insert({"value": i}, source_id=f"same-time-{i}") for i in range(5)]
        report, first = self.export(since=ARRIVAL.isoformat(), limit=2)
        self.assertTrue(report["has_more"])
        cursor = report["next_cursor"]
        report, second = self.export(since=cursor["since"], after_row_id=cursor["after_row_id"], limit=2)
        self.assertTrue(report["has_more"])
        cursor = report["next_cursor"]
        report, third = self.export(since=cursor["since"], after_row_id=cursor["after_row_id"], limit=2)
        self.assertFalse(report["has_more"])
        self.assertEqual([row["database_row_id"] for row in first + second + third], expected)

    def test_invalid_original_does_not_replace_previous_export(self):
        self.insert({"valid": "original"}, source_id="good")
        self.insert(None, source_id="missing-original")
        self.output.write_text("previous complete export\n")
        with self.assertRaisesRegex(ValueError, "non-null original"):
            self.export()
        self.assertEqual(self.output.read_text(), "previous complete export\n")
        self.assertEqual(list(self.output.parent.glob(f".{self.output.name}.*.tmp")), [])

    def test_falsy_json_values_and_read_only_database_session(self):
        for index, raw in enumerate((False, 0, "", [])):
            self.insert(raw, source_id=f"primitive-{index}")
        # The server also enforces read-only for this connection independently of
        # the exporter's own set_session call; INSERT/UPDATE/temp-table work fails.
        self.settings["options"] = "-c default_transaction_read_only=on"
        report, rows = self.export()
        self.assertEqual(report["count"], 4)
        self.assertEqual([row["raw"] for row in rows], [False, 0, "", []])
        with psycopg2.connect(**self.settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM raw_messages")
                self.assertEqual(cursor.fetchone()[0], 4)


class ExportValidationTests(unittest.TestCase):
    def test_bad_cursor_or_naive_since_fails_before_connect(self):
        for kwargs in ({"since": "2026-10-19"}, {"limit": 0}, {"after_row_id": 1}):
            with self.subTest(kwargs=kwargs), patch.object(exporter.psycopg2, "connect") as connect:
                with self.assertRaises(ValueError):
                    exporter.export_observations("unused.jsonl", **kwargs)
                connect.assert_not_called()

    def test_cli_does_not_print_database_credentials_on_error(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output.jsonl"
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(sys, "argv", ["export_observations.py", "--output", str(output)]), \
                    patch.object(exporter.psycopg2, "connect", side_effect=psycopg2.OperationalError("password=TOP-SECRET")), \
                    redirect_stdout(stdout), redirect_stderr(stderr):
                status = exporter.main()
            self.assertEqual(status, 1)
            self.assertNotIn("TOP-SECRET", stdout.getvalue() + stderr.getvalue())
            self.assertIn("OperationalError", stderr.getvalue())
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
