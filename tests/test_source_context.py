"""Native source context tests against real isolated PostgreSQL originals."""
from __future__ import annotations

from datetime import datetime, timezone
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pgserver
import psycopg2
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
from source_context import extract_native_context, source_context
from evidence import digest


def tweet(message, parent=None, *, milliseconds=1700000000000, text="Original source"):
    raw = {"rest_id": str(message), "details": {"created_at_ms": milliseconds, "full_text": text}}
    if parent is not None:
        raw["reply_to_results"] = {"rest_id": str(parent), "__typename": "Tweet"}
    return raw


class ExtractionTests(unittest.TestCase):
    def test_nested_quoted_post_never_supplies_message_identity_or_time(self):
        raw = {"quotedPostResults": {"result": tweet(9, 8)}, "reply_to_results": {"rest_id": "7"}}
        context = extract_native_context("twitter", "twitter-9", raw)
        self.assertIsNone(context["message_id"])
        self.assertIsNone(context["event_ts"])
        self.assertEqual(context["parent_message_id"], "7")
        self.assertEqual(context["identity_status"], "missing_native_identity")

    def test_no_inferred_thread_chat_or_event_time(self):
        raw = tweet(1)
        del raw["details"]["created_at_ms"]
        raw.update({"thread_id": "unverified-custom-field", "chat_id": "unverified-custom-field"})
        context = extract_native_context("twitter", "twitter-1", raw)
        self.assertEqual(context["identity_status"], "verified_native_identity")
        self.assertIsNone(context["event_ts"])
        self.assertIsNone(context["thread_id"])
        self.assertIsNone(context["chat_id"])


class HTTPProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(ROOT / "backend-fastapi"))
        spec = importlib.util.spec_from_file_location("source_context_api_test", ROOT / "backend-fastapi/app.py")
        cls.api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.api)

    def test_http_boundary_redacts_credentials_without_rehashing_display(self):
        raw = {"api_key": "private-value", "details": {"full_text": "Bearer private-credential"}}
        original_hash = digest(raw)
        result = {"records": [{"sha256": original_hash, "payload": {"raw": raw}}], "warnings": []}
        with patch.object(self.api, "source_context", return_value=result):
            response = asyncio.run(self.api.api_source_context(1, 30))
        encoded = json.dumps(response)
        self.assertNotIn("private-value", encoded)
        self.assertNotIn("private-credential", encoded)
        self.assertEqual(response["records"][0]["sha256"], original_hash)
        self.assertTrue(any("redacted or bounded" in warning for warning in response["warnings"]))


class SourceContextPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="attention-source-context-")
        cls.server = pgserver.get_server(Path(cls.directory.name) / "postgres", cleanup_mode="stop")
        cls.settings = parse_dsn(cls.server.get_uri())
        with psycopg2.connect(**cls.settings) as conn, conn.cursor() as cursor:
            cursor.execute("""CREATE TABLE raw_messages (
                id BIGSERIAL PRIMARY KEY, payload JSONB NOT NULL, raw JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
            cursor.execute("CREATE INDEX idx_raw_messages_source_id ON raw_messages ((payload->>'source_id'))")
            cursor.execute("CREATE INDEX idx_raw_messages_source ON raw_messages ((payload->>'source'))")

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()
        cls.directory.cleanup()

    def setUp(self):
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE raw_messages RESTART IDENTITY")

    def connect(self):
        return psycopg2.connect(**self.settings)

    def insert(self, raw, *, source="twitter", source_id=None, enriched=None,
               arrived="2026-10-19T12:00:00+00:00"):
        identity = source_id if source_id is not None else f"twitter-{raw['rest_id']}"
        envelope = {"source": source, "source_id": identity, "content_type": "social_post",
                    "timestamp": "2026-10-19T12:00:00+00:00", "data": {"text": "ENRICHED COPY"},
                    "raw": enriched if enriched is not None else {"text": "not authoritative"}}
        with self.connect() as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO raw_messages(payload,raw,created_at) VALUES(%s,%s,%s) RETURNING id",
                           (Json(envelope), Json(raw), arrived))
            return cursor.fetchone()[0]

    def test_child_to_parent_uses_native_pointer_authoritative_raw_and_event_time(self):
        child = self.insert(tweet(2, 1, milliseconds=1700000002000), arrived="2026-10-19T10:00:00Z")
        parent_body = tweet(1, milliseconds=1700000000000, text="Original parent")
        parent = self.insert(parent_body, arrived="2026-10-19T13:00:00Z")
        unrelated = self.insert(tweet(3, milliseconds=1700000001000))
        result = source_context(self.connect, child)
        self.assertEqual([row["id"] for row in result["records"]], [parent, child])
        self.assertNotIn(unrelated, [row["id"] for row in result["records"]])
        self.assertEqual(result["records"][0]["payload"]["raw"], parent_body)
        self.assertEqual(result["records"][0]["sha256"], digest(parent_body))
        self.assertEqual(result["edges"], [{"from_raw_id": child, "to_raw_id": parent, "relation": "reply_to",
                                            "verification": "native_parent_reference", "version_scope": "message_identity_only"}])
        self.assertEqual(result["coverage"], "locally_collected")
        self.assertFalse(result["frozen_evidence"])
        self.assertEqual(result["missing_references"], [])

    def test_missing_parent_and_source_namespace_collision_remain_unresolved(self):
        child = self.insert(tweet(2, 1))
        self.insert(tweet(1), source="other-service", source_id="twitter-1")
        result = source_context(self.connect, child)
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["edges"], [])
        self.assertEqual(result["missing_references"][0]["reason"], "not_collected")

    def test_candidate_native_identity_mismatch_is_reported_not_joined(self):
        child = self.insert(tweet(2, 1))
        conflicting = self.insert(tweet(999), source_id="twitter-1")
        result = source_context(self.connect, child)
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["edges"], [])
        self.assertEqual(result["identity_conflicts"][0]["candidate_raw_id"], conflicting)
        self.assertEqual(result["missing_references"][0]["reason"], "identity_mismatch")
        self.assertTrue(any("conflicting native" in warning for warning in result["warnings"]))

    def test_multiple_original_versions_are_explicit_and_never_select_a_historical_version(self):
        child = self.insert(tweet(2, 1))
        old = self.insert(tweet(1, text="earlier collected text"))
        new = self.insert(tweet(1, text="later collected text"))
        result = source_context(self.connect, child)
        self.assertEqual({row["id"] for row in result["records"]}, {child, old, new})
        self.assertEqual(len(result["edges"]), 2)
        self.assertTrue(all(edge["version_scope"] == "message_identity_only" for edge in result["edges"]))
        self.assertTrue(any("multiple stored originals" in warning for warning in result["warnings"]))

    def test_native_cycle_terminates_and_reports_the_closing_reference(self):
        first = self.insert(tweet(1, 2))
        second = self.insert(tweet(2, 1))
        result = source_context(self.connect, first)
        self.assertEqual(len(result["records"]), 2)
        self.assertEqual(len(result["edges"]), 2)
        self.assertEqual(result["cycles"][0]["from_raw_id"], second)
        self.assertEqual(result["cycles"][0]["reason"], "native_reply_cycle")

    def test_limit_bounds_records_and_exposes_unexpanded_reference(self):
        last = self.insert(tweet(3, 2))
        self.insert(tweet(2, 1))
        self.insert(tweet(1))
        result = source_context(self.connect, last, 2)
        self.assertEqual(len(result["records"]), 2)
        self.assertTrue(result["limits"]["truncated"])
        self.assertEqual(result["missing_references"][0]["reason"], "limit_reached")

    def test_unsupported_and_mismatched_anchor_do_not_expand(self):
        mismatch = self.insert(tweet(1, 2), source_id="twitter-999")
        unsupported = self.insert({"text": "source"}, source="rss", source_id="https://example.org")
        self.assertEqual(source_context(self.connect, mismatch)["status"], "identity_mismatch")
        self.assertEqual(source_context(self.connect, unsupported)["status"], "unsupported")
        self.assertIsNone(source_context(self.connect, 99999))
        for args in ((True, 30), (1, True), (1, 0), (1, 51)):
            with self.assertRaises(ValueError):
                source_context(self.connect, *args)

    def test_large_original_is_bounded_but_hash_covers_complete_stored_raw(self):
        original = tweet(1, text="x" * 80000)
        record = self.insert(original)
        result = source_context(self.connect, record)
        self.assertTrue(result["records"][0]["display_limited"])
        self.assertEqual(result["records"][0]["sha256"], digest(original))
        self.assertLess(len(json.dumps(result)), 40000)

    def test_nullable_and_malformed_optional_provider_nodes_preserve_parent_read(self):
        child_raw = tweet(2, 1)
        child_raw["note_tweet"] = None
        child = self.insert(child_raw)
        parent_raw = tweet(1, text="fallback parent text")
        parent_raw["note_tweet"] = {"note_tweet_results": {"result": None}}
        parent = self.insert(parent_raw)
        result = source_context(self.connect, child)
        displayed = next(row for row in result["records"] if row["id"] == parent)
        self.assertEqual(displayed["payload"]["data"]["text"], "fallback parent text")
        for details in (None, [], "not-an-object"):
            raw = {"rest_id": "8", "details": details, "note_tweet": [], "padding": "x" * 80000}
            record = self.insert(raw)
            self.assertTrue(source_context(self.connect, record)["records"][0]["display_limited"])

    def test_large_unsupported_non_object_source_is_bounded(self):
        record = self.insert(["x" * 80000], source="unsupported", source_id="source-1")
        result = source_context(self.connect, record)
        self.assertEqual(result["status"], "unsupported")
        self.assertTrue(result["records"][0]["display_limited"])

    def test_cli_and_api_helper_return_the_same_context(self):
        child = self.insert(tweet(2, 1))
        self.insert(tweet(1))
        env = dict(os.environ)
        env.update({f"DB_{key}": str(self.settings.get(name, "5432" if name == "port" else "postgres")) for key, name in
                    (("HOST", "host"), ("PORT", "port"), ("NAME", "dbname"), ("USER", "user"))})
        result = subprocess.run([sys.executable, str(ROOT / "skills/source_context.py"), json.dumps({"raw_id": child, "limit": 2})],
                                capture_output=True, text=True, env=env, check=True)
        self.assertEqual(json.loads(result.stdout), source_context(self.connect, child, 2))


if __name__ == "__main__":
    unittest.main()
