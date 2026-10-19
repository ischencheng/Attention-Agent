"""A standalone read projection must work without a live WAL writer or sidecars."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "skills"), str(ROOT / "environments/attention_judgment")]
from ledger import Ledger, digest
from trace_projection import publish_projection

spec = importlib.util.spec_from_file_location("trace_api_projection_test", ROOT / "backend-fastapi/trace_api.py")
trace = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trace)


def append(ledger, version):
    state = {"theses": {"thesis": {"claim": f"revision {version}"}}}
    ledger.append("production_research_state", str(version),
                  {"agent_id": "codex-strong", "version": version, "state": state, "state_sha256": digest(state)})


class ProjectionTests(unittest.TestCase):
    def test_idle_wal_source_projects_to_read_only_directory_without_sidecars(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            source, target = base / "ledger.sqlite3", base / "trace.sqlite3"
            ledger = Ledger(source)
            append(ledger, 1)
            ledger.close()
            self.assertFalse(Path(str(source) + "-wal").exists())
            self.assertFalse(Path(str(source) + "-shm").exists())
            before = source.read_bytes()
            metadata = publish_projection(source, target)
            self.assertEqual(source.read_bytes(), before)
            mounted = base / "readonly"
            mounted.mkdir()
            only_database = mounted / "trace.sqlite3"
            shutil.copy2(target, only_database)
            only_database.chmod(0o444)
            mounted.chmod(0o555)
            try:
                with patch.dict(os.environ, ATTENTION_PRODUCTION_LEDGER=str(only_database)):
                    found, notes, projection = trace.archived_research(with_projection=True)
                self.assertEqual(len(found), 1)
                self.assertTrue(found[0]["entry_hash_verified"])
                self.assertEqual(projection["status"], "available")
                self.assertEqual(projection["source_sequence"], 1)
                self.assertEqual(projection["captured_at"], metadata["captured_at"])
                self.assertLess(projection["age_seconds"], 5)
                self.assertEqual([path.name for path in mounted.iterdir()], ["trace.sqlite3"])
                with sqlite3.connect(only_database.as_uri() + "?mode=ro", uri=True) as db:
                    self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            finally:
                mounted.chmod(0o755)
                only_database.chmod(0o600)

    def test_backup_includes_live_wal_commits_and_publishes_new_version_atomically(self):
        with tempfile.TemporaryDirectory() as folder:
            source, target = Path(folder) / "ledger.sqlite3", Path(folder) / "trace.sqlite3"
            ledger = Ledger(source)
            try:
                append(ledger, 1)
                self.assertTrue(Path(str(source) + "-wal").exists())
                publish_projection(source, target)
                with patch.dict(os.environ, ATTENTION_PRODUCTION_LEDGER=str(target)):
                    first, _ = trace.archived_research()
                    append(ledger, 2)
                    still_first, _ = trace.archived_research()
                    publish_projection(source, target)
                    refreshed, _, projection = trace.archived_research(with_projection=True)
                self.assertEqual(len(first), 1)
                self.assertEqual(len(still_first), 1)
                self.assertEqual(len(refreshed), 2)
                self.assertEqual(projection["source_sequence"], 2)
                self.assertEqual(ledger.verify(), refreshed[0]["hash"])
            finally:
                ledger.close()

    def test_failed_refresh_preserves_previous_complete_projection(self):
        with tempfile.TemporaryDirectory() as folder:
            source, target = Path(folder) / "ledger.sqlite3", Path(folder) / "trace.sqlite3"
            ledger = Ledger(source)
            append(ledger, 1)
            ledger.close()
            publish_projection(source, target)
            before = target.read_bytes()
            bad = Path(folder) / "empty.sqlite3"
            sqlite3.connect(bad).close()
            with self.assertRaises(sqlite3.OperationalError):
                publish_projection(bad, target)
            self.assertEqual(target.read_bytes(), before)
            self.assertEqual(list(Path(folder).glob(".trace-projection-*")), [])
            with self.assertRaises(ValueError):
                publish_projection(source, source)


if __name__ == "__main__":
    unittest.main()
