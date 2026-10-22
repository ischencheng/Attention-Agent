"""Durable research identity guards and real PostgreSQL write serialization."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import redirect_stdout
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import pgserver
import psycopg2
from psycopg2.extensions import parse_dsn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills"))
import agent_state


def theme(identity, **fields):
    return {"theme_id": identity, **fields}


class StateGuardTests(unittest.TestCase):
    def test_incident_list_to_grouped_object_is_rejected(self):
        current = {"pending_themes": [theme(f"theme-{i}") for i in range(480)]}
        partial = {"pending_themes": {"batch": [theme("new-theme")]}}
        for candidate in (partial, agent_state.deep_merge(current, partial)):
            with self.subTest(candidate_type=type(candidate["pending_themes"]).__name__):
                with self.assertRaisesRegex(agent_state.StateValidationError, "container type"):
                    agent_state.validate_state_update(current, candidate)

    def test_list_identity_loss_and_same_length_replacement_are_rejected(self):
        current = {"pending_themes": [theme("a"), theme("b")]}
        for replacement in ([], [theme("a")], [theme("a"), theme("c")]):
            with self.subTest(replacement=replacement):
                with self.assertRaises(agent_state.StateValidationError):
                    agent_state.validate_state_update(current, {"pending_themes": replacement})

    def test_dictionary_and_grouped_list_identities_cannot_disappear(self):
        cases = [
            ({"theses": {"a": {}, "b": {}}}, {"theses": {"a": {}}}),
            ({"pending_themes": {"a": {}}}, {"pending_themes": []}),
            ({"pending_themes": {"batch": [theme("a"), theme("b")]}},
             {"pending_themes": {"batch": [theme("a")]}}),
            ({"theses": [{"thesis_id": "a"}]}, {"theses": []}),
        ]
        for current, proposed in cases:
            with self.subTest(current=current):
                with self.assertRaises(agent_state.StateValidationError):
                    agent_state.validate_state_update(current, proposed)

    def test_missing_protected_fields_and_invalid_records_fail_closed(self):
        for current in ({"pending_themes": []}, {"theses": {}}, {"handled_ticket_ids": [1]}):
            with self.subTest(current=current), self.assertRaises(agent_state.StateValidationError):
                agent_state.validate_state_update(current, {})
        for records in ([{}], [theme(1)], [theme("a"), theme("a")], None, {"a": None}):
            with self.subTest(records=records), self.assertRaises(agent_state.StateValidationError):
                agent_state.validate_state_update({}, {"pending_themes": records})

    def test_record_compaction_reordering_and_additions_are_allowed(self):
        current = {"pending_themes": [theme("a", payload_snapshot={"bulky": [1] * 50},
                                           raw_ids=[1], open_questions=["Verify source"])],
                   "theses": {"thesis-a": {"payload_snapshot": {"bulky": True}}},
                   "handled_ticket_ids": [1, 2], "reviewed_ticket_ids": [1, 2]}
        proposed = {"pending_themes": [theme("b"), theme("a", raw_ids=[1],
                                                         open_questions=["Verify source"])],
                    "theses": {"thesis-a": {"raw_ids": [1]}, "thesis-b": {}},
                    "handled_ticket_ids": [2, 1, 3], "reviewed_ticket_ids": [3]}
        agent_state.validate_state_update(current, proposed)

    def test_partial_dictionary_merge_preserves_omitted_collections(self):
        current = {"pending_themes": {"a": {"raw_ids": [1]}}, "theses": {"t": {}},
                   "handled_ticket_ids": [1], "unrelated": {"keep": True}}
        updated = agent_state.deep_merge(current, {"pending_themes": {"b": {}}, "last_cycle": "new"})
        agent_state.validate_state_update(current, updated)
        self.assertEqual(set(updated["pending_themes"]), {"a", "b"})
        self.assertEqual(updated["unrelated"], current["unrelated"])

    def test_handled_ticket_ids_are_cumulative_but_reviewed_ids_are_not(self):
        current = {"handled_ticket_ids": list(range(1, 136)), "reviewed_ticket_ids": [1]}
        partial = {"handled_ticket_ids": list(range(911, 918)), "reviewed_ticket_ids": list(range(911, 918))}
        with self.assertRaisesRegex(agent_state.StateValidationError, "handled_ticket_ids"):
            agent_state.validate_state_update(current, agent_state.deep_merge(current, partial))
        agent_state.validate_state_update(current, {**current, "reviewed_ticket_ids": [911]})
        for invalid in ([True], [0], "1", None):
            with self.subTest(invalid=invalid), self.assertRaises(agent_state.StateValidationError):
                agent_state.validate_state_update({}, {"handled_ticket_ids": invalid})

    def test_cli_rejection_is_nonzero_without_echoing_state(self):
        output = io.StringIO()
        args = {"agent_id": "test", "action": "set", "state": {"private": "do-not-echo"}}
        with patch.object(sys, "argv", ["agent_state.py", json.dumps(args)]), \
                patch.object(agent_state, "set_state", side_effect=agent_state.StateValidationError("Identity loss")), \
                redirect_stdout(output), self.assertRaises(SystemExit) as failure:
            agent_state.main()
        self.assertEqual(failure.exception.code, 1)
        self.assertEqual(json.loads(output.getvalue()), {"error": "Identity loss", "status": "rejected"})


class StatePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="attention-state-tests-")
        cls.server = pgserver.get_server(Path(cls.directory.name) / "postgres", cleanup_mode="stop")
        cls.settings = parse_dsn(cls.server.get_uri())
        with psycopg2.connect(**cls.settings) as conn, conn.cursor() as cur:
            cur.execute("""CREATE TABLE agent_state (
                agent_id text PRIMARY KEY, state jsonb NOT NULL DEFAULT '{}',
                updated_at timestamptz NOT NULL DEFAULT now())""")

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()
        cls.directory.cleanup()

    def setUp(self):
        self.config = patch.object(agent_state, "DB_CONFIG", self.settings)
        self.config.start()
        self.addCleanup(self.config.stop)
        with psycopg2.connect(**self.settings) as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE agent_state")

    def test_rejected_set_and_merge_leave_state_and_timestamp_unchanged(self):
        original = {"pending_themes": [theme(f"theme-{i}") for i in range(480)],
                    "handled_ticket_ids": list(range(1, 136)), "reviewed_ticket_ids": [1]}
        agent_state.set_state("mid", original)
        before = agent_state.get_state("mid")
        destructive = {"pending_themes": {"batch": [theme("new")]},
                       "handled_ticket_ids": list(range(911, 918))}
        for writer in (agent_state.set_state, agent_state.merge_state):
            with self.subTest(writer=writer.__name__), self.assertRaises(agent_state.StateValidationError):
                writer("mid", destructive)
            self.assertEqual(agent_state.get_state("mid"), before)

    def test_invalid_initial_write_rolls_back_placeholder(self):
        with self.assertRaises(agent_state.StateValidationError):
            agent_state.merge_state("mid", {"pending_themes": [{}]})
        self.assertEqual(agent_state.get_state("mid"), {"agent_id": "mid", "state": {}, "updated_at": None})

    def test_valid_compaction_and_partial_merge_persist(self):
        original = {"pending_themes": [theme("a", raw_ids=[1], payload_snapshot={"large": True})],
                    "handled_ticket_ids": [1], "reviewed_ticket_ids": [1]}
        agent_state.set_state("mid", original)
        compact = copy.deepcopy(original)
        del compact["pending_themes"][0]["payload_snapshot"]
        self.assertEqual(agent_state.set_state("mid", compact)["state"], compact)
        updated = agent_state.merge_state("mid", {"last_cycle": "new", "reviewed_ticket_ids": [2]})
        self.assertEqual(updated["state"]["pending_themes"], compact["pending_themes"])
        self.assertEqual(updated["state"]["handled_ticket_ids"], [1])
        self.assertEqual(agent_state.get_state("mid")["state"], updated["state"])

    def test_concurrent_merges_include_first_insert_and_preserve_both_updates(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                agent = f"concurrent-{existing}"
                if existing:
                    agent_state.set_state(agent, {})
                first_read = threading.Event()
                release_first = threading.Event()
                original_merge = agent_state.deep_merge

                def pause_first(current, partial):
                    if partial == {"left": True}:
                        first_read.set()
                        if not release_first.wait(5):
                            raise TimeoutError("Test did not release first writer")
                    return original_merge(current, partial)

                with patch.object(agent_state, "deep_merge", side_effect=pause_first), \
                        ThreadPoolExecutor(max_workers=2) as pool:
                    first = pool.submit(agent_state.merge_state, agent, {"left": True})
                    try:
                        self.assertTrue(first_read.wait(5))
                        second = pool.submit(agent_state.merge_state, agent, {"right": True})
                        # A separate connection must wait while the first writer
                        # holds its read/modify/write transaction, even on insert.
                        with self.assertRaises(TimeoutError):
                            second.result(timeout=0.3)
                    finally:
                        release_first.set()
                    first.result(timeout=5)
                    second.result(timeout=5)
                self.assertEqual(agent_state.get_state(agent)["state"], {"left": True, "right": True})


if __name__ == "__main__":
    unittest.main()
