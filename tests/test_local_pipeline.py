"""Scheduler checkpoint/backlog tests with no models, containers or live DB."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("local_pipeline", ROOT / "scripts/local_pipeline.py")
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class StateConnection:
    def __init__(self, saved=True, state=None, updated_at=None):
        self.saved = saved
        self.state = state if state is not None else {}
        self.updated_at = updated_at

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self

    def execute(self, sql, args=()):
        assert sql.startswith("SELECT state, updated_at FROM agent_state")

    def fetchone(self):
        updated_at = self.updated_at or datetime.now(timezone.utc) + timedelta(seconds=1)
        return (self.state, updated_at) if self.saved else None


def completed(prompt, *_args, **_kwargs):
    role = "mid" if "one mid-agent cycle" in prompt else "strong"
    ids = json.loads(re.search(r"pending \w+ IDs: (\[[^\]]*\])", prompt).group(1))
    return {"final": {"role": role, "status": "completed", "summary": "Fixture reviewed inputs",
                      "reviewed_ticket_ids": ids if role == "mid" else [],
                      "reviewed_annotation_ids": ids if role == "strong" else [],
                      "published_count": 0, "state_saved": True, "blockers": []}, "usage": {}}


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = Path(self.directory.name)
        self.runtime_patch = patch.object(supervisor, "RUNTIME", self.runtime)
        self.runtime_patch.start()
        self.versions = {str(index): f"v{index}" for index in range(1, 46)}
        self.signature_patch = patch.object(supervisor, "signature", side_effect=lambda _role: dict(self.versions))
        self.signature_patch.start()
        self.connection = StateConnection(state={"handled_ticket_ids": list(range(1, 46))})
        self.connection_patch = patch.object(supervisor, "connect", return_value=self.connection)
        self.connection_patch.start()
        self.run_patch = patch.object(supervisor, "run", side_effect=completed)
        self.run = self.run_patch.start()

    def tearDown(self):
        self.run_patch.stop()
        self.connection_patch.stop()
        self.signature_patch.stop()
        self.runtime_patch.stop()
        self.directory.cleanup()

    def status(self, role="mid"):
        return json.loads((self.runtime / f"{role}.json").read_text())

    def test_role_models_are_used_for_calls_and_audit_events(self):
        with patch.dict(os.environ, {"CODEX_MODEL": "fallback-model",
                                    "MID_CODEX_MODEL": "light-model",
                                    "STRONG_CODEX_MODEL": "strong-model"}):
            for role, expected in (("mid", "light-model"), ("strong", "strong-model")):
                result = supervisor.cycle(role)
                self.assertEqual(self.run.call_args.args[5], expected)
                self.assertEqual(self.run.call_args.kwargs["environment_profile"], "research")
                events = [json.loads(path.read_text()) for path in
                          (self.runtime / "cycle-audit" / result["cycle_id"]).glob("*.json")]
                started = next(event for event in events if event["event"] == "started")
                self.assertEqual(started["model"], expected)
                self.assertEqual(started["environment_profile"], "research")

    def test_blank_role_model_uses_generic_fallback(self):
        with patch.dict(os.environ, {"MID_CODEX_MODEL": "", "CODEX_MODEL": "fallback-model"}):
            supervisor.cycle("mid")
        self.assertEqual(self.run.call_args.args[5], "fallback-model")

    def test_no_model_configuration_uses_role_defaults(self):
        with patch.dict(os.environ, {"CODEX_MODEL": "", "MID_CODEX_MODEL": "", "STRONG_CODEX_MODEL": ""}):
            for role, expected in (("mid", "gpt-6-luna"), ("strong", "gpt-6.1-sol")):
                supervisor.cycle(role)
                self.assertEqual(self.run.call_args.args[5], expected)

    def test_unchanged_backlog_drains_in_bounded_batches_without_new_arrivals(self):
        first = supervisor.cycle("mid")
        second = supervisor.cycle("mid")
        third = supervisor.cycle("mid")
        waiting = supervisor.cycle("mid")
        self.assertEqual(first["selected_input_ids"], list(range(1, 21)))
        self.assertEqual(first["pending_count"], 25)
        self.assertEqual(second["selected_input_ids"], list(range(21, 41)))
        self.assertEqual(third["selected_input_ids"], list(range(41, 46)))
        self.assertEqual(waiting["status"], "waiting_for_new_evidence")
        self.assertEqual(self.run.call_count, 3)
        self.assertEqual(self.status()["processed_versions"], self.versions)

    def test_partial_review_only_acknowledges_returned_input_ids(self):
        def partial(*args, **kwargs):
            result = completed(*args, **kwargs)
            result["final"]["reviewed_ticket_ids"] = [1, 2, 3]
            return result
        self.run.side_effect = partial
        self.connection.state = {"handled_ticket_ids": [99, 1, 2, 3]}
        first = supervisor.cycle("mid")
        self.assertEqual(first["pending_count"], 42)
        self.assertEqual(set(first["processed_versions"]), {"1", "2", "3"})
        self.run.side_effect = completed
        self.connection.state = {"handled_ticket_ids": [99, *range(1, 24)]}
        second = supervisor.cycle("mid")
        self.assertEqual(second["selected_input_ids"], list(range(4, 24)))

    def test_existing_annotation_payload_revision_wakes_strong(self):
        self.versions = {"7": "old-payload"}
        supervisor.cycle("strong")
        self.versions["7"] = "edited-payload"
        second = supervisor.cycle("strong")
        self.assertEqual(second["selected_input_ids"], [7])
        self.assertEqual(second["processed_versions"]["7"], "edited-payload")
        self.assertEqual(self.run.call_count, 2)

    def test_payload_edit_during_cycle_is_not_accidentally_acknowledged(self):
        self.versions = {"1": "before-cycle"}
        def edit_during(*args, **kwargs):
            self.versions["1"] = "after-cycle-start"
            return completed(*args, **kwargs)
        self.run.side_effect = edit_during
        first = supervisor.cycle("mid")
        self.assertEqual(first["processed_versions"]["1"], "before-cycle")
        self.run.side_effect = completed
        second = supervisor.cycle("mid")
        self.assertEqual(second["selected_input_ids"], [1])
        self.assertEqual(second["processed_versions"]["1"], "after-cycle-start")

    def test_failed_cycle_preserves_checkpoint_and_persistent_error_until_success(self):
        supervisor.cycle("mid")
        saved = self.status()["processed_versions"]
        self.run.side_effect = RuntimeError("model timed out")
        with self.assertRaisesRegex(RuntimeError, "timed out"):
            supervisor.cycle("mid")
        self.assertEqual(self.status()["processed_versions"], saved)
        self.assertEqual(self.status()["status"], "failed")
        self.assertIn("timed out", self.status()["error"])
        self.run.side_effect = completed
        success = supervisor.cycle("mid")
        self.assertEqual(success["selected_input_ids"], list(range(21, 41)))
        self.assertNotIn("error", self.status())
        events = [json.loads(path.read_text()) for path in (self.runtime / "cycle-audit").glob("*/*.json")]
        self.assertEqual(sum(event["event"] == "failed" for event in events), 1)
        self.assertEqual(sum(event["event"] == "completed" for event in events), 2)
        self.assertEqual(len({event["cycle_id"] for event in events}), 3)

    def test_model_report_without_actual_state_save_cannot_advance(self):
        with patch.object(supervisor, "connect", return_value=StateConnection(saved=False)):
            with self.assertRaisesRegex(RuntimeError, "state_saved=False"):
                supervisor.cycle("mid")
        self.assertNotIn("processed_versions", self.status())
        self.assertEqual(self.status()["status"], "failed")

    def test_recent_state_save_without_handled_history_cannot_advance(self):
        self.versions = {"1": "one"}
        for state in ({}, {"reviewed_ticket_ids": [1]}, {"handled_ticket_ids_this_cycle": [1]},
                      {"handled_ticket_ids": []}):
            with self.subTest(state=state):
                self.connection.state = state
                with self.assertRaisesRegex(RuntimeError, "handled_ticket_ids"):
                    supervisor.cycle("mid")
                self.assertNotIn("processed_versions", self.status())
                self.assertEqual(self.status()["status"], "failed")

    def test_malformed_saved_handled_history_cannot_advance(self):
        self.versions = {"1": "one"}
        for handled in (None, "1", {"1": True}, 1, [True], [1.0], ["1"], [1, 0], [1, -1], [1, False]):
            with self.subTest(handled=handled):
                self.connection.state = {"handled_ticket_ids": handled}
                with self.assertRaisesRegex(RuntimeError, "list of positive integer IDs"):
                    supervisor.cycle("mid")
                self.assertNotIn("processed_versions", self.status())

    def test_partially_saved_reviewed_batch_cannot_advance(self):
        self.versions = {"1": "one", "2": "two"}
        self.connection.state = {"handled_ticket_ids": [1]}
        with self.assertRaisesRegex(RuntimeError, r"missing reviewed ticket IDs \[2\]"):
            supervisor.cycle("mid")
        self.assertNotIn("processed_versions", self.status())

    def test_missing_handled_history_preserves_checkpoint_and_retries_batch(self):
        supervisor.cycle("mid")
        saved = self.status()["processed_versions"]
        self.connection.state = {"handled_ticket_ids": list(range(1, 21)),
                                 "reviewed_ticket_ids": list(range(21, 41))}
        with self.assertRaisesRegex(RuntimeError, "missing reviewed ticket IDs"):
            supervisor.cycle("mid")
        self.assertEqual(self.status()["processed_versions"], saved)
        self.assertEqual(self.status()["status"], "failed")
        self.assertIn("handled_ticket_ids", self.status()["error"])
        self.connection.state["handled_ticket_ids"] = list(range(1, 41))
        success = supervisor.cycle("mid")
        self.assertEqual(success["selected_input_ids"], list(range(21, 41)))
        self.assertEqual(success["pending_count"], 5)
        self.assertEqual(set(success["processed_versions"]), {str(index) for index in range(1, 41)})
        self.assertNotIn("error", self.status())
        events = [json.loads(path.read_text()) for path in (self.runtime / "cycle-audit").glob("*/*.json")]
        failures = [event for event in events if event["event"] == "failed"]
        self.assertEqual(len(failures), 1)
        self.assertIn("handled_ticket_ids", failures[0]["summary"])
        self.assertEqual(sum(event["event"] == "completed" for event in events), 2)

    def test_saved_history_does_not_replace_timestamp_or_report_checks(self):
        self.connection.updated_at = datetime.now(timezone.utc) - timedelta(hours=1)
        with self.assertRaisesRegex(RuntimeError, "state_saved=False"):
            supervisor.cycle("mid")
        self.assertNotIn("processed_versions", self.status())
        self.connection.updated_at = None
        def unsaved(*args, **kwargs):
            result = completed(*args, **kwargs)
            result["final"]["state_saved"] = False
            return result
        self.run.side_effect = unsaved
        with self.assertRaisesRegex(RuntimeError, "state_saved=False"):
            supervisor.cycle("mid")
        self.assertNotIn("processed_versions", self.status())

    def test_strong_does_not_require_mid_handled_history(self):
        self.connection.state = {"theses": {}, "handled_ticket_ids": "unused by Strong"}
        first = supervisor.cycle("strong")
        self.assertEqual(set(first["processed_versions"]), {str(index) for index in range(1, 21)})
        self.connection.state = {"theses": {}}
        second = supervisor.cycle("strong")
        self.assertEqual(second["selected_input_ids"], list(range(21, 41)))

    def test_forced_empty_mid_cycle_does_not_require_handled_history(self):
        self.versions = {}
        self.connection.state = {}
        result = supervisor.cycle("mid", force=True)
        self.assertEqual(result["processed_versions"], {})
        self.assertEqual(result["final"]["reviewed_ticket_ids"], [])

    def test_invalid_or_unselected_acknowledgments_cannot_advance(self):
        for ids in ([], [21], [True], [1, 1], [-1]):
            with self.subTest(ids=ids):
                def bad(*args, **kwargs):
                    result = completed(*args, **kwargs)
                    result["final"]["reviewed_ticket_ids"] = ids
                    return result
                self.run.side_effect = bad
                with self.assertRaises(RuntimeError):
                    supervisor.cycle("mid")
                self.assertNotIn("processed_versions", self.status())

    def test_wrong_role_or_blocked_cycle_cannot_advance(self):
        for update in ({"role": "strong"}, {"status": "blocked"}):
            with self.subTest(update=update):
                def bad(*args, **kwargs):
                    result = completed(*args, **kwargs)
                    result["final"].update(update)
                    return result
                self.run.side_effect = bad
                with self.assertRaises(RuntimeError):
                    supervisor.cycle("mid")
                self.assertNotIn("processed_versions", self.status())

    def test_due_daily_review_runs_without_new_annotations_and_without_reset_by_other_cycles(self):
        self.versions = {"1": "one"}
        with patch.object(supervisor.time, "time", return_value=1000):
            supervisor.cycle("strong")
        self.versions["2"] = "two"
        with patch.object(supervisor.time, "time", return_value=2000):
            second = supervisor.cycle("strong")
        self.assertEqual(second["last_review_epoch"], 1000)
        # Even an empty table does not suppress the continuing thesis review.
        self.versions = {}
        with patch.object(supervisor.time, "time", return_value=90000):
            review = supervisor.cycle("strong")
        self.assertTrue(review["daily_review"])
        self.assertEqual(review["selected_input_ids"], [])
        self.assertEqual(review["last_review_epoch"], 90000)
        self.assertIn("Daily thesis review due: True", self.run.call_args.args[0])

    def test_empty_new_deployment_waits_without_model_cost(self):
        self.versions = {}
        self.assertEqual(supervisor.cycle("strong")["status"], "waiting_for_new_evidence")
        self.run.assert_not_called()


class Process:
    _pid = 100

    def __init__(self, command, role_exit):
        type(self)._pid += 1
        self.pid = self._pid
        self.returncode = role_exit if "cycle" in command else None

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode = 0

    def wait(self, timeout):
        return self.returncode

    def kill(self):
        self.returncode = -9


class HealthTests(unittest.TestCase):
    def daemon(self, root, *, bridge_error=None, role_exit=1):
        handlers, snapshots = {}, []
        sleeps = 0
        original_save = supervisor.save
        def save(path, value):
            if path.name == "health.json":
                snapshots.append(copy.deepcopy(value))
            original_save(path, value)
        def sleep(_seconds):
            nonlocal sleeps
            sleeps += 1
            if sleeps == 3:
                handlers[signal.SIGTERM]()
        with patch.object(supervisor, "RUNTIME", root), \
                patch.object(supervisor.signal, "signal", side_effect=lambda kind, handler: handlers.__setitem__(kind, handler)), \
                patch.object(supervisor.subprocess, "Popen", side_effect=lambda cmd, **kw: Process(cmd, role_exit)), \
                patch.object(supervisor, "bridge", side_effect=bridge_error), \
                patch.object(supervisor.time, "time", return_value=1000), \
                patch.object(supervisor.time, "sleep", side_effect=sleep), \
                patch.object(supervisor, "save", side_effect=save):
            supervisor.daemon()
        return snapshots

    def test_errors_persist_across_idle_health_refreshes(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshots = self.daemon(Path(directory), bridge_error=RuntimeError("evaluation unavailable"))
        self.assertIn("evaluation", snapshots[0]["errors"])
        self.assertIn("mid", snapshots[1]["errors"])
        self.assertIn("mid", snapshots[2]["errors"])
        self.assertIn("evaluation", snapshots[2]["errors"])

    def test_prior_errors_survive_restart_until_successful_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "health.json").write_text(json.dumps({"errors": {"mid": "old failed cycle", "evaluation": "old failure"}}))
            snapshots = self.daemon(root, role_exit=0)
        self.assertIn("mid", snapshots[0]["errors"])
        self.assertNotIn("evaluation", snapshots[0]["errors"])
        self.assertNotIn("mid", snapshots[1]["errors"])


if __name__ == "__main__":
    unittest.main()
