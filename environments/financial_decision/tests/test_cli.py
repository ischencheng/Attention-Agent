"""Offline CLI contract, auditable artifacts, public policy inputs and HTML safety."""
from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest

from environments.financial_decision.cli import _read_actions, run_episode, serve_stdio
from environments.financial_decision.env import FinancialDecisionEnv
from environments.financial_decision.trace_view import render_trace


FIXTURE = Path(__file__).parents[1] / "fixtures" / "demo.json"


class CliTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def demo(self, name="demo"):
        output = self.root / name
        summary = run_episode(FIXTURE, output, seed=7)
        records = [json.loads(line) for line in (output / "trajectory.jsonl").read_text().splitlines()]
        return output, summary, records

    def test_demo_freezes_forecasts_then_receives_delayed_reward_and_preserves_unknown(self):
        output, summary, records = self.demo()
        self.assertEqual(summary["status"], "terminated")
        self.assertEqual(summary["rejected_actions"], 0)
        self.assertEqual((summary["resolved_forecasts"], summary["unresolved_forecasts"], summary["pending_forecasts"]), (1, 1, 0))
        self.assertAlmostEqual(summary["total_reward"], 0.1205)
        initial = records[0]["observation"]
        self.assertTrue(all("resolution" not in question for question in initial["questions"]))
        final = records[-1]["next_observation"]
        unknown = next(item for item in final["settlements"] if item["status"] == "unresolved")
        self.assertIsNone(unknown["value"])
        self.assertNotIn("score", unknown)
        for record in records[1:]:
            if record["action"]["type"] == "forecast":
                self.assertEqual(record["observation"]["current_time"], "2025-06-02T18:00:00Z")
                self.assertEqual(record["reward"], 0)
        self.assertEqual(set(summary["artifacts"].values()), {path.name for path in output.iterdir()})
        exported = json.loads((output / "episode.json").read_text())
        self.assertIn("reward_contract", exported)
        restored = FinancialDecisionEnv(FIXTURE)
        restored.load_checkpoint(output / "checkpoint.json")
        self.assertEqual(restored.summary()["total_reward"], summary["total_reward"])

    def test_demo_retains_counterevidence_and_does_not_revise_for_syndication(self):
        _, _, records = self.demo()
        final = records[-1]["next_observation"]
        contrary = final["theses"]["lanyu-capacity-contract"]["discovery"]["counterevidence_ids"]
        cancellation = next(item for item in final["read_observations"] if item["raw"]["event_type"] == "cancellation")
        self.assertIn(cancellation["id"], contrary)
        self.assertFalse(any(record["action"]["type"] == "judge" and
                             record["observation"]["current_time"] == "2025-06-03T18:00:00Z" for record in records[1:]))
        self.assertFalse(any(item["raw"]["event_type"] == "financing" for record in records
                             if record["observation"]["current_time"] < "2025-06-09T18:00:00Z"
                             for item in record["observation"]["read_observations"]))

    def test_external_transcript_replays_the_same_transitions_and_rejects_extra_action(self):
        first, _, records = self.demo()
        actions = [record["action"] for record in records[1:]]
        replay = self.root / "replay"
        summary = run_episode(FIXTURE, replay, actions=actions, seed=7)
        self.assertEqual(summary["policy"], "external action transcript")
        self.assertEqual((first / "episode.json").read_bytes(), (replay / "episode.json").read_bytes())
        extra = self.root / "extra"
        with self.assertRaisesRegex(ValueError, "extra actions"):
            run_episode(FIXTURE, extra, actions=actions + [{"type": "wait", "reason": "extra"}], seed=7)
        failed = json.loads((extra / "summary.json").read_text())
        self.assertEqual(failed["status"], "error")
        self.assertEqual(failed["steps"], len(actions))

    def test_existing_output_is_not_overwritten(self):
        output, _, _ = self.demo()
        before = {path.name: path.read_bytes() for path in output.iterdir()}
        with self.assertRaises(FileExistsError):
            run_episode(FIXTURE, output)
        self.assertEqual(before, {path.name: path.read_bytes() for path in output.iterdir()})

    def test_incomplete_transcript_and_step_limit_are_distinct(self):
        unfinished = run_episode(FIXTURE, self.root / "unfinished", actions=[])
        truncated = run_episode(FIXTURE, self.root / "truncated", max_steps=1)
        self.assertEqual(unfinished["status"], "unfinished")
        self.assertEqual(truncated["status"], "truncated")
        self.assertFalse(truncated["terminated"])

    def test_jsonl_rejects_nonfinite_numbers_and_nonobjects(self):
        path = self.root / "actions.jsonl"
        for text in ('{"type":"forecast","probability":NaN}\n',
                     '{"type":"forecast","probability":1e309}\n',
                     '{"type":"judge","judgment":{"nested":[-1e309]}}\n', '[]\n'):
            path.write_text(text)
            with self.assertRaises(ValueError):
                _read_actions(path)

    def test_nonfinite_python_action_preserves_complete_failure_artifacts(self):
        output = self.root / "overflow"
        with self.assertRaisesRegex(ValueError, "partial episode artifacts"):
            run_episode(FIXTURE, output, actions=[{"type": "forecast", "probability": float("inf")}])
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(summary["status"], "error")
        self.assertEqual(summary["steps"], 0)
        self.assertEqual(set(summary["artifacts"].values()), {p.name for p in output.iterdir()})
        self.assertIn("error", (output / "trace.html").read_text())

    def test_stdio_requires_reset_and_returns_public_step_results(self):
        requests = [
            {"op": "step", "action": {"type": "wait", "reason": "too early"}},
            {"op": "reset", "seed": 7},
            {"op": "step", "action": {"type": "wait", "reason": "next cutoff"}},
            {"op": "summary"},
        ]
        outgoing = StringIO()
        serve_stdio(FIXTURE, incoming=StringIO(''.join(json.dumps(item) + '\n' for item in requests)), outgoing=outgoing)
        replies = [json.loads(line) for line in outgoing.getvalue().splitlines()]
        self.assertFalse(replies[0]["ok"])
        self.assertTrue(all(item["ok"] for item in replies[1:]))
        self.assertTrue(all("resolution" not in item for item in replies[1]["observation"]["questions"]))
        self.assertEqual(replies[2]["observation"]["current_time"], "2025-06-03T18:00:00Z")
        self.assertEqual(replies[3]["summary"]["steps"], 1)

    def test_html_escapes_source_action_and_error_content(self):
        _, summary, records = self.demo()
        unsafe = '<script src="https://example.invalid/x">alert(1)</script>'
        records[1]["next_observation"]["read_observations"][0]["raw"]["text"] = unsafe
        records[1]["action"]["untrusted"] = unsafe
        records[1]["info"]["rejection"] = unsafe
        text = render_trace(records, summary)
        self.assertNotIn('<script', text)
        self.assertIn('&lt;script', text)
        self.assertIn("default-src 'none'", text)
        self.assertIn("SYNTHETIC", text)

    def test_rejected_malformed_action_still_exports_readable_trace(self):
        for index, action in enumerate(({"type": "judge", "judgment": "invalid"}, [], None, "invalid", 37, True)):
            with self.subTest(action=action):
                output = self.root / f"invalid-{index}"
                summary = run_episode(FIXTURE, output, actions=[action])
                self.assertEqual(summary["status"], "unfinished")
                self.assertEqual(summary["rejected_actions"], 1)
                self.assertIn("Action rejected", (output / "trace.html").read_text())
                self.assertEqual(set(summary["artifacts"].values()), {p.name for p in output.iterdir()})


if __name__ == "__main__":
    unittest.main()
