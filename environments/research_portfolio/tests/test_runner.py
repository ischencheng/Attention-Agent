import json
from pathlib import Path
import tempfile
import unittest

from environments.research_portfolio.demo import demo_actions
from environments.research_portfolio.runner import CodexPolicy, run_episode, policy_observation


FIXTURE = Path(__file__).parents[1] / "fixtures" / "demo.json"


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def test_policy_memory_delivers_new_original_once_and_preserves_read_receipts(self):
        old = {"tool_call_id": "tool-1", "tool": "read", "result": {"id": "one", "body": "old original"}}
        new = {"tool_call_id": "tool-2", "tool": "read", "result": {"id": "two", "body": "new original"}}
        observation = {"read_sources": [old["result"], new["result"]], "tool_results": [old, new],
                       "execution_feedback": [{"result": {"result": new}}],
                       "themes": [{"facts": [{"source_id": "one", "claim": "attributed fact"}]}]}
        projected = policy_observation(observation)
        self.assertEqual(json.dumps(projected).count("new original"), 1)
        self.assertNotIn("old original", json.dumps(projected))
        self.assertEqual(projected["tool_results"][0]["result"]["id"], "one")
        self.assertEqual(projected["themes"], observation["themes"])
        self.assertEqual(observation["tool_results"][0]["result"]["body"], "old original")

    def test_complete_workflow_replays_with_the_same_loss_and_provenance(self):
        first = run_episode(FIXTURE, self.root / "first", actions=demo_actions())
        self.assertTrue(first["evaluation_eligible"])
        self.assertEqual(first["rejected_actions"], 0)
        self.assertLess(first["reward"], 0)
        self.assertEqual(first["counts"]["thesis_revisions"], 4)
        self.assertGreater(first["account"]["cash"], 80000)
        transcript = json.loads((self.root / "first/actions.json").read_text())
        second = run_episode(self.root / "first/episode.json", self.root / "second", actions=transcript)
        self.assertEqual(first["reward"], second["reward"])
        self.assertEqual(first["account"], second["account"])
        fills = [x for x in first["account"]["ledger"] if x["type"] == "fill"]
        self.assertEqual(fills[-1]["decision_refs"], ["power-thesis:v2"])
        self.assertTrue((self.root / "first/trace.html").is_file())

    def test_transcript_exhaustion_marks_failure_but_settles_existing_exposure(self):
        actions = demo_actions()
        first_wait = next(i for i, a in enumerate(actions) if a["type"] == "wait")
        result = run_episode(FIXTURE, self.root / "partial", actions=actions[:first_wait + 1])
        self.assertEqual(result["run_status"], "failed")
        self.assertFalse(result["evaluation_eligible"])
        self.assertLess(result["reward"], 0)
        self.assertEqual(result["as_of"], "2026-06-05T20:00:00Z")

    def test_model_transport_failure_is_not_a_successful_cash_policy(self):
        def fail(*args, **kwargs):
            raise RuntimeError("transport unavailable")
        result = run_episode(FIXTURE, self.root / "failure", codex=True, policy_runner=fail)
        self.assertEqual(result["model_invocations"], 1)
        self.assertFalse(result["evaluation_eligible"])
        self.assertIn("transport unavailable", result["error"])
        self.assertEqual(result["reward"], 0)

    def test_external_tool_events_are_rejected_before_actions_execute(self):
        def fake(*args, **kwargs):
            return {"final": {"actions_json": '[{"role":"fast","type":"triage","source_id":"capex","disposition":"ignore","reason":"x"}]'},
                    "events": [{"type": "item.completed", "item": {"type": "command_execution"}}]}
        result = run_episode(FIXTURE, self.root / "external", codex=True, policy_runner=fake)
        self.assertEqual(result["run_status"], "failed")
        self.assertEqual(result["counts"]["tickets"], 0)

    def test_policy_gets_no_future_prices_and_metadata_records_observed_call(self):
        observed = {}
        def fake(prompt, schema, cwd, output_dir, timeout, model, **kwargs):
            observed.update(prompt=prompt, kwargs=kwargs)
            return {"final": {"actions_json": '[{"role":"strong","type":"wait","reason":"await evidence"}]'},
                    "events": [{"type": "item.completed", "item": {"type": "agent_message"}}],
                    "usage": {"output_tokens": 20}, "cli_version": "test"}
        policy = CodexPolicy(self.root, runner=fake, codex_bin="/explicit/codex", max_calls=1)
        actions, metadata = policy.choose({"suggested_role": "strong", "as_of": "2026-06-01T20:00:00Z"})
        self.assertEqual(actions[0]["type"], "wait")
        self.assertEqual(metadata["role"], "strong")
        self.assertTrue(observed["kwargs"]["disable_tools"])
        self.assertEqual(observed["kwargs"]["codex_bin"], "/explicit/codex")
        self.assertNotIn("future_prices", observed["prompt"])
        exhausted, invocation = policy.choose({})
        self.assertEqual(exhausted[0]["type"], "stop")
        self.assertIsNone(invocation)

    def test_nonempty_output_is_preserved(self):
        out = self.root / "existing"
        out.mkdir()
        (out / "keep").write_text("prior run")
        with self.assertRaisesRegex(ValueError, "new or empty"):
            run_episode(FIXTURE, out, actions=demo_actions())
        self.assertEqual((out / "keep").read_text(), "prior run")

    def test_explicit_relative_codex_path_survives_isolated_working_directory(self):
        observed = {}
        def fake(*args, **kwargs):
            observed.update(kwargs)
            return {"events": [], "final": {"actions_json": '[{"type":"stop","role":"strong","reason":"done"}]'}}
        policy = CodexPolicy(self.root, runner=fake, codex_bin="runtime/bin/codex")
        policy.choose({"suggested_role": "strong", "as_of": "2026-06-01T20:00:00Z"})
        self.assertEqual(observed["codex_bin"], str(Path("runtime/bin/codex").resolve()))

    def test_rejection_only_rollout_is_distinct_from_a_cash_decision(self):
        def fake(*args, **kwargs):
            return {"events": [], "final": {"actions_json": '[{"role":"fast","triage":{"source_id":"capex"}}]'}}
        summary = run_episode(FIXTURE, self.root / "rejections", codex=True,
                              policy_runner=fake, max_calls=2)
        self.assertEqual(summary["policy_outcome"], "rejected_actions_only")
        self.assertEqual(summary["rejected_actions"], 2)
        self.assertTrue(summary["policy_budget_exhausted"])
        self.assertEqual(summary["reward"], 0)  # The frozen accounting reward is unchanged.

    def test_nonfinite_or_nonobject_transcripts_fail_before_execution(self):
        for actions in ([None], [{"role": "strong", "type": "stop", "reason": float("nan")}],
                        [{"role": "strong", "type": "stop", "reason": "done"}, None]):
            with self.subTest(actions=actions), self.assertRaises(ValueError):
                run_episode(FIXTURE, self.root / "invalid", actions=actions)
        self.assertFalse((self.root / "invalid").exists())

    def test_model_resamples_after_tool_result_before_routing(self):
        calls = []
        def fake(prompt, *args, **kwargs):
            calls.append(prompt)
            if len(calls) == 1:
                actions = [{"type": "tool", "role": "fast", "tool": "read", "args": {"source_id": "capex"}},
                           {"type": "triage", "role": "fast", "source_id": "capex", "disposition": "advance", "reason": "unseen result"}]
            else:
                raise RuntimeError("stop after boundary probe")
            return {"events": [{"type": "item.completed", "item": {"type": "error", "message": "configuration notice"}}],
                    "final": {"actions_json": json.dumps(actions)}}
        result = run_episode(FIXTURE, self.root / "boundary", codex=True, policy_runner=fake)
        self.assertEqual(result["counts"]["read_sources"], 1)
        self.assertEqual(result["counts"]["tickets"], 0)
        self.assertEqual(len(calls), 2)
        self.assertIn("Backlog", calls[1].replace("backlog", "Backlog"))
        metadata = json.loads((self.root / "boundary/model-calls.json").read_text())
        self.assertEqual(metadata[0]["executed_action_indices"], [0])
        self.assertEqual(metadata[0]["discarded_action_indices"], [1])

    def test_rejected_action_feedback_reaches_the_corrective_model_call(self):
        prompts = []
        def fake(prompt, *args, **kwargs):
            prompts.append(prompt)
            if len(prompts) == 2:
                self.assertIn('"execution_feedback"', prompt)
                self.assertIn('"disposition": "retain"', prompt)
                self.assertIn("disposition must be advance or ignore", prompt)
            candidate = {"type": "triage", "role": "fast", "source_id": "capex",
                         "disposition": "retain" if len(prompts) == 1 else "advance",
                         "reason": "Track the original issuer report."}
            return {"events": [], "final": {"actions_json": json.dumps([candidate])}}
        result = run_episode(FIXTURE, self.root / "correction", codex=True,
                             policy_runner=fake, max_calls=2)
        self.assertEqual(result["run_status"], "completed")
        self.assertEqual(result["rejected_actions"], 1)
        self.assertEqual(result["counts"]["tickets"], 1)
        self.assertEqual(len(prompts), 2)

    def test_malformed_inner_json_is_preserved_and_next_call_can_repair(self):
        prompts = []
        raw = '[{"type":"triage","role":"fast", BROKEN'

        def fake(prompt, *args, **kwargs):
            prompts.append(prompt)
            if len(prompts) == 1:
                return {"events": [], "final": {"actions_json": raw}, "usage": {"output_tokens": 12}}
            self.assertIn("JSONDecodeError", prompt)
            self.assertIn('"action_origin": "decoder"', prompt)
            self.assertIn("Expecting property name enclosed in double quotes", prompt)
            return {"events": [], "final": {"actions_json": json.dumps([
                {"type": "triage", "role": "fast", "source_id": "capex",
                 "disposition": "advance", "reason": "Track the issuer original."}])}}

        output = self.root / "decode-repair"
        summary = run_episode(FIXTURE, output, codex=True, policy_runner=fake, max_calls=2)
        self.assertEqual(summary["run_status"], "completed")
        self.assertTrue(summary["evaluation_eligible"])
        self.assertTrue(summary["policy_budget_exhausted"])
        self.assertEqual(summary["invalid_policy_outputs"], 1)
        self.assertEqual(summary["rejected_actions"], 1)
        self.assertEqual(summary["counts"]["tickets"], 1)
        self.assertEqual(summary["steps"], 3)  # Invalid attempt, correction, budget stop.
        self.assertEqual(summary["as_of"], "2026-06-05T20:00:00Z")
        records = [json.loads(line) for line in (output / "trajectory.jsonl").read_text().splitlines()]
        rejected = records[0]
        self.assertEqual(rejected["action_origin"], "decoder")
        self.assertEqual(rejected["action"]["type"], "invalid_policy_output")
        self.assertFalse(rejected["invocation"]["trainable"])
        self.assertTrue(rejected["invocation"]["raw_output_trainable"])
        self.assertNotIn("action_index", rejected["invocation"])
        raw_artifact = json.loads((output / rejected["invocation"]["raw_output_artifact"]).read_text())
        self.assertEqual(raw_artifact["actions_json"], raw)
        self.assertEqual(raw_artifact["output_origin"], "model")
        calls = json.loads((output / "model-calls.json").read_text())
        self.assertEqual(calls[0]["status"], "invalid_output")
        self.assertEqual(calls[0]["executed_action_indices"], [])
        self.assertEqual(calls[0]["decoder_step"], 1)
        self.assertEqual(calls[0]["usage"], {"output_tokens": 12})
        self.assertEqual(calls[1]["status"], "completed")
        self.assertIn("decoder", (output / "trace.html").read_text())

    def test_invalid_output_retries_obey_model_step_and_clock_budgets(self):
        def fake(*args, **kwargs):
            return {"events": [], "final": {"actions_json": "[malformed"}}

        cases = [
            ("calls", {"max_calls": 2}, 2, 3, True, False),
            ("steps", {"max_calls": 8, "max_steps": 2}, 2, 2, False, True),
            ("clock", {"max_calls": 8, "actions_per_cutoff": 1}, 4, 4, False, False),
        ]
        for name, config, calls, steps, budget_stop, truncated in cases:
            with self.subTest(name=name):
                result = run_episode(FIXTURE, self.root / name, codex=True, policy_runner=fake, **config)
                self.assertEqual(result["run_status"], "completed")
                self.assertEqual(result["model_invocations"], calls)
                self.assertEqual(result["invalid_policy_outputs"], calls)
                self.assertEqual(result["rejected_actions"], calls)
                self.assertEqual(result["steps"], steps)
                self.assertEqual(result["policy_budget_exhausted"], budget_stop)
                self.assertEqual(result["truncated"], truncated)
                self.assertEqual(result["as_of"], "2026-06-05T20:00:00Z")
                self.assertEqual(result["counts"]["tickets"], 0)

    def test_nonfinite_or_wrong_shape_inner_payload_is_a_rejected_attempt(self):
        for index, raw in enumerate(('[]', '{"type":"triage"}',
                                     '[{"role":"fast","value":NaN}]',
                                     '[{"role":"strong","type":"stop","reason":"wrong role"}]')):
            with self.subTest(raw=raw):
                def fake(*args, **kwargs):
                    return {"events": [], "final": {"actions_json": raw}}
                result = run_episode(FIXTURE, self.root / f"format-{index}", codex=True,
                                     policy_runner=fake, max_calls=1)
                self.assertEqual(result["invalid_policy_outputs"], 1)
                self.assertEqual(result["run_status"], "completed")
                self.assertEqual(result["model_invocations"], 1)

    def test_malformed_output_cannot_escape_loss_on_already_filled_positions(self):
        actions = demo_actions()
        first_wait = next(index for index, action in enumerate(actions) if action["type"] == "wait")
        prefix = actions[:first_wait + 1]
        by_role = {role: [action for action in prefix if action["role"] == role]
                   for role in ("fast", "mid", "strong")}

        def fake(prompt, *args, **kwargs):
            observation = json.loads(prompt.split("\n\n", 1)[1])
            if observation["account"]["positions"]:
                payload = "[malformed after position was filled"
            else:
                payload = json.dumps([by_role[observation["suggested_role"]].pop(0)])
            return {"events": [], "final": {"actions_json": payload}}

        summary = run_episode(FIXTURE, self.root / "held-loss", codex=True,
                              policy_runner=fake, max_calls=len(prefix) + 2)
        self.assertEqual(summary["run_status"], "completed")
        self.assertTrue(summary["policy_budget_exhausted"])
        self.assertEqual(summary["invalid_policy_outputs"], 2)
        self.assertLess(summary["reward"], 0)
        self.assertTrue(summary["account"]["positions"])
        self.assertEqual(summary["as_of"], "2026-06-05T20:00:00Z")
        self.assertEqual(sum(event["type"] == "fill" for event in summary["account"]["ledger"]), 1)

    def test_outer_protocol_and_external_tool_failures_do_not_enter_decode_recovery(self):
        responses = [
            {"events": [], "final": {"actions_json": []}},
            {"events": [{"type": "item.completed", "item": {"type": "command_execution"}}],
             "final": {"actions_json": "[malformed"}},
        ]
        for index, response in enumerate(responses):
            with self.subTest(index=index):
                result = run_episode(FIXTURE, self.root / f"hard-{index}", codex=True,
                                     policy_runner=lambda *a, **kw: response, max_calls=8)
                self.assertEqual(result["run_status"], "failed")
                self.assertFalse(result["evaluation_eligible"])
                self.assertEqual(result["model_invocations"], 1)
                self.assertEqual(result["invalid_policy_outputs"], 0)


if __name__ == "__main__":
    unittest.main()
