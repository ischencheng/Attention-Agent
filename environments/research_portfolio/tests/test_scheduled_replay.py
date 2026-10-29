"""Adversarial clock and evidence-interface checks using independent fixtures."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from environments.research_portfolio.env import ResearchPortfolioEnv
from environments.research_portfolio.runner import run_episode
from environments.research_portfolio.reconcile import reconcile
from environments.research_portfolio.demo import demo_actions
from environments.research_portfolio.tests.test_env import fixture, action, tool, judgment
from environments.research_portfolio.tests import test_env as fixtures


class ScheduledReplayTests(unittest.TestCase):
    def test_action_contract_advertises_flat_discriminated_objects(self):
        for strict in (False, True):
            with self.subTest(strict_replay=strict):
                env = ResearchPortfolioEnv(fixture(), strict_replay=strict, scheduled=True)
                definitions = env.observation()["action_contract"]["actions"]
                for kind, definition in definitions.items():
                    self.assertEqual(definition["type"], kind)
                    self.assertIn("role", definition)
                triage = {**definitions["triage"], "source_id": "earnings",
                          "disposition": "advance", "reason": "Investigate the official original"}
                nested = {"role": "fast", "triage": {key: value for key, value in triage.items()
                                                       if key not in {"type", "role"}}}
                self.assertIn("rejection", env.step(nested)[4])
                self.assertFalse(env.triage)
                self.assertNotIn("rejection", env.step(triage)[4])
                self.assertIn("ticket-earnings", env.tickets)

    def test_strict_mode_has_no_search_and_requires_actual_counterevidence_reads(self):
        env = ResearchPortfolioEnv(fixture(), strict_replay=True)
        helper = fixtures.ResearchPortfolioTests()
        helper.prepare_question(env)
        self.assertNotIn("search", env.observation()["action_contract"]["tools"])
        self.assertIn("rejection", env.step(tool("search", query="power", intent="contradiction"))[4])
        bad = tool("counterevidence_review", source_ids=["contrary"], assessment="Checked")
        self.assertIn("rejection", env.step(bad)[4])
        for item in (tool("discover", query="power"), tool("price_history", asset_id="power"),
                     tool("framework", asset_id="power"),
                     tool("counterevidence_review", source_ids=["earnings"], assessment="Read execution and leverage risks; customer coverage remains incomplete."), judgment()):
            self.assertNotIn("rejection", env.step(item)[4])
        self.assertNotIn("rejection", env.step(action("allocate", target_weights={"power": 0.2},
                            thesis_ids=["power-thesis"], reason="Bounded fixture position"))[4])

    def test_catalog_is_paginated_and_future_evidence_is_absent(self):
        env = ResearchPortfolioEnv(fixture(), strict_replay=True)
        fixtures.ResearchPortfolioTests().prepare_question(env)
        result = env.step(tool("catalog", offset=0, limit=1))[4]["result"]["result"]
        self.assertEqual([item["id"] for item in result["originals"]], ["earnings"])
        self.assertNotIn("FUTURE_EVIDENCE_CANARY", json.dumps(env.observation()))
        self.assertIn("rejection", env.step(tool("catalog", offset=0, limit=1000))[4])

    def test_hidden_future_mutations_do_not_change_observation(self):
        original = fixture()
        changed = copy.deepcopy(original)
        changed["sources"][1]["body"] = "A completely different future"
        changed["bars"][-1]["close"] = 900
        self.assertEqual(ResearchPortfolioEnv(original, strict_replay=True).observation(),
                         ResearchPortfolioEnv(changed, strict_replay=True).observation())

    def test_arrival_order_uses_availability_and_stable_id(self):
        data = fixture()
        second = copy.deepcopy(data["sources"][0])
        second["id"] = "aaa"
        data["sources"].insert(0, second)
        reversed_data = copy.deepcopy(data)
        reversed_data["sources"].reverse()
        self.assertEqual(ResearchPortfolioEnv(data).observation(), ResearchPortfolioEnv(reversed_data).observation())

    def test_independent_mid_strong_cycles_and_idle_skip(self):
        data = fixture()
        data["sources"][0]["available_at"] = "2025-06-02T20:05:00Z"
        env = ResearchPortfolioEnv(data, strict_replay=True, scheduled=True, max_steps=1000)
        self.assertIsNone(env.observation()["suggested_role"])
        env.step(action("wait", reason="Idle"))
        self.assertEqual(env.observation()["suggested_role"], "fast")
        env.step({"type": "triage", "role": "fast", "source_id": "earnings", "disposition": "advance", "reason": "An official clue"})
        self.assertIsNone(env.observation()["suggested_role"])
        env.step(action("wait", reason="Await Mid"))
        self.assertEqual(env.now, "2025-06-02T20:30:00+00:00")
        self.assertEqual(env.observation()["suggested_role"], "mid")
        env.step({"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}})
        env.step({"type": "accumulate", "role": "mid", "theme_id": "power", "ticket_ids": ["ticket-earnings"],
                  "summary": "Power results", "facts": [{"source_id": "earnings", "claim": "Revenue rose"}],
                  "open_questions": ["Is growth durable?"], "authoritative_exception": True,
                  "exception_reason": "Official filing"})
        self.assertIsNone(env.observation()["suggested_role"])
        env.step(action("wait", reason="Await Strong"))
        self.assertEqual(env.now, "2025-06-02T20:40:00+00:00")
        self.assertEqual(env.observation()["suggested_role"], "strong")
        env.step(action("open_research", question_id="capacity", theme_id="power", question="Is growth durable?"))
        env.step(tool("read", source_id="earnings"))
        env.step(judgment(status="research", asset_ids=[], framework_analysis={}, risk_checks={},
                          next_review_at="2025-06-02T20:47:00Z"))
        env.step(action("wait", reason="Wait for deadline"))
        self.assertEqual(env.now, "2025-06-02T20:47:00+00:00")
        self.assertEqual(env.observation()["suggested_role"], "strong")
        self.assertEqual(env.observation()["clock"]["due_reviews"][0]["thesis_id"], "power-thesis")

    def test_empty_scheduled_episode_uses_no_model_calls(self):
        data = fixture()
        data["sources"] = []
        def forbidden(*args, **kwargs):
            raise AssertionError("An idle interval called a model")
        with tempfile.TemporaryDirectory() as output:
            result = run_episode(data, output, codex=True, strict_replay=True, scheduled=True,
                                 policy_runner=forbidden, max_steps=1000)
        self.assertEqual(result["run_status"], "completed")
        self.assertEqual(result["model_invocations"], 0)
        self.assertGreater(result["skipped_idle_cutoffs"], 100)

    def test_reset_discards_policy_selected_review_deadlines(self):
        env = ResearchPortfolioEnv(fixture(), strict_replay=True, scheduled=True)
        initial = env.observation()
        initial_timeline = list(env.manifest["timeline"])
        fixtures.ResearchPortfolioTests().prepare_question(env)
        self.assertNotIn("rejection", env.step(judgment(
            status="research", asset_ids=[], framework_analysis={}, risk_checks={},
            next_review_at="2025-06-02T20:07:00Z"))[4])
        env.step(action("wait", reason="Await the research deadline"))
        self.assertEqual(env.now, "2025-06-02T20:07:00+00:00")
        self.assertEqual(len(env.manifest["timeline"]), len(initial_timeline) + 1)
        observation, _ = env.reset()
        self.assertEqual(observation, initial)
        self.assertEqual(env.manifest["timeline"], initial_timeline)
        self.assertEqual(env.manifest["timeline"][1], "2025-06-02T20:30:00+00:00")

    def test_rejected_strong_action_keeps_unprocessed_evidence_eligible(self):
        attempts = [action("open_research", question_id="capacity", theme_id="missing", question="Durable?"),
                    action("cancel", reason="There is no pending order"),
                    action("wait", reason="")]
        for attempted in attempts:
            with self.subTest(action=attempted["type"]):
                env = ResearchPortfolioEnv(fixture(), strict_replay=True, scheduled=True,
                                           actions_per_cutoff=4)
                for item in (
                    {"type": "triage", "role": "fast", "source_id": "earnings",
                     "disposition": "advance", "reason": "An official clue"},
                    {"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}},
                    {"type": "accumulate", "role": "mid", "theme_id": "power",
                     "ticket_ids": ["ticket-earnings"], "summary": "Power results",
                     "facts": [{"source_id": "earnings", "claim": "Revenue rose"}],
                     "open_questions": ["Is growth durable?"], "authoritative_exception": True,
                     "exception_reason": "Official filing"},
                ):
                    self.assertNotIn("rejection", env.step(item)[4])
                result = env.step(attempted)
                self.assertIn("rejection", result[4])
                self.assertEqual(env.steps, 4)
                self.assertEqual(env.now, "2025-06-02T20:30:00+00:00")
                self.assertFalse(env.questions)
                self.assertFalse(env.theses)
                self.assertIsNone(env.observation()["suggested_role"])
                env.step(action("wait", reason="Await the next Strong tick"))
                self.assertEqual(env.now, "2025-06-02T20:40:00+00:00")
                self.assertEqual(env.observation()["suggested_role"], "strong")

    def test_forced_budget_advance_keeps_unfinished_strong_research_pending(self):
        for cap in (4, 5):
            with self.subTest(actions_per_cutoff=cap):
                env = ResearchPortfolioEnv(fixture(), strict_replay=True, scheduled=True,
                                           actions_per_cutoff=cap)
                steps = [
                    {"type": "triage", "role": "fast", "source_id": "earnings",
                     "disposition": "advance", "reason": "An official clue"},
                    {"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}},
                    {"type": "accumulate", "role": "mid", "theme_id": "power",
                     "ticket_ids": ["ticket-earnings"], "summary": "Power results",
                     "facts": [{"source_id": "earnings", "claim": "Revenue rose"}],
                     "open_questions": ["Is growth durable?"], "authoritative_exception": True,
                     "exception_reason": "Official filing"},
                    action("open_research", question_id="capacity", theme_id="power", question="Is growth durable?"),
                    tool("read", source_id="earnings"),
                ]
                for item in steps[:cap]:
                    self.assertNotIn("rejection", env.step(item)[4])
                self.assertEqual(env.now, "2025-06-02T20:30:00+00:00")
                self.assertIn("capacity", env.questions)
                self.assertFalse(env.theses)
                self.assertIsNone(env.observation()["suggested_role"])
                env.step(action("wait", reason="Idle until the next Strong tick"))
                self.assertEqual(env.now, "2025-06-02T20:40:00+00:00")
                self.assertEqual(env.observation()["suggested_role"], "strong")
                self.assertNotIn("rejection", env.step(tool("read", source_id="earnings"))[4])

    def test_read_body_reaches_next_model_prompt_after_automatic_idle_wait(self):
        data = fixture()
        batches = [
            {"type": "triage", "role": "fast", "source_id": "earnings",
             "disposition": "advance", "reason": "An official clue"},
            {"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}},
            {"type": "accumulate", "role": "mid", "theme_id": "power",
             "ticket_ids": ["ticket-earnings"], "summary": "Power results",
             "facts": [{"source_id": "earnings", "claim": "Revenue rose"}],
             "open_questions": ["Is growth durable?"], "authoritative_exception": True,
             "exception_reason": "Official filing"},
            action("open_research", question_id="capacity", theme_id="power", question="Is growth durable?"),
            tool("read", source_id="earnings"),
            tool("catalog", offset=0, limit=1),
            action("stop", reason="Finish the prompt delivery probe"),
        ]
        prompts = []

        def fake(prompt, *args, **kwargs):
            prompts.append(json.loads(prompt.rsplit("\n\n", 1)[1]))
            return {"events": [], "final": {"actions_json": json.dumps([batches[len(prompts) - 1]])}}

        with tempfile.TemporaryDirectory() as output:
            summary = run_episode(data, output, codex=True, strict_replay=True, scheduled=True,
                                  policy_runner=fake, actions_per_cutoff=5, max_calls=8)
            records = [json.loads(line) for line in (Path(output) / "trajectory.jsonl").read_text().splitlines()]
        self.assertEqual(summary["run_status"], "completed")
        self.assertEqual(summary["rejected_actions"], 0)
        self.assertEqual(len(prompts), 7)
        self.assertEqual(records[4]["action"]["tool"], "read")
        self.assertEqual(records[4]["observation"]["as_of"], "2025-06-02T20:30:00+00:00")
        self.assertEqual(records[5]["action"]["type"], "wait")
        self.assertIsNone(records[5]["invocation"])

        delivered = prompts[5]
        self.assertEqual(delivered["as_of"], "2025-06-02T20:40:00+00:00")
        self.assertEqual(delivered["suggested_role"], "strong")
        self.assertEqual([item["step"] for item in delivered["execution_feedback"]], [5])
        read_id = delivered["execution_feedback"][0]["result"]["result"]["tool_call_id"]
        receipt = next(item for item in delivered["tool_results"] if item["tool_call_id"] == read_id)
        self.assertEqual(receipt["result"]["body"], data["sources"][0]["body"])
        self.assertEqual(json.dumps(delivered).count(data["sources"][0]["body"]), 1)

        consumed = prompts[6]
        self.assertEqual([item["action"]["tool"] for item in consumed["execution_feedback"]], ["catalog"])
        old_receipt = next(item for item in consumed["tool_results"] if item["tool_call_id"] == read_id)
        self.assertNotIn("body", old_receipt["result"])
        self.assertEqual(old_receipt["result"]["id"], "earnings")

    def test_strong_read_waits_for_its_role_across_fast_and_mid_invocations(self):
        for disposition in ("ignore", "advance"):
            with self.subTest(new_arrival_disposition=disposition):
                data = fixture()
                data["sources"][1].update(available_at="2025-06-02T20:30:00Z",
                                           published_at="2025-06-02T20:30:00Z")
                accumulation = {"type": "accumulate", "role": "mid", "theme_id": "power",
                                "ticket_ids": ["ticket-earnings"], "summary": "Power results",
                                "facts": [{"source_id": "earnings", "claim": "Revenue rose"}],
                                "open_questions": ["Is growth durable?"], "authoritative_exception": True,
                                "exception_reason": "Official filing"}
                batches = [
                    {"type": "triage", "role": "fast", "source_id": "earnings",
                     "disposition": "advance", "reason": "An official clue"},
                    {"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}},
                    accumulation,
                    action("open_research", question_id="capacity", theme_id="power", question="Is growth durable?"),
                    tool("read", source_id="earnings"),
                    {"type": "tool", "role": "fast", "tool": "read", "args": {"source_id": "contrary"}},
                    {"type": "triage", "role": "fast", "source_id": "contrary",
                     "disposition": disposition, "reason": "Route the later customer report"},
                ]
                if disposition == "advance":
                    batches.extend([
                        {"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "contrary"}},
                        {**accumulation, "ticket_ids": ["ticket-earnings", "ticket-contrary"],
                         "facts": [*accumulation["facts"], {"source_id": "contrary", "claim": "Customer delay"}]},
                    ])
                resumed_index = len(batches)
                batches.extend([tool("catalog", offset=0, limit=1),
                                action("stop", reason="Finish the cross-role prompt probe")])
                prompts = []

                def fake(prompt, *args, **kwargs):
                    prompts.append(json.loads(prompt.rsplit("\n\n", 1)[1]))
                    return {"events": [], "final": {"actions_json": json.dumps([batches[len(prompts) - 1]])}}

                with tempfile.TemporaryDirectory() as output:
                    summary = run_episode(data, output, codex=True, strict_replay=True, scheduled=True,
                                          policy_runner=fake, actions_per_cutoff=5, max_calls=12)
                self.assertEqual(summary["run_status"], "completed")
                self.assertEqual(summary["rejected_actions"], 0)
                self.assertEqual([item["suggested_role"] for item in prompts[5:resumed_index]],
                                 ["fast", "fast"] + (["mid", "mid"] if disposition == "advance" else []))
                for intervening in prompts[5:resumed_index]:
                    self.assertNotIn(data["sources"][0]["body"], json.dumps(intervening))
                resumed = prompts[resumed_index]
                self.assertEqual(resumed["suggested_role"], "strong")
                self.assertEqual(resumed["as_of"], "2025-06-02T20:40:00+00:00")
                own_read = next(item for item in resumed["tool_results"]
                                if item["tool"] == "read" and item["role"] == "strong")
                self.assertEqual(own_read["result"].get("body"), data["sources"][0]["body"])
                read_feedback = [item for item in resumed["execution_feedback"]
                                 if item["result"].get("result", {}).get("tool_call_id") == own_read["tool_call_id"]]
                self.assertEqual(len(read_feedback), 1)
                self.assertEqual(read_feedback[0]["step"], 5)
                self.assertEqual(json.dumps(resumed).count(data["sources"][0]["body"]), 1)
                next_prompt = prompts[resumed_index + 1]
                self.assertNotIn(data["sources"][0]["body"], json.dumps(next_prompt))
                self.assertEqual([item["action"]["tool"] for item in next_prompt["execution_feedback"]], ["catalog"])

    def test_explicit_strong_wait_acknowledges_the_cycle_and_preserves_idle_skip(self):
        env = ResearchPortfolioEnv(fixture(), strict_replay=True, scheduled=True)
        fixtures.ResearchPortfolioTests().prepare_question(env)
        self.assertNotIn("rejection", env.step(action("wait", reason="Defer this open question until new evidence"))[4])
        self.assertEqual(env.now, "2025-06-02T20:30:00+00:00")
        env.step(action("wait", reason="Idle until the next Strong tick"))
        self.assertEqual(env.now, "2025-06-02T20:40:00+00:00")
        self.assertIsNone(env.observation()["suggested_role"])
        self.assertIn("capacity", env.questions)
        self.assertFalse(env.theses)

    def test_independent_reconciliation_detects_tampered_result(self):
        path = Path(__file__).parents[1] / "fixtures" / "demo.json"
        manifest = json.loads(path.read_text())
        with tempfile.TemporaryDirectory() as output:
            result = run_episode(manifest, output, actions=demo_actions())
        self.assertEqual(result["reconciliation"]["status"], "passed")
        self.assertGreater(result["reconciliation"]["fills_checked"], 0)
        result["account"]["cash"] += 100
        self.assertEqual(reconcile(manifest, result)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
