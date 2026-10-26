"""Independent synthetic invariants, not measured investment performance."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from environments.research_portfolio.env import FRAMEWORK_FIELDS, RISK_FIELDS, ResearchPortfolioEnv
from environments.research_portfolio.scenario import load_manifest


TIMES = [f"2025-06-0{day}T20:00:00+00:00" for day in range(2, 6)]


def fixture():
    return {"schema_version": 1, "episode_id": "independent-research-process",
            "mode": "synthetic", "price_basis": "raw", "timeline": TIMES,
            "scope": ["power", "datacenter", "earnings"],
            "portfolio": {"initial_cash": 1000, "fee_bps": 0, "slippage_bps": 0, "max_weight": 1},
            "assets": [{"asset_id": "power", "symbol": "PWRX", "name": "Fictional Power",
                        "available_at": TIMES[0], "terms": ["power"]}],
            "bars": [{"asset_id": "power", "open_at": f"2025-06-0{day}T13:30:00+00:00",
                      "close_at": TIMES[day - 2], "open": opened, "close": closed}
                     for day, opened, closed in ((2, 100, 100), (3, 100, 110), (4, 90, 80), (5, 70, 60))],
            "corporate_actions": [],
            "sources": [{"id": "earnings", "source": "official", "event_id": "earnings-event",
                         "available_at": TIMES[0], "published_at": TIMES[0], "title": "Power revenue",
                         "body": "Power revenue rose; leverage and execution still require investigation.",
                         "terms": ["power", "earnings"], "authority": True},
                        {"id": "contrary", "source": "customer", "event_id": "delay-event",
                         "available_at": TIMES[1], "published_at": TIMES[1], "title": "Customer delay",
                         "body": "FUTURE_EVIDENCE_CANARY customer delays power expansion.",
                         "terms": ["power", "delay"], "authority": False}]}


def action(kind, **fields):
    return {"type": kind, "role": "strong", **fields}


def tool(name, **args):
    return action("tool", question_id="capacity", tool=name, args=args)


def judgment(**updates):
    result = action("judge", thesis_id="power-thesis", question_id="capacity",
                    claim="Power demand may support capacity expansion.", evidence_ids=["earnings"],
                    counterevidence_ids=[], uncertainty="Orders may not convert to cash.",
                    invalidation_conditions=["The customer cancels the expansion."],
                    status="investable", asset_ids=["power"],
                    framework_analysis={key: "Observed evidence is limited; use a small unlevered allocation." for key in FRAMEWORK_FIELDS},
                    risk_checks={key: {"status": "PASS", "evidence_ids": ["earnings"],
                                       "reason": "Synthetic test assumption tied to the read original."}
                                 for key in RISK_FIELDS})
    result.update(updates)
    return result


class ResearchPortfolioTests(unittest.TestCase):
    def setUp(self):
        self.env = ResearchPortfolioEnv(fixture())

    def ok(self, item, env=None):
        result = (env or self.env).step(item)
        self.assertNotIn("rejection", result[4], result[4])
        return result

    def prepare_question(self, env=None):
        env = env or self.env
        self.ok({"type": "triage", "role": "fast", "source_id": "earnings",
                 "disposition": "advance", "reason": "Official earnings may reveal power demand."}, env)
        self.ok({"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}}, env)
        self.ok({"type": "accumulate", "role": "mid", "theme_id": "power-demand",
                 "ticket_ids": ["ticket-earnings"], "summary": "The issuer reports higher power revenue.",
                 "facts": [{"source_id": "earnings", "claim": "Issuer reports revenue growth."}],
                 "open_questions": ["Will expansion convert to cash?"],
                 "authoritative_exception": True, "exception_reason": "Official earnings is hard data."}, env)
        self.ok(action("open_research", question_id="capacity", theme_id="power-demand",
                       question="Will power expansion convert to cash?"), env)
        self.ok(tool("read", source_id="earnings"), env)

    def prepare_investment(self, env=None):
        env = env or self.env
        self.prepare_question(env)
        for item in (tool("discover", query="power"), tool("price_history", asset_id="power"),
                     tool("framework", asset_id="power"), tool("search", query="power delay", intent="contradiction")):
            self.ok(item, env)
        self.ok(judgment(), env)

    def invest(self, env=None):
        env = env or self.env
        self.prepare_investment(env)
        self.ok(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"],
                       reason="Test a limited research-backed position."), env)
        self.ok(action("wait", reason="Continue research after the next close."), env)

    def test_public_observation_hides_unread_original_and_future_prices(self):
        obs, _ = self.env.reset()
        encoded = json.dumps(obs)
        self.assertNotIn("FUTURE_EVIDENCE_CANARY", encoded)
        self.assertNotIn("revenue rose", encoded)
        self.assertNotIn('"close": 60', encoded)
        self.assertEqual(obs["discovered_assets"], [])
        self.assertEqual(obs["suggested_role"], "fast")
        obs["feed"][0]["id"] = "mutated"
        self.assertEqual(self.env.observation()["feed"][0]["id"], "earnings")

    def test_public_execution_contract_distinguishes_and_enforces_limits(self):
        manifest = fixture()
        manifest["portfolio"].update(initial_cash=1234, fee_bps=7, slippage_bps=13, max_weight=0.2)
        limited = ResearchPortfolioEnv(manifest)
        manifest["portfolio"]["max_weight"] = 1
        unrestricted = ResearchPortfolioEnv(manifest)
        narrow = limited.observation()
        broad = unrestricted.observation()
        expected = {"initial_cash": 1234, "fee_bps": 7, "slippage_bps": 13, "max_weight": 0.2,
                    "account_type": "cash", "long_only": True, "leverage_allowed": False,
                    "fractional_shares": True, "price_basis": "raw"}
        contract = narrow["execution_contract"]
        self.assertEqual({key: contract[key] for key in expected}, expected)
        self.assertEqual(broad["execution_contract"]["max_weight"], 1)
        self.assertNotEqual(narrow, broad)
        self.assertEqual({key: value for key, value in narrow.items() if key != "execution_contract"},
                         {key: value for key, value in broad.items() if key != "execution_contract"})
        self.assertNotIn("assets", contract)
        self.assertNotIn("bars", contract)
        self.assertNotIn("PWRX", json.dumps(contract))
        self.assertNotIn("FUTURE_EVIDENCE_CANARY", json.dumps(narrow))
        contract["max_weight"] = 1
        self.assertEqual(limited.observation()["execution_contract"]["max_weight"], 0.2)
        self.prepare_investment(limited)
        self.prepare_investment(unrestricted)
        allocation = action("allocate", target_weights={"power": 0.3}, thesis_ids=["power-thesis"],
                            reason="Check the public requested-weight limit.")
        before = limited.observation()["account"]
        rejected = limited.step(allocation)
        self.assertIn("concentration limit", rejected[4]["rejection"])
        self.assertEqual(rejected[0]["account"], before)
        self.ok(allocation, unrestricted)
        allocation["target_weights"]["power"] = 0.2
        accepted = self.ok(allocation, limited)
        self.assertEqual(accepted[0]["account"]["pending_order"]["weights"], {"power": 0.2})

    def test_native_publication_and_receipt_both_gate_visibility(self):
        manifest = fixture()
        manifest["sources"][0]["published_at"] = TIMES[1]
        env = ResearchPortfolioEnv(manifest)
        self.assertEqual(env.observation()["feed"], [])
        self.assertIn("rejection", env.step({"type": "triage", "role": "fast", "source_id": "earnings",
                        "disposition": "advance", "reason": "Early access attempt."})[4])

    def test_full_process_provenance_and_terminal_return(self):
        self.invest()
        self.assertEqual(self.env.observation()["account"]["positions"]["power"]["quantity"], 5)
        self.assertEqual(self.env.summary()["reward"], None)
        self.assertTrue(all(item["reward"] == 0 for item in self.env.trajectory))
        self.ok(tool("read", source_id="contrary"))
        self.ok(judgment(counterevidence_ids=["contrary"], what_changed="Customer delay weakens the claim."))
        result = self.ok(action("stop", reason="End policy actions, retain original valuation horizon."))
        self.assertAlmostEqual(result[1], -0.2)
        self.assertTrue(result[2])
        self.assertEqual(self.env.now, TIMES[-1])
        versions = result[0]["theses"]["power-thesis"]["revisions"]
        self.assertEqual(versions[1]["prior_revision"], versions[0]["revision_id"])
        self.assertEqual(versions[1]["counterevidence_ids"], ["contrary"])
        self.assertEqual(result[0]["account"]["cash"], 500)
        with self.assertRaises(ValueError):
            self.env.step(action("stop", reason="Do not reward twice."))

    def test_budget_exhaustion_cannot_avoid_loss(self):
        env = ResearchPortfolioEnv(fixture(), max_steps=14)
        self.invest(env)
        while not env.truncated:
            result = env.step(tool("read", source_id="earnings"))
        self.assertEqual(env.now, TIMES[-1])
        self.assertAlmostEqual(result[1], -0.2)
        self.assertTrue(result[3])

    def test_public_action_contract_supports_a_valid_thesis_revision(self):
        self.prepare_investment()
        advertised = self.env.observation()["action_contract"]["actions"]["judge"]
        self.assertIn("required", advertised["what_changed"])
        revised = judgment(what_changed="Reassessed cash conversion uncertainty.")
        self.assertTrue(set(revised) - {"type"} <= set(advertised))
        self.ok(revised)
        versions = self.env.observation()["theses"]["power-thesis"]["revisions"]
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[-1]["what_changed"], revised["what_changed"])

    def test_invalid_actions_advance_clock_and_settle(self):
        env = ResearchPortfolioEnv(fixture(), actions_per_cutoff=1)
        for _ in range(3):
            result = env.step({"type": "bogus"})
            self.assertIn("rejection", result[4])
        self.assertTrue(result[2])
        self.assertEqual(env.now, TIMES[-1])
        self.assertEqual(result[1], 0)

    def test_malformed_json_and_shape_rejected_without_research_mutation(self):
        for candidate in (["wrong"], {"type": "wait", "role": []},
                          action("wait", reason="bad", extra=True),
                          action("wait", reason=float("nan")),
                          action("tool", tool="read", question_id="absent", args={"source_id": "earnings"})):
            result = self.env.step(candidate)
            self.assertIn("rejection", result[4])
            self.assertEqual(self.env.questions, {})
            self.assertEqual(self.env.read_sources, {})
        json.dumps(self.env.trajectory, allow_nan=False)

    def test_malformed_finite_json_fields_are_rejected_atomically(self):
        env = ResearchPortfolioEnv(fixture(), actions_per_cutoff=1000, max_steps=1000)
        self.prepare_investment(env)
        bad_risks = copy.deepcopy(judgment()["risk_checks"])
        bad_risks["bankruptcy"]["status"] = []
        candidates = [
            action("wait", role=[], reason="Malformed role."),
            {"type": "triage", "role": "fast", "source_id": "earnings", "disposition": [], "reason": "Malformed disposition."},
            tool("search", query="power", intent=[]),
            tool("framework", asset_id=[]),
            judgment(status=[]),
            judgment(risk_checks=bad_risks),
            judgment(what_changed=[]),
            {"type": "accumulate", "role": "mid", "theme_id": "power-demand", "ticket_ids": ["ticket-earnings"],
             "summary": "Revenue statement.", "facts": [{"source_id": [], "claim": "Bad identity."}],
             "open_questions": ["Cash conversion?"], "authoritative_exception": False},
            {"type": "accumulate", "role": "mid", "theme_id": "power-demand", "ticket_ids": ["ticket-earnings"],
             "summary": "Revenue statement.", "facts": [{"source_id": "earnings", "claim": "Revenue grows."}],
             "open_questions": ["Cash conversion?"], "authoritative_exception": False, "exception_reason": []},
        ]
        valid_metadata = {"id": "call-1", "role": "strong", "policy": "codex", "trainable": True}
        malformed_metadata = [[], {}, {**valid_metadata, "id": []}, {**valid_metadata, "role": []},
                              {**valid_metadata, "role": "mid"}, {**valid_metadata, "policy": []},
                              {**valid_metadata, "trainable": 1}, {**valid_metadata, "action_index": []},
                              {**valid_metadata, "action_index": -1}, {**valid_metadata, "extra": []}]
        candidates.extend(action("wait", reason="Malformed metadata.", invocation=item) for item in malformed_metadata)
        stable_keys = ("as_of", "read_sources", "triage", "tickets", "themes", "questions", "theses",
                       "discovered_assets", "tool_results", "account")
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                before = env.observation()
                step = env.steps
                result = env.step(candidate)
                self.assertIn("rejection", result[4])
                self.assertEqual(env.steps, step + 1)
                self.assertEqual({key: result[0][key] for key in stable_keys},
                                 {key: before[key] for key in stable_keys})
                if "invocation" in candidate:
                    self.assertNotIn("invocation", env.trajectory[-1])
        json.dumps(env.trajectory, allow_nan=False)

    def test_malformed_fields_still_exhaust_the_clock_budget(self):
        env = ResearchPortfolioEnv(fixture(), actions_per_cutoff=1)
        for expected_time in TIMES[1:]:
            result = env.step(action("wait", role=[], reason="Bad policy output cannot freeze time."))
            self.assertIn("rejection", result[4])
            self.assertEqual(result[0]["as_of"], expected_time)
            self.assertEqual(result[0]["account"]["cash"], 1000)
        self.assertTrue(result[2])
        self.assertEqual(result[1], 0)

    def test_internal_accounting_type_errors_are_not_policy_rejections(self):
        self.prepare_investment()
        with patch.object(self.env.market, "submit", side_effect=TypeError("accounting implementation failure")):
            with self.assertRaisesRegex(TypeError, "accounting implementation failure"):
                self.env.step(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"],
                                     reason="Valid action encountering an internal error."))

    def test_mid_cannot_publish_without_its_own_original_read(self):
        self.ok({"type": "triage", "role": "fast", "source_id": "earnings", "disposition": "advance", "reason": "Research."})
        self.ok({"type": "tool", "role": "fast", "tool": "read", "args": {"source_id": "earnings"}})
        result = self.env.step({"type": "accumulate", "role": "mid", "theme_id": "x", "ticket_ids": ["ticket-earnings"],
                               "summary": "Issuer claim.", "facts": [{"source_id": "earnings", "claim": "Revenue grows."}],
                               "open_questions": ["Cash?"], "authoritative_exception": True, "exception_reason": "Earnings."})
        self.assertIn("rejection", result[4])
        self.assertEqual(self.env.themes, {})

    def test_duplicate_reports_do_not_become_independent_signals(self):
        manifest = fixture()
        manifest["sources"] = []
        for index in range(3):
            source = copy.deepcopy(fixture()["sources"][0])
            source.update(id=f"copy-{index}", source=f"provider-{index}", authority=False)
            manifest["sources"].append(source)
        env = ResearchPortfolioEnv(manifest)
        for index in range(3):
            self.ok({"type": "triage", "role": "fast", "source_id": f"copy-{index}", "disposition": "advance", "reason": "Coverage."}, env)
            self.ok({"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": f"copy-{index}"}}, env)
        result = self.ok({"type": "accumulate", "role": "mid", "theme_id": "same-event",
                         "ticket_ids": [f"ticket-copy-{i}" for i in range(3)], "summary": "Three reports of one event.",
                         "facts": [{"source_id": f"copy-{i}", "claim": "Reports same earnings."} for i in range(3)],
                         "open_questions": ["Any independent corroboration?"], "authoritative_exception": False}, env)
        self.assertEqual(result[4]["result"]["signal_count"], 1)
        self.assertFalse(result[4]["result"]["eligible"])
        result = env.step(action("open_research", question_id="q", theme_id="same-event", question="Enough?"))
        self.assertIn("rejection", result[4])

    def test_research_can_remain_asset_free(self):
        self.prepare_question()
        result = self.ok(judgment(status="research", asset_ids=[], framework_analysis={}, risk_checks={}))
        self.assertEqual(result[0]["discovered_assets"], [])
        self.assertEqual(result[0]["account"]["positions"], {})
        self.assertEqual(self.env.summary()["counts"]["theses"], 1)

    def test_allocation_requires_actual_tools_before_judgment(self):
        self.prepare_question()
        self.ok(tool("discover", query="power"))
        self.ok(judgment())
        for item in (tool("price_history", asset_id="power"), tool("framework", asset_id="power"),
                     tool("search", query="power", intent="contradiction")):
            self.ok(item)
        result = self.env.step(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"], reason="Late tools cannot rewrite old judgment."))
        self.assertIn("rejection", result[4])
        self.assertIsNone(result[0]["account"]["pending_order"])
        self.ok(judgment(what_changed="New tool evidence is now explicitly reviewed."))
        self.ok(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"], reason="Updated judgment passed gates."))

    def test_failed_risk_allows_exit_without_allowing_new_buy(self):
        self.invest()
        risks = copy.deepcopy(judgment()["risk_checks"])
        risks["bankruptcy"]["status"] = "FAIL"
        self.ok(judgment(status="invalidated", risk_checks=risks, what_changed="New analysis rejects solvency assumption."))
        bad = self.env.step(action("allocate", target_weights={"power": 0.9}, thesis_ids=["power-thesis"], reason="Should fail."))
        self.assertIn("rejection", bad[4])
        self.ok(action("allocate", target_weights={}, thesis_ids=["power-thesis"], reason="Exit invalidated exposure."))
        self.ok(action("wait", reason="Observe next actual fill."))
        self.assertEqual(self.env.observation()["account"]["positions"], {})
        result = self.ok(action("stop", reason="Settle final account."))
        self.assertAlmostEqual(result[1], -0.05)

    def test_failed_risk_reduction_cannot_buy_after_price_gap(self):
        manifest = fixture()
        manifest["bars"][2]["open"] = 10
        env = ResearchPortfolioEnv(manifest)
        self.invest(env)
        self.ok(judgment(status="invalidated", what_changed="Thesis invalidation requires reducing exposure."), env)
        # Current weight is 550/1050. A 40% target at a $10 next open would buy
        # more shares without the broker's explicit quantity cap.
        self.ok(action("allocate", target_weights={"power": 0.4}, thesis_ids=["power-thesis"],
                       reason="Reduce exposure without passing positive gates."), env)
        self.ok(action("wait", reason="Observe execution at the next open."), env)
        self.assertLessEqual(env.observation()["account"]["positions"]["power"]["quantity"], 5)
        self.assertTrue(any(item["type"] == "allocation_reduction_cap" for item in env.observation()["account"]["ledger"]))

    def test_roles_cannot_skip_the_research_pipeline(self):
        for item in (action("open_research", question_id="q", theme_id="missing", question="Buy power?"),
                     action("allocate", target_weights={"power": 1}, thesis_ids=["fake"], reason="Skip research."),
                     {"type": "tool", "role": "mid", "tool": "discover", "args": {"query": "power"}}):
            result = self.env.step(item)
            self.assertIn("rejection", result[4])
        self.assertEqual(self.env.questions, {})
        self.assertEqual(self.env.observation()["account"]["positions"], {})

    def test_empty_price_history_does_not_pass_investment_gate(self):
        manifest = fixture()
        manifest["bars"] = manifest["bars"][1:]
        env = ResearchPortfolioEnv(manifest)
        self.prepare_investment(env)
        result = env.step(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"], reason="No observed entry history."))
        self.assertIn("rejection", result[4])
        self.assertIsNone(result[0]["account"]["pending_order"])

    def test_authoritative_exception_requires_authoritative_original(self):
        manifest = fixture()
        manifest["sources"][0]["authority"] = False
        env = ResearchPortfolioEnv(manifest)
        self.ok({"type": "triage", "role": "fast", "source_id": "earnings", "disposition": "advance", "reason": "Read report."}, env)
        self.ok({"type": "tool", "role": "mid", "tool": "read", "args": {"source_id": "earnings"}}, env)
        result = env.step({"type": "accumulate", "role": "mid", "theme_id": "x", "ticket_ids": ["ticket-earnings"],
                           "summary": "Third-party report.", "facts": [{"source_id": "earnings", "claim": "Revenue claim."}],
                           "open_questions": ["Verify original?"], "authoritative_exception": True, "exception_reason": "Unsupported exception."})
        self.assertIn("rejection", result[4])
        self.assertEqual(env.themes, {})

    def test_counterevidence_cannot_be_erased_by_revision(self):
        self.invest()
        self.ok(tool("read", source_id="contrary"))
        self.ok(judgment(counterevidence_ids=["contrary"], what_changed="Read delay."))
        result = self.env.step(judgment(what_changed="Try erasing prior contrary evidence."))
        self.assertIn("rejection", result[4])
        self.assertEqual(len(self.env.theses["power-thesis"]["revisions"]), 2)

    def test_stop_cancels_unfilled_order(self):
        self.prepare_investment()
        self.ok(action("allocate", target_weights={"power": 0.5}, thesis_ids=["power-thesis"], reason="Pending allocation."))
        result = self.ok(action("stop", reason="No further policy actions."))
        self.assertEqual(result[1], 0)
        self.assertEqual(result[0]["account"]["positions"], {})
        self.assertIsNone(result[0]["account"]["pending_order"])

    def test_role_routing_and_stale_action_guard(self):
        self.assertEqual(self.env.observation()["suggested_role"], "fast")
        self.prepare_question()
        self.assertEqual(self.env.observation()["suggested_role"], "strong")
        self.ok(action("wait", reason="Next arrival."))
        self.assertEqual(self.env.observation()["suggested_role"], "fast")
        result = self.env.step(action("wait", as_of=TIMES[0], reason="Stale batch."))
        self.assertIn("rejection", result[4])
        self.assertEqual(self.env.now, TIMES[1])

    def test_invocation_and_trace_export_preserve_process_identity(self):
        self.prepare_question()
        item = tool("search", query="power", intent="contradiction")
        item["invocation"] = {"id": "run-1", "role": "strong", "policy": "scripted", "trainable": False}
        self.ok(item)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "trajectory.jsonl"
            self.env.export_trajectory(path)
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(records[-1]["invocation"], item["invocation"])
            self.assertEqual(records[-1]["result"]["question_id"], "capacity")
            self.assertEqual(records[-1]["result"]["trace_id"], records[-1]["id"])
            with self.assertRaises(FileExistsError):
                self.env.export_trajectory(path)

    def test_manifest_is_frozen_and_rejects_unknown_or_nonfinite_values(self):
        manifest = fixture()
        env = ResearchPortfolioEnv(manifest)
        manifest["sources"][0]["body"] = "MUTATED"
        self.assertNotEqual(env.manifest["sources"][0]["body"], "MUTATED")
        for field, value in (("extra", True), ("schema_version", True)):
            bad = fixture()
            bad[field] = value
            with self.assertRaises(ValueError):
                load_manifest(bad)
        bad = fixture()
        bad["portfolio"]["initial_cash"] = float("inf")
        with self.assertRaises(ValueError):
            load_manifest(bad)
        for value in ([], {}, None, 1):
            bad = fixture()
            bad["mode"] = value
            with self.subTest(mode=value), self.assertRaises(ValueError):
                load_manifest(bad)


if __name__ == "__main__":
    unittest.main()
