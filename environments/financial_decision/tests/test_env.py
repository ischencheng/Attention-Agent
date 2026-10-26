"""Independent offline contracts for the financial research environment.

Fixtures are invented event releases, not investment results. No production
database, external source, model, or sample policy is used by these tests.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from environments.financial_decision.env import FinancialDecisionEnv


TIMES = [f"2025-06-0{day}T18:00:00+00:00" for day in range(2, 6)]


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def original(key, text, *, published=TIMES[0], available=TIMES[0], event=None):
    raw = {"title": key, "text": text, "synthetic": True}
    source_id = f"synthetic://independent-financial-env/{key}"
    raw_hash = digest(raw)
    return {"id": digest(["fictional-primary", source_id, raw_hash]),
            "source": "fictional-primary", "source_id": source_id,
            "published_at": published, "available_at": available,
            "event_id": event or key, "raw": raw, "raw_sha256": raw_hash}


def episode():
    records = {
        "initial": original("capacity-plan", "A fictional supplier plans new capacity; customer acceptance is unverified."),
        "contrary": original("customer-delay", "The customer delays acceptance and has not paid.",
                              published=TIMES[1], available=TIMES[1]),
        "correction": original("contract-correction", "FUTURE_PUBLICATION_CANARY: The contract remains valid, but acceptance is still delayed.",
                                published=TIMES[2], available=TIMES[0]),
        "delayed": original("late-financing", "DELAYED_RECEIPT_CANARY: Financing cost was much higher than planned.",
                             published=TIMES[0], available=TIMES[3]),
        "outcome": original("acceptance-record", "PRIVATE_OUTCOME_CANARY: The customer finally accepted the product.",
                             published=TIMES[3], available=TIMES[3]),
    }
    manifest = {"schema_version": 1, "episode_id": "independent-capacity-research-v1",
                "mode": "synthetic", "timeline": TIMES,
                "observations": list(records.values()),
                "questions": [{"question_id": "customer-acceptance",
                               "title": "Will the customer accept the new capacity?",
                               "criterion": {"metric": "customer_acceptance", "operator": "==", "target": 1,
                                             "unit": "binary", "source": "official acceptance record"},
                               "available_at": TIMES[0], "deadline": TIMES[1],
                               "resolve_at": TIMES[2],
                               "resolution": {"status": "resolved", "value": 1,
                                              "available_at": TIMES[3],
                                              "evidence_ids": [records["outcome"]["id"]],
                                              "reason": "The archived acceptance record confirms the event."}}]}
    return manifest, records


def thesis(support, *, contrary=(), thesis_id="capacity-research"):
    refs = list(support) + list(contrary)
    return {"thesis_id": thesis_id, "title": "Acceptance risk deserves investigation",
            "topics": ["datacenter", "capex"],
            "entities": [{"name": "Fictional supplier", "symbol": None}],
            "statement": "Acceptance and financing constrain realized capacity.",
            "thesis_horizon": "one year", "uncertainty": "The operating outcome remains uncertain.",
            "next_questions": ["Has the customer accepted the production line?"],
            "status": "unknown", "action": "research",
            "evidence_ids": [item["id"] for item in support],
            "counterevidence_ids": [item["id"] for item in contrary],
            "observed_event_ids": sorted({item["event_id"] for item in refs}),
            "framework_dimensions": ["cycle", "survivability"],
            "change_reason": "New operating evidence changes the research question.",
            "invalidation": "Independently verified acceptance and financing remove the stated constraints.",
            "next_review_at": "2025-07-01T18:00:00+00:00", "forecasts": []}


def judge(item):
    return {"type": "judge", "judgment": {"discoveries": [item], "no_action_reason": ""}}


def wait():
    return {"type": "wait", "reason": "Wait for independently available evidence."}


class FinancialEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.manifest, self.records = episode()
        self.env = FinancialDecisionEnv(self.manifest)
        self.observation, self.info = self.env.reset(seed=41)

    def read(self, name, env=None):
        return (env or self.env).step({"type": "read", "observation_id": self.records[name]["id"]})

    def prepare_thesis(self, thesis_id="capacity-research", env=None):
        env = env or self.env
        self.read("initial", env)
        result = env.step(judge(thesis([self.records["initial"]], thesis_id=thesis_id)))
        self.assertFalse(result[4].get("rejection"), result[4])
        return result

    def forecast(self, probability=0.8, thesis_id="capacity-research", env=None):
        return (env or self.env).step({"type": "forecast", "thesis_id": thesis_id,
                                     "question_id": "customer-acceptance", "probability": probability,
                                     "evidence_ids": [self.records["initial"]["id"]],
                                     "reason": "The initial contract supports a qualified forecast."})

    def assert_rejected_without_research_mutation(self, action):
        obs, reward, terminated, truncated, info = self.env.step(action)
        self.assertTrue(info.get("rejection"), info)
        self.assertLessEqual(reward, 0)
        self.assertFalse(terminated)
        for key in ("theses", "thesis_history", "forecasts", "settlements", "current_time"):
            # This helper is used before any accepted action. Rejected calls may
            # spend budget, but must preserve the initial research and clock.
            self.assertEqual(obs[key], self.observation[key], key)
        return obs, info

    def test_only_available_metadata_and_read_originals_are_exposed(self):
        serialized = json.dumps([self.observation, self.info])
        for canary in ("FUTURE_PUBLICATION_CANARY", "DELAYED_RECEIPT_CANARY", "PRIVATE_OUTCOME_CANARY"):
            self.assertNotIn(canary, serialized)
        public_ids = {item["id"] for item in self.observation["available_sources"]}
        self.assertEqual(public_ids, {self.records["initial"]["id"]})
        self.assertEqual(self.observation["read_observations"], [])
        self.assertNotIn("resolution", self.observation["questions"][0])
        observed, *_ = self.read("initial")
        self.assertEqual(observed["read_observations"][0]["raw"], self.records["initial"]["raw"])

    def test_manifest_rejects_tampered_original_and_impossible_resolution_times(self):
        corrupted = copy.deepcopy(self.manifest)
        corrupted["observations"][0]["raw"]["text"] = "Hash no longer matches this source."
        with self.assertRaises(ValueError):
            FinancialDecisionEnv(corrupted)
        premature = copy.deepcopy(self.manifest)
        premature["questions"][0]["resolution"]["available_at"] = TIMES[1]
        with self.assertRaises(ValueError):
            FinancialDecisionEnv(premature)
        reversed_time = copy.deepcopy(self.manifest)
        reversed_time["timeline"] = list(reversed(TIMES))
        with self.assertRaises(ValueError):
            FinancialDecisionEnv(reversed_time)

    def test_hidden_outcome_value_does_not_change_pre_resolution_observations(self):
        alternative = copy.deepcopy(self.manifest)
        alternative["questions"][0]["resolution"]["value"] = 0
        alternative["questions"][0]["resolution"]["reason"] = "PRIVATE_COUNTERFACTUAL_LABEL"
        other = FinancialDecisionEnv(alternative)
        other_observation, other_info = other.reset(seed=41)
        self.assertEqual(self.observation, other_observation)
        self.assertNotIn("PRIVATE_COUNTERFACTUAL_LABEL", json.dumps(other_info))
        actions = [{"type": "read", "observation_id": self.records["initial"]["id"]}, wait(), wait()]
        for action in actions:
            left, right = self.env.step(action), other.step(action)
            self.assertEqual(left[:4], right[:4])

    def test_same_seed_and_actions_reproduce_reset_and_entire_trajectory(self):
        other = FinancialDecisionEnv(copy.deepcopy(self.manifest))
        self.assertEqual((self.observation, self.info), other.reset(seed=41))
        actions = [{"type": "read", "observation_id": self.records["initial"]["id"]},
                   judge(thesis([self.records["initial"]])), wait(), wait(), wait(), wait()]
        for action in actions:
            self.assertEqual(self.env.step(copy.deepcopy(action)), other.step(copy.deepcopy(action)))
        self.assertEqual(self.env.get_state(), other.get_state())
        self.assertEqual((self.observation, self.info), self.env.reset(seed=41))

    def test_caller_mutation_cannot_change_private_manifest_or_research_history(self):
        self.manifest["questions"][0]["resolution"]["value"] = 0
        self.manifest["observations"][0]["raw"]["text"] = "CALLER_TAMPERING"
        self.observation["available_sources"].clear()
        observed, *_ = self.read("initial")
        self.assertNotIn("CALLER_TAMPERING", json.dumps(observed))
        observed["read_observations"][0]["raw"]["text"] = "RETURN_VALUE_TAMPERING"
        item = thesis([self.records["initial"]])
        self.env.step(judge(item))
        item["statement"] = "ACTION_TAMPERING"
        current, *_ = self.forecast()
        self.assertNotIn("TAMPERING", json.dumps(current))
        for _ in range(3):
            current, _, _, _, _ = self.env.step(wait())
        self.assertEqual(current["settlements"][0]["value"], 1)

    def test_future_read_and_unread_reference_are_rejected(self):
        self.assert_rejected_without_research_mutation({"type": "read", "observation_id": self.records["correction"]["id"]})
        self.assert_rejected_without_research_mutation(judge(thesis([self.records["initial"]])))
        self.assert_rejected_without_research_mutation({"type": "read", "observation_id": "invented-reference"})

    def test_bad_judgment_cannot_partially_commit_valid_thesis(self):
        self.read("initial")
        valid = thesis([self.records["initial"]])
        invalid = thesis([self.records["initial"]], thesis_id="other-research")
        invalid["evidence_ids"] = ["fabricated-original"]
        result = self.env.step({"type": "judge", "judgment": {"discoveries": [valid, invalid], "no_action_reason": ""}})
        self.assertTrue(result[4].get("rejection"), result[4])
        self.assertEqual(result[0]["theses"], {})
        self.assertEqual(result[0]["thesis_history"], [])

    def test_reposted_event_does_not_count_as_new_revision_evidence(self):
        duplicate = original("syndicated-plan", "A second URL repeats the same capacity announcement.",
                             published=TIMES[1], available=TIMES[1],
                             event=self.records["initial"]["event_id"])
        manifest = copy.deepcopy(self.manifest)
        manifest["observations"].append(duplicate)
        env = FinancialDecisionEnv(manifest)
        env.reset(seed=41)
        self.prepare_thesis(env=env)
        env.step(wait())
        env.step({"type": "read", "observation_id": duplicate["id"]})
        result = env.step(judge(thesis([self.records["initial"], duplicate])))
        self.assertTrue(result[4].get("rejection"), result[4])
        self.assertEqual(len(result[0]["thesis_history"]), 1)

    def test_same_original_source_can_publish_a_material_correction(self):
        corrected = copy.deepcopy(self.records["initial"])
        corrected["raw"] = {"title": "Official correction", "text": "The original capacity figure was overstated.", "synthetic": True}
        corrected["raw_sha256"] = digest(corrected["raw"])
        corrected["id"] = digest([corrected["source"], corrected["source_id"], corrected["raw_sha256"]])
        corrected["published_at"] = corrected["available_at"] = TIMES[1]
        manifest = copy.deepcopy(self.manifest)
        manifest["observations"].append(corrected)
        env = FinancialDecisionEnv(manifest)
        env.reset(seed=41)
        self.prepare_thesis(env=env)
        env.step(wait())
        env.step({"type": "read", "observation_id": corrected["id"]})
        result = env.step(judge(thesis([self.records["initial"], corrected])))
        self.assertFalse(result[4].get("rejection"), result[4])
        self.assertEqual(len(result[0]["thesis_history"]), 2)

    def test_revision_keeps_prior_counterevidence_and_history(self):
        self.prepare_thesis()
        self.env.step(wait())
        self.read("contrary")
        second = thesis([self.records["initial"]], contrary=[self.records["contrary"]])
        second["status"] = "weakened"
        result = self.env.step(judge(second))
        self.assertFalse(result[4].get("rejection"), result[4])
        self.env.step(wait())
        self.read("correction")
        erased = thesis([self.records["initial"], self.records["correction"]])
        failed = self.env.step(judge(erased))
        self.assertTrue(failed[4].get("rejection"), failed[4])
        repaired = thesis([self.records["initial"], self.records["correction"]], contrary=[self.records["contrary"]])
        final = self.env.step(judge(repaired))
        self.assertFalse(final[4].get("rejection"), final[4])
        self.assertEqual(len(final[0]["thesis_history"]), 3)
        self.assertIn(self.records["contrary"]["id"], json.dumps(final[0]["theses"]))

    def test_reclassifying_counterevidence_cannot_erase_it_from_later_revisions(self):
        self.read("initial")
        self.env.step(wait())
        self.read("contrary")
        first = thesis([self.records["initial"]], contrary=[self.records["contrary"]])
        self.assertFalse(self.env.step(judge(first))[4].get("rejection"))
        self.env.step(wait())
        self.read("correction")
        moved = thesis([self.records["initial"], self.records["contrary"], self.records["correction"]])
        second = self.env.step(judge(moved))
        self.assertFalse(second[4].get("rejection"), second[4])
        self.env.step(wait())
        self.read("delayed")
        dropped = thesis([self.records["initial"], self.records["correction"], self.records["delayed"]])
        failed = self.env.step(judge(dropped))
        self.assertTrue(failed[4].get("rejection"), failed[4])
        self.assertEqual(len(failed[0]["thesis_history"]), 2)
        self.assertEqual(failed[0]["theses"], second[0]["theses"])
        retained = thesis([self.records["initial"], self.records["correction"], self.records["delayed"]],
                          contrary=[self.records["contrary"]])
        final = self.env.step(judge(retained))
        self.assertFalse(final[4].get("rejection"), final[4])
        self.assertEqual(len(final[0]["thesis_history"]), 3)

    def test_duplicate_question_cannot_earn_reward_under_another_thesis_id(self):
        self.prepare_thesis()
        first = self.forecast()
        self.assertFalse(first[4].get("rejection"), first[4])
        self.env.step(judge(thesis([self.records["initial"]], thesis_id="second-thesis")))
        duplicate = self.forecast(thesis_id="second-thesis")
        self.assertTrue(duplicate[4].get("rejection"), duplicate[4])
        self.assertEqual(len(duplicate[0]["forecasts"]), 1)
        scores = []
        for _ in range(3):
            final = self.env.step(wait())
            scores.append(final[4]["reward_components"]["forecast_score"])
        self.assertAlmostEqual(sum(scores), 0.21)
        self.assertEqual(sum(value != 0 for value in scores), 1)
        self.assertEqual(len(final[0]["settlements"]), 1)

    def test_resolution_waits_for_actual_evidence_availability(self):
        self.prepare_thesis()
        self.forecast()
        at_deadline = self.env.step(wait())
        before_due = self.env.summary()
        self.assertEqual(before_due["pending_forecasts"], 1)
        self.assertEqual(before_due["unresolved_forecasts"], 0)
        at_due = self.env.step(wait())
        due = self.env.summary()
        self.assertEqual(due["pending_forecasts"], 0)
        self.assertEqual(due["unresolved_forecasts"], 1)
        self.assertEqual(due["results"][0]["status"], "unresolved")
        self.assertNotIn("available_at", due["results"][0])
        self.assertNotIn(TIMES[3], json.dumps(due))
        self.assertNotIn(self.manifest["questions"][0]["resolution"]["reason"], json.dumps(due))
        for transition in (at_deadline, at_due):
            self.assertEqual(transition[4]["reward_components"]["forecast_score"], 0)
            self.assertNotIn("PRIVATE_OUTCOME_CANARY", json.dumps(transition))
            self.assertFalse(any(item.get("status") == "resolved" for item in transition[0]["settlements"]))
        completed = self.env.step(wait())
        self.assertAlmostEqual(completed[4]["reward_components"]["forecast_score"], 0.21)
        self.assertEqual(self.env.summary()["resolved_forecasts"], 1)
        self.assertEqual(self.env.summary()["unresolved_forecasts"], 0)
        self.assertFalse(completed[2])
        self.assertFalse(completed[3])
        ended = self.env.step(wait())
        self.assertTrue(ended[2])
        self.assertFalse(ended[3])
        self.assertEqual(ended[4]["reward_components"]["forecast_score"], 0)

    def test_late_forecast_is_rejected_without_creating_a_prediction(self):
        self.prepare_thesis()
        self.env.step(wait())
        self.env.step(wait())
        result = self.forecast()
        self.assertTrue(result[4].get("rejection"), result[4])
        self.assertEqual(result[0]["forecasts"], [])
        self.assertEqual(result[4]["reward_components"]["forecast_score"], 0)

    def test_nonfinite_and_boolean_probabilities_cannot_enter_history(self):
        self.prepare_thesis()
        for probability in (float("nan"), float("inf"), True, -0.1, 1.1):
            with self.subTest(probability=probability):
                result = self.forecast(probability)
                self.assertTrue(result[4].get("rejection"), result[4])
                self.assertEqual(result[0]["forecasts"], [])

    def test_missing_resolution_is_unresolved_without_a_forecast_score(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["questions"][0]["resolution"] = {"status": "unresolved", "value": None,
                                                   "available_at": TIMES[2], "evidence_ids": [],
                                                   "reason": "The independent resolution record is unavailable."}
        env = FinancialDecisionEnv(manifest)
        env.reset(seed=41)
        self.prepare_thesis(env=env)
        self.forecast(env=env)
        for _ in range(3):
            result = env.step(wait())
            self.assertEqual(result[4]["reward_components"]["forecast_score"], 0)
        self.assertEqual(result[0]["settlements"][0]["status"], "unresolved")
        self.assertIsNone(result[0]["settlements"][0].get("score"))

    def test_absent_resolution_availability_becomes_unresolved_only_when_due(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["questions"][0]["resolution"] = {"status": "unresolved", "value": None,
                                                   "available_at": None, "evidence_ids": [],
                                                   "reason": "PRIVATE_MISSING_LABEL_REASON"}
        env = FinancialDecisionEnv(manifest)
        env.reset(seed=41)
        self.prepare_thesis(env=env)
        self.forecast(env=env)
        first = env.step(wait())
        self.assertEqual(env.summary()["pending_forecasts"], 1)
        self.assertEqual(env.summary()["unresolved_forecasts"], 0)
        self.assertEqual(first[4]["reward_components"]["forecast_score"], 0)
        for _ in range(3):
            result = env.step(wait())
            summary = env.summary()
            self.assertEqual(summary["pending_forecasts"], 0)
            self.assertEqual(summary["unresolved_forecasts"], 1)
            self.assertEqual(summary["results"][0]["status"], "unresolved")
            self.assertIsNone(summary["results"][0].get("score"))
            self.assertEqual(result[4]["reward_components"]["forecast_score"], 0)
            self.assertNotIn("PRIVATE_MISSING_LABEL_REASON", json.dumps([result, summary]))
        self.assertEqual(result[0]["settlements"], [])

    def test_no_forecast_does_not_become_prediction_success(self):
        for _ in range(3):
            result = self.env.step(wait())
            self.assertEqual(result[4]["reward_components"]["forecast_score"], 0)
        self.assertEqual(result[0]["forecasts"], [])
        self.assertEqual(result[0]["settlements"], [])

    def test_budget_exhaustion_is_distinct_from_natural_termination(self):
        short = FinancialDecisionEnv(copy.deepcopy(self.manifest), max_steps=1)
        short.reset(seed=41)
        result = self.read("initial", short)
        self.assertFalse(result[2])
        self.assertTrue(result[3])
        before = short.get_state()
        with self.assertRaises((RuntimeError, ValueError)):
            short.step(wait())
        self.assertEqual(short.get_state(), before)

    def test_checkpoint_resume_replays_identical_future_rewards(self):
        self.prepare_thesis()
        self.forecast()
        self.env.step(wait())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            self.env.save_checkpoint(path)
            resumed = FinancialDecisionEnv(copy.deepcopy(self.manifest))
            resumed.load_checkpoint(path)
            self.assertEqual(self.env.get_state(), resumed.get_state())
            for _ in range(2):
                self.assertEqual(self.env.step(wait()), resumed.step(wait()))
            self.assertEqual(self.env.get_state(), resumed.get_state())

    def test_checkpoint_tampering_is_rejected_without_replacing_live_state(self):
        self.prepare_thesis()
        original_state = self.env.get_state()
        corrupted = copy.deepcopy(original_state)
        # Change an action in the stored trajectory while retaining all original
        # recorded transition/state hashes. It must fail deterministic replay.
        def alter_read(value):
            if isinstance(value, dict):
                if value.get("type") == "read" and "observation_id" in value:
                    value["observation_id"] = "tampered-source"
                    return True
                return any(alter_read(child) for child in value.values())
            if isinstance(value, list):
                return any(alter_read(child) for child in value)
            return False
        self.assertTrue(alter_read(corrupted), "checkpoint must retain auditable actions")
        with self.assertRaises((ValueError, RuntimeError)):
            self.env.load_state(corrupted)
        self.assertEqual(self.env.get_state(), original_state)
        # Recomputing the outer checksum must not bypass the independently
        # recorded observation, transition, and summary replay commitments.
        corrupted["checkpoint_hash"] = digest({key: value for key, value in corrupted.items()
                                               if key != "checkpoint_hash"})
        with self.assertRaises((ValueError, RuntimeError)):
            self.env.load_state(corrupted)
        self.assertEqual(self.env.get_state(), original_state)

    def test_checkpoint_cannot_resume_against_a_changed_resolution_contract(self):
        self.prepare_thesis()
        self.forecast()
        changed = copy.deepcopy(self.manifest)
        changed["questions"][0]["resolution"]["value"] = 0
        other = FinancialDecisionEnv(changed)
        other.reset(seed=41)
        original = other.get_state()
        with self.assertRaises(ValueError):
            other.load_state(self.env.get_state())
        self.assertEqual(other.get_state(), original)

    def test_export_reconciles_reward_components_and_evidence_path(self):
        self.prepare_thesis()
        self.forecast()
        for _ in range(4):
            self.env.step(wait())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trajectory.json"
            self.env.export_trajectory(path)
            exported = json.loads(path.read_text())
            self.assertAlmostEqual(sum(step["reward"] for step in exported["steps"]),
                                   exported["summary"]["total_reward"])
            for step in exported["steps"]:
                self.assertAlmostEqual(step["reward"], sum(step["reward_components"].values()))
            final = exported["steps"][-1]["next_observation"]
            forecast = final["forecasts"][0]
            settlement = final["settlements"][0]
            self.assertEqual(settlement["forecast_id"], forecast["forecast_id"])
            self.assertEqual(forecast["judgment_hash"], final["thesis_history"][0]["judgment_hash"])
            self.assertEqual(settlement["evidence_ids"], [self.records["outcome"]["id"]])
            original_bytes = path.read_bytes()
            with self.assertRaises(FileExistsError):
                self.env.export_trajectory(path)
            self.assertEqual(path.read_bytes(), original_bytes)


if __name__ == "__main__":
    unittest.main()
