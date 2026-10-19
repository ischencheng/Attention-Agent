"""Deterministic replay/independent-grader checks; no network or model calls."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ledger import Ledger, timestamp
import replay


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def write_trace(self, *, memory="versioned", transform=None, failed_step=None):
        """Hand-authored reference trace, not a claimed model trajectory."""
        manifest = replay.prepare(self.root, memory=memory, model="test-double")
        records = manifest["records"]
        previous = None
        for index, step in enumerate(manifest["steps"]):
            ledger = Ledger(replay.ledger_path(self.root, memory, step))
            try:
                for name in step["records"]:
                    record = records[name]
                    ledger.append("observation", record["id"], record)
                if index != 1:
                    names = [["initial"], [], ["initial", "cancellation"], ["initial", "cancellation", "correction"]][index]
                    # The skeptical reference uses cancellation as SUPPORT for
                    # research, demonstrating that the grader mandates no side.
                    item = {
                        "thesis_id": "capacity-contract-risk", "title": "Expansion needs verification",
                        "topics": ["semiconductor", "capex"],
                        "entities": [{"name": "Lanyu Packaging Lab", "symbol": None}],
                        "statement": "Customer commitments require further investigation.",
                        "thesis_horizon": "12 months", "uncertainty": "Economics remain unknown.",
                        "next_questions": ["What terms govern acceptance and payment?"],
                        "status": "unknown", "action": "research",
                        "evidence_ids": [records[name]["id"] for name in names],
                        "counterevidence_ids": [],
                        "observed_event_ids": [records[name]["event_id"] for name in names],
                        "framework_dimensions": ["cycle", "risk"],
                        "change_reason": "New operating disclosure merits investigation.",
                        "invalidation": "Independent evidence contradicts the operating facts.",
                        "next_review_at": "2025-06-30T18:00:00Z", "forecasts": [],
                    }
                    payload = {"mode": "replay", "as_of": step["as_of"], "discovery": item,
                               "version": 1 if memory == "none" else [1, None, 2, 3][index],
                               "previous_judgment_hash": previous if memory == "versioned" else None}
                    if transform:
                        transform(index, payload, records)
                    previous = ledger.append("judgment", step["name"], payload)
                elif transform:
                    transform(index, ledger, records)
                ledger.append("cycle", step["name"], {"mode": "replay", "as_of": step["as_of"]})
            finally:
                ledger.close()
            directory = self.root / "steps" / step["name"]
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "execution.json").write_text(json.dumps({"returncode": 2 if index == failed_step else 0}))
        return manifest

    def result(self, manifest, name):
        result = replay.grade(self.root, manifest)
        return next(check for check in result["checks"] if check["id"] == name), result

    def test_fixture_distinguishes_repost_future_publication_and_late_availability(self):
        case = replay.scenario()
        records = case["records"]
        self.assertEqual(case, replay.scenario())
        self.assertEqual(records["initial"]["event_id"], records["duplicate"]["event_id"])
        self.assertNotEqual(records["initial"]["id"], records["duplicate"]["id"])
        cutoff = timestamp(case["steps"][-1]["as_of"])
        self.assertGreater(timestamp(records["future"]["published_at"]), cutoff)
        self.assertLess(timestamp(records["delayed"]["published_at"]), cutoff)
        self.assertGreater(timestamp(records["delayed"]["available_at"]), cutoff)
        self.assertTrue(all(record["raw"]["synthetic"] for record in records.values()))
        self.assertTrue(all(record["raw"]["listed"] is False for record in records.values()))

    def test_complete_research_trace_passes_without_directional_answer(self):
        result = replay.grade(self.root, self.write_trace())
        self.assertTrue(result["passed"], result)
        self.assertEqual(result["judgment_count"], 3)
        self.assertFalse(result["directional_answer_graded"])
        self.assertEqual(result["kind"], "synthetic")

    def test_duplicate_only_revision_fails_even_with_new_source_id(self):
        def insert(index, target, records):
            if index == 1:
                first = target.entries("judgment")[0]
                payload = copy.deepcopy(first["payload"])
                payload["as_of"] = "2025-06-03T18:00:00Z"
                payload["discovery"]["evidence_ids"] = [records["duplicate"]["id"]]
                payload["version"] = 2
                payload["previous_judgment_hash"] = first["hash"]
                target.append("judgment", "needless-duplicate", payload)
        check, result = self.result(self.write_trace(transform=insert), "duplicate_does_not_create_revision")
        self.assertFalse(check["passed"])
        self.assertFalse(result["passed"])

    def test_cancellation_cannot_be_hidden_in_unrelated_new_thesis(self):
        def change(index, payload, _):
            if index == 2:
                payload["discovery"]["thesis_id"] = "unrelated-new-thesis"
        check, _ = self.result(self.write_trace(transform=change), "contradiction_updates_existing_thesis")
        self.assertFalse(check["passed"])

    def test_correction_cannot_erase_previous_cancellation(self):
        def change(index, payload, records):
            if index == 3:
                payload["discovery"]["evidence_ids"].remove(records["cancellation"]["id"])
        check, _ = self.result(self.write_trace(transform=change), "correction_keeps_conflicting_evidence")
        self.assertFalse(check["passed"])

    def test_future_publication_and_delayed_receipt_both_fail(self):
        def change(index, payload, records):
            if index == 3:
                payload["discovery"]["evidence_ids"].extend(records[k]["id"] for k in ("future", "delayed"))
                payload["discovery"]["observed_event_ids"].extend(records[k]["event_id"] for k in ("future", "delayed"))
        check, _ = self.result(self.write_trace(transform=change), "future_and_delayed_evidence_never_cited")
        self.assertFalse(check["passed"])
        self.assertEqual(len(check["detail"]), 2)

    def test_plausible_thesis_ids_do_not_replace_hash_linked_history(self):
        def change(index, payload, _):
            if index == 3:
                payload["previous_judgment_hash"] = "0" * 64
        check, _ = self.result(self.write_trace(transform=change), "versioned_history_survives_process_restarts")
        self.assertFalse(check["passed"])

    def test_no_memory_control_does_not_pass_continuity_from_matching_labels(self):
        check, result = self.result(self.write_trace(memory="none"), "versioned_history_survives_process_restarts")
        self.assertFalse(check["passed"])
        self.assertEqual(result["memory"], "none")

    def test_invented_stock_is_not_synthetic_market_performance(self):
        def change(index, payload, _):
            if index == 0:
                payload["discovery"]["entities"][0]["symbol"] = "FAKE"
        check, _ = self.result(self.write_trace(transform=change), "no_invented_security_or_price_forecast")
        self.assertFalse(check["passed"])

    def test_incomplete_process_cannot_report_success_from_present_judgments(self):
        check, result = self.result(self.write_trace(failed_step=2), "03-material-contradiction:completed")
        self.assertFalse(check["passed"])
        self.assertFalse(result["passed"])

    def test_manifest_is_frozen_before_execution(self):
        first = replay.prepare(self.root)
        self.assertEqual(first, replay.prepare(self.root))
        with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
            replay.prepare(self.root, model="changed-after-preview")

    def test_orchestrator_uses_four_separate_processes_and_preserves_failure(self):
        with patch.object(replay.subprocess, "run", return_value=subprocess.CompletedProcess([], 2, "", "controlled failure")) as invoke:
            result = replay.run_replay(self.root, model="test-model", codex_bin="/test/codex", timeout=12)
        self.assertEqual(invoke.call_count, 4)
        commands = [call.args[0] for call in invoke.call_args_list]
        self.assertEqual(len({command[command.index("--ledger") + 1] for command in commands}), 1)
        for command in commands:
            self.assertEqual(command[2], "cycle")
            self.assertEqual(command[command.index("--model") + 1], "test-model")
            self.assertEqual(command[command.index("--codex-bin") + 1], "/test/codex")
            self.assertNotIn("--fetch", command)
        self.assertFalse(result["passed"])
        self.assertTrue((self.root / "grade.json").exists())
        with self.assertRaisesRegex(ValueError, "already started"):
            replay.run_replay(self.root, model="test-model", codex_bin="/test/codex", timeout=12)

    def test_no_memory_orchestration_uses_separate_ledger_per_step(self):
        with patch.object(replay.subprocess, "run", return_value=subprocess.CompletedProcess([], 2, "", "controlled failure")) as invoke:
            replay.run_replay(self.root, memory="none")
        ledgers = {call.args[0][call.args[0].index("--ledger") + 1] for call in invoke.call_args_list}
        self.assertEqual(len(ledgers), 4)


if __name__ == "__main__":
    unittest.main()
