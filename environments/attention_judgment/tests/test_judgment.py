"""Deterministic lifecycle checks; no network, market API, or model calls."""
from __future__ import annotations

from contextlib import redirect_stdout
import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from ledger import Ledger, timestamp
import market
import run as runner
from sources import observation


AS_OF = datetime(2025, 6, 3, 18, tzinfo=timezone.utc)
DATES = {"entry": "2025-06-04", "day": "2025-06-04", "week": "2025-06-06", "month": "2025-06-30"}


def observed(source_id="release-1", **overrides):
    params = {
        "source": "fictional-official-feed", "source_id": source_id,
        "published_at": "2025-06-02T14:00:00+00:00",
        "available_at": "2025-06-02T14:05:00+00:00",
        "raw": {"title": "Fictional datacenter supply update", "capacity": 20},
    }
    params.update(overrides)
    return observation(**params)


def discovery(evidence, *, counter=(), symbol=None, forecasts=(), thesis_id="capacity-thesis"):
    refs = list(evidence) + list(counter)
    return {
        "thesis_id": thesis_id, "title": "Capacity bottleneck needs research",
        "topics": ["datacenter", "power"],
        "entities": [{"name": "Fictional supplier", "symbol": symbol}],
        "statement": "The supply bottleneck may persist beyond the current quarter.",
        "thesis_horizon": "12 months", "uncertainty": "Demand could weaken.",
        "next_questions": ["Is the announced capacity actually commissioned?"],
        "status": "unknown", "action": "research",
        "evidence_ids": [o["id"] for o in evidence],
        "counterevidence_ids": [o["id"] for o in counter],
        "observed_event_ids": sorted({o["event_id"] for o in refs}),
        "framework_dimensions": ["cycle", "moat"],
        "change_reason": "A newly available operating disclosure merits investigation.",
        "invalidation": "Sustained independently verified excess capacity.",
        "next_review_at": "2099-06-04T18:00:00+00:00",
        "forecasts": list(forecasts),
    }


def batch(item=None):
    return {"discoveries": [item] if item else [], "no_action_reason": "" if item else "No material new evidence."}


class LedgerLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "ledger.sqlite3"
        self.ledger = Ledger(self.path)

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def freeze(self, final, observations, *, previous=None, run_id="run-one", mode="forward"):
        with patch.object(market, "horizon_dates", return_value=DATES):
            return runner.freeze(
                self.ledger, final, observations, previous or {}, AS_OF, mode,
                run_id, {"requested_model": "deterministic-test-double"},
            )

    def test_idempotent_append_survives_connection_reopen(self):
        original = {"raw": {"文字": [1, False, None]}}
        value = self.ledger.append("observation", "one", original)
        self.assertEqual(self.ledger.append("observation", "one", copy.deepcopy(original)), value)
        with self.assertRaises(ValueError):
            self.ledger.append("observation", "one", {"raw": "different"})
        self.ledger.close()
        self.ledger = Ledger(self.path)
        self.assertEqual(len(self.ledger.entries()), 1)
        self.assertEqual(self.ledger.verify(), value)

    def test_normal_sql_cannot_rewrite_frozen_entries(self):
        self.ledger.append("judgment", "one", {"statement": "original"})
        for sql in ("UPDATE entries SET payload='{}'", "DELETE FROM entries"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.ledger.db.execute(sql)
            self.ledger.db.rollback()
        self.assertEqual(self.ledger.entries()[0]["payload"]["statement"], "original")

    def test_freeze_failure_does_not_consume_observations_or_partial_judgments(self):
        one, two = observed(), observed("release-2")
        final = {"discoveries": [discovery([one]), discovery([two], thesis_id="second-thesis")], "no_action_reason": ""}
        original_append = self.ledger.append
        judgment_calls = 0

        def fail_second_judgment(kind, key, payload):
            nonlocal judgment_calls
            if kind == "judgment":
                judgment_calls += 1
                if judgment_calls == 2:
                    raise RuntimeError("simulated process failure at publication boundary")
            return original_append(kind, key, payload)

        with patch.object(self.ledger, "append", side_effect=fail_second_judgment):
            with self.assertRaises(RuntimeError):
                self.freeze(final, [one, two])
        self.assertEqual(self.ledger.entries(), [], "failed cycle must be wholly retryable")
        self.freeze(final, [one, two])
        self.assertEqual(len(self.ledger.entries("judgment")), 2)
        self.assertEqual(len(self.ledger.entries("cycle")), 1)

    def test_complete_cycle_retry_is_identical_after_reopen_and_clock_change(self):
        item = observed()
        final = batch(discovery([item]))
        with patch.object(runner, "utcnow", return_value="2025-06-03T18:01:00+00:00"):
            first = self.freeze(final, [item])
        original_entries = self.ledger.entries()
        self.ledger.close()
        self.ledger = Ledger(self.path)
        with patch.object(runner, "utcnow", return_value="2025-06-05T18:01:00+00:00"):
            second = self.freeze(final, [item])
        self.assertEqual(first, second)
        self.assertEqual(self.ledger.entries(), original_entries)

    def test_same_cycle_key_cannot_rewrite_a_prediction(self):
        item = observed()
        final = batch(discovery([item], symbol="ABC", forecasts=[{
            "symbol": "ABC", "horizon": "week", "p_outperform_spy": 0.6,
            "reason": "Fictional forecast for testing.",
        }]))
        self.freeze(final, [item])
        before = self.ledger.verify()
        changed = copy.deepcopy(final)
        changed["discoveries"][0]["forecasts"][0]["p_outperform_spy"] = 0.9
        with self.assertRaises(ValueError):
            self.freeze(changed, [item])
        self.assertEqual(self.ledger.verify(), before)

    def test_replay_judgments_do_not_become_forward_memory(self):
        item = observed()
        self.freeze(batch(discovery([item])), [item], mode="replay")
        self.assertEqual(runner.latest_theses(self.ledger), {})

    def test_nonfinite_prediction_cannot_enter_ledger(self):
        item = observed()
        final = batch(discovery([item], symbol="ABC", forecasts=[{
            "symbol": "ABC", "horizon": "day", "p_outperform_spy": float("nan"),
            "reason": "Invalid numeric payload.",
        }]))
        with self.assertRaises(ValueError):
            self.freeze(final, [item])
        self.assertEqual(self.ledger.entries(), [])


class JudgmentValidationTests(unittest.TestCase):
    def validate(self, final, observations, previous=None):
        return runner.validate_final(final, observations, previous or {}, AS_OF)

    def test_event_time_and_availability_both_constrain_evidence(self):
        for fields in (
            {"published_at": "2025-06-04T14:00:00+00:00"},
            {"available_at": "2025-06-04T14:00:00+00:00"},
        ):
            with self.subTest(fields=fields):
                future = observed(**fields)
                with self.assertRaises(ValueError):
                    self.validate(batch(discovery([future])), [future])

    def test_naive_timestamp_is_rejected(self):
        with self.assertRaises(ValueError):
            timestamp("2025-06-03T18:00:00")

    def test_no_ticker_and_no_forecast_is_a_valid_research_outcome(self):
        item = observed()
        result = self.validate(batch(discovery([item])), [item])
        self.assertIsNone(result["discoveries"][0]["entities"][0]["symbol"])
        self.assertEqual(result["discoveries"][0]["forecasts"], [])

    def test_empty_discovery_requires_honest_reason(self):
        self.validate(batch(), [])
        with self.assertRaises(ValueError):
            self.validate({"discoveries": [], "no_action_reason": " "}, [])

    def test_source_identity_and_content_revision_are_distinct(self):
        one = observed()
        repeated = observed(available_at="2025-06-03T10:00:00+00:00")
        revised = observed(raw={"title": "Correction", "capacity": 10})
        other_source = observed(source="second-official-feed")
        self.assertEqual(one["id"], repeated["id"])
        self.assertNotEqual(one["id"], revised["id"])
        self.assertEqual(one["event_id"], revised["event_id"])
        self.assertNotEqual(one["id"], other_source["id"])

    def test_event_identity_grouping_does_not_count_versions_twice(self):
        one = observed()
        revised = observed(raw={"capacity": 10})
        final = batch(discovery([one, revised]))
        self.validate(final, [one, revised])
        final["discoveries"][0]["observed_event_ids"].append(one["event_id"])
        with self.assertRaises(ValueError):
            self.validate(final, [one, revised])

    def test_one_document_can_supply_both_support_and_counterevidence(self):
        release = observed(raw={
            "title": "Official customer commitment and operating conditions",
            "commitment": "Two-year minimum purchase commitment covers 70% of planned capacity.",
            "conditions": "Acceptance remains pending; contract price and unit costs are undisclosed.",
        })
        final = batch(discovery([release], counter=[release]))
        final["discoveries"][0]["statement"] = (
            "A customer commitment supports demand visibility, while pending acceptance "
            "and undisclosed economics prevent a conclusion about durable profitability."
        )
        result = self.validate(final, [release])
        thesis = result["discoveries"][0]
        self.assertEqual(thesis["evidence_ids"], [release["id"]])
        self.assertEqual(thesis["counterevidence_ids"], [release["id"]])
        self.assertEqual(thesis["observed_event_ids"], [release["event_id"]])

        # Different argumentative roles do not create two observed events.
        thesis["observed_event_ids"].append(release["event_id"])
        with self.assertRaises(ValueError):
            self.validate(final, [release])

    def test_duplicate_revision_without_new_evidence_is_rejected(self):
        item = observed()
        original = discovery([item])
        with self.assertRaises(ValueError):
            self.validate(batch(copy.deepcopy(original)), [item], {
                original["thesis_id"]: {"discovery": original},
            })

    def test_material_counterevidence_survives_a_new_revision(self):
        support = observed()
        counter = observed("counter", raw={"title": "Official demand reduction"})
        update = observed("update", raw={"title": "Another capacity announcement"})
        prior = discovery([support], counter=[counter])
        previous = {prior["thesis_id"]: {"discovery": prior}}
        with self.assertRaises(ValueError):
            self.validate(batch(discovery([support, update])), [support, counter, update], previous)
        self.validate(batch(discovery([support, update], counter=[counter])), [support, counter, update], previous)

    def test_forecast_cannot_choose_a_symbol_not_discovered(self):
        item = observed()
        final = batch(discovery([item], symbol="ABC", forecasts=[{
            "symbol": "UNRELATED", "horizon": "day", "p_outperform_spy": 0.7,
            "reason": "Must not choose a convenient later winner.",
        }]))
        with self.assertRaises(ValueError):
            self.validate(final, [item])


class CycleAndSettlementTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.ledger = Ledger(self.root / "ledger.sqlite3")

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def freeze_symbol(self, *, forecast=False, mode="forward"):
        item = observed()
        predictions = [{"symbol": "ABC", "horizon": "week", "p_outperform_spy": 0.75,
                        "reason": "Fictional test prediction."}] if forecast else []
        with patch.object(market, "horizon_dates", return_value=DATES), \
                patch.object(runner, "utcnow", return_value="2025-06-03T18:01:00+00:00"):
            runner.freeze(self.ledger, batch(discovery([item], symbol="ABC", forecasts=predictions)),
                          [item], {}, AS_OF, mode, "settlement-input", {"model": "test-double"})

    def settle_with(self, outcome):
        def snapshot(symbol, start, end, as_of):
            return {"symbol": symbol, "start": start, "end": end, "as_of": as_of.isoformat(), "bars": []}

        with patch.object(market, "fetch_prices", side_effect=snapshot), \
                patch.object(market, "evaluate_horizon", return_value=outcome), redirect_stdout(io.StringIO()):
            runner.settle(SimpleNamespace(), self.ledger)

    def test_reobserved_original_reuses_all_original_metadata(self):
        import codex_runner
        original = observed()
        self.ledger.append("observation", original["id"], original)
        # Identical raw/source identity, but feed metadata has changed. The
        # archived original must remain the source cited by the existing ID.
        repeated = observed(published_at="2099-01-01T00:00:00+00:00", event_id="new-feed-metadata")
        fresh = observed("new-evidence")
        input_path = self.root / "observations.jsonl"
        input_path.write_text("\n".join(json.dumps(o) for o in (repeated, fresh)))
        args = SimpleNamespace(input=input_path, fetch=False, as_of=None,
                               run_root=self.root / "attempts", timeout=10, model=None, codex_bin="unused-test-double")
        output = {"final": batch(discovery([original, fresh])), "events": [], "usage": {}}
        with patch.object(codex_runner, "run", return_value=output) as execute, \
                patch.object(market, "horizon_dates", return_value=DATES), redirect_stdout(io.StringIO()):
            runner.cycle(args, self.ledger)
        self.assertEqual(execute.call_count, 1)
        stored = {e["key"]: e["payload"] for e in self.ledger.entries("observation")}
        self.assertEqual(stored[original["id"]], original)
        self.assertEqual(len(self.ledger.entries("judgment")), 1)

    def test_bounded_context_preserves_backlog_and_retry_consumption(self):
        import codex_runner
        items = [observed(f"item-{i:03}", available_at=f"2025-06-03T18:{i:02}:00+00:00") for i in range(45)]
        input_path = self.root / "bulk.jsonl"
        input_path.write_text("\n".join(json.dumps(o) for o in items))
        args = SimpleNamespace(input=input_path, fetch=False, as_of=None,
                               run_root=self.root / "attempts", timeout=10, model=None, codex_bin="unused")
        output = {"final": {"discoveries": [], "no_action_reason": "No material discovery."}, "events": [], "usage": {}}
        with patch.object(codex_runner, "run", return_value=output), redirect_stdout(io.StringIO()):
            runner.cycle(args, self.ledger)
            self.assertEqual(len(self.ledger.entries("observation")), 45)
            self.assertEqual(len(self.ledger.entries("cycle")[0]["payload"]["reviewed_observation_ids"]), 40)
            runner.cycle(args, self.ledger)
        cycles = self.ledger.entries("cycle")
        self.assertEqual(len(cycles), 2)
        reviewed = [r for c in cycles for r in c["payload"]["reviewed_observation_ids"]]
        self.assertEqual(len(reviewed), 45)
        self.assertEqual(len(set(reviewed)), 45)

    def test_unknown_tool_event_cannot_escape_frozen_input_protocol(self):
        import codex_runner
        item = observed()
        path = self.root / "input.jsonl"
        path.write_text(json.dumps(item))
        args = SimpleNamespace(input=path, fetch=False, as_of=None, run_root=self.root / "attempts",
                               timeout=10, model=None, codex_bin="unused")
        output = {"final": batch(discovery([item])), "usage": {},
                  "events": [{"type": "item.completed", "item": {"type": "future_new_browser_tool"}}]}
        with patch.object(codex_runner, "run", return_value=output), self.assertRaisesRegex(ValueError, "outside the frozen"):
            runner.cycle(args, self.ledger)
        self.assertEqual(len(self.ledger.entries("observation")), 1)
        self.assertEqual(self.ledger.entries("cycle"), [])
        self.assertEqual(self.ledger.entries("judgment"), [])

    def test_passive_price_observations_are_not_prediction_success(self):
        self.freeze_symbol(forecast=False)
        self.settle_with({"status": "resolved", "excess_return": 0.2})
        summary = runner.report(self.ledger)
        self.assertEqual(summary["resolved_price_observations"], 3)
        self.assertEqual(summary["resolved_predeclared_forward_forecasts"], 0)
        self.assertIsNone(summary["mean_brier"])
        self.assertTrue(all(e["payload"]["forecast_score"] is None for e in self.ledger.entries("settlement")))

    def test_missing_prices_are_unresolved_and_can_be_retried(self):
        self.freeze_symbol(forecast=True)
        self.settle_with({"status": "unresolved", "reason": "missing-bars"})
        self.assertEqual(self.ledger.entries("settlement"), [])
        self.assertIsNone(runner.report(self.ledger)["mean_brier"])
        self.settle_with({"status": "resolved", "excess_return": 0.1})
        self.assertEqual(len(self.ledger.entries("settlement")), 3)
        summary = runner.report(self.ledger)
        self.assertEqual(summary["resolved_predeclared_forward_forecasts"], 1)
        self.assertAlmostEqual(summary["mean_brier"], 0.0625)

    def test_settled_horizons_are_not_recounted_on_restart(self):
        self.freeze_symbol(forecast=True)
        self.settle_with({"status": "resolved", "excess_return": -0.1})
        before = self.ledger.entries("settlement")
        self.ledger.close()
        self.ledger = Ledger(self.root / "ledger.sqlite3")
        self.settle_with({"status": "resolved", "excess_return": 0.9})
        self.assertEqual(self.ledger.entries("settlement"), before)
        self.assertAlmostEqual(runner.report(self.ledger)["mean_brier"], 0.5625)

    def test_replay_results_are_not_claimed_as_forward_predictive_performance(self):
        self.freeze_symbol(forecast=True, mode="replay")
        self.settle_with({"status": "resolved", "excess_return": 0.1})
        summary = runner.report(self.ledger)
        self.assertEqual(summary["resolved_price_observations"], 3)
        self.assertEqual(summary["resolved_forward_price_observations"], 0)
        self.assertEqual(summary["resolved_predeclared_forward_forecasts"], 0)
        self.assertIsNone(summary["mean_brier"])


if __name__ == "__main__":
    unittest.main()
