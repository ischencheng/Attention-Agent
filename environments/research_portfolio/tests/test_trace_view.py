"""Trace content remains inert and preserves the whole research/account path."""
from __future__ import annotations

import base64
import hashlib
from html.parser import HTMLParser
import json
import unittest

from environments.research_portfolio.trace_view import render_trace


class Elements(HTMLParser):
    def __init__(self, document):
        super().__init__()
        self.elements = []
        self.scripts = []
        self.in_script = False
        self.feed(document)

    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))
        if tag == "script":
            self.in_script = True

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_script = False

    def handle_data(self, data):
        if self.in_script:
            self.scripts.append(data)


class TraceViewTests(unittest.TestCase):
    def test_configured_checkpoints_do_not_imply_role_execution(self):
        summary = {"policy_manifest": {"roles": {"strong": {
            "model_id": "frozen/model", "revision": "immutable-revision"}}}}
        document = render_trace(summary, [])
        self.assertIn("strong: frozen/model @ immutable-revision", document)
        self.assertIn("Models recorded on transitions</dt><dd>See invocation artifacts", document)
        self.assertIn("0 model calls", document)

    def test_untrusted_source_content_cannot_add_markup_links_or_script(self):
        payload = '</script><script>alert("source")</script><img src=x onerror=alert(1)><a href="javascript:alert(2)">x</a>'
        record = {"step": 1, "role": payload, "action": {"type": payload, "source_id": payload},
                  "result": {"reason": payload}, "observation": {"originals": [{"body": payload}]}}
        document = render_trace({"episode_id": payload, "mode": "synthetic", "provenance": payload}, [record])
        parsed = Elements(document)
        self.assertEqual(sum(tag == "script" for tag, _ in parsed.elements), 1)
        self.assertFalse(any(tag in {"img", "a", "iframe", "object"} for tag, _ in parsed.elements))
        self.assertFalse(any(key.startswith("on") for _, attrs in parsed.elements for key in attrs))
        self.assertNotIn(payload, document)
        self.assertIn('&lt;/script&gt;', document)
        script = ''.join(parsed.scripts)
        digest = base64.b64encode(hashlib.sha256(script.encode()).digest()).decode()
        csp = next(attrs["content"] for tag, attrs in parsed.elements if tag == "meta" and attrs.get("http-equiv") == "Content-Security-Policy")
        self.assertIn("'sha256-" + digest + "'", csp)
        self.assertIn("default-src 'none'", csp)
        self.assertNotIn(payload, script)

    def test_pipeline_filters_and_identifier_navigation_preserve_actions(self):
        records = [
            {"step": 1, "role": "Fast", "action": {"type": "triage", "source_id": "source-1"},
             "result": {"ticket": {"ticket_id": "ticket-1"}}},
            {"step": 2, "role": "Mid", "action": {"type": "accumulate", "ticket_ids": ["ticket-1"]}},
            {"step": 3, "role": "Strong", "action": {"type": "open_research", "question_id": "question-1"}},
            {"step": 4, "role": "Strong", "action": {"type": "tool", "tool": "read_original", "question_id": "question-1"}},
            {"step": 5, "role": "Strong", "action": {"type": "tool", "tool": "discover_assets", "question_id": "question-1"}},
            {"step": 6, "role": "Strong", "action": {"type": "judge", "thesis_id": "thesis-1"}},
            {"step": 7, "role": "Strong", "action": {"type": "allocate", "thesis_id": "thesis-1"}},
            {"step": 8, "action": {"type": "wait"}, "result": {"events": [{"type": "fill", "order_id": "order-1"}]}, "terminated": True},
        ]
        document = render_trace({"mode": "synthetic", "terminated": True, "reward": .01}, records)
        parsed = Elements(document)
        events = [attrs for tag, attrs in parsed.elements if tag == "article"]
        self.assertEqual(len(events), len(records))
        self.assertEqual(events[0]["data-role"], "Fast")
        self.assertEqual(events[4]["data-stages"], "Target")
        self.assertEqual(events[7]["data-stages"], "Fill,Return")
        references = [attrs["data-reference"] for tag, attrs in parsed.elements if tag == "button" and "data-reference" in attrs]
        self.assertIn("question-1", references)
        self.assertIn("order-1", references)
        self.assertIn('"source_id": "source-1"', events[0]["data-search"])
        self.assertIn("Synthetic replay — engineering validation, not investment performance", document)
        self.assertIn("+1.00%", document)

    def test_account_chart_uses_recorded_nav_and_ignores_nonfinite_values(self):
        records = [{"step": i, "as_of": f"cutoff-{i}", "observation": {"account": {"nav": nav}}}
                   for i, nav in enumerate([10000, 9990, float("nan"), 10100])]
        document = render_trace({"account": {"cash": 5100, "nav": 10100, "positions": {"ABC": {"quantity": 50, "mark": 100, "market_value": 5000}}}}, records)
        parsed = Elements(document)
        self.assertEqual(sum(tag == "circle" for tag, _ in parsed.elements), 3)
        self.assertIn("3 observed account snapshots", document)
        self.assertIn("10,100.00", document)
        self.assertIn("ABC", document)
        for tag, attrs in parsed.elements:
            if tag in {"circle", "polyline"}:
                self.assertFalse(any("nan" in str(value).lower() for value in attrs.values()))

    def test_cumulative_ledger_fill_is_attributed_to_first_observation_only(self):
        fill = {"event_id": "event-1", "type": "fill", "order_id": "order-7", "asset_id": "asset-1"}
        records = [
            {"step": 1, "action": {"type": "wait"}, "observation": {"account": {"ledger": [fill]}}},
            {"step": 2, "action": {"type": "judge"}, "observation": {"account": {"ledger": [fill]}}},
        ]
        parsed = Elements(render_trace({}, records))
        events = [attrs for tag, attrs in parsed.elements if tag == "article"]
        self.assertIn("Fill", events[0]["data-stages"])
        self.assertNotIn("Fill", events[1]["data-stages"])
        self.assertIn("order-7", events[0]["data-search"])
        self.assertNotIn("order-7", events[1]["data-search"])

    def test_nav_timestamps_follow_resulting_account_snapshot(self):
        records = [{"as_of": "before-market-step", "action": {"type": "wait"},
                    "observation": {"as_of": "after-market-step", "account": {"nav": 10100}}}]
        document = render_trace({}, records)
        self.assertIn("before-market-step → after-market-step", document)
        self.assertIn("<title>after-market-step: 10,100.00</title>", document)
        self.assertNotIn("<title>before-market-step: 10,100.00</title>", document)

    def test_empty_trace_is_explicit_and_preserves_absent_account(self):
        document = render_trace({}, [])
        self.assertIn("No transitions have been recorded yet", document)
        self.assertIn("No account snapshots have been recorded yet", document)
        self.assertIn("No positions are recorded", document)
        self.assertIn("Policy provenance not supplied", document)
        self.assertIn("Pending", document)

    def test_unfinished_episode_does_not_present_zero_as_final_reward(self):
        document = render_trace({"reward": 0, "terminated": False}, [])
        self.assertIn('metric-value">Pending', document)
        self.assertNotIn('metric-value">+0.00%', document)
        document = render_trace({"reward": 0, "terminated": True, "complete": False}, [])
        self.assertIn('metric-value">Not scored', document)

    def test_failed_run_preserves_accounting_but_does_not_display_successful_reward(self):
        summary = {"run_status": "failed", "evaluation_eligible": False, "error": "Policy transport failed",
                   "terminated": True, "complete": True, "reward": .02,
                   "account": {"nav": 10200, "cash": 10200, "net_return": .02}, "model_invocations": 1}
        document = render_trace(summary, [])
        self.assertIn('metric-value">Run failed', document)
        self.assertIn('metric-value">Ineligible', document)
        self.assertNotIn('metric-value">+2.00%', document)
        self.assertIn("Accounting return +2.00%", document)
        self.assertIn("10,200.00", document)
        self.assertIn("Policy run failed — excluded from policy evaluation", document)
        self.assertIn("Policy transport failed", document)
        self.assertIn("1 model call", document)
        summary.pop("evaluation_eligible")
        self.assertIn('metric-value">Ineligible', render_trace(summary, []))

    def test_scripted_provenance_is_not_counted_as_model_calls_and_batches_count_once(self):
        records = [{"invocation": {"id": "script-1", "policy": "scripted"}}]
        document = render_trace({}, records)
        self.assertIn("0 model calls", document)
        self.assertIn("Policy provenance (scripted)", document)
        self.assertNotIn("Recorded Codex invocation", document)
        records.extend([{"invocation": {"id": "codex-1", "policy": "codex"}}] * 3)
        document = render_trace({}, records)
        self.assertIn("1 Codex model call", document)
        self.assertIn("Recorded Codex invocation", document)
        # Supplied call counts include failed invocations with no accepted action.
        self.assertIn("2 Codex model calls", render_trace({"model_invocations": 2}, records))

    def test_non_codex_model_calls_preserve_their_policy_identity(self):
        records = [{"invocation": {"id": "local-1", "policy": "local", "model": "frozen-model-revision"}}] * 2
        document = render_trace({"policy": "local", "training_performed": False}, records)
        self.assertIn("1 model call", document)
        self.assertIn("Recorded model invocation", document)
        self.assertIn("frozen-model-revision", document)
        self.assertNotIn("Codex model calls", document)

    def test_ineligible_completed_rollout_is_distinct_from_failed_run(self):
        document = render_trace({"run_status": "completed", "evaluation_eligible": False,
                                 "terminated": True, "complete": True, "reward": 0}, [])
        self.assertIn("Episode is ineligible — excluded from policy evaluation", document)
        self.assertNotIn("Policy run failed", document)

    def test_legacy_budget_exhausted_run_keeps_settlement_separate_from_policy_outcome(self):
        # Shape of the actual v2 replay saved before policy_outcome was added.
        summary = {"run_status": "completed", "evaluation_eligible": True,
                   "policy_budget_exhausted": True, "rejected_actions": 96,
                   "model_invocations": 96, "terminated": True, "complete": True, "reward": 0,
                   "counts": dict.fromkeys(("read_sources", "tickets", "themes", "questions", "theses",
                                             "thesis_revisions", "tool_calls"), 0)}
        document = render_trace(summary, [])
        self.assertIn('data-policy-outcome="rejected_actions_only"', document)
        self.assertIn('<h2 id="policy-outcome-title">Model budget exhausted</h2>', document)
        self.assertIn('outcome-label">Rejected actions only</span>', document)
        self.assertIn("0 originals read · 0 tickets · 0 questions · 0 theses · 96 actions rejected", document)
        self.assertIn('metric-label">Harness &amp; settlement</span><strong class="metric-value">Settled', document)
        self.assertIn('metric-value">+0.00%', document)
        self.assertIn("accounting valid; research unfinished", document)
        self.assertNotIn('metric-value">Ineligible', document)
        self.assertNotIn("Policy execution finished", document)

    def test_policy_outcome_precedence_and_valid_no_trade_finish(self):
        base = {"run_status": "completed", "terminated": True, "complete": True,
                "reward": 0, "evaluation_eligible": True}
        document = render_trace({**base, "policy_outcome": "failed", "run_status": "failed",
                                 "policy_budget_exhausted": True}, [])
        self.assertIn('data-policy-outcome="failed"', document)
        self.assertIn('<h2 id="policy-outcome-title">Policy run failed</h2>', document)
        document = render_trace({**base, "policy_outcome": "budget_exhausted",
                                 "counts": {"read_sources": 4, "tickets": 2, "questions": 1, "theses": 0}}, [])
        self.assertIn('data-policy-outcome="budget_exhausted"', document)
        self.assertIn("4 originals read · 2 tickets · 1 questions · 0 theses", document)
        document = render_trace({**base, "policy_outcome": "finished", "rejected_actions": 0,
                                 "counts": dict.fromkeys(("read_sources", "tickets", "themes", "questions",
                                                           "theses", "thesis_revisions", "tool_calls"), 0)}, [])
        self.assertIn('data-policy-outcome="finished"', document)
        self.assertIn('metric-value">+0.00%', document)
        self.assertNotIn("research unfinished", document)

    def test_budget_fallback_uses_explicit_stop_and_outcome_is_frozen_outside_playback(self):
        records = [{"action": {"type": "wait"}, "observation": {"as_of": "earlier"}},
                   {"action": {"type": "stop", "reason": "Model invocation budget exhausted."},
                    "observation": {"as_of": "terminal"}}]
        document = render_trace({"run_status": "completed", "terminated": True, "reward": 0}, records)
        self.assertIn('data-policy-outcome="budget_exhausted"', document)
        self.assertIn("Whole-episode agent outcome · frozen during playback", document)
        self.assertLess(document.index('data-scope="full-episode"'), document.index('id="replay-title"'))
        parsed = Elements(document)
        self.assertNotIn('policy-outcome', ''.join(parsed.scripts))
        for tag, attrs in parsed.elements:
            if tag == "article":
                self.assertNotIn("policy_outcome", json.loads(attrs["data-snapshot"]))
        records[1]["action"] = {"type": "tool", "reason": "Model invocation budget exhausted."}
        document = render_trace({"run_status": "completed"}, records)
        self.assertNotIn('<h2 id="policy-outcome-title">Model budget exhausted</h2>', document)

    def test_replay_snapshots_follow_each_public_observation(self):
        early = {"id": "early", "title": "Initial filing", "source": "Issuer", "published_at": "day-1", "available_at": "day-2"}
        late = {"id": "late", "title": "Later correction", "source": "Regulator", "published_at": "day-3", "available_at": "day-4"}
        records = [
            {"observation": {"as_of": "day-2", "feed": [early], "read_sources": [early],
                             "account": {"nav": 10000, "cash": 10000, "positions": {}, "net_return": 0}}},
            {"observation": {"as_of": "day-4", "feed": [early, late], "read_sources": [early, late],
                             "questions": [{"id": "q1"}], "theses": {"t1": {}},
                             "account": {"nav": 9900, "cash": 5000, "positions": {"ABC": {"quantity": 50, "mark_price": 98, "market_value": 4900}}, "net_return": -.01}}},
        ]
        document = render_trace({"terminated": True, "reward": -.01, "account": {"nav": 9900}}, records)
        parsed = Elements(document)
        events = [attrs for tag, attrs in parsed.elements if tag == "article"]
        first, second = [json.loads(attrs["data-snapshot"]) for attrs in events]
        self.assertEqual(first["time"], "day-2")
        self.assertEqual(first["nav"], "10,000.00")
        self.assertEqual([source["id"] for source in first["sources"]], ["early"])
        self.assertEqual(first["positions"], [])
        self.assertEqual(second["positions"], [["ABC", "50", "98.00", "4,900.00"]])
        self.assertEqual(second["question_count"], 1)
        self.assertEqual(second["thesis_count"], 1)
        self.assertEqual([attrs["data-index"] for attrs in events], ["0", "1"])
        chart_points = [attrs for tag, attrs in parsed.elements if tag == "circle"]
        self.assertEqual([attrs["data-index"] for attrs in chart_points], ["0", "1"])
        self.assertIn("Frozen full-run result · independent of the replay position", document)
        self.assertIn("The full-run result above stays frozen", document)

    def test_source_metadata_and_identifier_navigation_are_inert(self):
        source = {"id": 'source-1" onclick="bad()', "title": '<img src=x onerror="bad()">', "source": "Primary source",
                  "published_at": "2026-10-01T12:00:00Z", "available_at": "2026-10-02T12:00:00Z"}
        parsed = Elements(render_trace({}, [{"observation": {"feed": [source], "read_sources": [source]}}]))
        source_buttons = [attrs for tag, attrs in parsed.elements if tag == "button" and attrs.get("class") == "source-title"]
        self.assertEqual(source_buttons[0]["data-reference"], source["id"])
        self.assertFalse(any(tag == "img" or any(key.startswith("on") for key in attrs) for tag, attrs in parsed.elements))
        snapshot = json.loads(next(attrs["data-snapshot"] for tag, attrs in parsed.elements if tag == "article"))
        self.assertEqual(snapshot["sources"][0]["published_at"], source["published_at"])
        self.assertEqual(snapshot["sources"][0]["available_at"], source["available_at"])
        self.assertTrue(snapshot["sources"][0]["read"])

    def test_historical_mode_does_not_imply_verified_cutoff_or_training(self):
        document = render_trace({"mode": "historical_replay", "training_performed": False,
                                 "replay_integrity": {"model_cutoff_verified": "true", "source_hashes_verified": False}}, [])
        self.assertIn("<dt>Cutoff verification</dt><dd>Not supplied</dd>", document)
        self.assertIn("<dt>Source hashes</dt><dd>Not verified</dd>", document)
        self.assertIn("No training performed", document)
        self.assertNotIn("<dd>Verified</dd>", document)

    def test_action_descriptions_use_actual_wrapped_results_and_rejections(self):
        document = render_trace({}, [
            {"action": {"type": "tool", "tool": "read"}, "result": {"result": {"tool_call_id": "tool-1", "result": {"title": "Issuer delivery update"}}}},
            {"action": {"type": "allocate"}, "result": {"rejection": "Evidence gate failed", "result": {"reason": "Evidence gate failed"}}},
        ])
        self.assertIn('<p class="description">Issuer delivery update</p>', document)
        self.assertIn('<p class="description error">Evidence gate failed</p>', document)

    def test_archive_admission_limits_remain_visible_beside_verified_receipts(self):
        document = render_trace({"mode": "historical_replay", "replay_integrity": {
            "status": "admitted_with_limits", "benchmark_eligible": True,
            "benchmark_scope": "Retrospective market-price reconstruction; captured source arrivals",
            "strict_point_in_time_eligible": False, "model_boundary_verified": True,
            "model_cutoff_verified": False, "training_cutoff": {"fast": None, "mid": None, "strong": None},
            "model_files_verified": False, "source_receipts_verified": True,
            "source_hashes_verified": True, "price_provenance_verified": True,
            "price_vintage": "historical_reconstruction", "episode_start": "2026-01-01T00:00:00Z",
            "episode_end": "2026-02-01T00:00:00Z", "reasons": ["Original historical price vintages were not captured."],
        }}, [])
        self.assertIn("Admitted with limits", document)
        self.assertIn("fast: Unknown · mid: Unknown · strong: Unknown", document)
        self.assertIn("<dt>Model release boundary</dt><dd>Verified</dd>", document)
        self.assertIn("<dt>Model files</dt><dd>Not verified</dd>", document)
        self.assertIn("<dt>Strict point-in-time eligibility</dt><dd>Not certified</dd>", document)
        self.assertIn("Original historical price vintages were not captured.</li>", document)


if __name__ == "__main__":
    unittest.main()
