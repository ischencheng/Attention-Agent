"""Trace content remains inert and preserves the whole research/account path."""
from __future__ import annotations

import base64
import hashlib
from html.parser import HTMLParser
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
        self.assertIn("1 Codex model calls", document)
        summary.pop("evaluation_eligible")
        self.assertIn('metric-value">Ineligible', render_trace(summary, []))

    def test_scripted_provenance_is_not_counted_as_model_calls_and_batches_count_once(self):
        records = [{"invocation": {"id": "script-1", "policy": "scripted"}}]
        document = render_trace({}, records)
        self.assertIn("0 Codex model calls", document)
        self.assertIn("Policy provenance (scripted)", document)
        self.assertNotIn("Recorded Codex invocation", document)
        records.extend([{"invocation": {"id": "codex-1", "policy": "codex"}}] * 3)
        document = render_trace({}, records)
        self.assertIn("1 Codex model calls", document)
        self.assertIn("Recorded Codex invocation", document)
        # Supplied call counts include failed invocations with no accepted action.
        self.assertIn("2 Codex model calls", render_trace({"model_invocations": 2}, records))

    def test_ineligible_completed_rollout_is_distinct_from_failed_run(self):
        document = render_trace({"run_status": "completed", "evaluation_eligible": False,
                                 "terminated": True, "complete": True, "reward": 0}, [])
        self.assertIn("Episode is ineligible — excluded from policy evaluation", document)
        self.assertNotIn("Policy run failed", document)


if __name__ == "__main__":
    unittest.main()
