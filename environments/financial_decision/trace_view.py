"""Render public episode transitions as a self-contained, escaped HTML report."""
from __future__ import annotations

import html
import json


def render_trace(records: list[dict], summary: dict) -> str:
    """No scripts, remote resources, raw HTML, or private scenario input."""
    def escape(value):
        return html.escape(str(value), quote=True)

    def formatted(value):
        return escape(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))

    cards = []
    for record in records:
        if record["type"] == "reset":
            initial = record["observation"]
            cards.append('<section><h2>Initial evidence cutoff</h2><p>'
                         + escape(initial["current_time"]) + ' · '
                         + escape(len(initial["available_sources"])) + ' available sources · '
                         + escape(len(initial["questions"])) + ' registered questions</p>'
                         + '<details><summary>Initial public observation</summary><pre>'
                         + formatted(initial) + '</pre></details></section>')
            continue
        before, after = record["observation"], record["next_observation"]
        action = record["action"]
        description = ''
        if record["info"].get("rejection"):
            description = '<p>Action rejected: ' + escape(record["info"]["rejection"]) + '</p>'
        elif action.get("type") == "read":
            source = next((item for item in after["read_observations"]
                           if item["id"] == action.get("observation_id")), None)
            if source:
                description = '<h3>' + escape(source["raw"].get("title", source["source_id"])) + '</h3><p>'
                description += escape(source["raw"].get("text", "")) + '</p>'
        elif action.get("type") == "judge":
            for thesis in action.get("judgment", {}).get("discoveries", []):
                description += '<h3>' + escape(thesis["title"]) + '</h3><p>' + escape(thesis["statement"]) + '</p>'
                description += '<p><strong>' + escape(thesis["action"]) + ' · ' + escape(thesis["status"]) + '</strong> · '
                description += escape(len(thesis["evidence_ids"])) + ' supporting references · '
                description += escape(len(thesis["counterevidence_ids"])) + ' contrary references</p>'
                description += '<p>' + escape(thesis["change_reason"]) + '</p>'
        elif action.get("type") == "forecast":
            question = next((item for item in before["questions"] if item["question_id"] == action.get("question_id")), {})
            description = '<h3>' + escape(question.get("title", action.get("question_id", "Forecast"))) + '</h3>'
            description += '<p>Preregistered probability: <strong>' + escape(action.get("probability")) + '</strong></p>'
            description += '<p>' + escape(action.get("reason", "")) + '</p>'
        else:
            description = '<p>' + escape(action.get("reason", "")) + '</p>'
        old_results = {item["forecast_id"] for item in before["settlements"]}
        for result in after["settlements"]:
            if result["forecast_id"] not in old_results:
                description += '<aside><strong>' + escape(result["question_id"]) + ': ' + escape(result["status"]) + '</strong><p>'
                description += escape(result["reason"]) + '</p>'
                if result["status"] == "resolved":
                    description += '<p>Brier error: ' + escape(round(result["brier"], 6)) + ' · score relative to 0.5 baseline: '
                    description += escape(round(result["score"], 6)) + '</p>'
                description += '</aside>'
        cards.append('<section><h2>Step ' + escape(record["step"]) + ' · '
                     + escape(action.get("type", "invalid action")) + '</h2>'
                     + '<p>' + escape(before["current_time"]) + ' → ' + escape(after["current_time"])
                     + ' · Reward ' + escape(record["reward"]) + '</p>' + description
                     + '<details><summary>Exact action and reward breakdown</summary><pre>'
                     + formatted({"action": action, "reward": record["reward"], "info": record["info"]}) + '</pre></details>'
                     + '<details><summary>Returned evidence, thesis history and visible outcomes</summary><pre>'
                     + formatted(after) + '</pre></details></section>')
    return '''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'">
<title>Financial decision episode trace</title><style>
body{font:16px/1.5 system-ui,sans-serif;max-width:1080px;margin:36px auto;padding:0 24px;background:#f5f7fa;color:#172438}
h1,h2,h3{line-height:1.2}section{background:white;border:1px solid #dce3ec;border-radius:12px;padding:20px;margin:20px 0}
pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf2f7;padding:16px;border-radius:8px;font-size:13px}
.notice{padding:14px;border-left:4px solid #d08d21;background:#fff4da}summary{cursor:pointer;font-weight:600;margin:12px 0}
aside{padding:14px;border-left:4px solid #4e7d9c;background:#edf6fa;margin:18px 0}.metrics{display:flex;flex-wrap:wrap;gap:20px}
</style></head><body><h1>Financial decision episode trace</h1>
<p class="notice">''' + escape(summary.get("evidence_label", "Offline episode; inspect its declared mode.")) + '''
 This trace contains only observations returned during the episode. It shows recorded evidence and actions, not hidden model reasoning.</p>
<section><h2>Episode result</h2><div class="metrics"><span>Status: <strong>''' + escape(summary["status"]) + '''</strong></span>
<span>Steps: <strong>''' + escape(summary["steps"]) + '''</strong></span><span>Resolved: <strong>''' + escape(summary["resolved_forecasts"]) + '''</strong></span>
<span>Unresolved: <strong>''' + escape(summary["unresolved_forecasts"]) + '''</strong></span><span>Reward: <strong>''' + escape(summary["total_reward"]) + '''</strong></span></div>
<details><summary>Complete result and artifact index</summary><pre>''' + formatted(summary) + '</pre></details></section>' + ''.join(cards) + '</body></html>\n'
