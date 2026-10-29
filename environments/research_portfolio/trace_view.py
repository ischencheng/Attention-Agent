"""Self-contained trace of public research actions and simulated account events.

The renderer never reads a scenario, source file, or network endpoint. Every
displayed fact must already be present in the supplied public trajectory.
"""
from __future__ import annotations

import base64
import hashlib
import html
import json
import math


STAGES = ("Source", "Question", "Evidence", "Target", "Judgment", "Order", "Fill", "Return")


def _escape(value):
    return html.escape(str(value), quote=True)


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _json(value):
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
        return float(value)
    return None


def _money(value):
    number = _number(value)
    return f"{number:,.2f}" if number is not None else "Not available"


def _percent(value):
    number = _number(value)
    return f"{number:+.2%}" if number is not None else "Not scored"


def _details(label, value, *, opened=False):
    return ('<details' + (' open' if opened else '') + '><summary>' + _escape(label)
            + '</summary><pre>' + _escape(_json(value)) + '</pre></details>')


def _observation(record):
    return _mapping(record.get("next_observation", record.get("observation", {})))


def _action_name(record):
    action = _mapping(record.get("action"))
    return str(action.get("type", action.get("action", record.get("type", "observation"))))


def _role(record):
    explicit = record.get("role", _mapping(record.get("action")).get("role"))
    if explicit:
        return str(explicit)
    name = _action_name(record)
    if name == "triage":
        return "Fast"
    if name == "accumulate":
        return "Mid"
    if name in {"open_research", "tool", "judge", "allocate", "cancel"}:
        return "Strong"
    return "Environment"


def _as_of(record):
    observation = _observation(record)
    return record.get("as_of", observation.get("as_of", observation.get("current_time", "Time unavailable")))


def _snapshot_time(record):
    observation = _observation(record)
    return observation.get("as_of", observation.get("current_time", _as_of(record)))


def _model_invocation(value):
    invocation = _mapping(value)
    return invocation.get("policy") == "codex" or (
        invocation.get("policy") != "scripted" and isinstance(invocation.get("model"), str) and bool(invocation["model"]))


def _stages(record):
    name = _action_name(record)
    result = _mapping(record.get("result", record.get("info", {})))
    action = _mapping(record.get("action"))
    found = set()
    if name in {"reset", "observation", "triage", "accumulate", "read"}:
        found.add("Source")
    if name in {"accumulate", "open_research"}:
        found.add("Question")
    if name == "tool":
        tool = str(action.get("tool", ""))
        found.add("Target" if "discover" in tool or "asset" in tool else "Evidence")
    if name in {"judge", "judgment"}:
        found.add("Judgment")
    if name in {"allocate", "cancel"}:
        found.add("Order")
    if name in {"wait", "stop", "advance", "settle"} or record.get("terminated") or record.get("truncated"):
        found.add("Return")
    # Inspect event names, not arbitrary source prose, to classify broker output.
    def broker_events(value):
        if isinstance(value, list):
            for item in value:
                broker_events(item)
        elif isinstance(value, dict):
            event = str(value.get("type", value.get("event", value.get("kind", "")))).lower()
            if "fill" in event:
                found.add("Fill")
            if "order" in event or event.startswith("allocation_"):
                found.add("Order")
            if event in {"split", "dividend_entitlement", "dividend_payment"}:
                found.add("Return")
            for key in ("events", "fills", "ledger", "account_events", "broker", "execution"):
                if key in value:
                    if key == "fills" and value[key]:
                        found.add("Fill")
                    broker_events(value[key])
    broker_events(result)
    return [stage for stage in STAGES if stage in found]


def _references(value):
    """Collect concrete identifiers for local trace navigation, never URL links."""
    found = []

    def visit(item):
        if isinstance(item, dict):
            for key, child in item.items():
                if key == "id" or key.endswith("_id") or key.endswith("_ids"):
                    candidates = child if isinstance(child, list) else [child]
                    for candidate in candidates:
                        if isinstance(candidate, (str, int)) and not isinstance(candidate, bool):
                            text = str(candidate)
                            if text and text not in found:
                                found.append(text)
                if isinstance(child, (dict, list)):
                    visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)
    visit(value)
    return found[:32]


def _description(action, result):
    if _mapping(result).get("rejection"):
        return str(result["rejection"])
    # Environment info wraps the actual returned tool/action value in `result`.
    result = _mapping(result).get("result", result)
    for item in (result, _mapping(result).get("result"), action):
        item = _mapping(item)
        for key in ("error", "reason", "claim", "question", "title", "summary", "message"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value[:420] + ("…" if len(value) > 420 else "")
        for key in ("revision", "question", "ticket", "theme"):
            child = _mapping(item.get(key))
            for field in ("claim", "question", "title", "summary", "reason"):
                if isinstance(child.get(field), str):
                    return child[field][:420]
    return "Recorded action and its returned result are available below."


def _position_rows(account):
    positions = account.get("positions", {})
    if isinstance(positions, dict):
        items = positions.items()
    elif isinstance(positions, list):
        items = [(item.get("asset_id", item.get("symbol", "Unknown")), item)
                 for item in positions if isinstance(item, dict)]
    else:
        items = []
    rows = []
    for identifier, value in items:
        position = _mapping(value)
        rows.append([str(identifier), str(position.get("quantity", position.get("shares", value if _number(value) is not None else "—"))),
                     _money(position.get("mark", position.get("price", position.get("mark_price")))),
                     _money(position.get("market_value", position.get("value")))])
    return rows


def _snapshot(record):
    observation = _observation(record)
    account = _mapping(observation.get("account"))
    sources = [source for source in observation.get("feed", []) if isinstance(source, dict)]
    read_ids = {str(source.get("id")) for source in observation.get("read_sources", []) if isinstance(source, dict)}
    return {"time": str(_snapshot_time(record)), "nav": _money(account.get("nav")),
            "cash": _money(account.get("cash")), "return": _percent(account.get("net_return")),
            "positions": _position_rows(account),
            "sources": [{"id": str(source.get("id", "Unknown")), "title": str(source.get("title", "Untitled original")),
                         "provider": str(source.get("source", "Provider not supplied")),
                         "published_at": str(source.get("published_at", "Not supplied")),
                         "available_at": str(source.get("available_at", "Not supplied")),
                         "read": str(source.get("id")) in read_ids} for source in sources],
            "read_count": len(read_ids), "question_count": len(observation.get("questions", [])),
            "thesis_count": len(_mapping(observation.get("theses")))}


def _source_list(sources):
    if not sources:
        return '<p class="empty">No source metadata is present in this public observation.</p>'
    return ''.join('<div class="source-row"><button type="button" class="source-title" data-reference="'
                   + _escape(source["id"]) + '">' + _escape(source["title"]) + '</button><p>'
                   + _escape(source["provider"]) + ' · ' + _escape(source["id"]) + ' · '
                   + ('Read' if source["read"] else 'Arrived') + '</p><dl><dt>Published</dt><dd>'
                   + _escape(source["published_at"]) + '</dd><dt>Available</dt><dd>'
                   + _escape(source["available_at"]) + '</dd></dl></div>' for source in sources)


def _policy_outcome(summary, trajectory):
    """Separate policy progress from a successfully settled simulated account."""
    declared = summary.get("policy_outcome")
    counts = _mapping(summary.get("counts"))
    rejected = summary.get("rejected_actions")
    if type(rejected) is not int or rejected < 0:
        rejected = sum(bool(_mapping(row.get("result", row.get("info"))).get("rejection")) for row in trajectory)
    budget = summary.get("policy_budget_exhausted") is True or declared == "budget_exhausted"
    if not budget:
        # Older saved runs have no outcome enum. Only inspect the explicit stop
        # action, never arbitrary article text containing the same phrase.
        budget = any(_action_name(row) == "stop" and
                     "model invocation budget exhausted" in str(_mapping(row.get("action")).get("reason", "")).lower()
                     for row in trajectory)
    research_keys = ("read_sources", "tickets", "themes", "questions", "theses", "thesis_revisions", "tool_calls")
    empty_research = all(type(counts.get(key)) is int and counts[key] == 0 for key in research_keys)
    if summary.get("run_status") == "failed" or declared == "failed":
        outcome = "failed"
    elif declared == "rejected_actions_only" or (rejected > 0 and empty_research):
        outcome = "rejected_actions_only"
    elif budget:
        outcome = "budget_exhausted"
    elif declared == "finished" or summary.get("run_status") == "completed":
        outcome = "finished"
    else:
        outcome = "unknown"
    labels = {"failed": "Policy run failed", "rejected_actions_only": "Rejected actions only",
              "budget_exhausted": "Model budget exhausted", "finished": "Policy execution finished",
              "unknown": "Policy outcome not supplied"}
    # A rejected-only run can also exhaust its budget. Keep both facts visible.
    title = "Model budget exhausted" if budget and outcome != "failed" else labels[outcome]
    if outcome == "rejected_actions_only":
        description = "No research progress was recorded. Rejected actions did not produce a research judgment."
    elif outcome == "budget_exhausted":
        description = "The model-call limit stopped the research process before the policy finished."
    elif outcome == "failed":
        description = "The policy run failed. Inspect the recorded error and accepted transitions below."
    elif outcome == "finished":
        description = "The policy finished issuing actions. Its recorded research and investment results remain separate from execution completion."
    else:
        description = "The account status alone does not establish whether the research process finished."
    if budget:
        description += " The harness can still settle the account after model calls stop; settlement does not mean research completed."
    def count(key):
        value = counts.get(key)
        return str(value) if type(value) is int and value >= 0 else "Unknown"
    progress = (count("read_sources") + " originals read · " + count("tickets") + " tickets · "
                + count("questions") + " questions · " + count("theses") + " theses · " + str(rejected) + " actions rejected")
    return (outcome, budget,
            '<section class="policy-outcome' + (' attention-needed' if outcome in {"failed", "rejected_actions_only", "budget_exhausted"} else '')
            + '" data-policy-outcome="' + outcome + '" data-scope="full-episode" aria-labelledby="policy-outcome-title">'
            + '<p class="eyebrow">Whole-episode agent outcome · frozen during playback</p>'
            + '<div class="outcome-heading"><h2 id="policy-outcome-title">' + title + '</h2><span class="outcome-label">'
            + labels[outcome] + '</span></div><p>' + description + '</p><p class="outcome-counts">'
            + progress + '</p></section>')


def _integrity(summary, trajectory):
    """Present recorded admission metadata without manufacturing verification."""
    integrity = _mapping(summary.get("replay_integrity"))
    admission = _mapping(summary.get("admission", summary.get("archive_admission")))
    recorded = admission or integrity
    models = sorted({str(_mapping(record.get("invocation")).get("model")) for record in trajectory
                     if _mapping(record.get("invocation")).get("model")})
    configured = _mapping(_mapping(summary.get("policy_manifest")).get("roles"))
    checkpoints = [str(role) + ": " + str(identity.get("model_id", "Unknown"))
                   + " @ " + str(identity.get("revision", "Unknown"))
                   for role, identity in configured.items() if isinstance(identity, dict)]
    def check(key):
        value = recorded.get(key)
        return "Verified" if value is True else "Not verified" if value is False else "Not supplied"
    cutoff = recorded.get("training_cutoff")
    cutoff_label = (" · ".join(str(role) + ": " + (str(value) if value else "Unknown") for role, value in cutoff.items())
                    if isinstance(cutoff, dict) else str(cutoff) if cutoff else "Not supplied")
    strict = recorded.get("strict_point_in_time_eligible")
    strict_label = "Eligible" if strict is True else "Not certified" if strict is False else "Not supplied"
    status_value = str(recorded.get("status", "Not supplied"))
    status_label = {"admitted_with_limits": "Admitted with limits", "admitted": "Admitted", "rejected": "Rejected"}.get(status_value, status_value)
    interval = (str(recorded["episode_start"]) + " → " + str(recorded.get("episode_end", "Not supplied"))
                if recorded.get("episode_start") else "Not supplied")
    rows = [("Dataset", "Synthetic fixture" if summary.get("mode") == "synthetic" else
             "Historical replay" if summary.get("mode") == "historical_replay" else "Not declared"),
            ("Admission status", status_label),
            ("Episode interval", interval),
            ("Source receipts", check("source_receipts_verified")),
            ("Source hashes", check("source_hashes_verified")),
            ("Source receipt policy", str(recorded.get("source_receipt_policy", "Not supplied"))),
            ("Price vintage", str(recorded.get("price_vintage", "Not supplied")).replace("_", " ")),
            ("Price provenance", check("price_provenance_verified")),
            ("Strict point-in-time eligibility", strict_label),
            ("Model release boundary", check("model_boundary_verified")),
            ("Model cutoff", cutoff_label),
            ("Cutoff verification", check("model_cutoff_verified")),
            ("Model files", check("model_files_verified")),
            ("Policy", str(summary.get("policy", "Not supplied"))),
            ("Configured checkpoint identities", " · ".join(checkpoints) or "Not supplied"),
            ("Models recorded on transitions", ", ".join(models) or "See invocation artifacts"),
            ("Training", "No training performed" if summary.get("training_performed") is False else
             "Training reported in summary" if summary.get("training_performed") is True else "Not supplied")]
    reasons = recorded.get("reasons", [])
    reasons = [reason for reason in reasons if isinstance(reason, str)] if isinstance(reasons, list) else []
    return (('<p class="subtle">Admitted scope: ' + _escape(recorded["benchmark_scope"]) + '</p>' if recorded.get("benchmark_scope") else '')
            + '<dl class="integrity-grid">' + ''.join('<div><dt>' + _escape(label) + '</dt><dd>'
            + _escape(value) + '</dd></div>' for label, value in rows) + '</dl>'
            + ('<ul class="integrity-reasons">' + ''.join('<li>' + _escape(reason) + '</li>' for reason in reasons) + '</ul>' if reasons else '')
            + (_details("Recorded data admission and model evidence", recorded) if recorded else
               '<p class="subtle">No archive admission record was supplied. A historical label alone does not verify data or model cutoffs.</p>'))


def _chart(trajectory):
    points = []
    indices = []
    for index, record in enumerate(trajectory):
        account = _mapping(_observation(record).get("account"))
        nav = _number(account.get("nav"))
        if nav is not None:
            point = (str(_snapshot_time(record)), nav)
            if not points or point != points[-1]:
                points.append(point)
                indices.append(index)
    if not points:
        return '<p class="empty">No account snapshots have been recorded yet.</p>'
    values = [value for _, value in points]
    minimum, maximum = min(values), max(values)
    spread = maximum - minimum
    padding = max(spread * .15, abs(maximum) * .001, .01)
    low, high = minimum - padding, maximum + padding
    width, height, left, top = 960, 200, 82, 22
    plot_width, plot_height = 850, 140
    coords = [(left + index * plot_width / max(len(points) - 1, 1),
               top + (high - value) / (high - low) * plot_height)
              for index, (_, value) in enumerate(points)]
    path = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
    descriptions = f"{len(points)} observed account snapshots; NAV ranges from {minimum:,.2f} to {maximum:,.2f}."
    circles = ''.join('<circle data-index="' + str(index) + '" data-value="' + str(value)
                      + '" data-time="' + _escape(date) + '" cx="' + f'{x:.2f}' + '" cy="' + f'{y:.2f}'
                      + '" r="3"><title>' + _escape(date) + ': ' + _money(value)
                      + '</title></circle>' for (x, y), (date, value), index in zip(coords, points, indices))
    return ('<svg class="nav-chart" role="img" aria-labelledby="nav-title nav-description" viewBox="0 0 '
            + str(width) + ' ' + str(height) + '"><title id="nav-title">Observed paper account NAV</title>'
            + '<desc id="nav-description">' + _escape(descriptions) + ' Horizontal spacing follows recorded transitions.</desc>'
            + '<line x1="82" y1="162" x2="932" y2="162" class="axis"/>'
            + '<text id="nav-high" x="72" y="30" text-anchor="end">' + _money(maximum) + '</text>'
            + '<text id="nav-low" x="72" y="160" text-anchor="end">' + _money(minimum) + '</text>'
            + '<polyline points="' + path + '"/>' + circles + '</svg>'
            + '<div class="chart-labels"><span id="chart-start">' + _escape(points[0][0]) + '</span><span id="chart-end">'
            + _escape(points[-1][0]) + '</span></div><p class="subtle">Account value at recorded transitions, '
            + 'including cash and marked positions. Research can continue between market updates.</p>')


def _positions(account):
    positions = account.get("positions", {})
    if not positions:
        return '<p class="empty">No positions are recorded in this account snapshot.</p>'
    rows = []
    if isinstance(positions, dict):
        items = positions.items()
    elif isinstance(positions, list):
        items = [(item.get("asset_id", item.get("symbol", "Unknown")), item)
                 for item in positions if isinstance(item, dict)]
    else:
        return _details("Recorded positions", positions)
    for identifier, value in items:
        position = _mapping(value)
        quantity = position.get("quantity", position.get("shares", value if isinstance(value, (int, float)) else "—"))
        mark = position.get("mark", position.get("price", position.get("mark_price")))
        market_value = position.get("market_value", position.get("value"))
        rows.append('<tr><th scope="row">' + _escape(identifier) + '</th><td>' + _escape(quantity)
                    + '</td><td>' + _money(mark) + '</td><td>' + _money(market_value) + '</td></tr>')
    return ('<div class="table-wrap"><table><thead><tr><th>Asset</th><th>Quantity</th><th>Mark</th><th>Value</th>'
            + '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>')


_SCRIPT = r"""
(() => {
  const events = [...document.querySelectorAll('.event')];
  const search = document.getElementById('trace-search');
  const role = document.getElementById('role-filter');
  const action = document.getElementById('action-filter');
  const cursor = document.getElementById('replay-position');
  let stage = '';
  function filter() {
    const query = search.value.trim().toLowerCase();
    let visible = 0;
    const limit = Number(cursor.value);
    for (const event of events) {
      const show = Number(event.dataset.index) <= limit && (!query || event.dataset.search.includes(query)) &&
        (!role.value || event.dataset.role === role.value) &&
        (!action.value || event.dataset.action === action.value) &&
        (!stage || event.dataset.stages.split(',').includes(stage));
      event.hidden = !show;
      visible += Number(show);
    }
    document.getElementById('event-count').textContent = `${visible} of ${events.length} recorded transitions`;
    document.getElementById('no-matches').hidden = visible !== 0 || events.length === 0;
    document.querySelectorAll('[data-stage]').forEach(button => {
      button.setAttribute('aria-pressed', String(button.dataset.stage === stage));
      const count = events.filter(event => Number(event.dataset.index) <= limit && event.dataset.stages.split(',').includes(button.dataset.stage)).length;
      button.querySelector('small').textContent = `${count} transitions`;
    });
  }
  function node(tag, text, className) {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  }
  function updateChart(limit) {
    const chart = document.querySelector('.nav-chart');
    if (!chart) return;
    const circles = [...chart.querySelectorAll('circle')];
    const visible = circles.filter(circle => Number(circle.dataset.index) <= limit);
    circles.forEach(circle => { circle.style.display = visible.includes(circle) ? '' : 'none'; });
    const line = chart.querySelector('polyline');
    if (!visible.length) {
      line.setAttribute('points', '');
      ['nav-high', 'nav-low', 'chart-start', 'chart-end'].forEach(id => { document.getElementById(id).textContent = 'Not available'; });
      document.getElementById('nav-description').textContent = 'No account snapshots at this replay position.';
      return;
    }
    const values = visible.map(circle => Number(circle.dataset.value));
    const minimum = Math.min(...values), maximum = Math.max(...values);
    const padding = Math.max((maximum - minimum) * .15, Math.abs(maximum) * .001, .01);
    const low = minimum - padding, high = maximum + padding;
    const points = visible.map((circle, index) => {
      const x = 82 + index * 850 / Math.max(visible.length - 1, 1);
      const y = 22 + (high - Number(circle.dataset.value)) / (high - low) * 140;
      circle.setAttribute('cx', x); circle.setAttribute('cy', y);
      return `${x},${y}`;
    });
    line.setAttribute('points', points.join(' '));
    const money = value => value.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    document.getElementById('nav-high').textContent = money(maximum);
    document.getElementById('nav-low').textContent = money(minimum);
    document.getElementById('chart-start').textContent = visible[0].dataset.time;
    document.getElementById('chart-end').textContent = visible[visible.length - 1].dataset.time;
    document.getElementById('nav-description').textContent = `${visible.length} observed account snapshots; NAV ranges from ${money(minimum)} to ${money(maximum)}. Horizontal spacing follows recorded transitions.`;
  }
  function updateSnapshot() {
    const limit = Number(cursor.value);
    const event = events[limit];
    if (!event) return;
    const snapshot = JSON.parse(event.dataset.snapshot);
    document.getElementById('view-time').textContent = snapshot.time;
    document.getElementById('view-position').textContent = `Transition ${limit + 1} / ${events.length}`;
    document.getElementById('view-nav').textContent = snapshot.nav;
    document.getElementById('view-cash').textContent = snapshot.cash;
    document.getElementById('view-return').textContent = snapshot.return;
    document.getElementById('view-research').textContent = `${snapshot.read_count} originals read · ${snapshot.question_count} questions · ${snapshot.thesis_count} theses`;
    cursor.setAttribute('aria-valuetext', `Transition ${limit + 1} of ${events.length}, ${snapshot.time}`);
    document.getElementById('previous-transition').disabled = limit <= 0;
    document.getElementById('next-transition').disabled = limit >= events.length - 1;
    const positions = document.getElementById('view-positions');
    positions.replaceChildren();
    if (!snapshot.positions.length) positions.append(node('p', 'No positions are recorded in this account snapshot.', 'empty'));
    else {
      const wrap = node('div', undefined, 'table-wrap'), table = node('table'), head = node('thead'), row = node('tr');
      ['Asset', 'Quantity', 'Mark', 'Value'].forEach(text => row.append(node('th', text)));
      head.append(row); table.append(head);
      const body = node('tbody');
      snapshot.positions.forEach(values => {
        const tr = node('tr');
        values.forEach((value, index) => { const cell = node(index ? 'td' : 'th', value); if (!index) cell.scope = 'row'; tr.append(cell); });
        body.append(tr);
      });
      table.append(body); wrap.append(table); positions.append(wrap);
    }
    const sources = document.getElementById('view-sources');
    sources.replaceChildren();
    if (!snapshot.sources.length) sources.append(node('p', 'No source metadata is present in this public observation.', 'empty'));
    snapshot.sources.forEach(source => {
      const row = node('div', undefined, 'source-row');
      const button = node('button', source.title, 'source-title');
      button.type = 'button'; button.dataset.reference = source.id;
      row.append(button, node('p', `${source.provider} · ${source.id} · ${source.read ? 'Read' : 'Arrived'}`));
      const metadata = node('dl');
      [['Published', source.published_at], ['Available', source.available_at]].forEach(([label, value]) => metadata.append(node('dt', label), node('dd', value)));
      row.append(metadata); sources.append(row);
    });
    document.getElementById('source-count').textContent = `${snapshot.sources.length} arrived originals`;
    updateChart(limit);
    filter();
  }
  search.addEventListener('input', filter);
  role.addEventListener('change', filter);
  action.addEventListener('change', filter);
  cursor.addEventListener('input', updateSnapshot);
  document.getElementById('previous-transition').addEventListener('click', () => { cursor.value = Math.max(0, Number(cursor.value) - 1); updateSnapshot(); });
  document.getElementById('next-transition').addEventListener('click', () => { cursor.value = Math.min(events.length - 1, Number(cursor.value) + 1); updateSnapshot(); });
  document.querySelectorAll('[data-stage]').forEach(button => button.addEventListener('click', () => {
    stage = stage === button.dataset.stage ? '' : button.dataset.stage;
    filter();
    document.getElementById('trace-title').scrollIntoView({behavior: 'smooth', block: 'start'});
  }));
  document.addEventListener('click', event => {
    const reference = event.target.closest('[data-reference]');
    if (reference) {
      search.value = reference.dataset.reference; role.value = ''; action.value = ''; stage = ''; filter();
      document.getElementById('trace-title').scrollIntoView({behavior: 'smooth', block: 'start'});
      search.focus({preventScroll: true});
    }
    const jump = event.target.closest('[data-jump]');
    if (jump) document.getElementById(jump.dataset.jump).scrollIntoView({behavior: 'smooth', block: 'start'});
  });
  document.getElementById('clear-filters').addEventListener('click', () => { search.value = ''; role.value = ''; action.value = ''; stage = ''; filter(); });
  document.getElementById('expand-visible').addEventListener('click', () => {
    const visible = events.filter(event => !event.hidden);
    const shouldOpen = visible.some(event => [...event.querySelectorAll('details')].some(detail => !detail.open));
    visible.forEach(event => event.querySelectorAll('details').forEach(detail => { detail.open = shouldOpen; }));
  });
  if (events.length) updateSnapshot();
  else filter();
})();
"""

_STYLE = """
:root{color-scheme:light;--bg:#f5f6f8;--surface:#fff;--line:#dee4ed;--text:#1a2941;--muted:#68768b;--blue:#245ee8;--mint:#245ee8;--amber:#75520c}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.6 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1280px;margin:auto;padding:0 36px 52px}h1,h2,h3,p{margin-top:0}h1{font-size:clamp(32px,4.5vw,55px);font-weight:650;line-height:1.07;margin-bottom:20px;letter-spacing:-.055em;max-width:700px}h2{font-size:20px;line-height:1.3;letter-spacing:-.025em;font-weight:650}h3{font-size:16px;margin:0;font-weight:650}button,input,select{font:inherit}button,select,input{color:var(--text);background:var(--surface);border:1px solid var(--line);border-radius:7px;padding:9px 12px}button{cursor:pointer}button:hover,button[aria-pressed=true]{border-color:var(--blue);color:var(--blue);background:#eef3ff}button:disabled{opacity:.4;cursor:default}button:focus-visible,input:focus-visible,select:focus-visible,summary:focus-visible{outline:3px solid #85a8ff;outline-offset:3px}.topbar{display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line);padding:22px 0;gap:20px}.wordmark{font-size:17px;font-weight:720;letter-spacing:-.04em;display:flex;align-items:center;gap:10px}.brand-mark{background:var(--blue);color:#fff;border-radius:6px;display:inline-grid;place-items:center;width:27px;height:27px;font-weight:500;font-size:21px}.topbar nav{display:flex;gap:4px}.topbar nav button{background:none;border-color:transparent;font-size:12px;padding:6px 10px;color:var(--muted)}.hero{display:grid;grid-template-columns:1.6fr 1fr;gap:60px;padding:50px 0 28px;align-items:center}.eyebrow{color:var(--blue);letter-spacing:.14em;text-transform:uppercase;font-size:10px;font-weight:750;margin-bottom:17px}.hero-copy{font-size:15px;color:var(--muted);max-width:570px;margin-bottom:18px}.episode-id{font:11px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;color:var(--muted);overflow-wrap:anywhere}.hero-protocol{border-left:1px solid var(--line);padding-left:26px}.protocol-row{display:grid;grid-template-columns:30px 1fr;gap:12px;padding:13px 0}.protocol-row+.protocol-row{border-top:1px solid var(--line)}.protocol-number{font:11px ui-monospace,SFMono-Regular,monospace;color:var(--blue);padding-top:4px}.protocol-row strong{font-weight:600;font-size:13px;display:block}.protocol-row p{font-size:12px;color:var(--muted);margin:3px 0 0}.subtle,.meta,.empty{color:var(--muted)}.subtle{font-size:12px}.mode-notice{padding:13px 16px;background:#fff9ec;border:1px solid #e9dbb5;border-radius:7px;margin:12px 0;color:var(--amber)}.mode-notice strong{display:block;font-size:12px;font-weight:650}.mode-notice p{margin:3px 0 0;font-size:11px}.section-label{font-size:10px;letter-spacing:.12em;text-transform:uppercase;font-weight:700;color:var(--muted);margin:28px 0 9px;display:flex;gap:10px;align-items:center}.section-label:after{content:"";height:1px;flex:1;background:var(--line)}.overview{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));margin:0 0 28px;background:#14233d;color:#edf3ff;border-radius:10px;overflow:hidden}.metric{padding:22px 20px;border-right:1px solid #2f3c52;min-width:0}.metric:last-child{border:0}.metric-label{font-size:11px;color:#a9b9d2;display:block}.metric-value{font-size:26px;font-weight:570;letter-spacing:-.035em;font-variant-numeric:tabular-nums;display:block;margin-top:5px;overflow-wrap:anywhere}.metric-foot{display:block;color:#a9b9d2;font-size:10px;margin-top:8px}.panel{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:24px;margin:20px 0}.panel-title{display:flex;align-items:baseline;justify-content:space-between;gap:16px}.panel-title h2{margin-bottom:12px}.panel-title p{font-size:11px;color:var(--muted);margin:0}.pipeline{display:flex;align-items:stretch;gap:5px;overflow:auto;padding:3px 1px 9px}.stage{min-width:96px;text-align:left;flex:1;padding:10px;font-size:12px;border-color:#e5eaf2}.stage small{display:block;font-size:10px;color:var(--muted);margin-top:3px}.stage-arrow{align-self:center;color:#a3afc2}.two-col{display:grid;grid-template-columns:1.6fr 1fr;gap:18px}.two-col .panel{margin-top:0}.nav-chart{width:100%;height:auto;display:block}.nav-chart text{fill:var(--muted);font-size:10px}.nav-chart polyline{fill:none;stroke:var(--blue);stroke-width:2.6;stroke-linejoin:round}.nav-chart circle{fill:var(--blue)}.axis{stroke:var(--line);stroke-width:1}.chart-labels{display:flex;justify-content:space-between;gap:15px;font:10px/1.5 ui-monospace,SFMono-Regular,monospace;color:var(--muted);overflow-wrap:anywhere}.table-wrap{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:12px}th,td{text-align:right;padding:9px 7px;border-bottom:1px solid var(--line);white-space:nowrap;font-variant-numeric:tabular-nums}th:first-child{text-align:left}thead{color:var(--muted);font-size:10px}.replay-bar{display:grid;grid-template-columns:1fr auto;gap:20px;align-items:center;margin-bottom:18px}.replay-meta{display:flex;align-items:baseline;justify-content:space-between;gap:15px}.replay-meta label{font-size:11px;font-weight:650}.replay-meta output{font:10px ui-monospace,SFMono-Regular,monospace;color:var(--muted)}input[type=range]{accent-color:var(--blue);width:100%;padding:0;margin:12px 0 0;cursor:pointer}.replay-buttons{display:flex;gap:5px}.replay-buttons button{padding:6px 11px}.snapshot-strip{display:flex;flex-wrap:wrap;gap:24px;padding-top:18px;border-top:1px solid var(--line)}.snapshot-strip span{display:grid;gap:2px;font-size:10px;color:var(--muted)}.snapshot-strip strong{font-size:18px;font-weight:600;color:var(--text);font-variant-numeric:tabular-nums;letter-spacing:-.02em}.snapshot-strip .snapshot-time{flex:1;min-width:230px}.snapshot-strip .snapshot-time strong{font-size:13px;letter-spacing:0;padding:4px 0}.research-count{font-size:11px;color:var(--muted);margin:15px 0 0}.source-list{max-height:350px;overflow:auto}.source-row{padding:12px 0;border-top:1px solid var(--line)}.source-row:first-child{border:0;padding-top:0}.source-title{padding:0;border:0;border-radius:0;background:none;color:var(--blue);font-size:12px;text-align:left;line-height:1.5}.source-title:hover{background:none;text-decoration:underline}.source-row p{font-size:10px;color:var(--muted);margin:5px 0}.source-row dl{display:grid;grid-template-columns:60px 1fr;gap:2px 8px;font:10px/1.5 ui-monospace,SFMono-Regular,monospace;margin:5px 0}.source-row dt{color:var(--muted)}.source-row dd{margin:0;overflow-wrap:anywhere}.integrity-reasons{font-size:12px;color:var(--muted);padding-left:18px}.integrity-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0 28px;margin:0}.integrity-grid div{padding:12px 0;border-bottom:1px solid #edf0f5;min-width:0}.integrity-grid dt{font-size:10px;color:var(--muted);margin-bottom:4px}.integrity-grid dd{font-size:12px;margin:0;overflow-wrap:anywhere}.filters{display:flex;gap:10px;align-items:end;flex-wrap:wrap;margin-bottom:13px}.filters label{display:grid;gap:5px;font-size:10px;color:var(--muted)}.search-label{flex:1;min-width:220px}.filters input{width:100%}.filters button,.filters select,.filters input{font-size:12px}.trace-heading{display:flex;gap:15px;justify-content:space-between;align-items:baseline;margin:30px 0 18px}.trace-heading h2{margin:0;scroll-margin-top:20px}.event{background:var(--surface);border:1px solid var(--line);border-left:3px solid #9eacbf;border-radius:8px;padding:20px;margin:12px 0;overflow-wrap:anywhere}.event[data-role=Fast],.event[data-role=fast]{border-left-color:#4682d9}.event[data-role=Mid],.event[data-role=mid]{border-left-color:#9271bc}.event[data-role=Strong],.event[data-role=strong]{border-left-color:#2d7694}.event[hidden]{display:none}.event-head{display:flex;justify-content:space-between;gap:12px;align-items:start}.badges{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}.badge{font-size:9px;border:1px solid #e1e7ef;border-radius:4px;padding:2px 6px;color:var(--muted)}.badge.role{color:var(--blue);background:#f1f5fe;border-color:#e1e9fb;font-weight:650}.reward{font-variant-numeric:tabular-nums;font-size:10px;color:var(--muted);white-space:nowrap}.meta{font:10px/1.6 ui-monospace,SFMono-Regular,monospace;margin:6px 0 12px}.description{font-size:13px;margin-bottom:12px;max-width:950px}.references{display:flex;gap:5px;flex-wrap:wrap;margin:13px 0}.references button{padding:3px 6px;font:10px/1.5 ui-monospace,SFMono-Regular,monospace;max-width:100%;overflow-wrap:anywhere;background:#f7f9fc;border-color:#e8edf4}.event-details{display:grid;grid-template-columns:1fr 1fr;gap:0 15px}.event-details .wide{grid-column:1/-1}details{border-top:1px solid var(--line);margin-top:10px;padding-top:10px;min-width:0}summary{cursor:pointer;font-size:11px;color:var(--muted)}pre{max-height:560px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;border-radius:6px;padding:14px;background:#f5f7fa;color:#30425e;font:11px/1.65 ui-monospace,SFMono-Regular,Consolas,monospace}.error{color:#a33125}.empty{padding:12px 0;font-size:12px}.account-line{display:flex;gap:18px;flex-wrap:wrap;font-size:11px;color:var(--muted)}.account-line strong{font-variant-numeric:tabular-nums;color:var(--text);font-weight:550}footer{margin-top:30px;padding-top:20px;border-top:1px solid var(--line);color:var(--muted);font-size:10px}section{scroll-margin-top:24px}.policy-outcome{border:1px solid var(--line);border-radius:10px;padding:22px 24px;margin:0 0 12px;background:var(--surface);overflow-wrap:anywhere}.policy-outcome.attention-needed{border-color:#e3c17b;background:#fff8e8}.policy-outcome .eyebrow{margin:0 0 10px;color:var(--muted)}.outcome-heading{display:flex;flex-wrap:wrap;align-items:baseline;justify-content:space-between;gap:8px 20px}.outcome-heading h2{font-size:24px;margin:0}.outcome-label{font-size:11px;font-weight:650;border:1px solid #dfd5c0;border-radius:5px;padding:3px 8px}.policy-outcome p:not(.eyebrow){margin:10px 0 0;max-width:1000px;font-size:13px}.policy-outcome .outcome-counts{font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}.hero>*{min-width:0}.hero{grid-template-columns:minmax(0,1.6fr) minmax(0,1fr)}.two-col>*{min-width:0}.filters .search-label{flex:1 1 260px;min-width:0}.filters select{max-width:100%}.outcome-heading>*{min-width:0}@media(min-width:601px) and (max-width:920px){.hero{grid-template-columns:1fr;gap:22px}.hero-protocol{border-left:0;padding-left:0;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:18px}.protocol-row{border-top:1px solid var(--line)}.replay-meta{flex-wrap:wrap;gap:5px}.snapshot-strip .snapshot-time{min-width:0;flex-basis:100%}.topbar{flex-wrap:wrap}}@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto!important}}@media(max-width:920px){main{padding:0 22px 40px}.hero{gap:28px}.two-col{grid-template-columns:1fr}.integrity-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.overview{grid-template-columns:1fr 1fr}.metric:nth-child(2){border-right:0}.metric:nth-child(-n+2){border-bottom:1px solid #2f3c52}.metric-value{font-size:24px}}@media(max-width:600px){main{padding:0 15px 35px}.topbar{align-items:start;gap:10px;flex-direction:column;padding:16px 0}.topbar nav{gap:0;flex-wrap:wrap}.topbar nav button{padding:5px 9px}.hero{display:block;padding:30px 0 15px}.hero-protocol{border:0;border-top:1px solid var(--line);padding:10px 0 0;margin-top:22px}.hero-copy{font-size:13px}.protocol-row{padding:9px 0}.panel,.event{padding:17px}.metric{padding:17px 13px}.metric-value{font-size:21px}.metric-foot{font-size:9px}.panel-title,.trace-heading{display:block}.panel-title p{margin:0 0 12px}.event-details{grid-template-columns:1fr}.event-details .wide{grid-column:auto}.event-head{display:block}.reward{display:block;margin-top:7px}.trace-heading p{margin:5px 0}.snapshot-strip{gap:16px}.snapshot-strip .snapshot-time{flex-basis:100%}.replay-bar{gap:10px}.integrity-grid{grid-template-columns:1fr}.filters label:not(.search-label){flex:1}.filters select{width:100%}.filters button{flex:1;font-size:11px}.source-row dl{grid-template-columns:55px 1fr}.pipeline{margin-right:-4px}}
"""



def render_trace(summary: dict, trajectory: list[dict]) -> str:
    """Render public transitions with local filtering and explicit provenance.

    All dynamic content is HTML-escaped. JavaScript is fixed, hash-authorized,
    and operates only on the rendered DOM; scenario content is never executable.
    """
    summary = _mapping(summary)
    trajectory = [record for record in trajectory if isinstance(record, dict)]
    policy_outcome, _, outcome_view = _policy_outcome(summary, trajectory)
    mode = str(summary.get("mode", "undeclared"))
    if mode == "synthetic":
        mode_label = "Synthetic replay — engineering validation, not investment performance"
    else:
        mode_label = "Declared episode mode: " + mode + " — simulated account"
    account = _mapping(summary.get("account"))
    reward = summary.get("reward", summary.get("total_reward"))
    settled = bool(summary.get("terminated") or summary.get("truncated"))
    failed = summary.get("run_status") == "failed"
    eligible = summary.get("evaluation_eligible") is not False and not failed
    reward_label = ("Ineligible" if not eligible else
                    _percent(reward) if settled and summary.get("complete") is not False else
                    "Not scored" if summary.get("complete") is False else "Pending")
    status = ("Run failed" if failed else "Ineligible" if not eligible else
              "Incomplete" if summary.get("complete") is False else
              "Truncated" if summary.get("truncated") else
              "Settled" if summary.get("terminated") else "In progress")
    roles = sorted({_role(record) for record in trajectory})
    actions = sorted({_action_name(record) for record in trajectory})
    # The account ledger is cumulative. Attribute each new event to its first
    # returned public snapshot rather than repeating every fill on later steps.
    seen_account_events = set()
    account_events = []
    record_stages = []
    for record in trajectory:
        new_events = []
        ledger = _mapping(_observation(record).get("account")).get("ledger", [])
        for event in ledger if isinstance(ledger, list) else []:
            identity = _json(_mapping(event).get("event_id", event))
            if identity not in seen_account_events:
                seen_account_events.add(identity)
                new_events.append(event)
        account_events.append(new_events)
        classified = dict(record)
        classified["result"] = {"execution": record.get("result", record.get("info", {})), "events": new_events}
        record_stages.append(_stages(classified))
    stage_counts = {stage: sum(stage in stages for stages in record_stages) for stage in STAGES}
    supplied_calls = summary.get("model_invocations")
    model_calls = (supplied_calls if type(supplied_calls) is int and supplied_calls >= 0 else
                   len({_json(_mapping(record.get("invocation")).get("id", index))
                        for index, record in enumerate(trajectory)
                        if _model_invocation(record.get("invocation"))}))
    call_label = ("Codex model calls" if summary.get("policy") == "codex" or any(
        _mapping(record.get("invocation")).get("policy") == "codex" for record in trajectory) else "model calls")
    if model_calls == 1:
        call_label = call_label[:-1]
    provenance = summary.get("policy", summary.get("provenance", "Policy provenance not supplied"))
    evaluation_notice = ''
    if not eligible:
        evaluation_notice = ('<aside class="mode-notice error" role="alert"><strong>'
                             + ('Policy run failed' if failed else 'Episode is ineligible')
                             + ' — excluded from policy evaluation</strong><p>'
                             + _escape(summary.get("error") or "This rollout is not eligible for a policy score.")
                             + '</p><p>Account figures below preserve the recorded accounting outcome; '
                             + 'they do not make this a successful policy run.</p></aside>')
    reward_foot = ('Accounting return ' + _percent(account.get("net_return"))
                   if settled and not eligible else 'Terminal net portfolio return')
    if eligible and policy_outcome in {"rejected_actions_only", "budget_exhausted"}:
        reward_foot += ' · accounting valid; research unfinished'
    cards = []
    for index, record in enumerate(trajectory):
        action = record.get("action", {})
        result = record.get("result", record.get("info", {}))
        observation = _observation(record)
        record_account = _mapping(observation.get("account"))
        action_name, role, stages = _action_name(record), _role(record), record_stages[index]
        transition_time = str(_as_of(record))
        if str(_snapshot_time(record)) != transition_time:
            transition_time += ' → ' + str(_snapshot_time(record))
        new_events = account_events[index]
        references = _references({"action": action, "result": result, "account_events": new_events})
        search = _json({"id": record.get("id"), "step": record.get("step", index), "as_of": _as_of(record),
                        "role": role, "action": action, "result": result, "account_events": new_events}).lower()
        badges = '<span class="badge role">' + _escape(role) + '</span>'
        badges += ''.join('<span class="badge">' + stage + '</span>' for stage in stages)
        if record.get("terminated") or record.get("truncated"):
            badges += '<span class="badge">' + ('Truncated' if record.get("truncated") else 'Terminal') + '</span>'
        links = ''.join('<button type="button" data-reference="' + _escape(reference)
                        + '" title="Find transitions mentioning this identifier">' + _escape(reference)
                        + '</button>' for reference in references)
        account_line = ''
        if record_account:
            account_line = ('<div class="account-line"><span>Cash <strong>' + _money(record_account.get("cash"))
                            + '</strong></span><span>NAV <strong>' + _money(record_account.get("nav"))
                            + '</strong></span><span>Account return <strong>' + _percent(record_account.get("net_return"))
                            + '</strong></span></div>')
        invocation = record.get("invocation")
        invocation_label = ("Recorded Codex invocation" if _mapping(invocation).get("policy") == "codex"
                            else "Policy provenance (scripted)" if _mapping(invocation).get("policy") == "scripted"
                            else "Recorded model invocation" if _model_invocation(invocation)
                            else "Policy provenance")
        invocation_view = _details(invocation_label, invocation) if invocation else ''
        error_class = ' error' if _mapping(result).get("error") or _mapping(result).get("rejection") else ''
        cards.append('<article class="event" id="event-' + str(index) + '" data-index="' + str(index)
                     + '" data-snapshot="' + _escape(_json(_snapshot(record))) + '" data-role="' + _escape(role)
                     + '" data-action="' + _escape(action_name) + '" data-stages="' + ','.join(stages)
                     + '" data-search="' + _escape(search) + '"><div class="event-head"><div><div class="badges">'
                     + badges + '</div><h3>Step ' + _escape(record.get("step", index)) + ' · ' + _escape(action_name)
                     + '</h3></div><span class="reward">Step reward ' + _escape(record.get("reward", 0))
                     + '</span></div><p class="meta">' + _escape(transition_time)
                     + ' · ' + _escape(record.get("id", 'event-' + str(index))) + '</p><p class="description'
                     + error_class + '">' + _escape(_description(action, result)) + '</p>' + account_line
                     + ('<div class="references" aria-label="Related trace identifiers">' + links + '</div>' if links else '')
                     + '<div class="event-details">' + _details("Submitted action", action) + _details("Returned result", result)
                     + '<div class="wide">' + (_details("Account events from this transition", new_events) if new_events else '')
                     + invocation_view + _details("Public observation after this transition", observation)
                     + '</div></div></article>')
    options = lambda values: ''.join('<option value="' + _escape(value) + '">' + _escape(value) + '</option>' for value in values)
    stage_buttons = '<span class="stage-arrow" aria-hidden="true">→</span>'.join(
        '<button class="stage" type="button" data-stage="' + stage + '" aria-pressed="false">' + stage
        + '<small>' + str(stage_counts[stage]) + ' transitions</small></button>' for stage in STAGES)
    latest = _snapshot(trajectory[-1]) if trajectory else _snapshot({})
    last_index = max(len(trajectory) - 1, 0)
    script_hash = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
    header = ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
              + '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-' + script_hash
              + '\'; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'">'
              + '<title>Research to portfolio · ' + _escape(summary.get("episode_id", "Episode trace")) + '</title><style>' + _STYLE + '</style></head><body><main>')
    return (header + '<div class="topbar"><div class="wordmark"><span class="brand-mark" aria-hidden="true">a</span>attention<span class="subtle"> / research replay</span></div>'
            + '<nav aria-label="Report sections"><button type="button" data-jump="replay-title">Replay</button><button type="button" data-jump="trace-title">Decision trace</button>'
            + '<button type="button" data-jump="integrity-title">Data &amp; policy</button></nav></div>'
            + '<header class="hero"><div><p class="eyebrow">A financial decision environment for language-model agents</p><h1>Research first.<br>Returns over time.</h1>'
            + '<p class="hero-copy">Follow the evidence from incoming signals to a judgment, a paper allocation, and its later outcome. The research loop continues while positions are held.</p>'
            + '<p class="episode-id">' + _escape(summary.get("episode_id", "Episode trace")) + '</p></div>'
            + '<div class="hero-protocol"><div class="protocol-row"><span class="protocol-number">01</span><div><strong>Discover a question</strong><p>Fast routes sources. Mid connects facts and opens questions.</p></div></div>'
            + '<div class="protocol-row"><span class="protocol-number">02</span><div><strong>Build and revisit a judgment</strong><p>Strong seeks evidence, checks contradictions, and discovers instruments.</p></div></div>'
            + '<div class="protocol-row"><span class="protocol-number">03</span><div><strong>Allocate, observe, settle</strong><p>Paper fills and a fixed horizon connect decisions to net return.</p></div></div></div></header>'
            + '<aside class="mode-notice"><strong>' + _escape(mode_label) + '</strong><p>This report shows recorded observations, '
            + 'tool results and decisions. It does not expose hidden model reasoning or establish investment skill.</p></aside>' + evaluation_notice
            + '<p class="section-label">Frozen full-run result · independent of the replay position</p>'
            + outcome_view
            + '<div class="overview" aria-label="Frozen full-run accounting result"><div class="metric"><span class="metric-label">Harness &amp; settlement</span><strong class="metric-value">' + status
            + '</strong><span class="metric-foot">' + _escape(summary.get("terminal_reason", summary.get("as_of", "Awaiting settlement"))) + '</span></div>'
            + '<div class="metric"><span class="metric-label">Final reward</span><strong class="metric-value">' + reward_label
            + '</strong><span class="metric-foot">' + reward_foot + '</span></div>'
            + '<div class="metric"><span class="metric-label">Final account NAV</span><strong class="metric-value">' + _money(account.get("nav"))
            + '</strong><span class="metric-foot">Cash ' + _money(account.get("cash")) + '</span></div>'
            + '<div class="metric"><span class="metric-label">Recorded transitions</span><strong class="metric-value">' + str(len(trajectory))
            + '</strong><span class="metric-foot">' + str(model_calls) + ' ' + call_label + '</span></div></div>'
            + '<section class="panel" aria-labelledby="replay-title"><div class="panel-title"><h2 id="replay-title">Inspect the unfolding decision</h2><p>Public observations at each recorded transition</p></div>'
            + '<div class="replay-bar"><div><div class="replay-meta"><label for="replay-position">Replay position</label><output id="view-position">'
            + (f'Transition {len(trajectory)} / {len(trajectory)}' if trajectory else 'No recorded transitions') + '</output></div>'
            + '<input id="replay-position" type="range" min="0" max="' + str(last_index) + '" value="' + str(last_index) + '" step="1"'
            + (' disabled' if not trajectory else '') + '></div><div class="replay-buttons"><button type="button" id="previous-transition" aria-label="Previous transition"'
            + (' disabled' if len(trajectory) < 2 else '') + '>←</button><button type="button" id="next-transition" aria-label="Next transition" disabled>→</button></div></div>'
            + '<div class="snapshot-strip"><span class="snapshot-time">Observation cutoff<strong id="view-time">' + _escape(latest["time"]) + '</strong></span>'
            + '<span>NAV at this transition<strong id="view-nav">' + latest["nav"] + '</strong></span><span>Cash<strong id="view-cash">' + latest["cash"]
            + '</strong></span><span>Accounting return<strong id="view-return">' + latest["return"] + '</strong></span></div>'
            + '<p class="research-count" id="view-research">' + str(latest["read_count"]) + ' originals read · ' + str(latest["question_count"]) + ' questions · '
            + str(latest["thesis_count"]) + ' theses</p><p class="subtle">The sources, account, chart and decision trace below follow this position. The full-run result above stays frozen.</p></section>'
            + '<section class="panel"><div class="panel-title"><h2>One continuous decision process</h2><p>Select a stage to follow its evidence</p></div><div class="pipeline" aria-label="Filter transitions by decision stage">'
            + stage_buttons + '</div><p class="subtle">Fast → Mid → Strong → broker. Counts describe recorded transitions, including rejected attempts.</p></section>'
            + '<div class="two-col"><section class="panel"><div class="panel-title"><h2>Paper account value</h2><p>Recorded NAV · cash + marked positions</p></div>' + _chart(trajectory)
            + '<h3>Positions at this transition</h3><div id="view-positions">' + _positions(_mapping(_observation(trajectory[-1]).get("account")) if trajectory else {}) + '</div></section>'
            + '<section class="panel"><div class="panel-title"><h2>Source arrivals</h2><p id="source-count">' + str(len(latest["sources"])) + ' arrived originals</p></div>'
            + '<p class="subtle">Publication and availability times stay attached to each original. Select a title to follow its references in the trace.</p><div class="source-list" id="view-sources">'
            + _source_list(latest["sources"]) + '</div></section></div>'
            + '<section class="panel" aria-labelledby="integrity-title"><div class="panel-title"><h2 id="integrity-title">Data &amp; policy integrity</h2><p>Run-level provenance · explicit verification status</p></div>'
            + _integrity(summary, trajectory) + '<p class="subtle">' + _escape(provenance) + '</p>'
            + _details("Frozen complete-run summary and execution ledger", summary) + '</section>'
            + '<section aria-labelledby="trace-title"><div class="trace-heading"><h2 id="trace-title">Decision trace</h2><p class="subtle" id="event-count" aria-live="polite">'
            + str(len(trajectory)) + ' recorded transitions</p></div><div class="filters"><label class="search-label">Search actions, results or identifiers'
            + '<input id="trace-search" type="search" placeholder="Question, evidence ID, thesis, order…" autocomplete="off"></label>'
            + '<label>Role<select id="role-filter"><option value="">All roles</option>' + options(roles) + '</select></label>'
            + '<label>Action<select id="action-filter"><option value="">All actions</option>' + options(actions) + '</select></label>'
            + '<button type="button" id="clear-filters">Clear filters</button><button type="button" id="expand-visible">Expand / collapse</button></div>'
            + '<p class="subtle">Identifier chips connect original sources, research, thesis revisions and orders. Expand each action for the exact returned result and public state.</p>'
            + '<p id="no-matches" class="empty" hidden>No transitions match these filters at this replay position. Clear filters or move the replay forward.</p>'
            + (''.join(cards) if cards else '<p class="empty">No transitions have been recorded yet. Run an episode to see source intake, research and account events here.</p>')
            + '</section><footer>Attention Proxy · Self-contained research replay · No external resources · Simulated account only · Prices and evidence appear as recorded</footer>'
            + '</main><script>' + _SCRIPT + '</script></body></html>\n')
