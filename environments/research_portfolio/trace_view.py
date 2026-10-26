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
    for item in (result, action):
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


def _chart(trajectory):
    points = []
    for record in trajectory:
        account = _mapping(_observation(record).get("account"))
        nav = _number(account.get("nav"))
        if nav is not None:
            point = (str(_snapshot_time(record)), nav)
            if not points or point != points[-1]:
                points.append(point)
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
    circles = ''.join('<circle cx="' + f'{x:.2f}' + '" cy="' + f'{y:.2f}'
                      + '" r="3"><title>' + _escape(date) + ': ' + _money(value)
                      + '</title></circle>' for (x, y), (date, value) in zip(coords, points))
    return ('<svg class="nav-chart" role="img" aria-labelledby="nav-title nav-description" viewBox="0 0 '
            + str(width) + ' ' + str(height) + '"><title id="nav-title">Observed paper account NAV</title>'
            + '<desc id="nav-description">' + _escape(descriptions) + ' Horizontal spacing follows recorded transitions.</desc>'
            + '<line x1="82" y1="162" x2="932" y2="162" class="axis"/>'
            + '<text x="72" y="30" text-anchor="end">' + _money(maximum) + '</text>'
            + '<text x="72" y="160" text-anchor="end">' + _money(minimum) + '</text>'
            + '<polyline points="' + path + '"/>' + circles + '</svg>'
            + '<div class="chart-labels"><span>' + _escape(points[0][0]) + '</span><span>'
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
  let stage = '';
  function filter() {
    const query = search.value.trim().toLowerCase();
    let visible = 0;
    for (const event of events) {
      const show = (!query || event.dataset.search.includes(query)) &&
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
    });
  }
  search.addEventListener('input', filter);
  role.addEventListener('change', filter);
  action.addEventListener('change', filter);
  document.querySelectorAll('[data-stage]').forEach(button => button.addEventListener('click', () => {
    stage = stage === button.dataset.stage ? '' : button.dataset.stage;
    filter();
    document.getElementById('trace-title').scrollIntoView({behavior: 'smooth', block: 'start'});
  }));
  document.querySelectorAll('[data-reference]').forEach(button => button.addEventListener('click', () => {
    search.value = button.dataset.reference;
    role.value = '';
    action.value = '';
    stage = '';
    filter();
    search.focus({preventScroll: true});
    document.getElementById('trace-title').scrollIntoView({behavior: 'smooth', block: 'start'});
  }));
  document.getElementById('clear-filters').addEventListener('click', () => {
    search.value = ''; role.value = ''; action.value = ''; stage = ''; filter();
  });
  document.getElementById('expand-visible').addEventListener('click', () => {
    const visible = events.filter(event => !event.hidden);
    const shouldOpen = visible.some(event => [...event.querySelectorAll('details')].some(detail => !detail.open));
    visible.forEach(event => event.querySelectorAll('details').forEach(detail => { detail.open = shouldOpen; }));
  });
  filter();
})();
"""

_STYLE = """
:root{color-scheme:dark;--bg:#101820;--surface:#17232e;--line:#344652;--text:#e7eff3;--muted:#a9beca;--mint:#8fe2c1;--amber:#ffd894}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.55 ui-sans-serif,system-ui,-apple-system,sans-serif}
main{max-width:1240px;margin:auto;padding:40px 26px 70px}h1,h2,h3,p{margin-top:0}h1{font-size:clamp(28px,4vw,42px);line-height:1.15;margin-bottom:12px;letter-spacing:-.03em}h2{font-size:22px;letter-spacing:-.02em}h3{font-size:17px;margin:0}button,input,select{font:inherit}button,select,input{color:var(--text);background:#13202a;border:1px solid var(--line);border-radius:7px;padding:9px 12px}button{cursor:pointer}button:hover,button[aria-pressed=true]{border-color:var(--mint);color:var(--mint)}button:focus-visible,input:focus-visible,select:focus-visible,summary:focus-visible{outline:2px solid var(--mint);outline-offset:3px}.eyebrow{color:var(--mint);letter-spacing:.13em;text-transform:uppercase;font-size:11px;font-weight:700;margin-bottom:10px}.subtle,.meta,.empty{color:var(--muted)}.subtle{font-size:13px}.mode-notice{padding:14px 18px;background:#302b21;border:1px solid #68563a;border-radius:8px;margin:20px 0;color:var(--amber)}.mode-notice strong{display:block}.mode-notice p{margin:4px 0 0;font-size:13px}.overview{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:24px 0}.metric,.panel,.event{background:var(--surface);border:1px solid var(--line);border-radius:11px}.metric{padding:17px}.metric-label{font-size:12px;color:var(--muted);display:block}.metric-value{font-size:24px;font-variant-numeric:tabular-nums;display:block;margin-top:4px;overflow-wrap:anywhere}.metric-foot{display:block;color:var(--muted);font-size:11px;margin-top:5px}.panel{padding:22px;margin:20px 0}.pipeline{display:flex;align-items:stretch;gap:7px;overflow:auto;padding:3px 1px 9px}.stage{min-width:106px;text-align:left;flex:1}.stage small{display:block;font-size:11px;color:var(--muted)}.stage-arrow{align-self:center;color:var(--muted)}.two-col{display:grid;grid-template-columns:1.4fr 1fr;gap:20px}.two-col .panel{margin-top:0}.nav-chart{width:100%;height:auto;display:block}.nav-chart text{fill:var(--muted);font-size:10px}.nav-chart polyline{fill:none;stroke:var(--mint);stroke-width:2.5;stroke-linejoin:round}.nav-chart circle{fill:var(--mint)}.axis{stroke:var(--line);stroke-width:1}.chart-labels{display:flex;justify-content:space-between;gap:15px;font-size:11px;color:var(--muted);overflow-wrap:anywhere}.table-wrap{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:right;padding:9px 7px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child{text-align:left}thead{color:var(--muted)}.filters{display:flex;gap:12px;align-items:end;flex-wrap:wrap;margin-bottom:13px}.filters label{display:grid;gap:5px;font-size:12px;color:var(--muted)}.search-label{flex:1;min-width:220px}.filters input{width:100%}.trace-heading{display:flex;gap:15px;justify-content:space-between;align-items:baseline;margin:20px 0 10px}.trace-heading h2{margin:0;scroll-margin-top:20px}.event{padding:20px;margin:13px 0;border-left:3px solid #516d80;overflow-wrap:anywhere}.event[data-role=Fast],.event[data-role=fast]{border-left-color:#88b9e4}.event[data-role=Mid],.event[data-role=mid]{border-left-color:#ccb0f2}.event[data-role=Strong],.event[data-role=strong]{border-left-color:var(--mint)}.event[hidden]{display:none}.event-head{display:flex;justify-content:space-between;gap:12px;align-items:start}.badges{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:7px}.badge{font-size:11px;border:1px solid var(--line);border-radius:4px;padding:2px 7px;color:var(--muted)}.badge.role{color:var(--text);font-weight:650}.reward{font-variant-numeric:tabular-nums;font-size:12px;color:var(--mint);white-space:nowrap}.meta{font-size:12px;margin:7px 0 12px}.description{font-size:14px}.references{display:flex;gap:6px;flex-wrap:wrap;margin:13px 0}.references button{padding:3px 7px;font-size:11px;max-width:100%;overflow-wrap:anywhere}.event-details{display:grid;grid-template-columns:1fr 1fr;gap:0 15px}.event-details .wide{grid-column:1/-1}details{border-top:1px solid var(--line);margin-top:10px;padding-top:10px;min-width:0}summary{cursor:pointer;font-size:13px;color:var(--muted)}pre{max-height:560px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;border-radius:6px;padding:13px;background:#101a23;font:12px/1.6 ui-monospace,SFMono-Regular,Consolas,monospace}.error{color:#ffb6aa}.empty{padding:14px 0}.account-line{display:flex;gap:18px;flex-wrap:wrap;font-size:13px}.account-line strong{font-variant-numeric:tabular-nums}footer{margin-top:30px;color:var(--muted);font-size:12px}@media(max-width:760px){main{padding:25px 15px}.overview{grid-template-columns:1fr 1fr}.two-col,.event-details{grid-template-columns:1fr}.event-details .wide{grid-column:auto}.event-head{display:block}.panel,.event{padding:16px}.trace-heading{display:block}.trace-heading p{margin-top:7px}.metric-value{font-size:21px}}
"""


def render_trace(summary: dict, trajectory: list[dict]) -> str:
    """Render public transitions with local filtering and explicit provenance.

    All dynamic content is HTML-escaped. JavaScript is fixed, hash-authorized,
    and operates only on the rendered DOM; scenario content is never executable.
    """
    summary = _mapping(summary)
    trajectory = [record for record in trajectory if isinstance(record, dict)]
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
                        if _mapping(record.get("invocation")).get("policy") == "codex"}))
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
                            else "Policy provenance")
        invocation_view = _details(invocation_label, invocation) if invocation else ''
        error_class = ' error' if _mapping(result).get("error") or _mapping(result).get("rejection") else ''
        cards.append('<article class="event" id="event-' + str(index) + '" data-role="' + _escape(role)
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
    script_hash = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
    header = ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
              + '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'sha256-' + script_hash
              + '\'; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'">'
              + '<title>Research to portfolio · ' + _escape(summary.get("episode_id", "Episode trace")) + '</title><style>' + _STYLE + '</style></head><body><main>')
    return (header + '<header><p class="eyebrow">Attention Proxy / Research portfolio environment</p><h1>From incoming evidence to portfolio return</h1>'
            + '<p class="subtle">' + _escape(summary.get("episode_id", "Episode trace")) + ' · The full research loop continues while positions are held.</p>'
            + '</header><aside class="mode-notice"><strong>' + _escape(mode_label) + '</strong><p>This report shows recorded observations, '
            + 'tool results and decisions. It does not expose hidden model reasoning or establish investment skill.</p></aside>' + evaluation_notice
            + '<div class="overview"><div class="metric"><span class="metric-label">Episode status</span><strong class="metric-value">' + status
            + '</strong><span class="metric-foot">' + _escape(summary.get("terminal_reason", summary.get("as_of", "Awaiting settlement"))) + '</span></div>'
            + '<div class="metric"><span class="metric-label">Final reward</span><strong class="metric-value">' + reward_label
            + '</strong><span class="metric-foot">' + reward_foot + '</span></div>'
            + '<div class="metric"><span class="metric-label">Account NAV</span><strong class="metric-value">' + _money(account.get("nav"))
            + '</strong><span class="metric-foot">Cash ' + _money(account.get("cash")) + '</span></div>'
            + '<div class="metric"><span class="metric-label">Recorded transitions</span><strong class="metric-value">' + str(len(trajectory))
            + '</strong><span class="metric-foot">' + str(model_calls) + ' Codex model calls</span></div></div>'
            + '<section class="panel"><h2>One continuous decision process</h2><div class="pipeline" aria-label="Filter transitions by decision stage">'
            + stage_buttons + '</div><p class="subtle">Fast routes information. Mid accumulates facts. Strong investigates, revises judgments '
            + 'and proposes allocations. Broker events and later returns complete the same trace. Select a stage to inspect its transitions.</p></section>'
            + '<div class="two-col"><section class="panel"><h2>Account value through the episode</h2>' + _chart(trajectory) + '</section>'
            + '<section class="panel"><h2>Latest recorded positions</h2>' + _positions(account) + _details("Account, orders and execution ledger", account)
            + '</section></div><section class="panel"><h2>Run provenance</h2><p class="subtle">' + _escape(provenance)
            + '</p>' + _details("Complete summary and recorded counts", summary) + '</section>'
            + '<section aria-labelledby="trace-title"><div class="trace-heading"><h2 id="trace-title">Decision trace</h2><p class="subtle" id="event-count" aria-live="polite">'
            + str(len(trajectory)) + ' recorded transitions</p></div><div class="filters"><label class="search-label">Search actions, results or identifiers'
            + '<input id="trace-search" type="search" placeholder="Question, evidence ID, thesis, order…" autocomplete="off"></label>'
            + '<label>Role<select id="role-filter"><option value="">All roles</option>' + options(roles) + '</select></label>'
            + '<label>Action<select id="action-filter"><option value="">All actions</option>' + options(actions) + '</select></label>'
            + '<button type="button" id="clear-filters">Clear filters</button><button type="button" id="expand-visible">Expand / collapse visible</button></div>'
            + '<p class="subtle">Identifier chips find connected actions and results. Expand observations for the exact public state available to the policy.</p>'
            + '<p id="no-matches" class="empty" hidden>No transitions match these filters. Clear filters to show the full trace.</p>'
            + (''.join(cards) if cards else '<p class="empty">No transitions have been recorded yet. Run an episode to see source intake, research and account events here.</p>')
            + '</section><footer>Self-contained report · No external resources · Simulated account only · Prices and evidence appear as recorded</footer>'
            + '</main><script>' + _SCRIPT + '</script></body></html>\n')
