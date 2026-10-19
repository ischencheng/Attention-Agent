import { useEffect, useId, useMemo, useState } from "react";
import RecordedConversation from "./RecordedConversation";
import { asArray, classificationPresentation, evidenceLabel, graphNodes, isInputRelay, lastRevision, rawRecordId, readable, recordKey, researchTitle,
  routingDisposition, routingGroups, routingStageLabel, sourceText, sourceTimes, sourceUrl, ticketClassifications, traceStages } from "./traceModel.js";

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const formatTime = (value) => value && !Number.isNaN(new Date(value).getTime()) ? new Date(value).toLocaleString() : "Not recorded";
const humanize = (value) => readable(value).replaceAll("_", " ");

async function getJson(path, signal) {
  const response = await fetch(`${apiBase}${path}`, { signal });
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return response.json();
}

function Disclosure({ label, children, className = "" }) {
  const [expanded, setExpanded] = useState(false);
  const id = useId();
  return <div className={`trace-disclosure ${className}`}><button type="button" onClick={() => setExpanded((value) => !value)} aria-expanded={expanded} aria-controls={id}>
    <span aria-hidden="true">{expanded ? "▾" : "▸"}</span> {label}
  </button>{expanded && <div id={id}>{children}</div>}</div>;
}

export function EvidenceStatus({ evidence }) {
  const status = evidence?.status;
  return <div className={`trace-notice ${status === "verified" ? "notice-success" : "notice-warning"}`}>
    <strong>{evidenceLabel(status)}</strong>
    <span> This verifies the recorded evidence lineage, not the correctness of an investment judgment.</span>
    {asArray(evidence?.errors).length > 0 && <ul>{evidence.errors.map((error, index) => <li key={index}>{readable(error)}</li>)}</ul>}
  </div>;
}

export function SourceRecord({ row, label, allowContext = true }) {
  const payload = row.payload || row;
  const text = sourceText(payload);
  const url = sourceUrl(payload);
  const times = sourceTimes(payload, row);
  const recordId = rawRecordId(row);
  return <article className="trace-record source-record">
    <div className="trace-record-heading"><strong>{label || `Original #${row.record_id ?? row.id ?? "?"}`}</strong><span className="trace-pill">{payload.source || row.source || "Source not recorded"}</span></div>
    {payload.data?.title && <h4>{payload.data.title}</h4>}
    {row.display_limited && <p className="trace-warning">Displayed fields are a bounded projection. The SHA256 covers the complete stored original, not only this displayed excerpt.</p>}
    <p className="source-excerpt">{text ? readable(text) : "Readable source text is unavailable in this record. Inspect the recorded source fields below."}</p>
    {url && <a className="source-link" href={url} target="_blank" rel="noreferrer">Open original source ↗</a>}
    <Metadata values={[
      ["Source ID", payload.source_id || row.source_id],
      ["Published", formatTime(times.published)],
      ["Observed / ingested", formatTime(times.observed)],
      ["Filing date", payload.data?.filing_date],
      ["Raw SHA256", row.sha256 || payload.raw_sha256 || payload.sha256],
      ["Integrity status", humanize(row.hash_status || row.status || payload.verification_status)],
    ]} />
    <Disclosure className="trace-raw" label="Inspect recorded source fields"><pre>{JSON.stringify(payload.raw ?? payload, null, 2)}</pre></Disclosure>
    {allowContext && recordId && payload.source === "twitter" && <RecordedConversation recordId={recordId} renderRecord={(record) => <SourceRecord row={record} allowContext={false} />} />}
  </article>;
}

function Metadata({ values }) {
  return <dl className="trace-metadata">{values.map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{readable(value)}</dd></div>)}</dl>;
}

function RecordedList({ title, items, empty = "None recorded" }) {
  const list = asArray(items);
  return <div className="recorded-list"><h4>{title}</h4>{list.length ? <ul>{list.map((item, index) => <li key={index}>{readable(item)}</li>)}</ul> : <p className="muted">{empty}</p>}</div>;
}

function RoutingRecord({ node }) {
  const payload = node.payload || {};
  const classification = payload.classifier || payload.classification || payload;
  const verdict = routingDisposition({ ...classification, stage: payload.stage, outcome: payload.outcome, verdict: payload.verdict ?? classification.verdict });
  const relay = isInputRelay(payload);
  const display = classificationPresentation(classification, payload.stage);
  return <article className="trace-record">
    <div className="trace-record-heading"><strong>{routingStageLabel(payload)}</strong><span className={`trace-pill ${verdict === "Filtered out" ? "pill-neutral" : ""}`}>{verdict}{relay ? " · No model call" : ""}</span></div>
    <p>{payload.reason || classification.reason || (relay ? (verdict === "Forwarded" ? "The original input was forwarded without a model classification." : "Relay handling was recorded without a model classification.") : "The classification reason was not recorded.")}</p>
    <Metadata values={[["Recorded", formatTime(payload.recorded_at || node.timestamp)], ["Result model", display.model], ["Execution", display.execution],
      ["Attempt started", formatTime(payload.started_at)], ["Attempt completed", formatTime(payload.completed_at)],
      ["Verdict origin", typeof classification.classified_by_model === "boolean" ? (classification.classified_by_model ? "Model classification" : "Routing policy") : null],
      ["Coverage", classification.classification_mode], ["Full review required", typeof classification.full_review_required === "boolean" ? (classification.full_review_required ? "Yes" : "No") : null],
      ["Source ID", payload.source_id], ["Input payload hash", payload.input_payload_sha256], ["Original raw hash", payload.raw_sha256],
      ["Runtime trace ID", node.runtime_trace_id || payload.runtime_trace_id],
      ["Runtime trace coverage", humanize(node.runtime_trace_status || payload.runtime_trace_status)]]} />
    <ClassificationUsage display={display} />
  </article>;
}

function ClassificationUsage({ display }) {
  if (!display.usage) return null;
  return <Disclosure className="classification-usage" label={display.usageTitle}>
    <p className="muted">{display.usageNote}</p>
    <Metadata values={Object.entries(display.usage).map(([key, value]) => [humanize(key), value])} />
  </Disclosure>;
}

function TicketRecord({ node }) {
  const payload = node.payload || {};
  const classifications = ticketClassifications(payload);
  return <article className="trace-record">
    <div className="trace-record-heading"><strong>Attention ticket #{node.record_id}</strong><span className="trace-pill">Accepted for research</span></div>
    <p>{payload.data?.title || payload.data?.text || "No source excerpt recorded."}</p>
    <div className="classification-pair">{classifications.map(([name, result]) => { const display = classificationPresentation(result); return <div key={name}>
      <h4>{name}</h4><p>{result?.reason || "Reason not recorded for this ticket."}</p>
      <span className="muted">{display.model} · {humanize(result?.classification_mode || "coverage_not_recorded")}</span>
      <p className="muted">{display.execution}</p>
      {result.cached === true && <p className="trace-warning">Stored classification reused. Any recorded token counts describe its original computation.</p>}
      <ClassificationUsage display={display} />
      {result?.full_review_required && <p className="trace-warning">Full source review required</p>}
    </div>; })}</div>
    {!classifications.length && <p className="muted">Classification metadata was not recorded in this ticket.</p>}
    <Metadata values={[["Source", payload.source], ["Source ID", payload.source_id], ["Ticket recorded", formatTime(node.timestamp)]]} />
  </article>;
}

function AnnotationRecord({ node, historical = false }) {
  const payload = node.payload || {};
  return <article className="trace-record">
    <div className="trace-record-heading"><strong>{historical ? "Frozen annotation" : "Annotation"} #{node.record_id ?? node.id}</strong><span className="trace-pill">Mid · evidence</span></div>
    <p>{payload.summary || payload.analysis || "Summary not recorded"}</p>
    <div className="trace-gate"><strong>Publication gate</strong><p>{payload.authoritative_exception === true ? payload.authoritative_exception_reason || "Authoritative exception recorded; reason not recorded." : `${payload.signal_count ?? "Unknown"} independent signals / ${payload.source_count ?? "unknown"} sources`}</p></div>
    {asArray(payload.facts).map((fact, index) => <div className="trace-fact" key={index}>
      <p>{typeof fact === "string" ? fact : readable(fact.observation || fact.fact || fact.statement || fact.text)}</p>
      {typeof fact === "object" && <><span className="muted">{fact.source || "Attribution not recorded"}{fact.raw_id ? ` · Original #${fact.raw_id}` : ""}</span>{fact.coverage && <p className="trace-warning">Coverage: {fact.coverage}</p>}</>}
    </div>)}
    <RecordedList title="Open questions" items={payload.open_questions} />
    <Metadata values={[["Ticket IDs", asArray(payload.attention_ticket_ids).join(", ")], ["Saved", formatTime(node.timestamp || payload.timestamp)]]} />
  </article>;
}

export function HistoricalEvidence({ evidence }) {
  return <div className="trace-history-evidence">
    <EvidenceStatus evidence={evidence} />
    {asArray(evidence?.evidence_snapshot?.annotations).map((row) => <AnnotationRecord key={row.id} node={{ ...row, record_id: row.id }} historical />)}
    {asArray(evidence?.raw_messages).map((row) => <SourceRecord key={row.id} row={row} />)}
  </div>;
}

function ResearchState({ detail, item }) {
  const thesis = detail.thesis || {};
  const revision = lastRevision(detail.revisions || thesis.revisions);
  return <>
    <div className="trace-notice notice-research"><strong>{humanize(revision.current_status || item.status || "Research state")}</strong><span> · Application research state. No market performance score is implied.</span></div>
    <p className="research-claim">{revision.claim || item.claim || "Claim not recorded"}</p>
    <div className="trace-two-column"><div>
      <h4>Recorded rationale / change</h4><p>{revision.what_changed || "Not recorded"}</p>
      <Metadata values={[["Confidence", revision.confidence ?? item.confidence], ["Horizon", revision.expected_horizon], ["Next review", formatTime(thesis.next_review_at || item.next_review_at)]]} />
    </div><div>
      <RecordedList title="Evidence still needed" items={thesis.pending_questions || item.pending_questions} />
      <RecordedList title="What would invalidate this judgment" items={revision.invalidation_conditions} />
    </div></div>
    <Metadata values={[["Supporting originals", asArray(revision.supporting_raw_ids).map((id) => `#${id}`).join(", ")],
      ["Supporting annotations", asArray(revision.supporting_annotation_ids).map((id) => `#${id}`).join(", ")],
      ["Contradicting originals", asArray(revision.contradicting_raw_ids).length ? revision.contradicting_raw_ids.map((id) => `#${id}`).join(", ") : "None recorded · absence is not corroboration"],
      ["State saved", formatTime(detail.updated_at)], ["Agent", detail.agent_id], ["Thesis ID", detail.thesis_id]]} />
  </>;
}

function RevisionHistory({ detail }) {
  const revisions = asArray(detail.revisions || detail.thesis?.revisions);
  const versions = asArray(detail.versions);
  return <>
    <h3>Judgment revisions</h3>
    <p className="muted">Recorded revisions explain how the application’s judgment changed. Frozen versions are separate audit snapshots.</p>
    {!revisions.length && <p className="trace-empty">No judgment revisions recorded.</p>}
    <div className="revision-timeline">{[...revisions].reverse().map((revision, index) => <article className="trace-record" key={revision.revision_id || index}>
      <div className="trace-record-heading"><strong>{revision.revision_id || `Revision ${revisions.length - index}`}</strong><span className="trace-pill">{humanize(revision.current_status)}</span><time>{formatTime(revision.timestamp)}</time></div>
      <p>{readable(revision.claim)}</p><h4>What changed</h4><p>{readable(revision.what_changed)}</p>
      <Metadata values={[["Previous revision", revision.prior_revision || "Initial revision"], ["Confidence", revision.confidence],
        ["Supporting originals", asArray(revision.supporting_raw_ids).join(", ")], ["Contradicting originals", asArray(revision.contradicting_raw_ids).join(", ") || "None recorded"]]} />
      <RecordedList title="Invalidation conditions" items={revision.invalidation_conditions} />
    </article>)}</div>
    <h3>Independent frozen state</h3>
    <p className="muted">A snapshot establishes what was observed at the freeze time. It does not validate a claim or create a forecast.</p>
    <Metadata values={[["Archive view captured", formatTime(detail.archive_projection?.captured_at)], ["Archive freshness", detail.archive_projection?.status === "available" ? (detail.archive_projection.stale ? "Over 15 minutes old · newer state may be pending" : "Current within the scheduled refresh window") : "Unavailable"]]} />
    {versions.length ? <div className="snapshot-list">{[...versions].sort((a, b) => (b.version || 0) - (a.version || 0)).map((version, index) => <details className="trace-record" key={version.version ?? index}>
      <summary>Version {version.version} <span className="muted">· frozen {formatTime(version.frozen_at)}</span></summary>
      <Metadata values={[["Application update", formatTime(version.updated_at)], ["State SHA256", version.state_sha256], ["Ledger record hash", version.hash], ["Snapshot integrity", version.entry_hash_verified === true ? "Entry and state hashes match" : version.entry_hash_verified === false ? "Hash verification failed" : "Not verified"]]} />
      {version.thesis && <p>{readable(lastRevision(version.thesis.revisions).claim, "No claim recorded in this snapshot.")}</p>}
    </details>)}</div> : <p className="trace-empty">No frozen version available in this view.</p>}
  </>;
}

function DecisionState({ item, evidence }) {
  const payload = item.payload || {};
  return <><EvidenceStatus evidence={evidence} /><p className="research-claim">{payload.summary || "Summary not recorded"}</p>
    <Metadata values={[["Published", formatTime(item.created_at)], ["Agent", payload.agent_id], ["Confidence", payload.confidence]]} />
    <RecordedList title="Published actions" items={asArray(payload.actions).map((action) => `${humanize(action.action)} · ${asArray(action.tickers).join(", ") || action.sector || "Asset not recorded"}${action.rationale ? ` — ${action.rationale}` : ""}`)} />
    <RecordedList title="Invalidation conditions" items={payload.invalidation_conditions} />
  </>;
}

function TracePath({ graph, item, detail, evidence }) {
  const [activeStage, setActiveStage] = useState("originals");
  const stages = traceStages(graph, item.kind, evidence);
  const raw = graphNodes(graph, "raw");
  const routing = graphNodes(graph, "routing");
  const tickets = graphNodes(graph, "ticket");
  const annotations = graphNodes(graph, "annotation");
  const historical = item.kind === "decision";
  const frozenAnnotations = asArray(evidence?.evidence_snapshot?.annotations);
  const originalRows = historical ? asArray(evidence?.raw_messages) : raw;
  return <>
    <p className="trace-path-caption">Pipeline stages · select a stage to inspect its recorded evidence. Missing telemetry is shown explicitly; the recorded links below define the actual lineage.</p>
    <div className="trace-path" aria-label="Recorded decision path">{stages.map((stage, index) => <button key={stage.id} className={`trace-step ${activeStage === stage.id ? "active" : ""}`} onClick={() => setActiveStage(stage.id)} aria-pressed={activeStage === stage.id}>
      <span className="trace-step-number">{String(index + 1).padStart(2, "0")}</span><strong>{stage.label}</strong><span>{stage.detail}</span>
    </button>)}</div>
    <div className="trace-stage-content" aria-live="polite">
      {activeStage === "originals" && <><h3>Original evidence</h3><p className="muted">{historical ? "Authoritative originals resolved from the decision’s frozen evidence snapshot." : "Actual source records linked to this research. Publication time and observation time are separate."}</p>
        {originalRows.length ? originalRows.map((row) => <SourceRecord key={row.id} row={row} />) : <p className="trace-empty">No resolvable original records in this trace. This is a lineage gap, not proof that no evidence exists.</p>}</>}
      {activeStage === "routing" && <><h3>Routing dispositions</h3><p className="muted">Input relay forwarding and Fast classification, with historical relevance-filter results retained. Forwarding does not call a model; missing telemetry cannot establish rejection.</p>
        {routing.length ? routing.map((node) => <RoutingRecord key={node.id} node={node} />) : <p className="trace-empty">No routing audit was recorded for these original versions. Earlier tickets preserve acceptance but may not preserve a reason.</p>}</>}
      {activeStage === "tickets" && <><h3>Fast attention selection</h3><p className="muted">A ticket routes attention to further research; it is not an investment decision.</p>
        {tickets.length ? tickets.map((node) => <TicketRecord key={node.id} node={node} />) : <p className="trace-empty">No linked ticket records available.</p>}</>}
      {activeStage === "mid" && <><h3>Mid evidence accumulation</h3><p className="muted">{historical ? "Historical annotation content captured at publication." : "Source-attributed facts, the publication gate, and questions retained for further work."}</p>
        {(historical ? frozenAnnotations : annotations).length ? (historical ? frozenAnnotations : annotations).map((node) => <AnnotationRecord key={node.id} node={node} historical={historical} />) : <p className="trace-empty">No {historical ? "frozen" : "linked"} annotation records available.</p>}</>}
      {activeStage === "strong" && <><h3>Strong judgment</h3>{historical ? <DecisionState item={item} evidence={evidence} /> : <ResearchState detail={detail} item={item} />}</>}
      {activeStage === "outcome" && <Outcome item={item} detail={detail} evidence={evidence} />}
    </div>
    <details className="trace-links"><summary>Recorded evidence links · {asArray(graph?.edges).length}</summary>
      <p className="muted">Links show recorded references and their reported verification. A stored reference alone does not establish independent corroboration.</p>
      {asArray(graph?.edges).length ? <ul>{graph.edges.map((edge, index) => <li key={index}><code>{edge.source}</code><span>→</span><code>{edge.target}</code><span>{humanize(edge.relation)}</span><span className="trace-pill">{humanize(edge.verification || "not_verified")}</span></li>)}</ul> : <p>No links were recorded.</p>}
    </details>
  </>;
}

function Outcome({ item, detail, evidence }) {
  return <><h3>Outcome and evaluation</h3>
    {item.kind === "research" ? <><div className="trace-notice notice-research"><strong>Unscored research</strong><span> · This record is a continuing thesis, not a registered market forecast.</span></div>
      <p>{lastRevision(detail.revisions || detail.thesis?.revisions).what_changed || "No supported investment action is recorded in this research view."}</p>
      <Metadata values={[["Recorded status", humanize(item.status)], ["Next review", formatTime(detail.thesis?.next_review_at || item.next_review_at)], ["Frozen versions", asArray(detail.versions).length]]} /></>
      : <><EvidenceStatus evidence={evidence} /><p className="trace-empty">Market settlement and forecast scores are not available in this view. Evidence integrity is not a performance result.</p></>}
  </>;
}

function CycleActivity({ data, error }) {
  return <details className="cycle-panel"><summary><span>Agent cycles</span><span className="muted">Recorded inputs, completion, and state saves</span></summary>
    {error ? <p className="trace-error" role="status">Cycle history is unavailable: {error}</p> : asArray(data?.items).length ? <div className="cycle-list">{data.items.slice(0, 10).map((cycle, index) => <article key={cycle.cycle_id || index} className="cycle-row">
      <div><span className="trace-pill">{humanize(cycle.role)}</span><strong>{humanize(cycle.status)}</strong><time>{formatTime(cycle.completed_at || cycle.failed_at || cycle.started_at)}</time></div>
      <p>{cycle.summary || (cycle.error_type ? `Cycle failed: ${cycle.error_type}` : "Summary not recorded")}</p>
      <p className="muted">Selected: {asArray(cycle.selected_input_ids).join(", ") || "none"} · Reviewed: {asArray(cycle.reviewed_input_ids).join(", ") || "none"} · State save: {cycle.state_verified === true ? "verified" : "not verified"}</p>
      {cycle.usage && <details className="cycle-usage"><summary>Reported usage</summary><Metadata values={Object.entries(cycle.usage).map(([key, value]) => [humanize(key), value])} /></details>}
      <details className="cycle-usage"><summary>Tool execution events · {cycle.tool_event_count ?? asArray(cycle.tool_events).length}</summary>
        {asArray(cycle.tool_events).length ? <><p className="muted">Recorded tool names and completion statuses. Per-tool timing was not recorded.</p><ol className="tool-event-list">{cycle.tool_events.map((event, eventIndex) => <li key={`${event.item_id}-${eventIndex}`}><code>{asArray(event.tool_names).join(", ") || event.type || "Tool"}</code><span>{humanize(event.status || event.event)}</span>{event.exit_code != null && <span>exit {event.exit_code}</span>}</li>)}</ol></> : <p className="muted">Tool events were not recorded for this summary.</p>}
      </details>
    </article>)}</div> : <p className="trace-empty">No cycle summaries available yet.</p>}
    {asArray(data?.limitations).map((limitation, index) => <p key={index} className="muted">{readable(limitation)}</p>)}
  </details>;
}

function OriginalInspector() {
  const [input, setInput] = useState("");
  const [request, setRequest] = useState(null);
  const [record, setRecord] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const inputId = useId();

  useEffect(() => {
    if (!request) return;
    const controller = new AbortController();
    setRecord(null); setError(null); setLoading(true);
    getJson(`/api/traces/raw/${request.id}`, controller.signal).then((result) => {
      const original = graphNodes(result.graph || result, "raw").find((node) => rawRecordId(node) === request.id);
      if (!original) throw new Error("The requested original was not returned");
      setRecord(original);
    }).catch((problem) => { if (problem.name !== "AbortError") setError(problem.message); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [request]);

  function inspect(event) {
    event.preventDefault();
    const id = /^\d+$/.test(input.trim()) ? Number(input.trim()) : NaN;
    if (!Number.isSafeInteger(id) || id <= 0) { setError("Enter a positive original record ID."); return; }
    setRequest({ id });
  }

  return <div className="original-inspector"><form onSubmit={inspect}>
    <label htmlFor={inputId}>Original record ID</label><div className="original-inspector-controls"><input id={inputId} type="number" inputMode="numeric" min="1" max={Number.MAX_SAFE_INTEGER} step="1" value={input} onChange={(event) => setInput(event.target.value)} placeholder="Enter a stored record ID" required /><button className="refresh-button" type="submit" disabled={loading}>{loading ? "Loading…" : "Inspect original"}</button></div>
  </form><p className="muted">Open any locally stored source, including messages outside the latest routing events.</p>
    {error && <p className="trace-error" role="status">Original lookup unavailable: {error}</p>}
    {record && <SourceRecord key={rawRecordId(record)} row={record} />}
  </div>;
}

function RoutingActivity({ refresh }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [selectedKey, setSelectedKey] = useState(null);
  const [selectedSnapshot, setSelectedSnapshot] = useState(null);
  const [graph, setGraph] = useState(null);
  const [graphError, setGraphError] = useState(null);
  const [loadedKey, setLoadedKey] = useState(null);
  const groups = useMemo(() => routingGroups(data?.items), [data]);
  const selected = groups.find((group) => group.key === selectedKey) || selectedSnapshot || groups[0];
  const displayGroups = selectedKey && selectedSnapshot && !groups.some((group) => group.key === selectedKey)
    ? [{ ...selectedSnapshot, pinned: true }, ...groups] : groups;
  const identity = selected?.key;

  useEffect(() => {
    const current = groups.find((group) => group.key === selectedKey);
    if (current) setSelectedSnapshot(current);
  }, [groups, selectedKey]);

  useEffect(() => {
    const controller = new AbortController();
    getJson("/api/routing?limit=20", controller.signal).then((result) => { setData(result); setError(null); })
      .catch((problem) => { if (problem.name !== "AbortError") setError(problem.message); });
    return () => controller.abort();
  }, [refresh]);

  useEffect(() => {
    if (!selected?.raw_message_id) { setGraph(null); setLoadedKey(identity); return; }
    const controller = new AbortController();
    setGraphError(null);
    getJson(`/api/traces/raw/${selected.raw_message_id}`, controller.signal).then((result) => { setGraph(result.graph || result); setLoadedKey(identity); })
      .catch((problem) => { if (problem.name !== "AbortError") { setGraphError(problem.message); setLoadedKey(identity); } });
    return () => controller.abort();
  }, [identity, refresh]);

  const visibleGraph = loadedKey === identity ? graph : null;
  const events = graphNodes(visibleGraph, "routing").sort((a, b) => String(a.payload?.started_at || a.timestamp || "").localeCompare(String(b.payload?.started_at || b.timestamp || "")));
  const links = asArray(visibleGraph?.edges).filter((edge) => edge.source.startsWith("routing:") && edge.target.startsWith("routing:"));

  return <section className="routing-activity" aria-labelledby="routing-title">
    <div className="section-heading"><div><span className="eyebrow">Attention includes saying no</span><h3 id="routing-title">Recent routing outcomes</h3><p>Inspect accepted and filtered messages, including those that never became a thesis.</p></div><span className="trace-pill">Latest 20 events</span></div>
    <Disclosure label="Inspect an original by record ID" className="original-inspector-disclosure"><OriginalInspector /></Disclosure>
    {error && <p className="trace-error" role="status">Routing activity unavailable: {error}. {data ? "Showing the last received snapshot." : "Use Refresh to retry."}</p>}
    {!displayGroups.length ? <p className="trace-empty">{data ? "No persisted routing events yet. Historical verdicts are not reconstructed." : error ? "Routing records could not be loaded." : "Loading routing events…"}</p> : <div className="routing-workspace">
      <div className="routing-list" aria-label="Messages with recorded routing outcomes">{displayGroups.map((group) => <button key={group.key} className={`routing-item ${identity === group.key ? "selected" : ""}`} onClick={() => { setSelectedKey(group.key); setSelectedSnapshot(group); }} aria-pressed={identity === group.key}>
        {group.pinned && <span className="trace-pill">Pinned selection · outside latest events</span>}
        <strong>{group.title || group.source || "Unknown source"}</strong>
        {group.excerpt && <span className="research-item-preview">{group.excerpt}</span>}
        <span className="routing-source-id">{group.source_id || "Source identity unavailable"}</span>
        <span className="routing-verdicts">{group.events.map((event) => <span key={event.id} className={`trace-pill ${event.outcome === "reject" ? "pill-rejected" : ""}`}>{routingStageLabel(event)} · {routingDisposition(event)}{isInputRelay(event) ? " · No model call" : ""}</span>)}</span>
        <time>{formatTime(group.events[0].payload?.recorded_at || group.events[0].created_at)}</time>
      </button>)}</div>
      <div className="routing-detail">
        <div className="trace-record-heading"><strong>Recorded routing attempts</strong><span className="trace-pill">{selected.raw_message_id ? `Original #${selected.raw_message_id}` : "Original unresolved"}</span></div>
        {!selected.raw_message_id ? <><p className="trace-empty">No matching original could be resolved by source identity and raw hash. These recorded attempts are shown without an inferred evidence link.</p>{selected.events.map((event) => <RoutingRecord key={event.id} node={{ id: event.id, payload: event.payload, timestamp: event.created_at }} />)}</>
          : loadedKey !== identity ? <p className="trace-empty" role="status">Loading this original’s routing path…</p>
          : graphError ? <p className="trace-error" role="status">Routing trace unavailable: {graphError}. Use Refresh to retry.</p>
          : <>
            {links.length ? <div className="routing-chain" aria-label="Verified routing links">{links.map((edge, index) => <div key={index}><code>{edge.source}</code><span>→</span><code>{edge.target}</code><span className="trace-pill">{humanize(edge.verification)}</span></div>)}</div>
              : <p className="muted">These attempts share an original. No inter-stage broker link is recorded in this view.</p>}
            {events.length ? events.map((node) => <RoutingRecord key={node.id} node={node} />) : <p className="trace-empty">No matching routing attempts available in this trace.</p>}
            {graphNodes(visibleGraph, "raw").map((node) => <Disclosure className="routing-original" key={node.id} label={`Read original source · #${node.record_id}`}><SourceRecord row={node} /></Disclosure>)}
          </>}
      </div>
    </div>}
    {asArray(data?.limitations).length > 0 && <details className="trace-limitations"><summary>Routing coverage</summary><ul>{data.limitations.map((value, index) => <li key={index}>{readable(value)}</li>)}</ul></details>}
  </section>;
}

export default function TraceExplorer() {
  const [research, setResearch] = useState([]);
  const [decisions, setDecisions] = useState([]);
  const [cycles, setCycles] = useState(null);
  const [cycleError, setCycleError] = useState(null);
  const [listError, setListError] = useState(null);
  const [listLoaded, setListLoaded] = useState(false);
  const [selectedKey, setSelectedKey] = useState(null);
  const [detail, setDetail] = useState(null);
  const [evidence, setEvidence] = useState(null);
  const [detailError, setDetailError] = useState(null);
  const [activeTab, setActiveTab] = useState("trace");
  const [refresh, setRefresh] = useState(0);
  const items = useMemo(() => [...research.map((item) => ({ ...item, kind: "research" })), ...decisions.map((item) => ({ ...item, kind: "decision" }))], [research, decisions]);
  const selected = items.find((item) => recordKey(item) === selectedKey) || items[0];
  const selectionIdentity = selected ? recordKey(selected) : null;

  useEffect(() => {
    const timer = setInterval(() => setRefresh((value) => value + 1), 30000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([getJson("/api/research?limit=50", controller.signal), getJson("/api/decisions?limit=50", controller.signal)])
      .then(([researchData, decisionData]) => {
        setResearch(asArray(researchData.items)); setDecisions(asArray(decisionData.items)); setListError(null); setListLoaded(true);
      }).catch((error) => { if (error.name !== "AbortError") { setListError(error.message); setListLoaded(true); } });
    getJson("/api/cycles?limit=20", controller.signal).then((data) => { setCycles(data); setCycleError(null); })
      .catch((error) => { if (error.name !== "AbortError") setCycleError(error.message); });
    return () => controller.abort();
  }, [refresh]);

  useEffect(() => { setDetail(null); setEvidence(null); setDetailError(null); }, [selectionIdentity]);

  useEffect(() => {
    if (!selected) return;
    const controller = new AbortController();
    const query = selected.kind === "research"
      ? getJson(`/api/research/detail?agent_id=${encodeURIComponent(selected.agent_id)}&thesis_id=${encodeURIComponent(selected.thesis_id)}`, controller.signal)
      : getJson(`/api/traces/decision/${selected.id}`, controller.signal);
    const evidenceQuery = selected.kind === "decision" ? getJson(`/api/decisions/${selected.id}/evidence`, controller.signal) : Promise.resolve(null);
    Promise.all([query, evidenceQuery]).then(([nextDetail, nextEvidence]) => {
      setDetail(nextDetail); setEvidence(nextEvidence); setDetailError(null);
    }).catch((error) => { if (error.name !== "AbortError") setDetailError(error.message); });
    return () => controller.abort();
    // The stable identity keeps a list refresh from causing duplicate requests.
  }, [selectionIdentity, refresh]);

  function selectItem(item) { setSelectedKey(recordKey(item)); setActiveTab("trace"); }
  const graph = detail?.graph || detail || {};
  const limitations = [...asArray(detail?.limitations), ...asArray(detail?.graph?.limitations)];

  return <section className="trace-explorer" aria-labelledby="trace-title">
    <div className="section-heading"><div><span className="eyebrow">Evidence → judgment → review</span><h2 id="trace-title">Decision trace</h2><p>Follow the evidence, see why attention moved forward, and inspect what remains unresolved.</p></div><button className="refresh-button" onClick={() => setRefresh((value) => value + 1)}>Refresh</button></div>
    {listError && <div className="trace-error" role="status">Research and decision list unavailable: {listError}. {items.length > 0 ? "Showing the last received list." : "Retry when the API is available."}</div>}
    <div className="trace-workspace"><aside className="trace-sidebar" aria-label="Research and decision records">
      <div className="trace-sidebar-heading"><strong>Continuing judgments</strong><span>{items.length}</span></div>
      {items.length > 0 && <label className="mobile-record-picker"><span className="sr-only">Select a research thesis or decision</span><select value={selectionIdentity || ""} onChange={(event) => { const item = items.find((record) => recordKey(record) === event.target.value); if (item) selectItem(item); }}>{items.map((item) => <option key={recordKey(item)} value={recordKey(item)}>{item.kind === "research" ? researchTitle(item) : item.payload?.title || `Decision #${item.id}`}</option>)}</select></label>}
      {!items.length && <p className="trace-empty">{!listLoaded ? "Loading research…" : listError ? "Research could not be loaded." : "No saved theses or decisions yet. Published activity appears below."}</p>}
      {items.map((item) => <button key={recordKey(item)} className={`research-item ${selectionIdentity === recordKey(item) ? "selected" : ""}`} onClick={() => selectItem(item)} aria-pressed={selectionIdentity === recordKey(item)}>
        <span className={`trace-pill ${item.kind === "research" ? "pill-research" : "pill-decision"}`}>{item.kind === "research" ? humanize(item.status || "research") : "Published decision"}</span>
        <strong>{item.kind === "research" ? researchTitle(item) : item.payload?.title || `Decision #${item.id}`}</strong>
        <span className="research-item-preview">{item.claim || item.payload?.summary || "No summary recorded"}</span>
        <span className="research-item-time">{formatTime(item.updated_at || item.created_at)}</span>
      </button>)}
      <p className="sidebar-note">Latest 50 research records and 50 decisions. No fixed portfolio.</p>
    </aside><div className="trace-main">
      {!selected ? <div className="trace-welcome"><span className="trace-orbit">◎</span><h3>Judgment begins with evidence</h3><p>Saved research will appear here even when it does not support an investment action.</p></div> : <>
        <div className="trace-detail-heading"><div><span className="eyebrow">{selected.kind === "research" ? "Continuing research" : "Published decision"}</span><h3>{selected.kind === "research" ? researchTitle(selected, detail?.thesis) : selected.payload?.title || `Decision #${selected.id}`}</h3></div><span className="trace-pill">{selected.kind === "research" ? "Unscored" : evidence ? humanize(evidence.status) : "Checking evidence"}</span></div>
        <div className="trace-tabs" role="tablist" aria-label="Judgment views">{[["trace", "Evidence trace"], ["state", "Current state"], ["history", "History"], ["outcome", "Outcome"]].map(([id, label]) => <button key={id} role="tab" id={`trace-tab-${id}`} aria-selected={activeTab === id} aria-controls="trace-content" onClick={() => setActiveTab(id)} className={activeTab === id ? "active" : ""}>{label}</button>)}</div>
        <div id="trace-content" role="tabpanel" aria-labelledby={`trace-tab-${activeTab}`}>
          {detailError ? <div className="trace-error" role="status">This trace could not be loaded: {detailError}. Use Refresh to retry.</div> : !detail ? <p className="trace-empty" role="status">Loading recorded evidence and state…</p> : <>
            {activeTab === "trace" && <TracePath key={selectionIdentity} graph={graph} item={selected} detail={detail} evidence={evidence} />}
            {activeTab === "state" && (selected.kind === "research" ? <ResearchState detail={detail} item={selected} /> : <DecisionState item={selected} evidence={evidence} />)}
            {activeTab === "history" && (selected.kind === "research" ? <RevisionHistory detail={detail} /> : <HistoricalEvidence evidence={evidence} />)}
            {activeTab === "outcome" && <Outcome item={selected} detail={detail} evidence={evidence} />}
            {limitations.length > 0 && <details className="trace-limitations"><summary>Coverage and limitations</summary><ul>{[...new Set(limitations.map((value) => readable(value)))].map((value) => <li key={value}>{value}</li>)}</ul></details>}
          </>}
        </div>
      </>}
    </div></div>
    <RoutingActivity refresh={refresh} />
    <CycleActivity data={cycles} error={cycleError} />
  </section>;
}
