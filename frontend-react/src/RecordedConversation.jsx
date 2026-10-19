import { useEffect, useId, useRef, useState } from "react";
import { asArray, readable, sourceUrl } from "./traceModel.js";

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const humanize = (value) => readable(value).replaceAll("_", " ");

export default function RecordedConversation({ recordId, renderRecord }) {
  const [expanded, setExpanded] = useState(false);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [retrievedAt, setRetrievedAt] = useState(null);
  const controllerRef = useRef(null);
  const panelId = `conversation-${useId().replaceAll(":", "")}`;

  useEffect(() => () => controllerRef.current?.abort(), []);
  useEffect(() => {
    controllerRef.current?.abort();
    setData(null); setError(null); setLoading(false); setExpanded(false); setRetrievedAt(null);
  }, [recordId]);

  async function loadContext() {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    setLoading(true); setError(null);
    try {
      const response = await fetch(`${apiBase}/api/raw/${recordId}/context?limit=30`, { signal: controller.signal });
      if (!response.ok) throw new Error(`Request failed (${response.status})`);
      const next = await response.json();
      if (next.anchor_raw_id !== recordId) throw new Error("The returned context does not match the selected original");
      if (controller.signal.aborted) return;
      setData(next); setRetrievedAt(new Date().toISOString());
    } catch (problem) {
      if (problem.name !== "AbortError") setError(problem.message);
    } finally { if (!controller.signal.aborted) setLoading(false); }
  }

  function toggle() {
    const next = !expanded;
    setExpanded(next);
    if (next && !data && !loading) loadContext();
  }

  const records = asArray(data?.records);
  const edges = asArray(data?.edges);
  const missing = asArray(data?.missing_references);
  const canShowRecords = data?.status === "ok";
  const recordAnchor = (id) => `${panelId}-raw-${id}`;

  return <section className="recorded-conversation">
    <button className="conversation-toggle" type="button" aria-expanded={expanded} aria-controls={panelId} onClick={toggle}>
      <span aria-hidden="true">{expanded ? "▾" : "▸"}</span> {expanded ? "Hide recorded conversation" : "Read recorded conversation"}
    </button>
    {expanded && <div id={panelId} className="conversation-panel">
      <div className="conversation-heading"><strong>Recorded conversation context</strong><span className="trace-pill">Locally collected</span></div>
      <p className="conversation-boundary">Context retrieved now is separate from frozen decision evidence. Nearby records were not necessarily available, read, or cited when the original judgment was made.</p>
      {loading && <p className="muted" role="status">Loading native reply-linked originals…</p>}
      {error && <div className="trace-error" role="status">Conversation context could not be loaded: {error}. <button className="inline-retry" type="button" onClick={loadContext}>Retry</button></div>}
      {data && <>
        <div className="conversation-capture"><span>Context view fetched {retrievedAt ? new Date(retrievedAt).toLocaleString() : "at an unrecorded time"}</span><button className="inline-retry" type="button" onClick={loadContext} disabled={loading}>Refresh context</button></div>
        <p className="muted">Scope: recorded reply ancestors only. This view does not claim to contain every reply or the full conversation.</p>
        {data.status === "unsupported" && <p className="trace-empty">No supported native message identity is available for this record. No conversation chain was inferred.</p>}
        {data.status === "identity_mismatch" && <p className="trace-error">The original’s native message identity does not match its envelope. No conversation chain is presented as verified.</p>}
        {!canShowRecords && !["unsupported", "identity_mismatch"].includes(data.status) && <p className="trace-error">Conversation lookup returned an unrecognized status. No linked context is shown.</p>}
        {canShowRecords && <>
          <div className="conversation-coverage"><span>{records.length} local record{records.length === 1 ? "" : "s"}</span><span>{edges.length} native reply link{edges.length === 1 ? "" : "s"}</span>{missing.length > 0 && <span className="trace-warning">{missing.length} missing reference{missing.length === 1 ? "" : "s"}</span>}</div>
          {data.limits?.truncated === true && <p className="trace-notice notice-warning">This context reached the bounded lookup limit ({data.limits.requested ?? 30} records). Additional ancestors may exist beyond the returned records.</p>}
          {edges.length > 0 ? <div className="conversation-links"><h4>Native reply links</h4><p className="muted">A parent reference links message identities. It does not verify which version of a parent was visible at reply time.</p>
            <ul>{edges.map((edge, index) => <li key={`${edge.from_raw_id}-${edge.to_raw_id}-${index}`}>
              <a href={`#${recordAnchor(edge.from_raw_id)}`}>Original #{edge.from_raw_id}</a><span>replies to</span><a href={`#${recordAnchor(edge.to_raw_id)}`}>Original #{edge.to_raw_id}</a><span className="trace-pill">Native parent reference</span>
            </li>)}</ul></div> : <p className="trace-empty">No reply-parent link was resolved from the locally collected records. This does not establish that the selected post starts or completes a conversation.</p>}
          {missing.length > 0 && <div className="conversation-missing"><h4>Missing context</h4><ul>{missing.map((reference, index) => {
            const url = sourceUrl({ source: reference.source, source_id: reference.source_id });
            return <li key={`${reference.from_raw_id}-${reference.native_message_id}-${index}`}><strong>Original #{reference.from_raw_id}</strong><span> references {readable(reference.source_id || reference.native_message_id)}.</span><p>{humanize(reference.reason)}</p>{url && <a href={url} target="_blank" rel="noreferrer">Open referenced source ↗</a>}</li>;
          })}</ul></div>}
          {asArray(data.identity_conflicts).length > 0 && <div className="conversation-missing"><h4>Identity conflicts</h4><ul>{data.identity_conflicts.map((conflict, index) => <li key={index}>Stored original #{conflict.candidate_raw_id} did not match the referenced native message identity. It was excluded from this chain.</li>)}</ul></div>}
          <div className="conversation-records">{records.map((record) => <div className="conversation-record" id={recordAnchor(record.id)} key={record.id}>
            <div className="conversation-record-label"><strong>{record.is_anchor ? "Selected original" : "Recorded ancestor"}</strong><span className="muted">Native message {readable(record.native_context?.message_id)}</span></div>
            {renderRecord(record)}
          </div>)}</div>
        </>}
        {asArray(data.warnings).length > 0 && <div className="conversation-warnings"><h4>Coverage notes</h4><ul>{data.warnings.map((warning, index) => <li key={index}>{readable(warning)}</li>)}</ul></div>}
      </>}
    </div>}
  </section>;
}
