import { useEffect, useState } from "react";
import { asArray, auditTraceLink, observabilityLabel, safeSourceUrl } from "./traceModel.js";

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const time = (value) => value && !Number.isNaN(new Date(value).getTime()) ? new Date(value).toLocaleString() : "Not recorded";
const count = (value) => Number.isSafeInteger(value) && value >= 0 ? value.toLocaleString() : "Unknown";

export function AuditTraceLink({ record }) {
  const url = auditTraceLink(record);
  return url ? <p className="audit-trace-link"><a href={url} target="_blank" rel="noreferrer">Open recorded audit trace ↗</a><span>Collector accepted · open Jaeger to check availability</span></p> : null;
}

export default function ObservabilityPanel({ refresh }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    fetch(`${apiBase}/api/observability`, { signal: controller.signal }).then((response) => {
      if (!response.ok) throw new Error("Status unavailable");
      return response.json();
    }).then((value) => { setData(value); setError(false); })
      .catch((problem) => { if (problem.name !== "AbortError") setError(true); });
    return () => controller.abort();
  }, [refresh]);
  const exporter = data?.exporter || {};
  const jaeger = data?.jaeger || {};
  return <section className="observability-panel" aria-labelledby="observability-title">
    <div className="section-heading"><div><span className="eyebrow">Recorded execution</span><h3 id="observability-title">Observability</h3><p>Inspect routing attempts and agent cycle durations in Jaeger.</p></div>
      <span className={`trace-pill ${error || data?.status !== "healthy" ? "pill-neutral" : ""}`}>{error ? "Status unavailable" : data ? observabilityLabel(data.status) : "Checking status…"}</span></div>
    {error && <p className="trace-error" role="status">The observability status request failed. {data ? "The information below is the last received snapshot." : "Use Refresh to retry."}</p>}
    {data && <>
      <div className="observability-grid"><div className="observability-card"><h4>Audit exporter</h4><strong>{observabilityLabel(exporter.status)}</strong>
        <p>Updated {time(exporter.updated_at)}{exporter.age_seconds != null ? ` · ${Math.round(exporter.age_seconds)}s old at check` : ""}</p>
        {exporter.stale && <p className="trace-warning">This status is over {exporter.stale_after_seconds} seconds old. Current exporter activity is unknown.</p>}
        <p>Last acceptance {time(exporter.last_success_at)}</p>
        {exporter.error_type && <p className="trace-warning">Recorded error: {exporter.error_type}</p>}
        {exporter.reported_status === "blocked" && <p className="muted">Some audits require operator repair and explicit replay before export.</p>}
      </div><div className="observability-card"><h4>Jaeger query service</h4><strong>{jaeger.status === "available" ? "Query API available" : "Query API unavailable"}</strong>
        <p>Checked {time(jaeger.checked_at)}</p>
        <p>{asArray(jaeger.application_services).length ? `Application services: ${jaeger.application_services.join(", ")}` : "No application services returned by this check."}</p>
        <div className="observability-searches">{asArray(jaeger.search_links).map((link) => safeSourceUrl(link.url) && <a key={link.service} href={safeSourceUrl(link.url)} target="_blank" rel="noreferrer">{link.service} · last 2 days ↗</a>)}</div>
        <p className="muted">Service discovery does not confirm that every accepted trace remains indexed.</p>
      </div></div>
      <div className="observability-table-wrap"><table className="observability-table"><caption>Collector acknowledgments and the latest scan</caption><thead><tr><th scope="col">Audit type</th><th scope="col">Accepted exports</th><th scope="col">Pending in scanned window</th><th scope="col">Blocked exports</th><th scope="col">Scan truncated</th></tr></thead><tbody>{["routing", "cycle"].map((kind) => <tr key={kind}><th scope="row">{kind === "routing" ? "Routing attempts" : "Agent cycles"}</th><td>{count(exporter.exported_counts?.[kind])}</td><td>{exporter.pending_scope === "scanned_window" ? count(exporter.pending_counts?.[kind]) : "Unknown"}</td><td>{count(exporter.blocked_counts?.[kind])}</td><td>{typeof exporter.scan_truncated?.[kind] === "boolean" ? (exporter.scan_truncated[kind] ? "Yes" : "No") : "Unknown"}</td></tr>)}</tbody></table></div>
      <p className="muted">Accepted exports are receipt totals. Pending counts cover the scanned window; they do not measure the entire backlog. A healthy exporter does not mean all historical records have been exported.</p>
      <details className="observability-recent" open><summary>Recent accepted audit traces · {asArray(data.recent_traces).length}</summary>
        {asArray(data.recent_traces).length ? <ol>{data.recent_traces.map((record) => <li key={`${record.kind}:${record.key}`}><div><strong>{record.service}</strong><span>{record.name}</span><time>{time(record.start)} → {time(record.end)}</time></div><AuditTraceLink record={record} /></li>)}</ol> : <p className="trace-empty">No recent trace with a matching export receipt is available.</p>}
      </details>
      <details className="trace-limitations"><summary>Timing and coverage</summary><ul>{asArray(data.limitations).map((note) => <li key={note}>{note}</li>)}</ul></details>
      <p className="observability-boundary">These spans use recorded audit start/end times. They do not reconstruct live propagation, per-tool timing, or model-internal activity.</p>
    </>}
  </section>;
}
