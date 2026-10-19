import { useEffect, useMemo, useState } from "react";
import TraceExplorer, { HistoricalEvidence } from "./TraceExplorer";

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");

function buildApiUrl(path) {
  return apiBase ? `${apiBase}${path}` : path;
}

function buildWsUrl() {
  if (import.meta.env.VITE_WS_URL) return import.meta.env.VITE_WS_URL;
  const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${wsProtocol}://${window.location.host}/ws/signals`;
}

function fmtTime(ts) {
  if (!ts) return "-";
  const date = new Date(ts);
  return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString();
}

const SOURCE_COLORS = {
  bluesky: "#3b82f6",
  twitter: "#1d9bf0",
  polygon: "#8b5cf6",
  finnhub: "#22c55e",
};

const URGENCY_COLORS = {
  critical: "#ef4444",
  high: "#f97316",
  medium: "#eab308",
  low: "#22c55e",
};

function SourceBadge({ source }) {
  const color = SOURCE_COLORS[source] || "#6b7280";
  return <span className="badge" style={{ borderColor: color, color }}>{source}</span>;
}

function UrgencyBadge({ urgency }) {
  if (!urgency) return null;
  const color = URGENCY_COLORS[urgency] || "#6b7280";
  return <span className="badge" style={{ borderColor: color, color }}>{urgency}</span>;
}

function ThemeTags({ themes }) {
  if (!themes || !themes.length) return null;
  return (
    <div className="tags">
      {themes.map((t) => <span key={t} className="tag">{t}</span>)}
    </div>
  );
}

function TicketDetail({ row }) {
  const p = row.payload || {};
  const data = p.data || {};
  return (
    <article className="feed-card ticket-card">
      <div className="card-header">
        <span className="card-type ticket-type">TICKET</span>
        <SourceBadge source={p.source || "?"} />
        <span className="muted">{fmtTime(p.timestamp || row.created_at)}</span>
      </div>
      <p>{data.text || "-"}</p>
      <div className="card-footer">
        <span className="muted">{data.author || ""}</span>
        {p.content_type && <span className="muted">{p.content_type}</span>}
      </div>
    </article>
  );
}

function ClusterBadge({ type }) {
  if (!type) return null;
  const colors = { convergence: "#3b82f6", divergence: "#f97316", anomaly: "#a855f7", escalation: "#ef4444" };
  const color = colors[type] || "#6b7280";
  return <span className="badge" style={{ borderColor: color, color }}>{type}</span>;
}

function AnnotationCard({ row }) {
  const [expanded, setExpanded] = useState(false);
  const [tickets, setTickets] = useState(null);
  const [ticketError, setTicketError] = useState(null);
  const p = row.payload || {};
  const ticketIds = p.attention_ticket_ids || (p.attention_ticket_id ? [p.attention_ticket_id] : []);
  const hasTickets = ticketIds.length > 0;
  const facts = p.facts || [];
  const openQuestions = p.open_questions || [];
  const sectors = p.related_sectors || p.sectors || [];

  async function handleToggle() {
    const next = !expanded;
    setExpanded(next);
    if (next && tickets === null && hasTickets) {
      try {
        setTicketError(null);
        const res = await fetch(buildApiUrl(`/api/tickets/by-ids?ids=${ticketIds.join(",")}`));
        if (!res.ok) throw new Error(`Request failed (${res.status})`);
        const data = await res.json();
        setTickets(data.items || []);
      } catch (error) { setTicketError(error.message); }
    }
  }

  return (
    <article className="feed-card annotation-card">
      <div className={`card-header ${hasTickets ? "card-toggle" : ""}`} onClick={hasTickets ? handleToggle : undefined}>
        <span className="card-type annotation-type">ANNOTATION</span>
        <ClusterBadge type={p.cluster_type} />
        {p.signal_count && <span className="badge ticket-count">{p.signal_count} signals / {p.source_count || "?"} sources</span>}
        {p.related_tickers && p.related_tickers.length > 0 && (
          <span className="badge" style={{ borderColor: "#6b7280", color: "#6b7280" }}>{p.related_tickers.join(", ")}</span>
        )}
        <span className="muted">{fmtTime(row.created_at)}</span>
        {hasTickets && <span className={`expand-indicator ${expanded ? "open" : ""}`}>&#9654;</span>}
      </div>
      <p>{p.summary || p.analysis || "-"}</p>
      {facts.length > 0 && (
        <ul className="facts-list">
          {facts.map((f, i) => (
            <li key={i} className="muted"><SourceBadge source={f.source || "?"} /> {typeof f === "string" ? f : (f.fact || f.observation || f.statement || f.text || JSON.stringify(f))}</li>
          ))}
        </ul>
      )}
      {openQuestions.length > 0 && (
        <div className="open-questions">
          <span className="muted">Open questions:</span>
          <ul>{openQuestions.map((q, i) => <li key={i} className="muted">{q}</li>)}</ul>
        </div>
      )}
      {sectors.length > 0 && (
        <p className="muted">sectors: {sectors.join(", ")}</p>
      )}
      <div className="card-footer">
        <span className="muted">agent: {p.agent_id || "?"}</span>
      </div>
      {expanded && (
        <div className="drill-down">
          {ticketError ? <p className="trace-error">Ticket lookup unavailable: {ticketError}. Collapse and reopen to retry.</p> : tickets === null
            ? <p className="muted">Loading...</p>
            : tickets.length === 0
              ? <p className="muted">No tickets found</p>
              : tickets.map((t) => <TicketDetail key={t.id} row={t} />)
          }
        </div>
      )}
    </article>
  );
}

function DecisionCard({ row }) {
  const [expanded, setExpanded] = useState(false);
  const [evidence, setEvidence] = useState(null);
  const [evidenceError, setEvidenceError] = useState(null);
  const p = row.payload || {};
  const actions = p.actions || [];

  async function handleToggle() {
    const next = !expanded;
    setExpanded(next);
    if (next && evidence === null) {
      try {
        setEvidenceError(null);
        const res = await fetch(buildApiUrl(`/api/decisions/${row.id}/evidence`));
        if (!res.ok) throw new Error(`Request failed (${res.status})`);
        const data = await res.json();
        setEvidence(data);
      } catch (error) { setEvidenceError(error.message); }
    }
  }

  return (
    <article className="feed-card decision-card">
      <div className="card-header">
        <span className="card-type decision-type">DECISION</span>
        <UrgencyBadge urgency={p.priority} />
        <span className="muted">{fmtTime(row.created_at)}</span>
        <button className="evidence-toggle" onClick={handleToggle} aria-expanded={expanded}>{expanded ? "Hide evidence" : "Verify historical evidence"}</button>
      </div>
      <h3>{p.title || p.decision_type || "Decision Packet"}</h3>
      <p>{p.summary || "-"}</p>
      {p.confidence != null && (
        <p className="muted">confidence: {(p.confidence * 100).toFixed(0)}%</p>
      )}
      {actions.length > 0 && (
        <div className="actions-list">
          {actions.map((a, i) => (
            <div key={i} className="action-item">
              <span className={`action-badge action-${a.action}`}>{a.action}</span>
              <span>{a.sector}</span>
              {a.tickers && <span className="tickers">{a.tickers.join(", ")}</span>}
              {a.rationale && <span className="muted"> — {a.rationale}</span>}
            </div>
          ))}
        </div>
      )}
      <div className="card-footer">
        <span className="muted">agent: {p.agent_id || "?"}</span>
      </div>
      {expanded && (
        <div className="drill-down">
          {evidenceError ? <p className="trace-error">Evidence verification unavailable: {evidenceError}. Collapse and reopen to retry.</p> : evidence === null
            ? <p className="muted">Loading...</p>
            : <HistoricalEvidence evidence={evidence} />
          }
        </div>
      )}
    </article>
  );
}

function FeedItem({ item }) {
  switch (item.item_type) {
    case "decision_packet": return <DecisionCard row={item} />;
    case "annotation": return <AnnotationCard row={item} />;
    default: return null;
  }
}

export default function App() {
  const [feed, setFeed] = useState([]);
  const [overview, setOverview] = useState({});
  const [dataState, setDataState] = useState("loading");
  const [wsState, setWsState] = useState("connecting");
  const wsUrl = useMemo(() => buildWsUrl(), []);

  async function loadData() {
    try {
      const [feedRes, overviewRes] = await Promise.all([
        fetch(buildApiUrl("/api/feed?limit=50")),
        fetch(buildApiUrl("/api/overview")),
      ]);
      if (!feedRes.ok || !overviewRes.ok) throw new Error("Pipeline API unavailable");
      const feedData = await feedRes.json();
      const overviewData = await overviewRes.json();
      setFeed(Array.isArray(feedData.items) ? feedData.items : []);
      setOverview(overviewData || {});
      setDataState("ready");
    } catch { setDataState("error"); }
  }

  useEffect(() => { loadData(); }, []);

  // Fallback polling: refresh data every 15s regardless of WebSocket state
  useEffect(() => {
    const timer = setInterval(loadData, 15000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    let reconnectTimer;
    function connect() {
      const ws = new WebSocket(wsUrl);
      ws.onopen = () => { setWsState("connected"); ws.send("subscribe"); };
      ws.onclose = () => { setWsState("disconnected"); reconnectTimer = setTimeout(connect, 3000); };
      ws.onerror = () => { setWsState("error"); };
      ws.onmessage = () => { loadData(); };
      return ws;
    }
    const ws = connect();
    return () => { clearTimeout(reconnectTimer); ws.close(); };
  }, [wsUrl]);

  const sources = overview.sources || {};
  const sourceEntries = Object.entries(sources);
  const agents = Array.isArray(overview.agents) ? overview.agents : [];
  const stages = [["raw", "raw_count"], ["clean deliveries", "clean_count"], ["embedded", "embedding_count"],
    ["tickets", "ticket_count"], ["annotations", "annotation_count"], ["decisions", "decision_count"]];

  return (
    <main className="container">
      <header className="header">
        <div><span className="eyebrow">A continuing research system</span><h1>AI Attention Proxy</h1>
        <p>Discover signals. Accumulate evidence. Revisit judgments.</p></div>
        <span className={`live-indicator status-${wsState}`}><span />{`Feed ${wsState}`}</span>
      </header>

      <section className="pipeline-summary" aria-label="Pipeline snapshot">
        {[["Raw records", "raw_count"], ["Tickets", "ticket_count"], ["Annotations", "annotation_count"], ["Decisions", "decision_count"]].map(([label, key]) => <div key={key}><strong>{overview[key] == null ? "—" : overview[key].toLocaleString()}</strong><span>{label}</span></div>)}
        <div><strong>{agents.find((agent) => agent.role === "mid")?.pending_theme_count ?? "—"}</strong><span>Pending themes</span></div>
        <div><strong>{agents.find((agent) => agent.role === "strong")?.thesis_count ?? "—"}</strong><span>Theses</span></div>
      </section>
      {dataState === "error" && <p className="trace-error" role="status">Pipeline counts are unavailable. Showing the last received snapshot.</p>}

      <TraceExplorer />

      <details className="pipeline-overview">
      <summary>Pipeline overview · sources, delivery counts, and saved agent state</summary>
      <section className="cards">
        <article className="card">
          <h3>Live Feed</h3>
          <p className={`status status-${wsState}`}>{wsState}</p>
        </article>
        <article className="card">
          <h3>Sources</h3>
          {sourceEntries.length > 0
            ? sourceEntries.map(([src, cnt]) => (
                <p key={src} style={{ margin: "2px 0" }}>
                  <SourceBadge source={src} /> {cnt.toLocaleString()}
                </p>
              ))
            : <p className="muted">-</p>
          }
        </article>
        <article className="card">
          <h3>Pipeline</h3>
          {stages.map(([label, key]) => (
            <p key={key} style={{ margin: "2px 0" }}>
              <span className="muted">{label}</span> {overview[key] == null ? "-" : overview[key].toLocaleString()}
            </p>
          ))}
          {dataState === "error" && <p className="muted">API unavailable. Showing the last received snapshot.</p>}
        </article>
      </section>

      <section className="cards" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))" }}>
        {["mid", "strong"].map((role) => {
          const agent = agents.find((item) => item.role === role);
          return (
            <article className="card" key={role}>
              <h3>{role === "mid" ? "Mid · evidence accumulation" : "Strong · continuing judgments"}</h3>
              {agent ? <>
                <p className="muted">Last state saved: {fmtTime(agent.updated_at)}</p>
                <p>{agent.pending_theme_count.toLocaleString()} pending themes · {agent.thesis_count.toLocaleString()} theses</p>
                <p className="muted">State key: {agent.agent_id}</p>
              </> : <p className="muted">{dataState === "loading" ? "Loading saved state…" : "No saved state received yet."}</p>}
            </article>
          );
        })}
      </section>
      </details>

      <section className="feed-section">
        <div className="section-heading"><div><span className="eyebrow">Published records</span><h2>Activity feed</h2></div></div>
        <div className="feed-list">
          {feed.map((item) => (
            <FeedItem key={`${item.item_type}-${item.id}`} item={item} />
          ))}
          {feed.length === 0 && <p className="muted">{
            dataState === "loading" ? "Loading published activity…" :
            dataState === "error" ? "Published activity is unavailable while the API cannot be reached." :
            overview.annotation_count === 0 ? "Waiting for evidence to meet publication criteria." :
            "No published activity in this view."
          }</p>}
        </div>
      </section>
    </main>
  );
}
