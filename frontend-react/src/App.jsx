import { useEffect, useMemo, useState } from "react";

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
        const res = await fetch(buildApiUrl(`/api/tickets/by-ids?ids=${ticketIds.join(",")}`));
        const data = await res.json();
        setTickets(data.items || []);
      } catch { /* ignore */ }
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
          {tickets === null
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
  const [relatedAnnotations, setRelatedAnnotations] = useState(null);
  const p = row.payload || {};
  const actions = p.actions || [];
  const annotationIds = p.related_annotations || [];

  async function handleToggle() {
    const next = !expanded;
    setExpanded(next);
    if (next && relatedAnnotations === null && annotationIds.length > 0) {
      try {
        const res = await fetch(buildApiUrl(`/api/annotations/by-ids?ids=${annotationIds.join(",")}`));
        const data = await res.json();
        setRelatedAnnotations(data.items || []);
      } catch { /* ignore */ }
    }
  }

  return (
    <article className="feed-card decision-card">
      <div className={`card-header ${annotationIds.length > 0 ? "card-toggle" : ""}`} onClick={annotationIds.length > 0 ? handleToggle : undefined}>
        <span className="card-type decision-type">DECISION</span>
        <UrgencyBadge urgency={p.priority} />
        <span className="muted">{fmtTime(row.created_at)}</span>
        {annotationIds.length > 0 && <span className={`expand-indicator ${expanded ? "open" : ""}`}>&#9654;</span>}
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
          {relatedAnnotations === null
            ? <p className="muted">Loading...</p>
            : relatedAnnotations.length === 0
              ? <p className="muted">No related annotations found</p>
              : relatedAnnotations.map((a) => <AnnotationCard key={a.id} row={a} />)
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
  const stages = [["raw", "raw_count"], ["filtered", "clean_count"], ["embedded", "embedding_count"],
    ["tickets", "ticket_count"], ["annotations", "annotation_count"], ["decisions", "decision_count"]];

  return (
    <main className="container">
      <header className="header">
        <h1>AI Attention Proxy</h1>
        <p>Multi-source intelligence pipeline</p>
      </header>

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

      <section className="feed-section">
        <h2>Activity Feed</h2>
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
