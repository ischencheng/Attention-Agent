import { useEffect, useMemo, useState } from "react";

const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");

function buildApiUrl(path) {
  return apiBase ? `${apiBase}${path}` : path;
}

function buildWsUrl() {
  if (import.meta.env.VITE_WS_URL) {
    return import.meta.env.VITE_WS_URL;
  }
  const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.host;
  return `${wsProtocol}://${host}/ws/signals`;
}

function fmtTime(ts) {
  if (!ts) return "-";
  const date = new Date(ts);
  return Number.isNaN(date.getTime()) ? "-" : date.toLocaleString();
}

export default function App() {
  const [chains, setChains] = useState([]);
  const [overview, setOverview] = useState({ candidate_count: 0, final_count: 0 });
  const [wsState, setWsState] = useState("connecting");
  const wsUrl = useMemo(() => buildWsUrl(), []);

  async function loadInitial() {
    const [chainsRes, overviewRes] = await Promise.all([
      fetch(buildApiUrl("/api/decision-chains?limit=20")),
      fetch(buildApiUrl("/api/overview")),
    ]);
    const chainsData = await chainsRes.json();
    const overviewData = await overviewRes.json();
    setChains(Array.isArray(chainsData.items) ? chainsData.items : []);
    setOverview(overviewData || {});
  }

  useEffect(() => {
    loadInitial().catch(() => undefined);
  }, []);

  useEffect(() => {
    const ws = new WebSocket(wsUrl);
    ws.onopen = () => {
      setWsState("connected");
      ws.send("subscribe");
    };
    ws.onclose = () => {
      setWsState("disconnected");
    };
    ws.onerror = () => {
      setWsState("error");
    };
    ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        if (message.event !== "strategy_signal_final" || !message.data) return;
        loadInitial().catch(() => undefined);
        setOverview((prev) => ({ ...prev, final_count: (prev.final_count || 0) + 1 }));
      } catch {
        // ignore malformed messages
      }
    };
    return () => ws.close();
  }, [wsUrl]);

  return (
    <main className="container">
      <header className="header">
        <h1>Bluesky Strategy Dashboard</h1>
        <p>FastAPI + LLM worker + WebSocket realtime loop</p>
      </header>

      <section className="cards">
        <article className="card">
          <h3>WebSocket</h3>
          <p className={`status status-${wsState}`}>{wsState}</p>
        </article>
        <article className="card">
          <h3>Candidates</h3>
          <p>{overview.candidate_count || 0}</p>
        </article>
        <article className="card">
          <h3>Final Signals</h3>
          <p>{overview.final_count || 0}</p>
        </article>
      </section>

      <section className="grid">
        <article className="panel">
          <h2>Decision Workbench (Phase 4)</h2>
          <ul className="chains">
            {chains.map((item) => {
              const decision = item.decision || {};
              const annotations = Array.isArray(item.annotations) ? item.annotations : [];
              return (
                <li key={decision.id || decision.cid}>
                  <div className="chain-head">
                    <strong>{decision.cid || "unknown cid"}</strong>
                    <span>{fmtTime(decision.created_at)}</span>
                  </div>
                  <p>
                    <strong>report</strong>
                  </p>
                  <p>{decision.report || "-"}</p>

                  <div className="chain-subsection">
                    <h4>Annotations</h4>
                    {annotations.length === 0 && <p className="muted">No linked annotations</p>}
                    {annotations.map((annotation) => {
                      const raws = Array.isArray(annotation.raw_messages) ? annotation.raw_messages : [];
                      return (
                        <div className="annotation" key={annotation.cid || "annotation"}>
                          <p>
                            <strong>cid</strong>: {annotation.cid || "-"}
                          </p>
                          <p className="muted">
                            mid_agent_relevant_cids=
                            {Array.isArray(annotation.mid_agent_relevant_cids)
                              ? annotation.mid_agent_relevant_cids.join(", ")
                              : "-"}
                          </p>
                          <p className="muted">related raw={raws.length}</p>
                          {raws.map((raw) => (
                            <div className="raw" key={raw.cid || "raw"}>
                              <p>
                                <strong>{raw.cid || "unknown-cid"}</strong> · {fmtTime(raw.created_at)}
                              </p>
                              <p>{raw.text || "-"}</p>
                            </div>
                          ))}
                        </div>
                      );
                    })}
                  </div>
                </li>
              );
            })}
            {chains.length === 0 && (
              <li>
                <p className="muted">No decision chains yet.</p>
              </li>
            )}
          </ul>
        </article>
      </section>
    </main>
  );
}
