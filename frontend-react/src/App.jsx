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
  const [signals, setSignals] = useState([]);
  const [insights, setInsights] = useState([]);
  const [overview, setOverview] = useState({ candidate_count: 0, final_count: 0 });
  const [wsState, setWsState] = useState("connecting");
  const wsUrl = useMemo(() => buildWsUrl(), []);

  async function loadInitial() {
    const [signalRes, insightRes, overviewRes] = await Promise.all([
      fetch(buildApiUrl("/api/signals?signal_type=final&limit=50")),
      fetch(buildApiUrl("/api/insights?limit=20")),
      fetch(buildApiUrl("/api/overview")),
    ]);
    const signalData = await signalRes.json();
    const insightData = await insightRes.json();
    const overviewData = await overviewRes.json();
    setSignals(Array.isArray(signalData.items) ? signalData.items : []);
    setInsights(Array.isArray(insightData.items) ? insightData.items : []);
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
        setSignals((prev) => [message.data, ...prev].slice(0, 100));
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
          <h2>Final strategy signals</h2>
          <table>
            <thead>
              <tr>
                <th>time</th>
                <th>author</th>
                <th>score</th>
                <th>tickers</th>
                <th>summary</th>
              </tr>
            </thead>
            <tbody>
              {signals.map((signal) => {
                const payload = signal.payload || {};
                const llm = payload.llm_insight || {};
                return (
                  <tr key={signal.id}>
                    <td>{fmtTime(signal.event_ts || signal.created_at)}</td>
                    <td>{signal.author_did || "-"}</td>
                    <td>{signal.score ?? "-"}</td>
                    <td>{Array.isArray(signal.tickers) ? signal.tickers.join(", ") : "-"}</td>
                    <td>{llm.summary || "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h2>Latest LLM insights</h2>
          <ul className="insights">
            {insights.map((insight) => (
              <li key={insight.id}>
                <strong>{insight.model_name || "unknown-model"}</strong>
                <p>{insight.summary || "-"}</p>
              </li>
            ))}
          </ul>
        </article>
      </section>
    </main>
  );
}
