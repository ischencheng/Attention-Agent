export function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function readable(value, fallback = "Not recorded") {
  if (value == null || value === "") return fallback;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

export function safeSourceUrl(value) {
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) ? url.href : null;
  } catch { return null; }
}

export function sourceUrl(payload = {}) {
  const raw = payload.raw || {};
  const data = payload.data || {};
  const direct = [data.document_url, data.url, data.link, payload.url, payload.source_id, raw.link, raw.url]
    .map(safeSourceUrl).find(Boolean) || null;
  if (direct) return direct;
  if (payload.source === "twitter" && /^twitter-\d+$/.test(payload.source_id || "")) {
    return `https://x.com/i/status/${payload.source_id.slice(8)}`;
  }
  return null;
}

export function sourceTimes(payload = {}, row = {}) {
  const data = payload.data || {};
  const raw = payload.raw || {};
  let published = data.published_at || payload.source_published_at || raw.published_at || null;
  // Research sources explicitly distinguish retrieval time from publication.
  if (!published && !["sec_edgar", "fred"].includes(payload.source)) {
    if (payload.source === "twitter" && Number.isFinite(raw.details?.created_at_ms)) {
      const date = new Date(raw.details.created_at_ms);
      if (!Number.isNaN(date.getTime())) published = date.toISOString();
    }
    else if (data.feed_url || raw.feed_url) published = payload.timestamp || null;
  }
  return { published, observed: data.observed_at || payload.retrieval?.observed_at || data.available_at || row.timestamp || row.created_at || payload.ingested_at || null };
}

export function routingGroups(items) {
  const groups = new Map();
  for (const item of asArray(items)) {
    const key = item.raw_sha256 ? JSON.stringify([item.source, item.source_id, item.raw_sha256]) : `event:${item.id}`;
    if (!groups.has(key)) groups.set(key, { key, source: item.source, source_id: item.source_id, raw_message_id: item.raw_message_id, title: item.title, excerpt: item.excerpt, events: [] });
    groups.get(key).events.push(item);
  }
  return [...groups.values()];
}

export function sourceText(payload = {}) {
  const raw = payload.raw || {};
  const data = payload.data || {};
  // Preserve the original long-form source where the API returned one.
  return raw.note_tweet?.note_tweet_results?.result?.text || raw.details?.full_text ||
    raw.full_text || raw.text || raw.body || raw.description || raw.summary ||
    data.text || data.snippet || data.title || raw.title || null;
}

export function graphNodes(graph, kind) {
  return asArray(graph?.nodes).filter((node) => node.kind === kind);
}

export function routingDisposition(payload = {}) {
  if (payload.outcome === "passed" || payload.outcome === "pass" || payload.outcome === "published") return "Passed";
  if (payload.outcome === "filtered" || payload.outcome === "rejected" || payload.outcome === "reject") return "Filtered out";
  if (payload.outcome === "skipped") return "Skipped";
  if (payload.verdict === true || payload.verdict === "yes" || payload.verdict === "pass") return "Passed";
  if (payload.verdict === false || payload.verdict === "no" || payload.verdict === "reject") return "Filtered out";
  return "Not recorded";
}

export function evidenceLabel(status) {
  return ({ verified: "Verified historical evidence", broken: "Evidence verification failed",
    legacy_unverified: "Historical evidence not recorded", unverified: "Not independently verified" })[status] || "Verification unavailable";
}

export function lastRevision(revisions) {
  const values = asArray(revisions);
  return values.length ? values[values.length - 1] : {};
}

export function recordKey(item) {
  return item.kind === "decision" ? `decision:${item.id}` : `research:${item.agent_id}:${item.thesis_id}`;
}

export function researchTitle(item = {}, thesis = {}) {
  if (thesis.title || item.title) return thesis.title || item.title;
  const id = item.thesis_id || "Research thesis";
  const match = id.match(/^(.*?)-(\d{4}-\d{2}-\d{2})$/);
  const words = (match ? match[1] : id).replaceAll("-", " ").replaceAll("_", " ");
  return `${words.charAt(0).toUpperCase()}${words.slice(1)}${match ? ` · ${match[2]}` : ""}`;
}

export function traceStages(graph, kind, evidence) {
  const counts = Object.fromEntries(["raw", "routing", "ticket", "annotation", "research", "decision"]
    .map((type) => [type, graphNodes(graph, type).length]));
  return [
    { id: "originals", label: "Originals", count: counts.raw, detail: counts.raw ? "Source records" : "No linked originals" },
    { id: "routing", label: "Routing", count: counts.routing, detail: counts.routing ? "Recorded verdicts" : "Telemetry not recorded" },
    { id: "tickets", label: "Fast", count: counts.ticket, detail: counts.ticket ? "Attention tickets" : "No linked tickets" },
    { id: "mid", label: "Mid", count: counts.annotation, detail: counts.annotation ? "Evidence annotations" : "No linked annotations" },
    { id: "strong", label: "Strong", count: counts.research + counts.decision,
      detail: kind === "research" ? "Continuing research" : "Published decision" },
    { id: "outcome", label: "Evaluation", count: null,
      detail: kind === "research" ? "Unscored research" : "Outcome not available" },
  ];
}
