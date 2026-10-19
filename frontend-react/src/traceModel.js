export function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function rawRecordId(row = {}) {
  const id = row.record_id ?? row.id;
  if (Number.isSafeInteger(id) && id > 0) return id;
  const match = typeof id === "string" && id.match(/^raw:([1-9]\d*)$/);
  if (match && Number.isSafeInteger(Number(match[1]))) return Number(match[1]);
  return null;
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

export function isInputRelay(record = {}) {
  const envelope = record.payload || record;
  const classifier = envelope.classifier || envelope.classification || envelope;
  return envelope.stage === "relay" || record.stage === "relay" || classifier.classification_mode === "relay";
}

export function routingStageLabel(record = {}) {
  const stage = record.stage || record.payload?.stage;
  if (isInputRelay(record)) return "Input relay";
  if (stage === "filter") return "Relevance filter";
  if (stage === "fast") return "Fast";
  return readable(stage, "Routing event").replaceAll("_", " ");
}

export function routingDisposition(payload = {}) {
  const envelope = payload.payload || payload;
  const outcome = payload.outcome ?? envelope.outcome;
  const verdict = payload.verdict ?? envelope.verdict;
  const passed = outcome ? ["passed", "pass", "published"].includes(outcome) : [true, "yes", "pass"].includes(verdict);
  const rejected = outcome ? ["filtered", "rejected", "reject"].includes(outcome) : [false, "no", "reject"].includes(verdict);
  if (isInputRelay(payload)) return passed ? "Forwarded" : rejected ? "Not forwarded" : "Not recorded";
  if (passed) return "Passed";
  if (rejected) return "Filtered out";
  if (outcome === "skipped") return "Skipped";
  return "Not recorded";
}

export function classificationPresentation(classifier = {}, stage = null) {
  const relay = stage === "relay" || classifier.classification_mode === "relay";
  const usage = classifier.usage && typeof classifier.usage === "object" && !Array.isArray(classifier.usage) ? classifier.usage : null;
  if (relay) return { model: "No model call", execution: "Deterministic input relay", usage: null,
    usageTitle: "No model call", usageNote: "The input relay does not invoke a model." };
  if (classifier.cached === true) return { model: classifier.model || "Producing model not recorded",
    execution: "Cached result reused · no model call for this request", usage,
    usageTitle: "Historical cached-result usage",
    usageNote: "These token counts belong to the stored result's original computation. They are not new model usage or newly billed usage for this request." };
  const policy = classifier.classified_by_model === false;
  return { model: policy ? (classifier.model || "No model call") : (classifier.model || "Model not recorded"),
    execution: policy ? "Routing policy · no model classification" : classifier.cached === false ? "Gateway response not served from cache" : "Model invocation / cache status not recorded",
    usage, usageTitle: "Reported model usage",
    usageNote: classifier.cached === false ? "Usage reported for this recorded computation. Billing is not measured in this view."
      : "Usage was recorded, but cache reuse was not recorded. It cannot be attributed to a new model call or a new charge." };
}

export function ticketClassifications(payload = {}) {
  return [["Relevance filter", payload.local_ml_filter], ["Fast", payload.fast_agent]]
    .filter(([, result]) => result && typeof result === "object" && !Array.isArray(result));
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
    { id: "routing", label: "Routing", count: counts.routing, detail: counts.routing ? "Recorded handling" : "Telemetry not recorded" },
    { id: "tickets", label: "Fast", count: counts.ticket, detail: counts.ticket ? "Attention tickets" : "No linked tickets" },
    { id: "mid", label: "Mid", count: counts.annotation, detail: counts.annotation ? "Evidence annotations" : "No linked annotations" },
    { id: "strong", label: "Strong", count: counts.research + counts.decision,
      detail: kind === "research" ? "Continuing research" : "Published decision" },
    { id: "outcome", label: "Evaluation", count: null,
      detail: kind === "research" ? "Unscored research" : "Outcome not available" },
  ];
}
