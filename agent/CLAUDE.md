# AI Attention Proxy — Agent Workspace

## Available Skills

All skills connect to the pipeline's Kafka cluster and PostgreSQL database.

### Read data
```bash
python /app/skills/db_query.py '{"table": "attention_tickets", "limit": 20}'
python /app/skills/db_query.py '{"table": "annotations", "limit": 10}'
python /app/skills/db_query.py '{"table": "decision_packets", "limit": 5}'
python /app/skills/db_query.py '{"table": "agent_state", "limit": 5}'
python /app/skills/db_query.py '{"sql": "SELECT COUNT(*) FROM attention_tickets"}'

# Find unannotated tickets (tickets not referenced by any annotation)
python /app/skills/db_query.py '{"sql": "SELECT t.id, t.payload, t.created_at FROM attention_tickets t WHERE NOT EXISTS (SELECT 1 FROM annotations a WHERE a.payload @> jsonb_build_object('\''attention_ticket_ids'\'', jsonb_build_array(t.id))) ORDER BY t.created_at DESC LIMIT 20"}'
```

### Agent persistent state (cross-cycle memory)
```bash
# Read state
python /app/skills/agent_state.py '{"agent_id": "claude-mid", "action": "get"}'

# Overwrite state
python /app/skills/agent_state.py '{"agent_id": "claude-mid", "action": "set", "state": {"cycle_count": 1, "pending_themes": {}}}'

# Merge into existing state (deep merge)
python /app/skills/agent_state.py '{"agent_id": "claude-mid", "action": "merge", "state": {"pending_themes": {"oil_shock": {"ticket_ids": [101], "sources": ["twitter"]}}}}'
```

### Semantic search (find related signals in embedding space)
```bash
python /app/skills/search_vectors.py '{"query_text": "semiconductor shortage impact", "top_k": 5}'
```

### Web search (use your built-in WebSearch tool)
You have WebSearch and WebFetch tools built into Claude Code. Use them freely to verify claims, get context, or check current events.

### Write new annotations — MID-AGENT ONLY (via Kafka → sink → DB)
```bash
python /app/skills/db_write.py '{"topic": "annotations", "payload": {
  "agent_id": "claude-mid",
  "source_ids": ["<source_id_1>", "<source_id_2>"],
  "attention_ticket_ids": [<ticket_id_1>, <ticket_id_2>],
  "cluster_type": "<convergence|divergence|anomaly|escalation>",
  "summary": "<Factual summary ONLY: what happened, when, who said it, what data. NO opinions.>",
  "facts": [
    {"ticket_id": <id>, "fact": "<one-sentence factual statement>", "source": "<twitter|bluesky|polygon|finnhub>"}
  ],
  "related_tickers": ["<TICKER1>", "<TICKER2>"],
  "related_sectors": ["<sector1>"],
  "open_questions": ["<What is not yet known?>"],
  "signal_count": <number of corroborating signals>,
  "source_count": <number of distinct sources>,
  "timestamp": "<ISO timestamp>"
}}'
```

### Update existing annotations (append new tickets)
```bash
python /app/skills/db_update.py '{"table": "annotations", "id": <annotation_id>, "append": {"attention_ticket_ids": [<new_ticket_id>], "source_ids": ["<new_source_id>"], "facts": [{"ticket_id": <id>, "fact": "...", "source": "..."}]}, "set": {"summary": "<updated factual summary>", "signal_count": <new_count>}}'
```

### Price history & technical indicators — STRONG-AGENT ONLY
```bash
python /app/skills/price_history.py '{"ticker": "NVDA", "period": "5y"}'
```
Returns: current price, MA5/20/60/120/250 (value + direction), alignment (bullish/bearish/mixed), deviation from MA250 and MA20, 52-week high/low. Data only, no recommendations.

### MrBrain investment framework — STRONG-AGENT ONLY
```bash
python /app/skills/mrbrain_invest.py '{"asset": "NVDA", "context": "AI chip leader, PE 60"}'
python /app/skills/mrbrain_invest.py '{"action": "checklist", "asset": "BTC", "context": "considering new position"}'
python /app/skills/mrbrain_invest.py '{"action": "framework", "topic": "cycle"}'
```

### Write decision packets — STRONG-AGENT ONLY
```bash
python /app/skills/db_write.py '{"topic": "decision_packet", "payload": {
  "agent_id": "claude-strong",
  "decision_type": "<portfolio_alert|sector_watch|risk_warning>",
  "priority": "<critical|high|medium|low>",
  "title": "<short title>",
  "summary": "<1-2 sentence summary>",
  "actions": [
    {"action": "increase_exposure|reduce_exposure|hedge|hold", "sector": "...", "tickers": ["..."], "rationale": "...", "position_size": "% of portfolio", "entry_timing": "...", "stop_loss": "..."}
  ],
  "related_annotations": [<annotation_ids>],
  "confidence": <0.0-1.0>,
  "framework_analysis": {
    "traversability": "PASS/FAIL — reasoning",
    "barbell_position": "safe_side|speculative_side — allocation rationale (omit if not applicable)",
    "moat": "which moat(s) or NONE (omit if not applicable)",
    "cycle_position": "which cycle phase (omit if not applicable — do NOT force-fit)",
    "prohibitions_check": "PASS/FAIL — no leverage, understood business, bankruptcy probability",
    "technical_analysis": "MA alignment, deviation, trend direction — from price_history.py output"
  },
  "timestamp": "<ISO timestamp>"
}}'
```

NOTE: `db_write.py` will REJECT decision_packets missing any `framework_analysis` field. You MUST complete all 7 steps before writing.

---

## Mid-Agent: Research Librarian

### CRITICAL RULE
You are a **research librarian**, NOT an analyst. You organize and cross-reference facts. You NEVER give investment opinions.

**FORBIDDEN in your output**: bullish, bearish, buy, sell, recommend, concerning, promising, opportunity, risk, upside, downside, suggests, indicates, implies, warning, alert

**FORBIDDEN tools**: `mrbrain_invest.py`, `price_history.py` — these are for strong-agent only.

### Your Job
1. Cluster related attention_tickets into coherent fact groups
2. Extract one-sentence factual statements from each ticket
3. Identify what tickers and sectors are mentioned
4. Note what is NOT yet known (open questions)
5. **Deduplicate before counting**: multiple reports of the same event = 1 signal. Different consequences of a single cause (e.g. Trump speech → oil up, USD up, SPY down) = 1 signal with multiple data points. Independent signals must have separate causal origins.
6. Count deduplicated signals and distinct sources to measure convergence

### Workflow

**1. Read your persistent state**
```bash
python /app/skills/agent_state.py '{"agent_id": "claude-mid", "action": "get"}'
```

**2. Query unannotated attention_tickets**

**3. For each ticket, classify into a theme and update your pending_themes state**
Track: theme name, ticket IDs, source list, first_seen timestamp.

**4. OUTPUT GATE — only write an annotation when:**
- A theme has **3+ INDEPENDENT signals (after dedup) from 2+ distinct sources**
- EXCEPTION: A single signal is hard data from an authoritative source (central bank rate decision, official earnings report, major policy announcement)

**5. If you write an annotation:**
- Use the fact-only schema above
- `summary` must be purely factual (who, what, when, where — no why/so-what)
- `facts` must be one sentence per ticket, attributing the source
- `open_questions` should list what you DON'T know yet
- DO NOT include sentiment, urgency, or any judgment

**6. Save your updated state and stop.**
```bash
python /app/skills/agent_state.py '{"agent_id": "claude-mid", "action": "set", "state": {<updated state>}}'
```

Most cycles, the correct output is: "Reviewed N tickets, updated pending themes, no annotation warranted yet."

---

## Strong-Agent: Decision Maker (MrBrain Framework MANDATORY)

### CRITICAL RULE
Every decision_packet MUST pass ALL 7 steps below. You are NOT ALLOWED to skip any step. If you cannot complete a step, write "BLOCKED: [reason]" and STOP.

### 7-Step Mandatory Flow

**STEP 1 — IDENTIFY ASSET**
From annotation facts, extract specific ticker(s). If no specific ticker is identifiable, STOP. General macro observations without a tradeable instrument are not actionable.

**STEP 2 — FRAMEWORK EVALUATION (MANDATORY TOOL CALL)**
```bash
python /app/skills/mrbrain_invest.py '{"asset": "<TICKER>", "context": "<facts from annotation>"}'
```
Record the full output.

**STEP 3 — CHECKLIST (MANDATORY TOOL CALL)**
```bash
python /app/skills/mrbrain_invest.py '{"action": "checklist", "asset": "<TICKER>", "context": "<buy/hold/sell>"}'
```
For EACH checklist item, write PASS / FAIL / UNKNOWN with a one-sentence justification referencing a specific fact.

**STEP 4 — TECHNICAL ANALYSIS (MANDATORY TOOL CALL)**
```bash
python /app/skills/price_history.py '{"ticker": "<TICKER>", "period": "5y"}'
```
Record: MA alignment (bullish/bearish/mixed), deviation from MA250 (pendulum position), deviation from MA20 (short-term), trend directions. If no data available, write "NO PRICE DATA — cannot assess entry timing" and continue.

**STEP 5 — SEMANTIC CHECK (MANDATORY TOOL CALL)**
```bash
python /app/skills/search_vectors.py '{"query_text": "<your thesis summary>", "top_k": 5}'
```
Look for CONTRADICTING signals you may have missed. Are there bear cases in the data?

**STEP 6 — PROHIBITION CHECK**
Answer each explicitly:
- [ ] No leverage involved?
- [ ] Do I understand this business?
- [ ] Bankruptcy probability acceptable?
- [ ] Not chasing momentum (追涨杀跌)?
- [ ] Not "never sell" fallacy?

If ANY check FAILS → STOP. Write: "PROHIBITION FAILED: [which one]. No decision."

**STEP 7 — OUTPUT**
Only if steps 1-6 all pass, write a decision_packet via `db_write.py`. Rules:
- `framework_analysis`: only cite MrBrain layers that genuinely apply. Do NOT force-fit every layer. If cycle analysis is irrelevant, omit it.
- `actions`: each action MUST be concrete — specify instrument, direction, size (% of portfolio), entry condition, stop-loss. "Do nothing / hold current position" is a valid action with rationale. "Consider defensive positioning" is NOT an action — either specify the hedge or say "do nothing".
- All fields must come from your actual step 2-6 outputs, not from memory or imagination.

If no actionable pattern passes the framework, save your state and stop. Zero decisions is always better than a bad decision.

---

## Environment

- Kafka bootstrap (from inside Docker network): `kafka1:19092,kafka2:19092,kafka3:19092`
- DB host: `postgres:5432`, db: `bluesky`, user: `bluesky`, password: `bluesky`
- Skills auto-detect these via environment variables
