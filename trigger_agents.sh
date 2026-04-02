#!/usr/bin/env bash
# Trigger mid-agent and strong-agent on independent schedules.
#
# Loop mode (default):
#   ./trigger_agents.sh                          # mid every 30m, strong every 40m
#   ./trigger_agents.sh --mid-interval 600 --strong-interval 900
#
# Manual one-shot:
#   ./trigger_agents.sh mid                      # trigger mid-agent once
#   ./trigger_agents.sh strong                   # trigger strong-agent once
#   ./trigger_agents.sh both                     # trigger both once (mid first)

set -euo pipefail

MID_CONTAINER="mid-agent"
STRONG_CONTAINER="strong-agent"
MID_INTERVAL=1800   # 30 minutes
STRONG_INTERVAL=2400 # 40 minutes
WATCHLIST="${WATCHLIST:-NVDA,SNDK,SPY,QQQ}"

MID_PROMPT='You are the mid-agent — a research librarian. Follow CLAUDE.md strictly. You MUST NOT give opinions, sentiment, or investment judgment.

1. Read your persistent state:
   python /app/skills/agent_state.py '"'"'{"agent_id": "claude-mid", "action": "get"}'"'"'

2. Query recent unannotated attention_tickets:
   python /app/skills/db_query.py '"'"'{"sql": "SELECT t.id, t.payload, t.created_at FROM attention_tickets t WHERE NOT EXISTS (SELECT 1 FROM annotations a WHERE a.payload @> jsonb_build_object('"'"'"'"'"'"'"'"'attention_ticket_ids'"'"'"'"'"'"'"'"', jsonb_build_array(t.id))) ORDER BY t.created_at DESC LIMIT 20"}'"'"'

3. For each ticket, classify it into a theme in your pending_themes state. Track: theme name, ticket_ids, sources, first_seen.

4. SIGNAL DEDUPLICATION — before counting signals, deduplicate:
   - Multiple reports of the SAME event (e.g. "oil up 7%" and "Brent above $110") = 1 signal, not 2.
   - Different consequences of a single cause (e.g. Trump speech → oil up, USD up, SPY down) = 1 signal with multiple data points.
   - A genuinely independent signal must have a SEPARATE causal origin. Count honestly.

5. OUTPUT GATE — only write an annotation when:
   - A theme has 3+ INDEPENDENT signals (after dedup) from 2+ distinct sources
   - EXCEPTION: single signal is hard data from authoritative source (central bank, official earnings, major policy)

6. If you write an annotation, use the FACT-ONLY schema from CLAUDE.md:
   - summary: purely factual (who, what, when, where — NO why/so-what)
   - signal_count: count of INDEPENDENT signals after dedup, not total tickets
   - facts: one sentence per ticket with source attribution
   - open_questions: what you do NOT know yet
   - FORBIDDEN words: bullish, bearish, buy, sell, recommend, concerning, promising, risk, upside, downside
   - FORBIDDEN tools: mrbrain_invest.py, price_history.py

7. Save your updated state:
   python /app/skills/agent_state.py '"'"'{"agent_id": "claude-mid", "action": "set", "state": <your updated state>}'"'"'

Most cycles, the correct output is: "Reviewed N tickets, updated pending themes, no annotation warranted yet."
If there are no new tickets, say so and stop.'

STRONG_PROMPT='You are the strong-agent — the decision maker. Follow CLAUDE.md strictly. You MUST complete ALL 7 steps for each annotation you evaluate. Skipping a step is forbidden.

WATCHLIST: '"$WATCHLIST"'
Your job is to think ONLY about the watchlist tickers above. For each annotation, judge whether the information is materially relevant to any watchlist ticker. If not, skip it.

1. Read your persistent state:
   python /app/skills/agent_state.py '"'"'{"agent_id": "claude-strong", "action": "get"}'"'"'

2. Query recent annotations with signal_count >= 3:
   python /app/skills/db_query.py '"'"'{"sql": "SELECT id, payload, created_at FROM annotations WHERE (payload->>'"'"'"'"'"'"'"'"'signal_count'"'"'"'"'"'"'"'"')::int >= 3 ORDER BY created_at DESC LIMIT 10"}'"'"'

3. Query existing decision_packets to avoid duplicates:
   python /app/skills/db_query.py '"'"'{"table": "decision_packets", "limit": 10}'"'"'

4. For EACH annotation worth evaluating, execute ALL 7 STEPS IN ORDER:

   STEP 1 — IDENTIFY: Judge which watchlist ticker(s) ('"$WATCHLIST"') are materially relevant to this annotation. No relevant ticker = SKIP.
   STEP 2 — FRAMEWORK (MUST CALL): python /app/skills/mrbrain_invest.py '"'"'{"asset": "<TICKER>", "context": "<facts>"}'"'"'
   STEP 3 — CHECKLIST (MUST CALL): python /app/skills/mrbrain_invest.py '"'"'{"action": "checklist", "asset": "<TICKER>", "context": "<context>"}'"'"'
     Write PASS/FAIL/UNKNOWN for each item with one-sentence justification citing a fact.
   STEP 4 — TECHNICAL (MUST CALL): python /app/skills/price_history.py '"'"'{"ticker": "<TICKER>", "period": "5y"}'"'"'
     Record MA alignment, deviation from MA250/MA20, trend. No data = "NO PRICE DATA".
   STEP 5 — SEMANTIC CHECK (MUST CALL): python /app/skills/search_vectors.py '"'"'{"query_text": "<thesis>", "top_k": 5}'"'"'
     Look for contradicting signals.
   STEP 6 — PROHIBITIONS: Answer all 5 explicitly. Any FAIL = STOP, no decision.
   STEP 7 — OUTPUT: Only if 1-6 pass, write decision_packet. Rules:
     - framework_analysis: only cite MrBrain layers that genuinely apply. Do NOT force-fit every layer. If cycle analysis is not relevant, omit it.
     - actions MUST be concrete: specify instrument, direction, size (% of portfolio), entry condition, stop-loss. "Do nothing / hold current position" is a valid action with rationale. "Consider defensive positioning" is NOT an action — either specify the hedge or say "do nothing".

5. Save your state:
   python /app/skills/agent_state.py '"'"'{"agent_id": "claude-strong", "action": "set", "state": <your updated state>}'"'"'

If no annotation passes the 7-step flow, that is the correct outcome. Zero decisions is always better than a bad decision.'

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

run_agent() {
    local container="$1"
    local prompt="$2"
    local label="$3"

    if ! docker inspect "$container" &>/dev/null; then
        log "WARN: container $container not found, skipping"
        return 1
    fi

    log ">>> Triggering $label ..."
    docker exec "$container" claude --dangerously-skip-permissions -p "$prompt" --output-format text --model claude-sonnet-4-6 2>&1 | while IFS= read -r line; do
        echo "  [$label] $line"
    done
    local exit_code=${PIPESTATUS[0]}
    if [ "$exit_code" -eq 0 ]; then
        log "<<< $label completed successfully"
    else
        log "<<< $label exited with code $exit_code"
    fi
    return "$exit_code"
}

# ─── Manual one-shot mode ───
case "${1:-}" in
    mid)
        log "Manual trigger: mid-agent"
        run_agent "$MID_CONTAINER" "$MID_PROMPT" "mid-agent"
        exit $?
        ;;
    strong)
        log "Manual trigger: strong-agent"
        run_agent "$STRONG_CONTAINER" "$STRONG_PROMPT" "strong-agent"
        exit $?
        ;;
    both)
        log "Manual trigger: both agents"
        run_agent "$MID_CONTAINER" "$MID_PROMPT" "mid-agent" || true
        run_agent "$STRONG_CONTAINER" "$STRONG_PROMPT" "strong-agent"
        exit $?
        ;;
esac

# ─── Parse optional interval overrides ───
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mid-interval)   MID_INTERVAL="$2"; shift 2 ;;
        --strong-interval) STRONG_INTERVAL="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Loop mode with independent intervals ───
log "Agent trigger loop started"
log "  Mid-agent:    every ${MID_INTERVAL}s ($(( MID_INTERVAL / 60 ))m)"
log "  Strong-agent: every ${STRONG_INTERVAL}s ($(( STRONG_INTERVAL / 60 ))m)"
log "  Watchlist:    $WATCHLIST"
log "  Manual trigger: ./trigger_agents.sh mid|strong|both"

LAST_MID=$(date +%s)     # wait a full interval before first trigger
LAST_STRONG=$(date +%s)  # wait a full interval before first trigger
TICK=30  # check every 30 seconds

while true; do
    # Exit if mid-agent container is no longer running (e.g. docker compose down)
    if ! docker inspect --format '{{.State.Running}}' "$MID_CONTAINER" 2>/dev/null | grep -q true; then
        log "Container $MID_CONTAINER is not running — exiting loop"
        exit 0
    fi

    NOW=$(date +%s)

    if (( NOW - LAST_MID >= MID_INTERVAL )); then
        log "=== Mid-agent cycle ==="
        run_agent "$MID_CONTAINER" "$MID_PROMPT" "mid-agent" || true
        LAST_MID=$(date +%s)
    fi

    if (( NOW - LAST_STRONG >= STRONG_INTERVAL )); then
        log "=== Strong-agent cycle ==="
        run_agent "$STRONG_CONTAINER" "$STRONG_PROMPT" "strong-agent" || true
        LAST_STRONG=$(date +%s)
    fi

    sleep "$TICK"
done
