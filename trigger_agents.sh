#!/usr/bin/env bash
# Codex Mid and Strong agents: independent 30m/40m schedules or one-shot runs.
# Each completed cycle leaves events, structured output and usage in /app/runs.
set -euo pipefail

MID_CONTAINER="${MID_CONTAINER:-mid-agent}"
STRONG_CONTAINER="${STRONG_CONTAINER:-strong-agent}"
MID_INTERVAL="${MID_INTERVAL_SECONDS:-1800}"
STRONG_INTERVAL="${STRONG_INTERVAL_SECONDS:-2400}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

run_agent() {
    local container="$1" role="$2" status=0
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != true ]]; then
        log "ERROR: $container is not running"
        return 1
    fi
    log ">>> Triggering $role with Codex (container CODEX_TIMEOUT_SECONDS budget)"
    # Network access is explicit: skills use PostgreSQL, Kafka and data APIs.
    # Files remain workspace-sandboxed; there is no approval/sandbox bypass.
    docker exec "$container" python /app/codex_runner.py \
        --role "$role" --cwd /app --network-access 2>&1 \
        | while IFS= read -r line; do printf '  [%s] %s\n' "$role" "$line"; done \
        || status=$?
    log "<<< $role finished with status $status"
    return "$status"
}

case "${1:-}" in
    mid) run_agent "$MID_CONTAINER" mid; exit $? ;;
    strong) run_agent "$STRONG_CONTAINER" strong; exit $? ;;
    both)
        status=0
        run_agent "$MID_CONTAINER" mid || status=$?
        run_agent "$STRONG_CONTAINER" strong || status=$?
        exit "$status"
        ;;
    --help|-h)
        echo "Usage: $0 [mid|strong|both] [--mid-interval SECONDS --strong-interval SECONDS]"
        exit 0
        ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mid-interval) [[ $# -ge 2 ]] || { echo "Missing mid interval" >&2; exit 2; }; MID_INTERVAL="$2"; shift 2 ;;
        --strong-interval) [[ $# -ge 2 ]] || { echo "Missing strong interval" >&2; exit 2; }; STRONG_INTERVAL="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done
for interval in "$MID_INTERVAL" "$STRONG_INTERVAL"; do
    [[ "$interval" =~ ^[1-9][0-9]*$ ]] || { echo "Intervals must be positive integer seconds" >&2; exit 2; }
done

agent_loop() {
    local container="$1" role="$2" interval="$3"
    # Wait one full interval before the first cycle, preserving prior behavior.
    while sleep "$interval"; do
        if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != true ]]; then
            log "$container stopped; ending its schedule"
            return 0
        fi
        run_agent "$container" "$role" || true
    done
}

log "Codex schedules started: mid=${MID_INTERVAL}s, strong=${STRONG_INTERVAL}s"
agent_loop "$MID_CONTAINER" mid "$MID_INTERVAL" &
mid_pid=$!
agent_loop "$STRONG_CONTAINER" strong "$STRONG_INTERVAL" &
strong_pid=$!
cleanup() { kill "$mid_pid" "$strong_pid" 2>/dev/null || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
wait "$mid_pid" "$strong_pid"
