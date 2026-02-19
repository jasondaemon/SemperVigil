#!/usr/bin/env bash
set -euo pipefail

# Simple Ollama latency watchdog:
# - Default mode is passive log analysis (no synthetic LLM calls)
# - Optional probe mode can be enabled with MODE=probe
# - Restarts Ollama only after sustained degradation
# - Enforces cooldown to avoid restart flapping

MODE="${MODE:-log}"
OLLAMA_URL="${OLLAMA_URL:-http://127.0.0.1:11434/api/generate}"
OLLAMA_MODEL="${OLLAMA_MODEL:-qwen2.5:7b-instruct}"
PROBE_TIMEOUT_S="${PROBE_TIMEOUT_S:-45}"
SLOW_THRESHOLD_S="${SLOW_THRESHOLD_S:-25}"
P95_THRESHOLD_S="${P95_THRESHOLD_S:-60}"
MAX_SOFT_S="${MAX_SOFT_S:-120}"
MAX_HARD_S="${MAX_HARD_S:-300}"
HIGH_STREAK_TRIGGER="${HIGH_STREAK_TRIGGER:-2}"
COOLDOWN_S="${COOLDOWN_S:-5400}"
STATE_FILE="${STATE_FILE:-/tmp/ollama_watchdog.state}"
LOG_FILE="${LOG_FILE:-/tmp/ollama_watchdog.log}"
RESTART_CMD="${RESTART_CMD:-docker restart ollama}"
LOG_WINDOW_M="${LOG_WINDOW_M:-15}"
MIN_SAMPLES="${MIN_SAMPLES:-4}"

ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_line() {
  printf "%s %s\n" "$(ts)" "$*" >>"$LOG_FILE"
}

high_streak=0
last_restart_epoch=0
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE"
fi

probe_s="na"
probe_ok=1
slow=0
sample_count=0
p95_s="na"
max_s="na"
soft_over_count=0
hard_over_count=0

if [[ "$MODE" == "probe" ]]; then
  payload_file="$(mktemp)"
  resp_file="$(mktemp)"
  cleanup() {
    rm -f "$payload_file" "$resp_file"
  }
  trap cleanup EXIT

  cat >"$payload_file" <<JSON
{"model":"$OLLAMA_MODEL","prompt":"Respond with OK.","stream":false,"options":{"num_predict":4,"temperature":0}}
JSON

  probe_s="$(
    curl -sS -m "$PROBE_TIMEOUT_S" \
      -o "$resp_file" \
      -w "%{time_total}" \
      -H "Content-Type: application/json" \
      --data-binary @"$payload_file" \
      "$OLLAMA_URL" || true
  )"

  probe_ok=0
  slow=1
  if [[ -n "$probe_s" ]] && grep -q '"done":true' "$resp_file"; then
    probe_ok=1
    if awk -v t="$probe_s" -v th="$SLOW_THRESHOLD_S" 'BEGIN { exit !(t >= th) }'; then
      slow=1
    else
      slow=0
    fi
  fi
else
  # Parse passive runtime from Ollama's existing request logs:
  # [GIN] ... | 200 |  2.544231886s | ... | POST "/api/generate"
  metrics="$(
    docker logs --since "${LOG_WINDOW_M}m" ollama 2>&1 | \
    awk -v p95_th="$P95_THRESHOLD_S" -v soft_th="$MAX_SOFT_S" -v hard_th="$MAX_HARD_S" '
      BEGIN { n=0; maxv=0; soft_count=0; hard_count=0 }
      /POST[[:space:]]+"\/api\/generate"/ {
        if (match($0, /\|[[:space:]]*([0-9.]+)s[[:space:]]*\|/, m)) {
          n += 1
          d = m[1] + 0
          vals[n] = d
          if (d > maxv) maxv = d
          if (d >= soft_th) soft_count += 1
          if (d >= hard_th) hard_count += 1
        }
      }
      END {
        if (n == 0) { print "0 na na 0 0 1 0"; exit }
        asort(vals)
        p95_idx = int(0.95 * (n - 1)) + 1
        p95 = vals[p95_idx]
        slow_p95 = (p95 >= p95_th) ? 1 : 0
        slow_spike = (hard_count >= 1 && soft_count >= 2) ? 1 : 0
        slow = (slow_p95 || slow_spike) ? 1 : 0
        print n, p95, maxv, soft_count, hard_count, 1, slow
      }'
  )"
  sample_count="$(awk '{print $1}' <<<"$metrics")"
  p95_s="$(awk '{print $2}' <<<"$metrics")"
  max_s="$(awk '{print $3}' <<<"$metrics")"
  soft_over_count="$(awk '{print $4}' <<<"$metrics")"
  hard_over_count="$(awk '{print $5}' <<<"$metrics")"
  probe_ok="$(awk '{print $6}' <<<"$metrics")"
  slow="$(awk '{print $7}' <<<"$metrics")"
  if [[ "$sample_count" -lt "$MIN_SAMPLES" ]]; then
    # Keep spike detector active even on low-volume windows.
    if [[ "$hard_over_count" -lt 1 || "$soft_over_count" -lt 2 ]]; then
      slow=0
    fi
  fi
fi

restart=0
reason="ok"
if [[ "$probe_ok" -eq 0 ]]; then
  high_streak=$((high_streak + 1))
  reason="probe_failed"
elif [[ "$slow" -eq 1 ]]; then
  high_streak=$((high_streak + 1))
  reason="slow_probe"
else
  high_streak=0
fi

now_epoch="$(date +%s)"
if [[ "$high_streak" -ge "$HIGH_STREAK_TRIGGER" ]] && (( now_epoch - last_restart_epoch >= COOLDOWN_S )); then
  if bash -lc "$RESTART_CMD" >/dev/null 2>&1; then
    restart=1
    last_restart_epoch="$now_epoch"
    high_streak=0
    reason="${reason}_restarted"
  else
    reason="${reason}_restart_failed"
  fi
fi

{
  echo "high_streak=$high_streak"
  echo "last_restart_epoch=$last_restart_epoch"
} >"$STATE_FILE"

log_line "mode=$MODE probe_s=${probe_s:-na} p95_s=$p95_s max_s=$max_s samples=$sample_count soft_over=$soft_over_count hard_over=$hard_over_count probe_ok=$probe_ok slow=$slow streak=$high_streak restart=$restart reason=$reason"
