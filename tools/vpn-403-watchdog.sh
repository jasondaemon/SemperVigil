#!/usr/bin/env bash
set -euo pipefail

# Rotates VPN IP when repeated fetch 403s indicate likely Cloudflare/IP blocking.
# Intended to run from cron every few minutes.

COMPOSE_DIR="${COMPOSE_DIR:-/nfs/website_cybernews.jasondaemon.net/sempervigil}"
WINDOW_MIN="${WINDOW_MIN:-10}"
THRESHOLD_403="${THRESHOLD_403:-6}"
COOLDOWN_S="${COOLDOWN_S:-1800}"
SOURCE_ID="${SOURCE_ID:-}"
STATE_FILE="${STATE_FILE:-/nfs/website_cybernews.jasondaemon.net/log/vpn-403-watchdog.state}"
LOG_FILE="${LOG_FILE:-/nfs/website_cybernews.jasondaemon.net/log/vpn-403-watchdog.log}"

mkdir -p "$(dirname "$STATE_FILE")" "$(dirname "$LOG_FILE")"

log_json() {
  local ts scope count403 queued restart reason ip
  ts="$1"
  scope="$2"
  count403="$3"
  queued="$4"
  restart="$5"
  reason="$6"
  ip="$7"
  local payload
  payload=$(printf '{"ts":"%s","level":"INFO","service":"vpn_watchdog","runner_type":"fetch","event":"vpn_watchdog_status","source_scope":"%s","count_403":%s,"queued":%s,"restart":%s,"reason":"%s","ip":"%s"}' \
    "$ts" "$scope" "$count403" "$queued" "$restart" "$reason" "$ip")
  printf '%s\n' "$payload" | tee -a "$LOG_FILE" >/dev/stdout
}

last_restart_epoch=0
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE" || true
fi

now_epoch=$(date +%s)

if [[ -n "$SOURCE_ID" ]]; then
  source_filter_403="and a.source_id='${SOURCE_ID}'"
  source_filter_queued="and a.source_id='${SOURCE_ID}'"
  source_scope="$SOURCE_ID"
else
  source_filter_403=""
  source_filter_queued=""
  source_scope="all"
fi

sql_403_count=$(cat <<SQL
select count(*)
from jobs j
join articles a on a.id = ((j.payload_json::jsonb)->>'article_id')::int
where j.job_type='fetch_article_content'
  and j.status='failed'
  and (j.finished_at::timestamptz) >= now() - interval '${WINDOW_MIN} minutes'
  and lower(coalesce(j.error,'')) like '%403%'
  ${source_filter_403};
SQL
)

sql_queued_count=$(cat <<SQL
select count(*)
from jobs j
join articles a on a.id = ((j.payload_json::jsonb)->>'article_id')::int
where j.job_type='fetch_article_content'
  and j.status='queued'
  ${source_filter_queued};
SQL
)

count_403=$(cd "$COMPOSE_DIR" && docker compose exec -T db psql -U sempervigil -d sempervigil -At -c "$sql_403_count" | tr -d '[:space:]')
queued_count=$(cd "$COMPOSE_DIR" && docker compose exec -T db psql -U sempervigil -d sempervigil -At -c "$sql_queued_count" | tr -d '[:space:]')

restart=0
reason="no_action"
new_ip=""

if [[ "${count_403:-0}" =~ ^[0-9]+$ ]] && [[ "${queued_count:-0}" =~ ^[0-9]+$ ]]; then
  if (( count_403 >= THRESHOLD_403 )) && (( queued_count > 0 )); then
    if (( now_epoch - last_restart_epoch >= COOLDOWN_S )); then
      if (cd "$COMPOSE_DIR" && docker compose restart vpn >/dev/null 2>&1); then
        sleep 6
        new_ip=$(cd "$COMPOSE_DIR" && docker compose exec -T vpn sh -lc 'wget -qO- https://api.ipify.org || curl -fsS https://api.ipify.org' 2>/dev/null || true)
        restart=1
        reason="restarted_vpn"
        last_restart_epoch="$now_epoch"
      else
        reason="restart_failed"
      fi
    else
      reason="cooldown"
    fi
  fi
fi

echo "last_restart_epoch=$last_restart_epoch" > "$STATE_FILE"

log_json \
  "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  "$source_scope" \
  "${count_403:-0}" \
  "${queued_count:-0}" \
  "$restart" \
  "$reason" \
  "$new_ip"
