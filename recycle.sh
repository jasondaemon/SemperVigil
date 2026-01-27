#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

DB_USER="${SV_DB_USER:-sempervigil}"
ADMIN_PORT="${SV_ADMIN_PORT:-8001}"
WEB_PORT="${SV_WEB_PORT:-8080}"

# -------- helpers --------
have_timeout() { command -v timeout >/dev/null 2>&1; }

run_compose() {
  # Usage: run_compose <timeout> <docker compose args...>
  local t="${1:-}"
  shift || true
  if have_timeout && [[ -n "$t" ]]; then
    timeout "$t" docker compose "$@" || return $?
  else
    docker compose "$@"
  fi
}

service_cid() {
  docker compose ps -q "$1" 2>/dev/null || true
}

service_state() {
  local cid
  cid="$(service_cid "$1")"
  [[ -n "$cid" ]] || { echo "missing"; return 0; }
  docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo "unknown"
}

ensure_up_detached() {
  # Best-effort "up -d" that won't hang the script forever.
  local svc="$1"
  echo "⚙️  Starting $svc…"
  set +e
  run_compose 30s up -d --no-deps "$svc"
  local rc=$?
  set -e

  if [[ $rc -ne 0 ]]; then
    echo "⚠️  docker compose up -d $svc returned $rc (may be a compose-client hang). Verifying container…"
  fi

  # Verify it is actually running (or at least not missing)
  local st
  st="$(service_state "$svc")"
  if [[ "$st" == "running" ]]; then
    echo "✅ $svc is running."
    return 0
  fi

  # If it isn't running, show logs and fail
  echo "❌ $svc is not running (state=$st). Recent logs:"
  docker compose logs --tail 200 "$svc" || true
  return 1
}

wait_for_pg() {
  local cid
  cid="$(service_cid db)"
  [[ -n "$cid" ]] || { echo "❌ db container id not found"; return 1; }

  echo "⏳ Waiting for Postgres to be ready…"
  for _ in {1..60}; do
    if docker exec "$cid" pg_isready -U "$DB_USER" >/dev/null 2>&1; then
      echo "✅ Database is ready."
      return 0
    fi
    sleep 1
  done
  echo "❌ Postgres did not become ready in time."
  docker compose logs --tail 200 db || true
  return 1
}

one_shot_builder() {
  # Run builder once, but don’t let compose hang the whole script forever.
  # Use a longer timeout here because Hugo can take time on first run.
  echo "📝 Running Hugo site build (one-shot)…"

  set +e
  if have_timeout; then
    timeout 10m docker compose --profile build run --rm --no-deps builder
    rc=$?
  else
    docker compose --profile build run --rm --no-deps builder
    rc=$?
  fi
  set -e

  if [[ $rc -eq 0 ]]; then
    echo "✅ Site build completed."
    return 0
  fi

  # If timeout or compose-client weirdness, we check whether site output exists
  echo "⚠️  Builder command returned $rc. Checking whether site volume has content…"
  # Basic “did anything get generated?” check: look for index.html in web root
  # (This assumes Hugo outputs to /site which is mounted to sv_site)
  if docker compose run --rm --no-deps --entrypoint sh builder -lc 'test -f /site/index.html' >/dev/null 2>&1; then
    echo "✅ /site/index.html exists — build likely succeeded despite compose client issue."
    return 0
  fi

  echo "❌ Build did not produce /site/index.html. Showing builder logs (if any) from last run isn’t possible because it was --rm."
  echo "   Re-run interactively to see output:"
  echo "     docker compose --profile build run --rm --no-deps builder"
  return 1
}

# -------- main flow --------

echo "🛑 Stopping running containers..."
# use a timeout so down can’t wedge either
set +e
run_compose 60s down --remove-orphans
set -e

echo "🔨 Rebuilding images (no cache)…"
run_compose 20m build --no-cache

# Load service lists *after* config is valid
SERVICES="$(docker compose config --services)"
SERVICES_BUILD="$(docker compose --profile build config --services 2>/dev/null || true)"
has() { echo "$SERVICES" | grep -qx "$1"; }
has_build() { echo "$SERVICES_BUILD" | grep -qx "$1"; }

echo "📋 Compose services:"
echo "$SERVICES" | sed 's/^/  - /'

# DB
if has "db"; then
  ensure_up_detached db
  wait_for_pg
else
  echo "⚠️  No db service found; continuing…"
fi

# Admin
if has "admin"; then
  ensure_up_detached admin
else
  echo "⚠️  No admin service found; skipping…"
fi

# Workers (explicitly name services so compose doesn’t start everything)
if has "worker_fetch"; then
  echo "⚙️  Starting worker_fetch (scale=2)…"
  set +e
  run_compose 30s up -d --scale worker_fetch=2 worker_fetch
  set -e
  # verify at least one is running
  if ! docker compose ps | grep -q 'worker_fetch.*Up'; then
    echo "❌ worker_fetch not running. Logs:"
    docker compose logs --tail 200 worker_fetch || true
    exit 1
  fi
fi

if has "worker_llm"; then
  echo "⚙️  Starting worker_llm (scale=1)…"
  set +e
  run_compose 30s up -d --scale worker_llm=1 worker_llm
  set -e
  if ! docker compose ps | grep -q 'worker_llm.*Up'; then
    echo "❌ worker_llm not running. Logs:"
    docker compose logs --tail 200 worker_llm || true
    exit 1
  fi
fi

# Builder: one-shot only
if has_build "builder"; then
  one_shot_builder
else
  echo "⚠️  No builder service in profile 'build'; skipping site build…"
fi

# Web
if has "web"; then
  ensure_up_detached web
else
  echo "⚠️  No web service found; skipping…"
fi

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
echo "   Site:  http://localhost:${WEB_PORT}"