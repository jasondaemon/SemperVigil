#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

# --- helpers ---
SERVICES="$(docker compose config --services)"
SERVICES_BUILD="$(docker compose --profile build config --services)"
has() { echo "$SERVICES" | grep -qx "$1"; }
has_build() { echo "$SERVICES_BUILD" | grep -qx "$1"; }

DB_USER="${SV_DB_USER:-sempervigil}"
ADMIN_PORT="${SV_ADMIN_PORT:-8001}"
WEB_PORT="${SV_WEB_PORT:-8080}"

# Prefer GNU timeout; fall back to gtimeout (macOS coreutils) if available
TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
fi

# --- stop ---
echo "🛑 Stopping running containers..."
docker compose down --remove-orphans

# --- build ---
echo "🔨 Rebuilding images (no cache)..."
docker compose build --no-cache

# --- db up ---
if has "db"; then
  echo "🗄️  Starting database..."
  docker compose up -d db

  echo "⏳ Waiting for Postgres to be ready..."
  DB_CID="$(docker compose ps -q db)"
  until docker exec "$DB_CID" pg_isready -U "$DB_USER" >/dev/null 2>&1; do
    sleep 1
  done
  echo "✅ Database is ready."
else
  echo "⚠️  No db service found; continuing..."
fi

echo "📋 Compose services:"
echo "$SERVICES" | sed 's/^/  - /'

# --- identify workers ---
FETCH_SVC=""
LLM_SVC=""

if has "worker_fetch"; then FETCH_SVC="worker_fetch"; fi
if has "worker_llm"; then LLM_SVC="worker_llm"; fi

# Legacy fallback
if [[ -z "$FETCH_SVC" && -z "$LLM_SVC" ]] && echo "$SERVICES" | grep -qx "worker"; then
  echo "⚠️  Detected legacy single worker service: worker"
  FETCH_SVC="worker"
fi

if [[ -z "$FETCH_SVC" && -z "$LLM_SVC" ]]; then
  echo "❌ Could not find worker services (worker_fetch/worker_llm or worker)."
  exit 1
fi

# --- start admin ---
if has "admin"; then
  echo "⚙️  Starting admin..."
  docker compose up -d admin
else
  echo "⚠️  No admin service found; skipping..."
fi

# --- start workers (IMPORTANT: specify service names so Compose doesn't start everything) ---
echo "⚙️  Starting workers..."
if [[ "$FETCH_SVC" == "worker_fetch" ]]; then
  docker compose up -d --scale worker_fetch=2 worker_fetch
elif [[ "$FETCH_SVC" == "worker" ]]; then
  docker compose up -d --scale worker=2 worker
fi

if [[ -n "$LLM_SVC" ]]; then
  docker compose up -d --scale worker_llm=1 worker_llm
fi

# --- ensure builder service container isn't running (prevents "extra builder") ---
if has "builder"; then
  echo "🧽 Ensuring builder service isn't running..."
  docker compose stop builder >/dev/null 2>&1 || true
  docker compose rm -f builder >/dev/null 2>&1 || true
fi

# --- build site once (profile-gated one-shot) ---
if has_build "builder"; then
  echo "📝 Running Hugo site build (one-shot via profile 'build')..."
  if [[ -n "$TIMEOUT_BIN" ]]; then
    "$TIMEOUT_BIN" 10m docker compose --profile build run --rm --no-deps builder
  else
    docker compose --profile build run --rm --no-deps builder
  fi
else
  echo "⚠️  No builder in '--profile build' services; skipping site build..."
fi

# --- start web ---
if has "web"; then
  echo "🌍 Starting public web server..."
  docker compose up -d web
else
  echo "⚠️  No web service found; skipping..."
fi

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
echo "   Site:  http://localhost:${WEB_PORT}"