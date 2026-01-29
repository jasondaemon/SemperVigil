#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

DB_USER="${SV_DB_USER:-sempervigil}"
ADMIN_PORT="${SV_ADMIN_PORT:-8001}"
WEB_PORT="${SV_WEB_PORT:-8080}"

# Optional toggles (set in env or .env before running)
SV_ENABLE_SEARCH="${SV_ENABLE_SEARCH:-0}"  # 1 => start searxng profile

# --- stop everything ---
echo "🛑 Stopping running containers..."
docker compose down --remove-orphans

# --- rebuild ---
echo "🔨 Rebuilding images (no cache)..."
docker compose build --no-cache

# --- start db ---
echo "🗄️  Starting database..."
docker compose up -d db

echo "⏳ Waiting for Postgres to be ready..."
DB_CID="$(docker compose ps -q db)"
if [[ -z "${DB_CID}" ]]; then
  echo "❌ Could not resolve db container id"
  exit 1
fi

# Wait up to 60s for DB
for i in $(seq 1 60); do
  if docker exec "${DB_CID}" pg_isready -U "${DB_USER}" >/dev/null 2>&1; then
    echo "✅ Database is ready."
    break
  fi
  sleep 1
  if [[ "$i" == "60" ]]; then
    echo "❌ Database did not become ready within 60s"
    docker logs "${DB_CID}" || true
    exit 1
  fi
done

# --- start app services ---
echo "⚙️  Starting admin + workers + web..."
docker compose up -d \
  --scale worker_fetch=2 \
  --scale worker_llm=1 \
  admin worker_fetch worker_llm web

# --- start builder scheduler (always-on) ---
echo "🏗️  Starting builder scheduler..."
docker compose up -d builder_scheduler

# Optional: internal search (SearXNG) via compose profile
if [[ "${SV_ENABLE_SEARCH}" == "1" ]]; then
  echo "🔎 Starting SearXNG (profile: search)..."
  docker compose --profile search up -d searxng
fi

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
echo "   Site:  http://localhost:${WEB_PORT}"
echo
echo "ℹ️  Builder scheduler is running (polling build jobs)."
echo "   To trigger a build manually (if needed):"
echo "     docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder && \\"
echo "     docker compose --profile build rm -f -s builder"