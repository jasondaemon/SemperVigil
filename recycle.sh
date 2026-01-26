#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

echo "🛑 Stopping running containers..."
docker compose down

echo "🔨 Rebuilding images (no cache)..."
docker compose build --no-cache

echo "🗄️  Starting database..."
docker compose up -d db

echo "⏳ Waiting for Postgres to be ready..."
until docker exec "$(docker compose ps -q db)" pg_isready -U "${SV_DB_USER:-sempervigil}" >/dev/null 2>&1; do
  sleep 1
done
echo "✅ Database is ready."

# --- detect services ---
SERVICES="$(docker compose config --services)"
has() { echo "$SERVICES" | grep -qx "$1"; }

echo "📋 Compose services:"
echo "$SERVICES" | sed 's/^/  - /'

# Determine worker services
FETCH_SVC=""
LLM_SVC=""

if has "worker_fetch"; then
  FETCH_SVC="worker_fetch"
fi
if has "worker_llm"; then
  LLM_SVC="worker_llm"
fi

# Legacy fallback
if [[ -z "$FETCH_SVC" && -z "$LLM_SVC" && $(echo "$SERVICES" | grep -c '^worker$' || true) -gt 0 ]]; then
  echo "⚠️  Detected legacy single worker service: worker"
  FETCH_SVC="worker"
fi

if [[ -z "$FETCH_SVC" && -z "$LLM_SVC" ]]; then
  echo "❌ Could not find worker services (worker_fetch/worker_llm or worker)."
  echo "   If you are using profiles, try: docker compose --profile <name> config --services"
  exit 1
fi

echo "⚙️  Starting admin..."
docker compose up -d admin

echo "⚙️  Starting workers..."
if [[ "$FETCH_SVC" == "worker_fetch" ]]; then
  docker compose up -d --scale worker_fetch=2
elif [[ "$FETCH_SVC" == "worker" ]]; then
  docker compose up -d --scale worker=2
fi

if [[ -n "$LLM_SVC" ]]; then
  docker compose up -d --scale worker_llm=1
fi

echo "📝 Running Hugo site build..."
docker compose run --rm builder

echo "🌍 Starting public web server..."
docker compose up -d web

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${SV_ADMIN_PORT:-8001}"
echo "   Site:  http://localhost:${SV_WEB_PORT:-8080}"