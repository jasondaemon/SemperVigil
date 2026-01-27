#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

DB_USER="${SV_DB_USER:-sempervigil}"
ADMIN_PORT="${SV_ADMIN_PORT:-8001}"
WEB_PORT="${SV_WEB_PORT:-8080}"

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
until docker exec "$(docker compose ps -q db)" pg_isready -U "$DB_USER" >/dev/null 2>&1; do
  sleep 1
done
echo "✅ Database is ready."

# --- start app services ---
echo "⚙️  Starting admin..."
docker compose up -d admin

echo "⚙️  Starting workers..."
docker compose up -d --scale worker_fetch=2 worker_fetch
docker compose up -d --scale worker_llm=1 worker_llm

echo "🌍 Starting public web server..."
docker compose up -d web

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
echo "   Site:  http://localhost:${WEB_PORT}"
echo
echo "ℹ️  Site builds are on-demand (builder is not started during recycle)."
echo "   To build the site now:"
echo "     docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder && \\"
echo "     docker compose --profile build rm -f -s builder"