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

echo "⚙️  Starting admin..."
docker compose up -d admin

echo "⚙️  Starting workers..."
docker compose up -d --scale worker_fetch=2 worker_fetch
docker compose up -d --scale worker_llm=1 worker_llm

# --- one-shot site build (AVOID `docker compose run` hangs) ---
echo "📝 Running Hugo site build (one-shot)..."
# Run builder in the foreground and exit when it exits
if command -v timeout >/dev/null 2>&1; then
  timeout 10m docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder
else
  docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder
fi

# Clean up the one-shot builder container (since `up` does not auto-remove)
docker compose --profile build rm -f -s builder >/dev/null 2>&1 || true

echo "🌍 Starting public web server..."
docker compose up -d web

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
echo "   Site:  http://localhost:${WEB_PORT}"