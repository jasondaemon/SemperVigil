#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"

# Optional: set project name explicitly (recommended)
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

# 1) Stop everything (keep volumes)
echo "🛑 Stopping running containers..."
docker compose down

# Uncomment the next line if you EVER want a truly clean slate (DELETES DB + site)
# docker compose down -v

# 2) Rebuild images with fresh code
echo "🔨 Rebuilding images (no cache)..."
docker compose build --no-cache

# 3) Start Postgres first
echo "🗄️  Starting database..."
docker compose up -d db

echo "⏳ Waiting for Postgres to be ready..."
until docker exec "$(docker compose ps -q db)" pg_isready -U "${SV_DB_USER:-sempervigil}" >/dev/null 2>&1; do
  sleep 1
done
echo "✅ Database is ready."

# 4) Start admin + workers (runs migrations, starts pipelines)
echo "⚙️  Starting admin and workers..."
docker compose up -d admin \
  --scale worker_fetch=2 \
  --scale worker_llm=1

# 5) Run Hugo build once (on-demand)
echo "📝 Running Hugo site build..."
docker compose run --rm builder

# 6) Start public web server
echo "🌍 Starting public web server..."
docker compose up -d web

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${SV_ADMIN_PORT:-8001}"
echo "   Site:  http://localhost:${SV_WEB_PORT:-8080}"