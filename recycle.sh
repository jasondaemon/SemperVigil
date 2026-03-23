#!/usr/bin/env bash
set -euo pipefail

echo "🧹 SemperVigil full recycle starting…"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

DB_USER="${SV_DB_USER:-sempervigil}"
ADMIN_PORT="${SV_ADMIN_PORT:-8001}"
WEB_PORT="${SV_WEB_PORT:-8080}"
START_WEB=0
STOP_WEB=0

for arg in "$@"; do
  case "${arg}" in
    --with-web)
      START_WEB=1
      STOP_WEB=1
      ;;
  esac
done

# Optional toggles (set in env or .env before running)
SV_ENABLE_SEARCH="${SV_ENABLE_SEARCH:-0}"  # 1 => start searxng profile

# --- stop services ---
if [[ "${STOP_WEB}" == "1" ]]; then
  echo "🛑 Stopping running containers (including web)..."
  docker compose down --remove-orphans
else
  echo "🛑 Stopping running containers (leaving web up)..."
  docker compose stop admin worker_fetch worker_llm worker_openai build_worker db vpn || true
  docker compose rm -f admin worker_fetch worker_llm worker_openai build_worker db vpn || true
fi

# --- NEW: remove ONLY locally-built service images (the ones that will be rebuilt) ---
echo "🧽 Removing old locally-built images for buildable services..."

# Get services that have a `build:` section in the fully-rendered compose config
BUILD_SERVICES="$(
  docker compose config 2>/dev/null \
  | awk '
      $1 == "services:" { in_services=1; next }
      in_services && /^[^[:space:]]/ { in_services=0 }
      in_services && /^[[:space:]]{2}[a-zA-Z0-9_.-]+:$/ { svc=$1; sub(/:$/,"",svc); in_svc=1; next }
      in_svc && /^[[:space:]]{4}build:/ { print svc; in_svc=0; next }
      in_svc && /^[[:space:]]{2}[a-zA-Z0-9_.-]+:$/ { svc=$1; sub(/:$/,"",svc); next }
    ' \
  | sort -u
)"

if [[ -z "${BUILD_SERVICES}" ]]; then
  echo "ℹ️  No buildable services found (no `build:` sections). Skipping image purge."
else
  echo "🔧 Buildable services: ${BUILD_SERVICES}"
  while read -r svc; do
    [[ -z "${svc}" ]] && continue
    IMG_IDS="$(docker image ls -q \
      --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}" \
      --filter "label=com.docker.compose.service=${svc}" \
      | sort -u || true)"
    if [[ -n "${IMG_IDS}" ]]; then
      echo "  🗑️  Removing images for service: ${svc}"
      echo "${IMG_IDS}" | xargs -r docker image rm -f || true
    else
      echo "  ℹ️  No images found for service: ${svc}"
    fi
  done <<< "${BUILD_SERVICES}"
  echo "✅ Locally-built service images removed."
fi

# --- rebuild ---
echo "🔨 Rebuilding images (no cache)..."
docker compose build --no-cache

# --- start db ---
echo "🗄️  Starting database..."
docker compose up -d db

# --- start vpn ---
echo "🗄️  Starting vpn..."
docker compose up -d vpn

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
echo "⚙️  Starting admin + workers..."
docker compose up -d \
  --scale worker_fetch=2 \
  --scale worker_llm=1 \
  --scale worker_openai=1 \
  admin worker_fetch worker_llm worker_openai

if [[ "${START_WEB}" == "1" ]]; then
  echo "🌐 Starting web (requested)..."
  docker compose up -d web
fi

# --- start builder scheduler (always-on) ---
echo "🏗️  Starting builder scheduler..."
docker compose up -d build_worker

# Optional: internal search (SearXNG) via compose profile
if [[ "${SV_ENABLE_SEARCH}" == "1" ]]; then
  echo "🔎 Starting SearXNG (profile: search)..."
  docker compose --profile search up -d searxng
fi

echo "🎉 SemperVigil recycle complete."
echo "   Admin: http://localhost:${ADMIN_PORT}"
if [[ "${START_WEB}" == "1" ]]; then
  echo "   Site:  http://localhost:${WEB_PORT}"
else
  echo "   Site:  (web left running; use --with-web to restart)"
fi
echo
echo "ℹ️  Builder scheduler is running (polling build jobs)."
echo "   To trigger a build manually (if needed):"
echo "     docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder && \\"
echo "     docker compose --profile build rm -f -s builder"
