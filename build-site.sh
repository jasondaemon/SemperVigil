#!/usr/bin/env bash
set -euo pipefail
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-sempervigil}"

if command -v timeout >/dev/null 2>&1; then
  timeout 10m docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder
else
  docker compose --profile build up --no-deps --abort-on-container-exit --exit-code-from builder builder
fi

docker compose --profile build rm -f -s builder >/dev/null 2>&1 || true
echo "✅ Site build complete."