#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

umask "${SV_UMASK:-002}"

mkdir -p \
  "$ROOT/config" \
  "$ROOT/data" \
  "$ROOT/site/public" \
  "$ROOT/site/content/posts" \
  "$ROOT/site/content/events" \
  "$ROOT/site/content/cves"

# Make dirs group-writable and keep group sticky so new files inherit group.
chmod -R u+rwX,g+rwX,o+rX "$ROOT/site" "$ROOT/data" "$ROOT/config" || true
find "$ROOT/site" "$ROOT/data" "$ROOT/config" -type d -exec chmod g+s {} \; 2>/dev/null || true

echo "OK: initialized paths under $ROOT"
echo "Tip: run once after clone on the NFS host:  ./tools/init_paths.sh"