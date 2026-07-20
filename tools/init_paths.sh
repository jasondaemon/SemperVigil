#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STACK_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
NFS_ROOT="${SV_NFS_ROOT:-${STACK_ROOT}}"
SITE_SRC_DIR="${SV_SITE_SRC_DIR:-${NFS_ROOT}/site-src}"
SITE_PUBLIC_DIR="${SV_SITE_PUBLIC_DIR:-${NFS_ROOT}/site-public}"
DATA_DIR="${SV_DATA_DIR:-${NFS_ROOT}/data}"
FEED_ARCHIVE_DIR="${SV_FEED_ARCHIVE_DIR:-${SITE_PUBLIC_DIR}/shared/feed}"

umask "${SV_UMASK:-002}"

mkdir -p \
  "${DATA_DIR}" \
  "${SITE_SRC_DIR}/content/posts" \
  "${SITE_SRC_DIR}/content/events" \
  "${SITE_SRC_DIR}/content/cves" \
  "${SITE_SRC_DIR}/content/daily" \
  "${SITE_SRC_DIR}/static/sempervigil" \
  "${SITE_PUBLIC_DIR}" \
  "${FEED_ARCHIVE_DIR}/days"

# Make dirs group-writable and keep group sticky so new files inherit group.
chmod -R u+rwX,g+rwX,o+rX "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" "${DATA_DIR}" || true
find "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" "${DATA_DIR}" -type d -exec chmod g+s {} \; 2>/dev/null || true

echo "OK: initialized NFS paths"
echo "  site-src: ${SITE_SRC_DIR}"
echo "  site-public: ${SITE_PUBLIC_DIR}"
echo "  feed-archive: ${FEED_ARCHIVE_DIR}"
echo "  data: ${DATA_DIR}"
