#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
NFS_ROOT="${SV_NFS_ROOT:-/nfs/sempervigil}"
SITE_SRC_DIR="${SV_SITE_SRC_DIR:-${NFS_ROOT}/site-src}"
SITE_PUBLIC_DIR="${SV_SITE_PUBLIC_DIR:-${NFS_ROOT}/site-public}"
DATA_DIR="${SV_DATA_DIR:-${NFS_ROOT}/data}"

umask "${SV_UMASK:-002}"

mkdir -p \
  "${DATA_DIR}" \
  "${SITE_SRC_DIR}/content/posts" \
  "${SITE_SRC_DIR}/content/events" \
  "${SITE_SRC_DIR}/content/cves" \
  "${SITE_SRC_DIR}/content/briefs" \
  "${SITE_SRC_DIR}/static/sempervigil" \
  "${SITE_PUBLIC_DIR}"

# Make dirs group-writable and keep group sticky so new files inherit group.
chmod -R u+rwX,g+rwX,o+rX "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" "${DATA_DIR}" || true
find "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" "${DATA_DIR}" -type d -exec chmod g+s {} \; 2>/dev/null || true

echo "OK: initialized NFS paths"
echo "  site-src: ${SITE_SRC_DIR}"
echo "  site-public: ${SITE_PUBLIC_DIR}"
echo "  data: ${DATA_DIR}"
