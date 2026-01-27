#!/bin/sh
set -e

UMASK_VALUE="${SV_UMASK:-002}"
umask "${UMASK_VALUE}" || umask 002

DATA_DIR="${SV_DATA_DIR:-/data}"
SITE_SRC_DIR="${SV_HUGO_SOURCE_DIR:-/site-src}"
SITE_PUBLIC_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
SV_UID="${SV_UID:-1000}"
SV_GID="${SV_GID:-1000}"

mkdir -p \
  "${DATA_DIR}/logs" \
  "${SITE_SRC_DIR}" \
  "${SITE_SRC_DIR}/content/posts" \
  "${SITE_SRC_DIR}/content/events" \
  "${SITE_SRC_DIR}/content/cves" \
  "${SITE_SRC_DIR}/content/briefs" \
  "${SITE_SRC_DIR}/static/sempervigil" \
  "${SITE_SRC_DIR}/static/sempervigil/briefs" \
  "${SITE_SRC_DIR}/resources" \
  "${SITE_PUBLIC_DIR}"

if [ "$(id -u)" = "0" ]; then
  chown -R "${SV_UID}:${SV_GID}" "${DATA_DIR}" "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" || true
else
  chmod -R u+rwX,g+rwX "${DATA_DIR}" "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" || true
  if command -v setfacl >/dev/null 2>&1; then
    setfacl -R -m "u:${SV_UID}:rwx" -m "g:${SV_GID}:rwx" "${DATA_DIR}" "${SITE_SRC_DIR}" "${SITE_PUBLIC_DIR}" || true
  fi
fi
