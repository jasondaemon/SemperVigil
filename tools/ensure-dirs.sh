#!/bin/sh
set -e

UMASK_VALUE="${SV_UMASK:-002}"
umask "${UMASK_VALUE}" || umask 002

DATA_DIR="${SV_DATA_DIR:-/data}"
SITE_SRC_DIR="${SV_HUGO_SOURCE_DIR:-/site-src}"
SITE_PUBLIC_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
HUGO_CACHE_DIR="${SV_HUGO_CACHE_DIR:-${DATA_DIR}/hugo_cache}"
HUGO_MODULES_DIR="${SV_HUGO_MODULES_DIR:-${DATA_DIR}/hugo_modules}"
SV_UID="${SV_UID:-1000}"
SV_GID="${SV_GID:-1000}"

mkdir -p \
  "${DATA_DIR}/logs" \
  "${HUGO_CACHE_DIR}" \
  "${HUGO_MODULES_DIR}"

ensure_if_mounted_or_writable() {
  target="$1"
  if [ -d "$target" ]; then
    mkdir -p "$target" || true
    return
  fi
  parent="$(dirname "$target")"
  if [ -w "$parent" ]; then
    mkdir -p "$target" || true
  fi
}

ensure_if_mounted_or_writable "${SITE_SRC_DIR}"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/content/posts"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/content/events"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/content/cves"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/content/briefs"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/static/sempervigil"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/static/sempervigil/briefs"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/resources"
ensure_if_mounted_or_writable "${SITE_PUBLIC_DIR}"

if [ "$(id -u)" = "0" ]; then
  chown -R "${SV_UID}:${SV_GID}" "${DATA_DIR}" || true
  [ -d "${SITE_SRC_DIR}" ] && chown -R "${SV_UID}:${SV_GID}" "${SITE_SRC_DIR}" || true
  [ -d "${SITE_PUBLIC_DIR}" ] && chown -R "${SV_UID}:${SV_GID}" "${SITE_PUBLIC_DIR}" || true
else
  chmod -R u+rwX,g+rwX "${DATA_DIR}" || true
  [ -d "${SITE_SRC_DIR}" ] && chmod -R u+rwX,g+rwX "${SITE_SRC_DIR}" || true
  [ -d "${SITE_PUBLIC_DIR}" ] && chmod -R u+rwX,g+rwX "${SITE_PUBLIC_DIR}" || true
  if command -v setfacl >/dev/null 2>&1; then
    setfacl -R -m "u:${SV_UID}:rwx" -m "g:${SV_GID}:rwx" "${DATA_DIR}" || true
    [ -d "${SITE_SRC_DIR}" ] && setfacl -R -m "u:${SV_UID}:rwx" -m "g:${SV_GID}:rwx" "${SITE_SRC_DIR}" || true
    [ -d "${SITE_PUBLIC_DIR}" ] && setfacl -R -m "u:${SV_UID}:rwx" -m "g:${SV_GID}:rwx" "${SITE_PUBLIC_DIR}" || true
  fi
fi
