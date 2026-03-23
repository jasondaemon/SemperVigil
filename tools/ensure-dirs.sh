#!/bin/sh
set -e

UMASK_VALUE="${SV_UMASK:-002}"
umask "${UMASK_VALUE}" || umask 002

DATA_DIR="${SV_DATA_DIR:-/data}"
LOGS_DIR="${SV_LOG_DIR:-/log}"
SITE_SRC_DIR="${SV_HUGO_SOURCE_DIR:-/site-src}"
SITE_PUBLIC_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
HUGO_CACHE_DIR="${SV_HUGO_CACHE_DIR:-${DATA_DIR}/hugo_cache}"
HUGO_MODULES_DIR="${SV_HUGO_MODULES_DIR:-${DATA_DIR}/hugo_modules}"
SV_UID="${SV_UID:-1000}"
SV_GID="${SV_GID:-1000}"
SV_FIX_SITE_PERMS="${SV_FIX_SITE_PERMS:-1}"

mkdir -p \
  "${LOGS_DIR}" \
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
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/content/daily"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/static/sempervigil"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/static/sempervigil/briefs"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/resources"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/data"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/data/articles"
ensure_if_mounted_or_writable "${SITE_SRC_DIR}/data/daily"
ensure_if_mounted_or_writable "${SITE_PUBLIC_DIR}"
ensure_if_mounted_or_writable "${SITE_PUBLIC_DIR}/releases"
ensure_if_mounted_or_writable "${SITE_PUBLIC_DIR}/standby"

fix_perms_tree() {
  target="$1"
  if [ ! -d "$target" ]; then
    return
  fi
  if [ "$(id -u)" = "0" ]; then
    find "$target" -name '@eaDir' -prune -o -exec chown "${SV_UID}:${SV_GID}" {} + || true
  else
    find "$target" -name '@eaDir' -prune -o -exec chmod u+rwX,g+rwX {} + || true
    if command -v setfacl >/dev/null 2>&1; then
      find "$target" -name '@eaDir' -prune -o -exec setfacl -m "u:${SV_UID}:rwx" -m "g:${SV_GID}:rwx" {} + || true
    fi
  fi
}

if [ "${SV_FIX_SITE_PERMS}" != "0" ]; then
  fix_perms_tree "${DATA_DIR}"
  fix_perms_tree "${LOGS_DIR}"
  fix_perms_tree "${SITE_SRC_DIR}"
  fix_perms_tree "${SITE_PUBLIC_DIR}"
fi
