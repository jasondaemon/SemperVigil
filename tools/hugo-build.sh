#!/bin/sh
set -e

sh /tools/ensure-dirs.sh

SOURCE_DIR="${SV_HUGO_SOURCE_DIR:-/repo/site}"
OUTPUT_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
CACHE_DIR="${SV_HUGO_CACHE_DIR:-/tmp/hugo_cache}"
MODULES_DIR="${SV_HUGO_MODULES_DIR:-/tmp/hugo_modules}"
SYNC_TEMPLATES="${SV_SYNC_SITE_TEMPLATES:-0}"
LOG_DIR="${SV_LOG_DIR:-/data/logs/builds}"
LOCK_FILE="${SV_HUGO_LOCK_FILE:-/data/hugo-build.lock}"
LOCK_DIR="${SV_HUGO_LOCK_DIR:-/data/hugo-build.lockdir}"
RESOURCE_BASE="${SV_HUGO_RESOURCE_BASE:-/data/hugo_resources}"
BUILD_CONFIG_DIR="${SV_HUGO_BUILD_CONFIG_DIR:-/data/hugo_build_configs}"

mkdir -p "$LOG_DIR"
mkdir -p "$RESOURCE_BASE"
mkdir -p "$BUILD_CONFIG_DIR"

if [ ! -f "${SOURCE_DIR}/hugo.toml" ] && [ ! -d "${SOURCE_DIR}/config" ] && [ ! -d "${SOURCE_DIR}/config/_default" ]; then
  if [ -d "/repo/site" ]; then
    echo "Seeding Hugo source into ${SOURCE_DIR}..."
    cp -a /repo/site/. "${SOURCE_DIR}/"
  fi
fi

if [ "$SYNC_TEMPLATES" = "1" ] || [ "$SYNC_TEMPLATES" = "true" ] || [ "$SYNC_TEMPLATES" = "yes" ]; then
  if [ -f "/repo/site/layouts/index.html" ]; then
    mkdir -p "${SOURCE_DIR}/layouts"
    echo "Syncing home index layout"
    cp -f /repo/site/layouts/index.html "${SOURCE_DIR}/layouts/index.html"
  fi
  if [ -f "/repo/site/layouts/partials/home/custom.html" ]; then
    mkdir -p "${SOURCE_DIR}/layouts/partials/home"
    echo "Syncing custom home partial"
    cp -f /repo/site/layouts/partials/home/custom.html "${SOURCE_DIR}/layouts/partials/home/custom.html"
  fi
fi

export HUGO_MODCACHEDIR="$MODULES_DIR"

acquire_lock() {
  if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCK_FILE"
    flock 9
    return
  fi
  while true; do
    if mkdir "$LOCK_DIR" 2>/dev/null; then
      echo "$$" > "${LOCK_DIR}/pid"
      date -u +%s > "${LOCK_DIR}/ts"
      return
    fi
    if [ -f "${LOCK_DIR}/ts" ]; then
      now="$(date -u +%s)"
      ts="$(cat "${LOCK_DIR}/ts" 2>/dev/null || echo 0)"
      if [ "$ts" -gt 0 ] && [ $((now - ts)) -gt 14400 ]; then
        rm -rf "$LOCK_DIR" || true
        continue
      fi
    fi
    sleep 2
  done
}

release_lock() {
  if command -v flock >/dev/null 2>&1; then
    flock -u 9 || true
    return
  fi
  rm -rf "$LOCK_DIR" || true
}

trap release_lock EXIT
acquire_lock

attempt=1
max_attempts=3
success=0

while [ $attempt -le $max_attempts ]; do
  ts="$(date -u +%Y%m%d%H%M%S)"
  resource_dir="${RESOURCE_BASE}/${ts}.$$"
  mkdir -p "$resource_dir"
  log_file="${LOG_DIR}/hugo-build-${ts}-attempt${attempt}.log"
  build_config="${BUILD_CONFIG_DIR}/hugo-build-${ts}.$$-attempt${attempt}.toml"
  printf 'resourceDir = "%s"\n' "$resource_dir" >"$build_config"

  config_list=""
  for cfg in hugo.toml hugo.yaml hugo.yml hugo.json config.toml config.yaml config.yml config.json; do
    if [ -f "${SOURCE_DIR}/${cfg}" ]; then
      if [ -z "$config_list" ]; then
        config_list="${SOURCE_DIR}/${cfg}"
      else
        config_list="${config_list},${SOURCE_DIR}/${cfg}"
      fi
    fi
  done

  config_flag=""
  config_value=""
  temp_config_dir=""
  if [ -n "$config_list" ]; then
    config_list="${config_list},${build_config}"
    config_flag="--config"
    config_value="$config_list"
  elif [ -d "${SOURCE_DIR}/config" ] || [ -d "${SOURCE_DIR}/config/_default" ]; then
    temp_config_dir="${BUILD_CONFIG_DIR}/config-${ts}.$$-attempt${attempt}"
    mkdir -p "$temp_config_dir"
    if [ -d "${SOURCE_DIR}/config" ]; then
      if command -v rsync >/dev/null 2>&1; then
        rsync -r --no-owner --no-group \
          --exclude='@eaDir/' \
          --exclude='.DS_Store' \
          --exclude='._*' \
          "${SOURCE_DIR}/config/" "$temp_config_dir/"
      else
        cp -R "${SOURCE_DIR}/config/." "$temp_config_dir/"
      fi
    else
      mkdir -p "${temp_config_dir}/_default"
      if command -v rsync >/dev/null 2>&1; then
        rsync -r --no-owner --no-group \
          --exclude='@eaDir/' \
          --exclude='.DS_Store' \
          --exclude='._*' \
          "${SOURCE_DIR}/config/_default/" "${temp_config_dir}/_default/"
      else
        cp -R "${SOURCE_DIR}/config/_default/." "${temp_config_dir}/_default/"
      fi
    fi
    mkdir -p "${temp_config_dir}/_default"
    cp "$build_config" "${temp_config_dir}/_default/zz_build.toml"
    config_flag="--configDir"
    config_value="$temp_config_dir"
  else
    config_flag="--config"
    config_value="$build_config"
  fi

  echo "Hugo build attempt ${attempt}/${max_attempts} (log: ${log_file})"
  {
    echo "Hugo version:"
    hugo version
  } >"$log_file" 2>&1
  set +e
  hugo -s "$SOURCE_DIR" -d "$OUTPUT_DIR" --baseURL "${SV_HUGO_BASEURL:-/}" --minify --cleanDestinationDir --logLevel info --cacheDir "$CACHE_DIR" "$config_flag" "$config_value" >>"$log_file" 2>&1
  exit_code=$?
  set -e

  if [ $exit_code -eq 0 ] && [ -f "$OUTPUT_DIR/index.html" ]; then
    rm -rf "$resource_dir"
    rm -f "$build_config"
    if [ -n "$temp_config_dir" ]; then
      rm -rf "$temp_config_dir"
    fi
    success=1
    break
  fi

  echo "Hugo build failed (attempt ${attempt}). Log: ${log_file}"
  rm -rf "$resource_dir"
  rm -f "$build_config"
  if [ -n "$temp_config_dir" ]; then
    rm -rf "$temp_config_dir"
  fi
  attempt=$((attempt + 1))
  if [ $attempt -le $max_attempts ]; then
    backoff=$((attempt * 5))
    sleep "$backoff"
  fi
done

if [ $success -ne 1 ]; then
  echo "error: Hugo build failed after ${max_attempts} attempts"
  exit 1
fi

echo "Hugo output:"
ls -la "$OUTPUT_DIR" || true

if [ ! -f "$OUTPUT_DIR/index.html" ]; then
  echo "error: ${OUTPUT_DIR}/index.html not found"
  exit 1
fi
