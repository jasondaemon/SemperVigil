#!/bin/sh
set -e

sh /tools/ensure-dirs.sh

SOURCE_DIR="${SV_HUGO_SOURCE_DIR:-/repo/site}"
OUTPUT_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
CACHE_DIR="${SV_HUGO_CACHE_DIR:-/tmp/hugo_cache}"
MODULES_DIR="${SV_HUGO_MODULES_DIR:-/tmp/hugo_modules}"
SYNC_TEMPLATES="${SV_SYNC_SITE_TEMPLATES:-0}"
LOG_DIR="${SV_LOG_DIR:-/log}"
LOG_FILE="${SV_HUGO_LOG_FILE:-${LOG_DIR}/hugo-build.log}"
MAX_LOG_BYTES="${SV_HUGO_BUILD_LOG_MAX_BYTES:-5242880}"
LOCK_FILE="${SV_HUGO_LOCK_FILE:-/data/hugo-build.lock}"
LOCK_DIR="${SV_HUGO_LOCK_DIR:-/data/hugo-build.lockdir}"
RESOURCE_BASE="${SV_HUGO_RESOURCE_BASE:-/data/hugo_resources}"
BUILD_CONFIG_DIR="${SV_HUGO_BUILD_CONFIG_DIR:-/data/hugo_build_configs}"
RELEASES_DIR="${OUTPUT_DIR}/releases"
CURRENT_LINK="${OUTPUT_DIR}/current"
STANDBY_DIR="${OUTPUT_DIR}/standby"
KEEP_RELEASES="${SV_HUGO_KEEP_RELEASES:-3}"

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$LOG_FILE")"
mkdir -p "$RESOURCE_BASE"
mkdir -p "$BUILD_CONFIG_DIR"
mkdir -p "$RELEASES_DIR"
mkdir -p "$STANDBY_DIR"

if [ ! -f "${STANDBY_DIR}/index.html" ]; then
  cat > "${STANDBY_DIR}/index.html" <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Stand By…</title>
    <style>
      body { font-family: system-ui, sans-serif; background:#0f172a; color:#e2e8f0; display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; }
      .card { max-width:520px; padding:24px 28px; border:1px solid rgba(148,163,184,.25); border-radius:16px; background:rgba(15,23,42,.6); }
      h1 { margin:0 0 8px; font-size:20px; }
      p { margin:6px 0; color:#94a3b8; }
      .time { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color:#cbd5f5; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Stand By… More news is rendering</h1>
      <p>This page will refresh automatically in a few seconds.</p>
      <p class="time">Building…</p>
    </div>
  </body>
</html>
EOF
fi

if [ ! -L "$CURRENT_LINK" ] && [ ! -d "$CURRENT_LINK" ]; then
  ln -s "standby" "$CURRENT_LINK"
fi

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

if [ -f "$LOG_FILE" ] && [ "$MAX_LOG_BYTES" -gt 0 ]; then
  log_size="$(wc -c < "$LOG_FILE" 2>/dev/null || echo 0)"
  if [ "$log_size" -gt "$MAX_LOG_BYTES" ]; then
    keep_bytes=$((MAX_LOG_BYTES / 2))
    if [ "$keep_bytes" -lt 1 ]; then
      keep_bytes=1
    fi
    tail -c "$keep_bytes" "$LOG_FILE" > "${LOG_FILE}.tmp" 2>/dev/null || true
    mv -f "${LOG_FILE}.tmp" "$LOG_FILE"
  fi
fi

while [ $attempt -le $max_attempts ]; do
  ts="$(date -u +%Y%m%d%H%M%S)"
  resource_dir="${RESOURCE_BASE}/${ts}.$$"
  mkdir -p "$resource_dir"
  release_dir="${RELEASES_DIR}/${ts}"
  mkdir -p "$release_dir"
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

  log_file="$LOG_FILE"
  echo "Hugo build attempt ${attempt}/${max_attempts} (log: ${log_file})"
  {
    echo "Hugo version:"
    hugo version
    echo "Build start: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$log_file" 2>&1
  set +e
  hugo -s "$SOURCE_DIR" -d "$release_dir" --baseURL "${SV_HUGO_BASEURL:-/}" --minify --cleanDestinationDir --logLevel info --cacheDir "$CACHE_DIR" "$config_flag" "$config_value" >>"$log_file" 2>&1
  exit_code=$?
  set -e

  if [ $exit_code -eq 0 ] && [ -f "$release_dir/index.html" ]; then
    buildinfo="${release_dir}/.buildinfo.json"
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '{\"generated_at\":\"%s\",\"release\":\"%s\"}\n' "$now" "$ts" >"$buildinfo"
    rel_release="releases/${ts}"
    ln -sfn "$rel_release" "$CURRENT_LINK"
    if [ -d "$RELEASES_DIR" ]; then
      count=0
      for dir in $(ls -1dt "$RELEASES_DIR"/* 2>/dev/null); do
        count=$((count + 1))
        if [ $count -gt "$KEEP_RELEASES" ]; then
          rm -rf "$dir" || true
        fi
      done
    fi
    rm -rf "$resource_dir"
    rm -f "$build_config"
    if [ -n "$temp_config_dir" ]; then
      rm -rf "$temp_config_dir"
    fi
    success=1
    break
  fi

  echo "Hugo build failed (attempt ${attempt}). Log: ${log_file}"
  rm -rf "$release_dir"
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
ls -la "$CURRENT_LINK" || true

if [ ! -f "$CURRENT_LINK/index.html" ]; then
  echo "error: ${CURRENT_LINK}/index.html not found"
  exit 1
fi
