#!/bin/sh
set -e

sh /tools/ensure-dirs.sh

SOURCE_DIR="${SV_HUGO_SOURCE_DIR:-/repo/site}"
OUTPUT_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"
CACHE_DIR="${SV_HUGO_CACHE_DIR:-/tmp/hugo_cache}"
MODULES_DIR="${SV_HUGO_MODULES_DIR:-/tmp/hugo_modules}"

if [ ! -f "${SOURCE_DIR}/hugo.toml" ] && [ ! -d "${SOURCE_DIR}/config" ] && [ ! -d "${SOURCE_DIR}/config/_default" ]; then
  if [ -d "/repo/site" ]; then
    echo "Seeding Hugo source into ${SOURCE_DIR}..."
    cp -a /repo/site/. "${SOURCE_DIR}/"
  fi
fi
mkdir -p "${SOURCE_DIR}/resources"
export HUGO_MODCACHEDIR="$MODULES_DIR"

hugo -s "$SOURCE_DIR" -d "$OUTPUT_DIR" --baseURL "${SV_HUGO_BASEURL:-/}" --minify --gc --cleanDestinationDir --logLevel info --noBuildLock --cacheDir "$CACHE_DIR"

echo "Hugo output:"
ls -la "$OUTPUT_DIR" || true

if [ ! -f "$OUTPUT_DIR/index.html" ]; then
  echo "error: ${OUTPUT_DIR}/index.html not found"
  exit 1
fi
