#!/bin/sh
set -e

sh /tools/ensure-dirs.sh

SOURCE_DIR="${SV_HUGO_SOURCE_DIR:-/repo/site}"
OUTPUT_DIR="${SV_HUGO_OUTPUT_DIR:-/site}"

hugo -s "$SOURCE_DIR" -d "$OUTPUT_DIR" --baseURL "${SV_HUGO_BASEURL:-/}" --minify --gc --cleanDestinationDir --logLevel info

echo "Hugo output:"
ls -la "$OUTPUT_DIR/public" || true

if [ ! -f "$OUTPUT_DIR/public/index.html" ]; then
  echo "warning: ${OUTPUT_DIR}/public/index.html not found"
fi
