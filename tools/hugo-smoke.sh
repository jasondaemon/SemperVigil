#!/bin/sh
set -e

sh /tools/hugo-build.sh

if [ ! -f "${SV_HUGO_OUTPUT_DIR:-/site}/index.html" ]; then
  echo "error: smoke build missing index.html"
  exit 1
fi

echo "ok: smoke build succeeded"
