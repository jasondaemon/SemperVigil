#!/bin/sh
set -e

UMASK_VALUE="${SV_UMASK:-002}"
umask "${UMASK_VALUE}" || umask 002

mkdir -p /data \
  /site/content/posts \
  /site/content/events \
  /site/content/cves \
  /site/public \
  /site/static/sempervigil

chmod -R u+rwX,g+rwX /data /site/content /site/public /site/static/sempervigil || true
