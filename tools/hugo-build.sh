#!/bin/sh
set -e

UMASK_VALUE="${SV_UMASK:-002}"
umask "${UMASK_VALUE}" || umask 002

mkdir -p /site/public /site/content/posts /site/content/events /site/content/cves
chmod 775 /site/public /site/content /site/content/posts /site/content/events /site/content/cves || true

exec hugo -s /site -d /site/public --minify --gc --cleanDestinationDir --logLevel info --baseURL "${SV_HUGO_BASEURL:-http://localhost:18080/}"
