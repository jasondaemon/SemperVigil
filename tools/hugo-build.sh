#!/bin/sh
set -e

sh /tools/ensure-dirs.sh

hugo -s /site -d /site/public --minify --gc --cleanDestinationDir --logLevel info

echo "Hugo output:"
ls -la /site/public || true

if [ ! -f /site/public/index.html ]; then
  echo "warning: /site/public/index.html not found"
fi
