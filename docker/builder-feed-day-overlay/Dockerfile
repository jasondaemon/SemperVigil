ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG SOURCE_REVISION
RUN printf '%s  %s\n' \
      cfaff3151682cde635decaf1a4621b544415029426887e3118d4b7fcdd0682ec /app/src/sempervigil/storage.py \
      9f079d28cb9cc4e32297a807878664bd79222e6721c33ed241bb78f3a80f7a98 /app/src/sempervigil/feed_inventory.py \
    | sha256sum -c -
COPY src/sempervigil/storage.py /tmp/feed-day-storage.py
COPY src/sempervigil/feed_inventory.py /app/src/sempervigil/feed_inventory.py
COPY docker/builder-feed-day-overlay/overlay.py /tmp/feed-day-overlay.py
RUN python /tmp/feed-day-overlay.py /app/src/sempervigil/storage.py /tmp/feed-day-storage.py \
    && python -m compileall -q /app/src/sempervigil/storage.py /app/src/sempervigil/feed_inventory.py \
    && rm /tmp/feed-day-storage.py /tmp/feed-day-overlay.py

LABEL org.opencontainers.image.revision="${SOURCE_REVISION}"
LABEL sempervigil.feed-day.overlay="eastern-cve-day-v1"
