# Deprecated: builder now uses hugomods/hugo:exts via docker-compose.
# Keep this file only for reference; it is not used by default.
FROM python:3.11-slim

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends hugo \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e .

CMD ["sempervigil-builder"]
