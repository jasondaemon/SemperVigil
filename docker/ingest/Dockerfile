FROM python:3.11-slim

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1

# System tools (sqlite3 for inspection, curl for debugging)
RUN apt-get update \
 && apt-get install -y --no-install-recommends sqlite3 curl \
 && rm -rf /var/lib/apt/lists/*

# Install SemperVigil + deps (from pyproject.toml)
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e .

# Default command (you can override in docker compose run ...)
CMD ["python", "-m", "sempervigil", "--help"]
