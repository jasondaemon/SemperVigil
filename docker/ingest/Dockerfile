FROM python:3.11-slim

# System deps (keep small)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy minimal project files (you'll add pyproject later)
COPY . /app

# Placeholder: once you add requirements/pyproject, install deps here
# RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Placeholder command: you'll implement `python -m sempervigil` soon
CMD ["python", "-c", "print('SemperVigil ingest container is up. Implement python -m sempervigil next.')"]