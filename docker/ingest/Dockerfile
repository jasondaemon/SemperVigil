FROM python:3.11-slim

# System deps (keep small)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "sempervigil", "run"]
