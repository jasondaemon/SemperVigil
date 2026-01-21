FROM python:3.11-slim

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1

# Install SemperVigil + deps (from pyproject.toml)
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e .

# Default command (you can override in docker compose run ...)
CMD ["python", "-m", "sempervigil", "--help"]