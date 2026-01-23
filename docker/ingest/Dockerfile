FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

ARG INSTALL_TEST_DEPS=0
ARG SV_UID=1000
ARG SV_GID=1000

WORKDIR /app
COPY . /app

# System tools (sqlite3 for inspection, curl for debugging)
RUN apt-get update -o Acquire::Retries=3 \
 && apt-get install -y --no-install-recommends --fix-missing sqlite3 curl \
 && rm -rf /var/lib/apt/lists/*

# Install SemperVigil + deps (from pyproject.toml)
RUN python -m pip install --upgrade pip \
 && if [ "$INSTALL_TEST_DEPS" = "1" ]; then \
      python -m pip install -e '.[test]'; \
    else \
      python -m pip install -e .; \
    fi

# Create a non-root user that matches your compose SV_UID/SV_GID (1000:1000 by default),
# and ensure writable runtime dirs for local Docker volumes.
RUN groupadd -g ${SV_GID} sv \
 && useradd -m -u ${SV_UID} -g ${SV_GID} sv \
 && mkdir -p /data /site /config \
 && chown -R ${SV_UID}:${SV_GID} /data /site /config \
 && chmod -R a+rX /app

USER ${SV_UID}:${SV_GID}

# Default command (override in compose)
CMD ["python", "-m", "sempervigil", "--help"]