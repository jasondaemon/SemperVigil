# Dependency-aware overlay: preserve the verified runtime, add the one new
# tokenizer dependency explicitly, then install the reviewed source revision.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG SOURCE_REVISION
ARG RUNTIME_USER=1000:1000
USER root
RUN python -m pip install --no-cache-dir 'tiktoken==0.14.0'
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
COPY src/ /app/src/
COPY pyproject.toml /app/pyproject.toml
USER ${RUNTIME_USER}
