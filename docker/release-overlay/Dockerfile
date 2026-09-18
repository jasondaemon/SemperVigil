# For source-only releases with an unchanged dependency contract. BASE_IMAGE must
# be the retained, verified production image; do not use this for dependency bumps.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
COPY src/ /app/src/
