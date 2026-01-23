# docker/builder/Dockerfile
FROM hugomods/hugo:exts

WORKDIR /app
COPY . /app

# Alpine Python is "externally managed" (PEP 668), so install into a venv.
RUN apk add --no-cache python3 py3-pip py3-setuptools \
 && python3 -m venv /venv \
 && /venv/bin/pip install --no-cache-dir --upgrade pip \
 && /venv/bin/pip install --no-cache-dir -e .

ENV PATH="/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# If you want, you can also make sure scripts are executable/readable:
# RUN chmod -R a+rX /app

# Default can be overridden by compose
CMD ["sempervigil-builder", "--once"]