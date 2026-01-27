# docker/builder/Dockerfile
FROM hugomods/hugo:0.154.5

WORKDIR /app
COPY . /app

RUN apk add --no-cache python3 py3-pip py3-setuptools \
 && python3 -m venv /venv \
 && /venv/bin/pip install --no-cache-dir --upgrade pip \
 && /venv/bin/pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/venv \
    PATH="/venv/bin:$PATH"

CMD ["sempervigil-builder", "--once"]
