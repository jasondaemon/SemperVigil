# Builder image derived from modern Hugo, with Python installed for job handling.
FROM hugomods/hugo:exts

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1

RUN apk add --no-cache python3 py3-pip py3-setuptools

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -e .

CMD ["sempervigil-builder", "--once"]
