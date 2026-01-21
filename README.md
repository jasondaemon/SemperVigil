# SemperVigil

## Quickstart

1) Copy the example config:

```bash
cp config.example.yml config/config.yml
```

2) Import sources into the state DB:

```bash
docker compose run --rm ingest sempervigil sources import /config/sources.example.yml
```

3) Run the ingest container:

```bash
docker compose up --build ingest
```

Outputs land in `site/content/posts` and the JSON index (if enabled) at
`site/static/sempervigil/index.json`.

## Test a source

```bash
docker compose run --rm ingest sempervigil test-source cisa-alerts
```

This prints a per-item accept/filter reason so you can adjust keywords or
diagnose feed issues quickly.

## License

This project is licensed under the **GNU General Public License v3.0 (GPL-3.0)**.

You are free to use, modify, and redistribute this software under the terms of the GPL.
Any distributed derivative works must also be licensed under GPL-3.0.
