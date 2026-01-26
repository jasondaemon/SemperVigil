# SemperVigil Docs

SemperVigil is a DB-backed ingestion and publishing pipeline with an internal Admin UI
and a static Hugo public site. SQLite is the system of record; configuration is stored
in the DB (no YAML config files).

## Architecture (Text Diagram)

```
Admin UI/API  -->  Jobs (SQLite)  -->  Worker  -->  Hugo Content  -->  Builder  -->  Public Site
       |                         (ingest/cve/events)            (markdown/json)   (static html)
       +-- Sources/Config in DB
```

## Containers & Volumes

- admin: FastAPI Admin UI/API
- worker: job runner (ingest, CVE sync, markdown writes)
- builder: Hugo build job runner (ephemeral)
- web: Nginx serving /site

Volumes:
- /data: SQLite + runtime state
- /site: Hugo content + public output

## Key Principles

- Deterministic-first processing.
- DB-backed configuration and sources.
- Job queue for orchestration (no shelling out to Docker).
- Hugo stays the public UI.
