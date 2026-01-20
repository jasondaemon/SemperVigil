# CODEX_RULES — SemperVigil

## Prime Directive
SemperVigil is a configurable, containerized news aggregation system that prioritizes:
- correctness, reproducibility, and debuggability
- configuration-driven behavior
- safe defaults (no secrets committed, no destructive operations)
- clean separation of ingestion, processing, and publishing

Codex must implement changes in small, reviewable steps and keep the system working at every commit.

---

## Workspace & Legacy Scripts
- The legacy scripts are located OUTSIDE the repo at:
  - `../legacyscripts/`
- Codex MAY read/reference those scripts for logic and migration ideas.
- Codex MUST NOT copy them wholesale into the repo.
- Codex MUST NOT add them to the repo, commit them, or create symlinks into the repo.
- Codex MUST treat `../legacyscripts` as read-only historical reference.

Repo must remain clean and portable without the legacy directory present.

---

## Repo Safety Rules
- NEVER commit secrets:
  - API keys, tokens, passwords, private URLs, internal hostnames
  - `.env` files must be ignored; provide `.env.example` instead
- All configuration must have:
  - defaults
  - validation
  - helpful error messages
- Avoid breaking changes to config formats without migration guidance.

---

## Licensing
This project is GPL-3.0.
- Codex must prefer dependencies compatible with GPL-3.0 distribution.
- If uncertain about a dependency’s license, flag it explicitly in the PR notes.

---

## Coding Standards
- Language: Python 3.11+ (unless repo dictates otherwise)
- Style:
  - type hints on public functions
  - structured logging (not print) for runtime paths
  - deterministic output ordering where applicable
- Error handling:
  - fail fast with clear messages
  - isolate per-source failures (one bad feed shouldn’t kill the whole run)
- Tests:
  - add unit tests for non-trivial parsing/normalization
  - keep tests fast and offline (no live network calls by default)

---

## Data & Pipeline Principles
SemperVigil is configuration-first:
- feed/source list is user-modifiable config
- per-source overrides are supported (headers, parser tweaks, rate limits)
- a "test source" command is required to quickly diagnose why a feed returns nothing

The system must preserve traceability:
- store enough metadata to explain why an item was included/excluded
- store ingest diagnostics per source: last fetch time, HTTP status, parse counts, errors

---

## Containers & Separation of Concerns
Target architecture uses multiple containers:
1) publisher / site (static site host)
2) build (e.g., Hugo builder)
3) ingest/processing service (scraping + enrichment + internal UI)

Codex should NOT collapse these into one container unless explicitly requested.

---

## Internal UI (Optional but Planned)
A lightweight internal-only UI may exist for:
- source health reports
- config editing (later)
- test feed fetch / parse preview
- viewing recent ingest runs and diagnostics

If implemented, it must be behind internal networking by default.

---

## File/Path Rules
- All runtime state should go under a single directory (e.g. `./data/`), configurable.
- Do not hardcode host-specific paths (NAS, Synology, etc.).
- Paths must work on macOS + Linux.

---

## Deliverable Rules
Every meaningful change should include:
- updated docs (README or docs/)
- config examples
- how to run locally (docker compose or python -m)
- basic troubleshooting steps

When uncertain, Codex should propose 2-3 options and implement the safest default.