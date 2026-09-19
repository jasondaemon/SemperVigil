# Local read-only MCP adapter

Status: implemented and tested locally, including a stdio client against a
disposable PostgreSQL database. Not connected to production or deployed.

## Scope and trust

`python -m sempervigil.investigation_mcp` is an explicitly enabled **stdio-only**
process. It has no HTTP listener, public route, scheduler entry, or model client.
Existing application jobs do not import it. MCP is an optional package extra,
not a new dependency in production worker images.

The authorized local operator controls the process environment and database
credentials. That OS/process boundary is the current access model, not OAuth or
per-request remote-user authentication. Do not put this process behind a network
bridge. Remote transport/authentication needs a separately reviewed release.
Only connect it to a client authorized to receive the selected data; enabling
evidence reads is an explicit data-access choice, not permission for arbitrary
cloud-provider disclosure.

Tools reuse the tested `InvestigationReader` services:

- `search_articles`: bounded stored-date/title/source discovery.
- `get_event_record`: bounded active legacy metadata, not validated reports.
- `get_article_evidence`: exact version-pinned stored text, only when separately
  enabled; source origin and incident scope remain unassigned.

No arbitrary SQL, shell, network research, proposals, merge/split, or publication
tool exists. Tool hints describe read-only behavior; service and database checks
enforce it. Source text remains untrusted data, never operational instructions.

## Configuration and startup

Use a dedicated virtual environment, not the running workers' environment:

```sh
python3 -m venv .cache/mcp-venv
.cache/mcp-venv/bin/python -m pip install -e '.[test,mcp]'
.cache/mcp-venv/bin/python -m pip check
```

The extra pins the official MIT-licensed Python SDK to `mcp==2.2.0`.
See [official SDK documentation](https://py.sdk.modelcontextprotocol.io/) and
[source/license](https://github.com/modelcontextprotocol/python-sdk).
Full transitive release locking remains part of the deployment gate; this extra
does not silently upgrade deployed application dependencies.

Supply secrets through the approved local client environment or secret mechanism,
never in a committed client configuration or command-line argument. The module
reads process environment; it does not load `.env` automatically.

| Variable | Default | Policy |
| --- | --- | --- |
| `SV_INVESTIGATION_MCP_ENABLED` | `0` | Must be exactly `1` to start |
| `SV_INVESTIGATION_DB_URL` | empty | Dedicated restricted login required; never falls back to `SV_DB_URL` |
| `SV_INVESTIGATION_EVIDENCE_ENABLED` | `0` | Exactly `0` or `1`; controls both discovery and execution |
| `SV_INVESTIGATION_MAX_CALLS` | `100` | 1-1,000 tool attempts per process, including invalid/denied calls |
| `SV_INVESTIGATION_SESSION_SECONDS` | `900` | 30-3,600 seconds; no new calls after expiry |

After configuring the environment, the local MCP client launches:

```sh
.cache/mcp-venv/bin/python -m sempervigil.investigation_mcp
```

Startup refuses superusers, role/database creators, replication/bypass-RLS roles,
table owners, missing SELECT access, and table or column write privileges on
articles/events. Provision only the required SELECT/schema USAGE permissions;
the check is not an audit of every unrelated database object or role membership.
Every read also uses the existing bounded read-only transaction factory. No
production database role or grant was created in this slice.

## Runtime bounds and diagnostics

- Frames are limited to 16 KiB before SDK parsing. Invalid UTF-8, duplicate keys,
  malformed JSON, and nonfinite numbers fail closed. Service arguments retain
  their separate 4 KiB limit. No frame/request content is logged.
- At most one service read runs at a time. Concurrent calls return `busy` instead
  of building a database-work queue. The worker releases the slot only after the
  read completes; cancellation cannot admit a replacement over a running read.
- Attempts and admission time are bounded per process, not globally across
  clients. Reconnecting starts a new session; deployment-wide admission is a
  separate gate. Expiry refuses new work rather than interrupting a running read.
- Service response payloads retain the 64 KiB cap. MCP returns both text and
  structured representations, so wire output can be larger than the payload cap.
- Audit lines on stderr contain only known tool name (or `unknown`), outcome,
  and elapsed milliseconds. Arguments, document text, connection details, and raw
  exceptions are not logged. Full production audit/identity integration is pending.
- Errors are stable and sanitized: `invalid_request`, `not_permitted`,
  `tool_unavailable`, `busy`, `session_budget_exhausted`, or `backend_unavailable`.
  Invalid framing/startup failures terminate with a generic diagnostic.

For `backend_unavailable`, use the documented direct service/database tests in a
disposable environment; do not enable raw exception echoing. For budget errors,
review investigation scope rather than starting an unattended retry loop.
For `stale_snapshot`, restart evidence reading from offset zero. Missing content
must not be replaced by an LLM summary presented as source evidence.

## Verification and next release

```sh
.cache/mcp-venv/bin/python -m pytest -m offline --strict-markers -q
# SV_TEST_DB_URL must point to a named disposable test database.
.cache/mcp-venv/bin/python -m pytest --run-db-tests tests/test_investigation_postgres.py -q
```

286 offline tests pass with the extra, including 26 adapter cases. Without it,
260 base tests pass and the optional SDK module is explicitly skipped. Four
PostgreSQL tests pass, including a real stdio subprocess/client, role preflight,
denied SQL tool, restricted reads, and column-write privilege rejection. The
isolated SDK environment passes `pip check`; no system packages were changed.

Next: adjudicated retrieval/evidence evaluation, a dedicated operator deployment
and data-access policy, dependency locking, and full-schema integration. No
production elevation yet. Stop the local process to roll back; published content,
jobs, and historical JSON do not depend on it.
