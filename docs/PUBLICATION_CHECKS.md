# Read-only publication checks

Run after a deployment or API-requested build. This tool never invokes Hugo,
queues work, imports application startup, connects to the database, or writes
production files. It uses Python's standard library and `curl` with normal TLS
verification; no additional Python package or runtime deployment is required.

```sh
python3 tools/check-publication.py --base-url https://YOUR-PUBLIC-HOST \
  --historic-day 2026-09-08 --historic-day 2017-10-09
```

The command prints a JSON report and exits 0 only when all checks pass. Keep
evidence outside Hugo input directories. It is an operator-run release check,
not a scheduled monitor or an automatic rollback mechanism.

## Coverage

- Homepage News/CVE controls and pager markup; search form and result container.
- Metrics chart markup, Events index, and one linked event page.
- Unique same-origin linked CSS and JavaScript: HTTP 200, nonempty, not a typical
  HTML error page. External assets are not requested. Redirects fail explicitly.
- Feed index ordering, uniqueness, oldest/latest markers, and generation age.
- Latest, oldest, and explicitly requested historical day JSON: dates, identities,
  duplicates, counts, NVD CVE destinations, and numeric CVSS/EPSS ranges.

The default maximum index age is 24 hours. Override with
`--max-index-age-hours` for a measured operational target, not to conceal stale
publication. Each request is bounded to 30 seconds and 20 MB. A larger legitimate
download should prompt review of the check's limit rather than a content cap.

## Limits and failure handling

This does not execute browser JavaScript, verify visual layout, exercise filters,
validate event prose, check computed CSS, or prove completeness against the DB.
An index refreshed today can still contain stale day files. Nor does it validate
the metrics data timestamp. Those checks remain separate acceptance work.

If a check fails, inspect its path and error; distinguish TLS/network failures
from wrong or missing content. Compare to the previous release and the canonical
database selection before changing application behavior. Do not respond by
disabling TLS, rebuilding history blindly, or modifying the public JSON contract.

Run offline regression coverage with:

```sh
python3 -m pytest -m offline --strict-markers -q
```

Tests inject an in-memory site and never connect to production. Full disposable
database integration coverage is a separate gate, not implied by this command.
