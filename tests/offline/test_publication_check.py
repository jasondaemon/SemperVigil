import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.offline
spec = importlib.util.spec_from_file_location("publication_check", Path(__file__).parents[2] / "tools/check-publication.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
NOW = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
INDEX = {"days": ["2026-09-18", "2026-09-17"], "latest_day": "2026-09-18",
         "oldest_day": "2026-09-17", "generated_at": NOW.isoformat()}
CVE = {"kind": "cve", "cve_id": "CVE-2026-12345", "epss_score": 0.2, "score": 8.8,
       "url": "https://nvd.nist.gov/vuln/detail/CVE-2026-12345",
       "nvd_url": "https://nvd.nist.gov/vuln/detail/CVE-2026-12345"}


def payload(day="2026-09-18"):
    return {"day": day, "items": [copy.deepcopy(CVE)], "counts": {"article": 0, "cve": 1}}


@pytest.mark.parametrize("value", [None, 0, 1, 0.02])
def test_valid_epss_and_cve_only_days(value):
    body = payload()
    body["items"][0]["epss_score"] = value
    assert checker.validate_day(body, body["day"]) == body["counts"]


@pytest.mark.parametrize("value", [20, -1, True, "0.2", float("nan"), float("inf")])
def test_bad_score_units_rejected(value):
    body = payload()
    body["items"][0]["epss_score"] = value
    with pytest.raises(checker.CheckError):
        checker.validate_day(body, body["day"])


@pytest.mark.parametrize("problem", ["duplicate", "counts", "date", "url", "kind"])
def test_contract_regressions(problem):
    body = payload()
    if problem == "duplicate":
        body["items"].append(copy.deepcopy(CVE))
    elif problem == "counts":
        body["counts"]["cve"] = 0
    elif problem == "date":
        body["day"] = "2026-09-17"
    else:
        body["items"][0][problem] = "wrong"
    with pytest.raises(checker.CheckError):
        checker.validate_day(body, "2026-09-18")


@pytest.mark.parametrize("change", [
    {"generated_at": "2026-09-16T00:00:00Z"},
    {"generated_at": "2026-09-19T00:00:00Z"},
    {"generated_at": "2026-09-18T12:00:00"},
    {"days": ["2026-09-18", "2026-09-18"]},
    {"days": []}, {"oldest_day": "1990-01-01"},
])
def test_invalid_index(change):
    with pytest.raises(checker.CheckError):
        checker.validate_index({**INDEX, **change}, NOW, 24)


def fake_site():
    common = '<title>Site</title><link rel="stylesheet" href="/style.css"><script src="/app.js"></script><script src="https://outside.example/a.js"></script>'
    def html(extra=""):
        return (common + extra).encode()
    return {
        "/": html(''.join(f'<div id="{id}"></div>' for id in ["front-feed-list", "front-view-news", "front-view-cves", "front-day-prev", "front-day-next"])),
        "/search/": html('<form id="sv-feed-search-form"></form><div id="sv-feed-search-results"></div>'),
        "/metrics/": html('<svg aria-label="Articles and CVEs per day chart"></svg>'
                          f'<p>Updated from SemperVigil DB at {NOW.isoformat()}.</p>'),
        "/events/": html('<a href="/events/example/">Event</a>'),
        "/events/example/": html(), "/style.css": b"body { color: white; }", "/app.js": b"void 0;",
        "/feed/index.json": json.dumps(INDEX).encode(),
        **{f"/feed/days/{day}.json": json.dumps(payload(day)).encode() for day in INDEX["days"]},
    }


@pytest.mark.parametrize("broken", [None, "/style.css", "/feed/days/2026-09-17.json", "/search/"])
def test_release_check_and_failure_exit_report(broken):
    bodies = fake_site()
    if broken:
        bodies[broken] = b"<html>not found</html>"
    visited = []
    def get(url):
        assert url.startswith("https://example.test/")
        visited.append(url)
        return bodies[url.removeprefix("https://example.test")]
    report = checker.check_site("https://example.test", historic_days=[], max_age_hours=24, get=get, now=NOW)
    assert report["ok"] == (broken is None)
    assert visited.count("https://example.test/style.css") == 1
    if broken:
        assert any(row["path"] == broken and not row["ok"] for row in report["checks"])


def test_external_and_non_http_references_not_requested():
    for ref in ("https://other.test/a", "//other.test/a", "javascript:alert(1)", "data:text/plain,test"):
        assert checker.local_url("https://example.test", ref) is None


@pytest.mark.parametrize("stamp,ok", [
    ("2026-09-18T12:00:00Z", True),
    ("2026-09-18T08:00:00-04:00", True),
    ("2026-09-18T09:00:00.000000+00:00", True),
    ("2026-09-18T08:59:59+00:00", False),
    ("2026-09-18T13:00:00Z", False),
    ("2026-09-18T12:00:00", False),
    ("", False),
])
def test_metrics_freshness_independent_of_fresh_feed(stamp, ok):
    bodies = fake_site()
    bodies["/metrics/"] = (f'<title>Metrics</title><link rel="stylesheet" href="/style.css">'
                           f'<svg aria-label="Articles and CVEs per day chart"></svg>'
                           f'<p>Updated from SemperVigil DB at {stamp}.</p>').encode()
    report = checker.check_site("https://example.test", historic_days=[], max_age_hours=24,
                               get=lambda url: bodies[url.removeprefix("https://example.test")], now=NOW)
    assert report["ok"] is ok
    metric = next(row for row in report["checks"] if row["path"] == "/metrics/")
    assert metric["ok"] is ok
    assert next(row for row in report["checks"] if row["path"] == "/feed/index.json")["ok"]
