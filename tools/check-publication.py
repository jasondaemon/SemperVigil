#!/usr/bin/env python3
"""Read-only public release checks; never imports app startup or invokes Hugo."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
from html.parser import HTMLParser
import json
import math
import re
import subprocess
from urllib.parse import urljoin, urlsplit


class CheckError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckError(message)


def day_key(value: object) -> str:
    require(isinstance(value, str) and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)), "Invalid day")
    date.fromisoformat(value)
    return value


def validate_index(payload: dict, now: datetime, max_age_hours: float) -> list[str]:
    days = payload.get("days")
    require(isinstance(days, list) and bool(days), "Empty/missing feed index")
    for day in days:
        day_key(day)
    require(days == sorted(set(days), reverse=True), "Index days must be unique and descending")
    require(payload.get("latest_day") == days[0], "Incorrect latest day")
    require(payload.get("oldest_day") == days[-1], "Incorrect oldest day")
    generated = datetime.fromisoformat(payload["generated_at"].replace("Z", "+00:00"))
    require(generated.tzinfo is not None, "Index timestamp must include timezone")
    age = (now - generated).total_seconds() / 3600
    require(-0.0834 <= age <= max_age_hours, "Feed index is stale or future-dated")
    return days


def validate_day(payload: dict, day: str) -> dict:
    require(payload.get("day") == day, "Download day mismatch")
    items = payload.get("items")
    require(isinstance(items, list), "Missing items array")
    counts = Counter(article=0, cve=0)
    seen = set()
    for item in items:
        kind = item.get("kind")
        require(kind in counts, "Unknown item kind")
        identity = item.get("article_id") if kind == "article" else item.get("cve_id")
        require(isinstance(identity, (str, int)) and not isinstance(identity, bool) and bool(identity), "Missing item identity")
        require((kind, identity) not in seen, "Duplicate item identity")
        seen.add((kind, identity))
        counts[kind] += 1
        if kind == "cve":
            require(isinstance(identity, str) and bool(re.fullmatch(r"CVE-\d{4}-\d{4,}", identity)), "Invalid CVE identity")
            expected = f"https://nvd.nist.gov/vuln/detail/{identity}"
            require(item.get("url") == expected and item.get("nvd_url") == expected, "CVE must link to NVD")
            for field, ceiling in (("epss_score", 1), ("epss_percentile", 1), ("score", 10), ("base_score", 10)):
                value = item.get(field)
                require(value is None or (type(value) in (int, float) and math.isfinite(value) and 0 <= value <= ceiling), f"Invalid {field} units/range")
    declared = payload.get("counts")
    require(isinstance(declared, dict) and all(type(value) is int for value in declared.values()), "Counts must be integers")
    require(declared == dict(counts), "Counts do not match items")
    return dict(counts)


class Page(HTMLParser):
    def __init__(self, body: bytes):
        super().__init__()
        self.ids = set()
        self.assets = []
        self.links = []
        self.has_title = False
        self.has_chart = False
        self.text = []
        self.feed(body.decode("utf-8"))

    def handle_data(self, data):
        self.text.append(data)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        self.ids.add(attrs.get("id"))
        self.has_title |= tag == "title"
        self.has_chart |= tag == "svg" and attrs.get("aria-label") == "Articles and CVEs per day chart"
        if tag == "script" and attrs.get("src"):
            self.assets.append((attrs["src"], "js"))
        if tag == "link" and "stylesheet" in attrs.get("rel", "").split() and attrs.get("href"):
            self.assets.append((attrs["href"], "css"))
        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])


def local_url(base: str, reference: str) -> str | None:
    url = urljoin(base, reference)
    parsed = urlsplit(url)
    origin = urlsplit(base)
    if parsed.scheme == origin.scheme and parsed.netloc == origin.netloc and not parsed.username:
        return url
    return None


def fetch(url: str) -> bytes:
    # Do not follow redirects or contact third-party assets. curl uses the host's
    # normal CA trust; never disable certificate verification for a release gate.
    result = subprocess.run([
        "curl", "--silent", "--show-error", "--fail", "--proto", "=https",
        "--connect-timeout", "10", "--max-time", "30", "--max-filesize", "20000000",
        "--write-out", "\n%{http_code}", url,
    ], capture_output=True, timeout=35, check=True)
    body, status = result.stdout.rsplit(b"\n", 1)
    require(status == b"200", "Expected HTTP 200 without redirects")
    require(bool(body), "Empty response")
    return body


def validate_metrics_age(page: Page, now: datetime, max_age_hours: float) -> dict:
    text = " ".join(" ".join(page.text).split())
    match = re.search(
        r"Updated from SemperVigil DB at (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
        r"(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))", text,
    )
    require(match is not None, "Missing metrics timestamp with timezone")
    timestamp = match.group(1)
    generated = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    age = (now - generated).total_seconds() / 3600
    require(-0.0834 <= age <= max_age_hours, "Metrics data is stale or future-dated")
    return {"generated_at": timestamp, "age_hours": round(age, 3)}


def check_site(base: str, *, historic_days: list[str], max_age_hours: float,
               max_metrics_age_hours: float = 3, get=fetch, now=None) -> dict:
    now = now or datetime.now(timezone.utc)
    results = []
    pages = {}
    assets = {}

    def check(path, action):
        try:
            details = action()
            results.append({"path": path, "ok": True, "details": details})
        except (ValueError, KeyError, TypeError, AttributeError, OSError, subprocess.SubprocessError) as exc:
            results.append({"path": path, "ok": False, "error": str(exc)})

    def page(path):
        url = urljoin(base, path)
        parsed = Page(get(url))
        require(parsed.has_title, "Missing HTML title")
        required = {
            "/": {"front-feed-list", "front-view-news", "front-view-cves", "front-day-prev", "front-day-next"},
            "/search/": {"sv-feed-search-form", "sv-feed-search-results"},
        }.get(path, set())
        require(required <= parsed.ids, "Missing expected page controls")
        details = {}
        if path == "/metrics/":
            require(parsed.has_chart, "Missing metrics chart")
            details = validate_metrics_age(parsed, now, max_metrics_age_hours)
        require(any(kind == "css" for _, kind in parsed.assets), "Missing stylesheet link")
        for ref, kind in parsed.assets:
            asset = local_url(url, ref)
            if asset:
                assets[asset] = kind
        pages[path] = parsed
        return {"local_assets": len(assets), **details}

    for path in ("/", "/search/", "/metrics/", "/events/"):
        check(path, lambda path=path: page(path))
    events = pages.get("/events/")
    event_paths = sorted({urlsplit(url).path for ref in (events.links if events else [])
                          if (url := local_url(base, ref)) and re.fullmatch(r"/events/[^/]+/", urlsplit(url).path)})
    if event_paths:
        check(event_paths[0], lambda: page(event_paths[0]))
    else:
        results.append({"path": "/events/<sample>/", "ok": False, "error": "No event detail link found"})
    for url, kind in sorted(assets.items()):
        def asset(url=url, kind=kind):
            body = get(url)
            require(bool(body.strip()) and not body.lstrip().lower().startswith((b"<!doctype html", b"<html")), f"Invalid {kind} asset response")
            return {"bytes": len(body)}
        check(urlsplit(url).path, asset)

    def feed():
        payload = json.loads(get(urljoin(base, "/feed/index.json")))
        days = validate_index(payload, now, max_age_hours)
        for day in sorted({days[0], days[-1], *historic_days}, reverse=True):
            day_key(day)
            path = f"/feed/days/{day}.json"
            def sample(day=day, path=path):
                require(day in days, "Requested historical day missing from index")
                return validate_day(json.loads(get(urljoin(base, path))), day)
            check(path, sample)
        return {"indexed_days": len(days), "generated_at": payload["generated_at"]}
    check("/feed/index.json", feed)
    return {"checked_at": now.isoformat(), "ok": all(row["ok"] for row in results), "checks": results,
            "scope": "HTTP, markup, linked local assets, and sampled JSON only; no browser execution or DB completeness proof."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--historic-day", action="append", default=[], type=day_key)
    parser.add_argument("--max-index-age-hours", type=float, default=24)
    parser.add_argument("--max-metrics-age-hours", type=float, default=3)
    args = parser.parse_args()
    url = urlsplit(args.base_url)
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ("", "/"):
        parser.error("--base-url must be an HTTPS origin without credentials, path, query, or fragment")
    if not math.isfinite(args.max_index_age_hours) or args.max_index_age_hours <= 0:
        parser.error("--max-index-age-hours must be positive and finite")
    if not math.isfinite(args.max_metrics_age_hours) or args.max_metrics_age_hours <= 0:
        parser.error("--max-metrics-age-hours must be positive and finite")
    report = check_site(args.base_url, historic_days=args.historic_day,
                        max_age_hours=args.max_index_age_hours,
                        max_metrics_age_hours=args.max_metrics_age_hours)
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
