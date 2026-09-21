"""Render an independently promoted, reproducible quote projection.

The expected revision must come from the trusted publication pointer, not from
an event's legacy metadata or a model response. No pointer store exists here.
"""
import string
from urllib.parse import quote as url_quote, urlparse

from .event_projection import prepare
from .enrichment.url import normalize_url
from .investigation import _version


def _canonical_sources(sources: list[dict]) -> tuple[list[dict], dict[int, int]]:
    unique = []
    number_by_url: dict[str, int] = {}
    number_by_article: dict[int, int] = {}
    for source in sources:
        key = normalize_url(str(source["url"]))
        number = number_by_url.get(key)
        if number is None:
            unique.append(source)
            number = len(unique)
            number_by_url[key] = number
        number_by_article[int(source["article_id"])] = number
    return unique, number_by_article


def _source_site(url: str) -> str:
    return (urlparse(url).hostname or url).lower().removeprefix("www.")


def literal(value: str) -> str:
    """Neutralize Markdown, HTML and Hugo shortcode syntax in source text."""
    return "".join(f"&#{ord(c)};" if c in string.punctuation else c
                   for c in " ".join(value.split()))


def resolve(bundle: dict, *, event_id: str, expected_revision: str) -> tuple[dict, dict]:
    if bundle.get("workflow") == "event-composition-public-revision-v1":
        from .event_composition_publication import validate_bundle
        projection = validate_bundle(bundle, event_id=event_id, expected_revision=expected_revision)
        dates = sorted(source["brief_day"] for source in projection["sources"] if source["brief_day"])
        unique_sources, _ = _canonical_sources(projection["sources"])
        from .event_composition import SECTIONS
        populated = [section for section in SECTIONS if projection["sections"][section]]
        metadata = {"title": projection["ledger"]["title"], "event_revision": expected_revision,
                    "event_report_format": bundle["workflow"],
                    "event_kind": projection["ledger"]["kind"],
                    "event_source_count": len(unique_sources),
                    "event_section_count": len(populated),
                    "event_has_open_questions": bool(projection["sections"]["open_questions"]),
                    "event_change_kind": projection["composition"]["change"]["kind"],
                    "first_seen_at": dates[0] if dates else None,
                    "last_seen_at": dates[-1] if dates else None}
        return metadata, projection
    if type(bundle) is not dict or bundle.keys() != {"packet", "scope", "qualification", "predecessor"}:
        raise ValueError("invalid_event_projection_bundle")
    # Reconstruct rather than accepting stored/generated prose. Pointer identity
    # binds the qualification too; this does not independently authorize it.
    projection = prepare(bundle["packet"], bundle["scope"], bundle["qualification"],
                         trusted_qualification_ids=frozenset({_version(bundle["qualification"])}),
                         predecessor=bundle["predecessor"])
    if projection["event_id"] != event_id or projection["revision_id"] != expected_revision:
        raise ValueError("event_publication_pointer_mismatch")
    entity = next(f["quote"] for f in bundle["scope"]["focus"] if f["role"] == "entity")
    metadata = {"title": entity + " | Incident coverage", "event_revision": expected_revision,
                "event_report_format": projection["workflow"]}
    return metadata, projection


def index_entry(bundle: dict, *, event_id: str, expected_revision: str) -> dict:
    metadata, projection = resolve(bundle, event_id=event_id, expected_revision=expected_revision)
    if bundle.get("workflow") == "event-composition-public-revision-v1":
        unique_sources, _ = _canonical_sources(projection["sources"])
        articles = [{"article_id": source["article_id"], "title": source["title"],
                     "url": source["url"]} for source in unique_sources]
        overview = " ".join(item["text"] for item in projection["sections"]["overview"])
        return {"event_id": event_id, "title": metadata["title"], "summary": overview,
                "severity": None, "kind": metadata["event_kind"], "status": "source_backed_event",
                "first_seen_at": metadata["first_seen_at"], "last_seen_at": metadata["last_seen_at"],
                "cves": [], "products": [], "articles": articles,
                "counts": {"cves": 0, "products": 0, "articles": len(articles)},
                "event_revision": expected_revision, "event_report_format": bundle["workflow"]}
    articles = {entry["article_id"]: {"article_id": entry["article_id"],
                "title": entry["source_title"], "url": entry["url"]}
                for entry in projection["entries"]}
    return {"event_id": event_id, "title": metadata["title"],
            "summary": "Attributed source quotations; incident date and source independence are unknown.",
            "severity": None, "kind": None, "status": "source_backed_coverage",
            "first_seen_at": None, "last_seen_at": None, "cves": [], "products": [],
            "articles": [articles[key] for key in sorted(articles)],
            "counts": {"cves": 0, "products": 0, "articles": len(articles)},
            "event_revision": expected_revision, "event_report_format": projection["workflow"],
            "quotations": projection["entries"],
            "unrepresented_article_ids": projection["unrepresented_article_ids"]}


def render(bundle: dict, *, event_id: str, expected_revision: str) -> tuple[dict, str]:
    metadata, projection = resolve(bundle, event_id=event_id, expected_revision=expected_revision)
    from html import escape
    if bundle.get("workflow") == "event-composition-public-revision-v1":
        from .event_composition import SECTIONS
        headings = {"overview": "Overview", "attack_vector": "Attack vector",
                    "attack_path": "Attack path", "timeline": "Timeline", "impact": "Impact",
                    "response_recovery": "Response and recovery", "mitigations": "Mitigations",
                    "attribution": "Attribution", "open_questions": "Open questions"}
        facts = {fact["fact_id"]: fact for fact in projection["ledger"]["facts"]}
        sources = {source["article_id"]: source for source in projection["sources"]}
        unique_sources, source_numbers = _canonical_sources(projection["sources"])
        lines = [f'<section id="sv-event-coverage" class="event-report" '
                 f'data-event-id="{escape(event_id, quote=True)}" '
                 f'data-event-revision="{expected_revision}">',
                 '<p class="event-evidence-note">This deconstruction is maintained from attributed reporting. '
                 'Claims link to the source material used to support them.</p>',
                 '<div class="event-report-sections">']
        # Stored composition JSON is canonicalized with sorted keys. Presentation
        # order is editorial, not an implementation detail of JSON serialization.
        for section in SECTIONS:
            items = projection["sections"][section]
            if not items:
                continue
            section_class = f"event-report-section event-report-section--{section.replace('_', '-')}"
            lines.append(f'<section class="{section_class}" id="{section.replace("_", "-")}">')
            lines.append(f"<h2>{headings[section]}</h2>")
            if section == "timeline":
                lines.append('<ol class="event-timeline">')
            for item in items:
                linked = []
                for fact_id in item["fact_ids"]:
                    source = sources[facts[fact_id]["article_id"]]
                    number = source_numbers[source["article_id"]]
                    if number not in [value[0] for value in linked]:
                        linked.append((number, unique_sources[number - 1]))
                citations = " ".join(
                    f'<a class="event-source" href="{escape(url_quote(source["url"], safe=":/?&=%#@+"), quote=True)}" '
                    f'title="Source {index}: {escape(source["title"], quote=True)}" '
                    f'target="_blank" rel="noopener" aria-label="Source {index}">[{index}]</a>'
                    for index, source in linked)
                text = escape(" ".join(item["text"].split()))
                kinds = {facts[fact_id]["kind"] for fact_id in item["fact_ids"]}
                state = ("Unresolved" if kinds & {"uncertainty", "disputed"}
                         else "Attributed" if "allegation" in kinds else "Reported")
                badge = f'<span class="event-claim-state event-claim-state--{state.lower()}">{state}</span>'
                if section == "timeline":
                    lines.append(f'<li><time>{escape(item["date_text"])}</time><div>{badge}<p>{text} {citations}</p></div></li>')
                else:
                    lines.append(f'<div class="event-claim">{badge}<p>{text} {citations}</p></div>')
            if section == "timeline":
                lines.append("</ol>")
            lines.append("</section>")
        lines.extend(["</div>", '<section class="event-report-section event-report-section--sources" id="sources">',
                      "<h2>Sources</h2>", '<ol class="event-sources">'])
        for source in unique_sources:
            url = escape(url_quote(source["url"], safe=":/?&=%#@+"), quote=True)
            date = escape(source["brief_day"] or "Date unknown")
            title = escape(source["title"])
            site = escape(_source_site(source["url"]))
            lines.append(f'<li><a href="{url}" target="_blank" rel="noopener">'
                         f'<time>{date}</time><span>{title}</span><small>{site}</small></a></li>')
        change = projection["composition"]["change"]
        delta = (f'{len(change["added_fact_ids"])} added, '
                 f'{len(change["supersedes_fact_ids"])} superseded, '
                 f'{len(change["conflict_fact_ids"])} disputed')
        lines.extend(["</ol>", "</section>", '<footer class="event-revision">',
                      f'<span>Revision {escape(expected_revision[:12])}</span>',
                      f'<span>{escape(change["kind"].title())}: {delta}</span>',
                      "</footer>", "</section>", ""])
        return metadata, "\n".join(lines)
    lines = [f'<section id="sv-event-coverage" data-event-id="{escape(event_id, quote=True)}" '
             f'data-event-revision="{expected_revision}">',
             '<h2>Source-backed coverage</h2>',
             '<p>These are attributed source quotations, not independently verified facts.</p>',
             '<p>Incident date: unknown. Feed dates below are not incident dates. '
             'Source independence has not been established.</p>']
    for entry in projection["entries"]:
        url = url_quote(entry["url"], safe=":/?&=%#@+")
        lines.extend([f'<h3><a href="{escape(url, quote=True)}">{literal(entry["source_title"])}</a></h3>',
                      '<blockquote><p>' + literal(entry["quote"]) + '</p></blockquote>',
                      f"<p>Feed date: {entry['feed_day'] or 'unknown'}. "
                      f"Source characters: {entry['start']}-{entry['end']}.</p>"])
    uncovered = len(projection["unrepresented_article_ids"])
    if uncovered:
        lines.append(f"<p>Coverage note: {uncovered} linked source(s) are not represented "
                     "in this qualified revision. No conclusions are inferred from them.</p>")
    lines.extend([f"<p>Revision: <code>{expected_revision}</code>.</p>", "</section>", ""])
    return metadata, "\n".join(lines)
