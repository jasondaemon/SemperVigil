"""Render an independently promoted, reproducible quote projection.

The expected revision must come from the trusted publication pointer, not from
an event's legacy metadata or a model response. No pointer store exists here.
"""
import string
from urllib.parse import quote as url_quote

from .event_projection import prepare
from .investigation import _version


def literal(value: str) -> str:
    """Neutralize Markdown, HTML and Hugo shortcode syntax in source text."""
    return "".join(f"&#{ord(c)};" if c in string.punctuation else c
                   for c in " ".join(value.split()))


def resolve(bundle: dict, *, event_id: str, expected_revision: str) -> tuple[dict, dict]:
    if bundle.get("workflow") == "event-composition-public-revision-v1":
        from .event_composition_publication import validate_bundle
        projection = validate_bundle(bundle, event_id=event_id, expected_revision=expected_revision)
        dates = sorted(source["brief_day"] for source in projection["sources"] if source["brief_day"])
        metadata = {"title": projection["ledger"]["title"], "event_revision": expected_revision,
                    "event_report_format": bundle["workflow"],
                    "event_kind": projection["ledger"]["kind"],
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
        articles = [{"article_id": source["article_id"], "title": source["title"],
                     "url": source["url"]} for source in projection["sources"]]
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
        lines = [f'<section id="sv-event-coverage" data-event-id="{escape(event_id, quote=True)}" '
                 f'data-event-revision="{expected_revision}">',
                 '<p class="event-evidence-note">This deconstruction is maintained from attributed reporting. '
                 'Claims link to the source material used to support them.</p>']
        # Stored composition JSON is canonicalized with sorted keys. Presentation
        # order is editorial, not an implementation detail of JSON serialization.
        for section in SECTIONS:
            items = projection["sections"][section]
            if not items:
                continue
            lines.append(f"<h2>{headings[section]}</h2>")
            if section == "timeline":
                lines.append('<ol class="event-timeline">')
            for item in items:
                linked = []
                for fact_id in item["fact_ids"]:
                    source = sources[facts[fact_id]["article_id"]]
                    if source["article_id"] not in [value[0] for value in linked]:
                        linked.append((source["article_id"], source))
                citations = " ".join(
                    f'<a class="event-source" href="{escape(url_quote(source["url"], safe=":/?&=%#@+"), quote=True)}" '
                    f'title="{escape(source["title"], quote=True)}">Source {index}</a>'
                    for index, (_, source) in enumerate(linked, 1))
                text = escape(" ".join(item["text"].split()))
                if section == "timeline":
                    lines.append(f'<li><time>{escape(item["date_text"])}</time> {text} {citations}</li>')
                else:
                    lines.append(f"<p>{text} {citations}</p>")
            if section == "timeline":
                lines.append("</ol>")
        lines.extend(["<h2>Sources</h2>", '<ol class="event-sources">'])
        for source in projection["sources"]:
            url = escape(url_quote(source["url"], safe=":/?&=%#@+"), quote=True)
            lines.append(f'<li><a href="{url}">{escape(source["title"])}</a>'
                         + (f' <span>{escape(source["brief_day"])}</span>' if source["brief_day"] else "")
                         + "</li>")
        lines.extend(["</ol>", f"<p>Revision: <code>{expected_revision}</code>.</p>", "</section>", ""])
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
