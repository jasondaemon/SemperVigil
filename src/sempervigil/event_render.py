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


def render(bundle: dict, *, event_id: str, expected_revision: str) -> tuple[dict, str]:
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
    lines = ["## Source-backed coverage", "",
             "These are attributed source quotations, not independently verified facts.", "",
             "Incident date: unknown. Feed dates below are not incident dates. "
             "Source independence has not been established.", ""]
    for entry in projection["entries"]:
        url = url_quote(entry["url"], safe=":/?&=%#@+")
        lines.extend([f"### [{literal(entry['source_title'])}](<{url}>)", "",
                      "> " + literal(entry["quote"]), "",
                      f"Feed date: {entry['feed_day'] or 'unknown'}. "
                      f"Source characters: {entry['start']}-{entry['end']}.", ""])
    uncovered = len(projection["unrepresented_article_ids"])
    if uncovered:
        lines.extend([f"Coverage note: {uncovered} linked source(s) are not represented "
                      "in this qualified revision. No conclusions are inferred from them.", ""])
    lines.extend([f"Revision: `{expected_revision}`.", ""])
    return metadata, "\n".join(lines)
