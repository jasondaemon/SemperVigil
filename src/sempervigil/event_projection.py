"""Quote-only revision preparation. No authorization, persistence or publishing.

The caller must load qualification identities from a separately trusted store.
Neither a model response nor possession of a digest establishes that trust.
"""
import json
import re

from .event_review import validate_packet
from .event_revision_store import source_version
from .event_scope import validate as validate_scope
from .investigation import _version

WORKFLOW = "event-attributed-projection-v1"
QUALIFICATION = "event-quote-qualification-v1"
HEX = re.compile(r"[0-9a-f]{64}\Z")


def prepare(packet: dict, scope: dict, qualification: dict, *,
            trusted_qualification_ids: frozenset[str] = frozenset(),
            predecessor: str | None = None) -> dict:
    """Prepare immutable data after independent qualification, never promote it.

No semantic evaluator is implemented here. An exact quotation proves attribution,
not truth, incident relevance, source independence, or publication authorization.
The default empty trust set refuses every qualification.
"""
    packet = validate_packet(json.dumps(packet).encode())
    scope = validate_scope(scope, packet)
    if packet["omissions"] or packet["links_truncated"]:
        raise ValueError("incomplete_projection_snapshot")
    if predecessor is not None and (type(predecessor) is not str or not HEX.fullmatch(predecessor)):
        raise ValueError("invalid_projection_predecessor")
    if type(qualification) is not dict or qualification.keys() != {
            "workflow", "event_id", "source_version", "scope_version", "reviewer", "quotes"}:
        raise ValueError("invalid_quote_qualification")
    raw = json.dumps(qualification, sort_keys=True, ensure_ascii=True).encode()
    if len(raw) > 32768:
        raise ValueError("oversized_quote_qualification")
    qualification_id = _version(qualification)
    if qualification_id not in trusted_qualification_ids:
        raise ValueError("untrusted_quote_qualification")
    # Work on a copy so caller mutations cannot change the prepared revision.
    qualification = json.loads(raw)
    version = source_version(packet)
    if (qualification["workflow"] != QUALIFICATION
            or qualification["event_id"] != packet["event"]["id"]
            or qualification["source_version"] != version
            or qualification["scope_version"] != scope["scope_version"]):
        raise ValueError("stale_quote_qualification")
    reviewer = qualification["reviewer"]
    if (type(reviewer) is not dict or reviewer.keys() != {"kind", "id", "version"}
            or type(reviewer["kind"]) is not str or reviewer["kind"] not in {"human", "policy"}
            or type(reviewer["id"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", reviewer["id"])
            or type(reviewer["version"]) is not str or not HEX.fullmatch(reviewer["version"])):
        raise ValueError("invalid_qualification_reviewer")
    quotes = qualification["quotes"]
    if type(quotes) is not list or not 1 <= len(quotes) <= 12:
        raise ValueError("invalid_qualified_quotes")
    documents = {d["article_id"]: d for d in packet["documents"]}
    entries, spans = [], {}
    for quote in quotes:
        if type(quote) is not dict or quote.keys() != {"article_id", "start", "end", "quote"}:
            raise ValueError("invalid_qualified_quote")
        article, start, end = (quote[k] for k in ("article_id", "start", "end"))
        if type(article) is not int or article not in documents:
            raise ValueError("qualified_source_unavailable")
        document = documents[article]
        if (type(start) is not int or type(end) is not int
                or not 0 <= start < end <= len(document["text"])
                or not 35 <= end - start <= 1200
                or document["text"][start:end] != quote["quote"]):
            raise ValueError("qualified_quote_mismatch")
        if any(start < right and left < end for left, right in spans.get(article, [])):
            raise ValueError("overlapping_qualified_quotes")
        spans.setdefault(article, []).append((start, end))
        entries.append({**quote, "source_title": document["title"], "url": document["url"],
                        "document_version": _version(document), "feed_day": document["feed_day"],
                        "assertion_role": "attributed_quotation", "origin_independence": "unknown",
                        "incident_date": None})
    payload = {"workflow": WORKFLOW, "event_id": packet["event"]["id"],
               "source_version": version, "scope_version": scope["scope_version"],
               "qualification_id": qualification_id, "reviewer": reviewer,
               "predecessor": predecessor,
               "entries": sorted(entries, key=lambda e: (e["article_id"], e["start"])),
               "unrepresented_article_ids": sorted(set(documents) - set(spans)),
               "publication_status": "not_promoted", "public_eligible": False}
    return {**payload, "revision_id": _version(payload)}
