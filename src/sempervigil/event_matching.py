"""Offline trusted passage and matching contracts; no runtime integration.

Scope and incident references must be independently established by the caller.
This module neither discovers semantic boundaries nor trusts model assignments.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
from urllib.parse import urlsplit

from .event_evidence import Evidence


@dataclass(frozen=True)
class SourceDocument:
    id: str
    origin_id: str
    url: str
    text: str


@dataclass(frozen=True)
class Passage:
    document_version: str
    document_id: str
    start: int
    end: int
    evidence: Evidence


def extract_passage(document: SourceDocument, *, start: int, end: int,
                    incident_id: str) -> Passage:
    """Slice a caller-approved scope, retaining offsets into the exact snapshot."""
    if not all(value.strip() for value in (document.id, document.origin_id, incident_id)):
        raise ValueError("missing_provenance")
    try:
        url = urlsplit(document.url)
        safe = url.scheme in {"http", "https"} and url.hostname and not url.username and not url.password
    except ValueError:
        safe = False
    if not safe:
        raise ValueError("unsafe_source_url")
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(document.text):
        raise ValueError("invalid_span")
    text = document.text[start:end]
    if not text.strip():
        raise ValueError("empty_passage")
    version = hashlib.sha256(json.dumps(asdict(document), sort_keys=True,
                                       ensure_ascii=True).encode()).hexdigest()
    identity = hashlib.sha256(json.dumps([version, start, end, incident_id]).encode()).hexdigest()
    evidence = Evidence(identity, incident_id, document.origin_id, document.url, text)
    return Passage(version, document.id, start, end, evidence)


@dataclass(frozen=True)
class IncidentReference:
    """Namespaced, incident-specific identifier, NOT a company or CVE identifier."""
    authority: str
    value: str


@dataclass(frozen=True)
class IncidentIdentity:
    incident_id: str
    entity_id: str
    references: frozenset[IncidentReference]


@dataclass(frozen=True)
class MatchDecision:
    incident_id: str | None
    reason: str


def match_incident(*, entity_id: str, references: frozenset[IncidentReference],
                   candidates: tuple[IncidentIdentity, ...]) -> MatchDecision:
    """Match only a unique trusted reference; abstain on inconsistent identity.

Entity IDs are canonical IDs supplied by an alias resolver, not normalized
display names. Unknown evidence does not create an incident automatically.
"""
    if (not entity_id.strip() or any(not ref.authority.strip() or not ref.value.strip()
                                     for ref in references)):
        return MatchDecision(None, "invalid_input")
    ids = [candidate.incident_id for candidate in candidates]
    if (len(ids) != len(set(ids)) or any(
            not candidate.incident_id.strip() or not candidate.entity_id.strip()
            or any(not ref.authority.strip() or not ref.value.strip() for ref in candidate.references)
            for candidate in candidates)):
        return MatchDecision(None, "invalid_candidates")
    if not references:
        return MatchDecision(None, "insufficient_evidence")
    matches = [candidate for candidate in candidates if references & candidate.references]
    if any(candidate.entity_id != entity_id for candidate in matches):
        return MatchDecision(None, "entity_conflict")
    if len(matches) > 1:
        return MatchDecision(None, "ambiguous_reference")
    if not matches:
        return MatchDecision(None, "no_reference_match")
    return MatchDecision(matches[0].incident_id, "unique_reference")
