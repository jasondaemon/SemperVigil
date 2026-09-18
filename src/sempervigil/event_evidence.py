"""Offline Events foundation. Not wired to jobs, storage, or publication.

Evidence is supplied by a trusted ingestion/evaluation caller, never by model
output. Structural validation cannot establish entailment or source independence.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import hashlib
import json
from typing import Mapping
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Evidence:
    id: str
    incident_id: str
    origin_id: str
    url: str
    text: str


@dataclass(frozen=True)
class Citation:
    evidence_id: str
    start: int
    end: int
    quote: str


@dataclass(frozen=True)
class Claim:
    id: str
    incident_id: str
    statement: str
    status: str
    citations: tuple[Citation, ...]
    date_role: str = "incident"
    date_precision: str = "unknown"
    date_value: str | None = None
    supersedes: str | None = None


def evidence_version(evidence: Mapping[str, Evidence]) -> str:
    """Stable snapshot identity, independent of mapping insertion order."""
    payload = [(key, asdict(value)) for key, value in sorted(evidence.items())]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _valid_date(precision: str, value: str | None) -> bool:
    if precision == "unknown":
        return value is None
    if not isinstance(value, str):
        return False
    lengths = {"year": 4, "month": 7, "day": 10}
    if precision not in lengths or len(value) != lengths[precision]:
        return False
    expanded = value + {"year": "-01-01", "month": "-01", "day": ""}[precision]
    try:
        parsed = date.fromisoformat(expanded)
    except ValueError:
        return False
    return parsed.isoformat() == expanded


def validate_claims(
    claims: tuple[Claim, ...], *, incident_id: str,
    evidence: Mapping[str, Evidence], expected_version: str,
    prior_claim_incidents: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return deterministic error codes; an empty tuple is NOT publish approval.

    Offsets are Python Unicode code-point offsets into the exact supplied text.
    No fuzzy quote matching, inferred dates, network access, or model calls.
    """
    errors = set()
    prior = prior_claim_incidents or {}
    if not incident_id.strip():
        errors.add("missing_incident")
    if expected_version != evidence_version(evidence):
        errors.add("stale_evidence")
    for key, item in evidence.items():
        if not key or key != item.id:
            errors.add("invalid_evidence_id")
        if not item.incident_id.strip() or not item.origin_id.strip():
            errors.add("missing_provenance")
        try:
            url = urlsplit(item.url)
            safe = url.scheme in {"https", "http"} and bool(url.hostname) and not url.username and not url.password
        except ValueError:
            safe = False
        if not safe:
            errors.add("unsafe_source_url")
    ids = set()
    if not claims:
        errors.add("empty_claims")
    for claim in claims:
        if not claim.id.strip() or claim.id in ids or claim.id in prior:
            errors.add("invalid_claim_id")
        ids.add(claim.id)
        if claim.incident_id != incident_id:
            errors.add("wrong_incident")
        if not claim.statement.strip():
            errors.add("empty_statement")
        if claim.status not in {"asserted", "alleged", "disputed", "retracted"}:
            errors.add("invalid_status")
        if claim.date_role not in {"incident", "disclosure", "publication", "observation"}:
            errors.add("invalid_date_role")
        if not _valid_date(claim.date_precision, claim.date_value):
            errors.add("invalid_date")
        if claim.supersedes is not None:
            if claim.supersedes == claim.id or claim.supersedes not in prior:
                errors.add("unknown_correction")
            elif prior[claim.supersedes] != incident_id:
                errors.add("cross_incident_correction")
        if not claim.citations:
            errors.add("missing_citation")
        for citation in claim.citations:
            item = evidence.get(citation.evidence_id)
            if item is None:
                errors.add("unknown_evidence")
                continue
            if item.incident_id != incident_id:
                errors.add("cross_incident_evidence")
            if (type(citation.start) is not int or type(citation.end) is not int
                    or not 0 <= citation.start < citation.end <= len(item.text)):
                errors.add("invalid_span")
            elif not citation.quote.strip() or item.text[citation.start:citation.end] != citation.quote:
                errors.add("quote_mismatch")
    return tuple(sorted(errors))
