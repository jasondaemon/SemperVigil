"""Bounded automatic quotation admission for explicitly enrolled incident scopes.

No new incident discovery, generated prose, inference on the scheduler, or builds.
The local model proposes relevance; the versioned policy independently limits
publication to exact short excerpts naming every approved scope focus.
"""
import json
import os
import re
import psycopg

from .event_approval import connection_factory, record_approval
from .event_assessment import PAIR_WORKFLOW, validate_assessment, request_for
from .event_projection import QUALIFICATION, prepare
from .event_render import resolve
from .event_review import draft, snapshot
from .event_review_jobs import read_material, submit, payload_for, JOB_TYPE
from .event_revision_store import source_version
from .event_scope import validate as validate_scope
from .investigation import EVIDENCE_SCOPE, READ_SCOPE, _version, postgres_reader
from .storage import get_job

POLICY = {"workflow": "scoped-auto-quotes-v1", "max_words_per_source": 25,
          "max_new_sources_per_tick": 1, "require_all_focus_terms": True,
          "require_paired_local_assessment": True, "assertion": "attributed_quotation",
          "incident_date": "unknown", "source_independence": "unknown",
          "retrieval": "non_entity_scope_focus"}


def enrollments() -> dict:
    raw = os.environ.get("SV_EVENT_AUTO_SCOPES", "{}")
    if len(raw) > 2048:
        raise ValueError("event_auto_scopes_too_large")
    values = json.loads(raw)
    if (type(values) is not dict or len(values) > 3 or any(
            type(k) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", k)
            or type(v) is not str or not re.fullmatch(r"[0-9a-f]{64}", v)
            for k, v in values.items())):
        raise ValueError("invalid_event_auto_scopes")
    return values


def candidates(packet: dict, scope: dict, represented: set[int]) -> list[dict]:
    validate_scope(scope, packet)
    if packet["omissions"] or packet["links_truncated"]:
        raise ValueError("incomplete_auto_snapshot")
    terms = [f["quote"].strip() for f in scope["focus"]]
    patterns = [re.compile(r"(?<!\w)" + re.escape(t) + r"(?!\w)", re.I) for t in terms]
    urls = {d["article_id"]: d["url"] for d in packet["documents"]}
    used_urls = {urls[i] for i in represented}
    return [p for p in draft(packet)["passages"]
            if p["article_id"] not in represented and urls[p["article_id"]] not in used_urls
            and len(p["quote"].split()) <= 25
            and all(pattern.search(p["quote"]) for pattern in patterns)]


def select_quote(packet: dict, scope: dict, assessment: dict, article_id: int,
                 represented: set[int]) -> dict | None:
    assessment = validate_assessment(assessment, packet)
    if (assessment["workflow"] != PAIR_WORKFLOW or assessment.get("paired") is not True
            or assessment.get("scope") != scope or assessment.get("article_id") != article_id):
        raise ValueError("paired_scoped_assessment_required")
    for passage in candidates(packet, scope, represented):
        if passage["article_id"] != article_id:
            continue
        suggestion = assessment["suggestions"].get(passage["id"], {})
        if suggestion.get("decision") == "include" and suggestion.get("reason") in {"same_incident", "explicit_update"}:
            return {k: passage[k] for k in ("article_id", "start", "end", "quote")}
    return None


def _bundle(conn, event_id: str, revision: str) -> dict:
    row = conn.execute("""SELECT r.bundle_json,r.qualification_id,q.qualification_json,q.revoked_at
        FROM event_public_revisions r JOIN event_quote_qualifications q
        ON q.event_id=r.event_id AND q.qualification_id=r.qualification_id
        WHERE r.event_id=%s AND r.revision_id=%s""", (event_id, revision)).fetchone()
    if row is None or row[3] is not None:
        raise ValueError("auto_scope_authority_unavailable")
    bundle = json.loads(row[0])
    if bundle["qualification"] != json.loads(row[2]) or _version(bundle["qualification"]) != row[1]:
        raise ValueError("auto_scope_authority_integrity")
    resolve(bundle, event_id=event_id, expected_revision=revision)
    return bundle


def advance(conn, event_id: str, root_revision: str) -> dict:
    """One bounded scheduler step; inference and promotion stay queued jobs."""
    root = _bundle(conn, event_id, root_revision)
    pending = conn.execute("""SELECT j.id FROM event_review_approvals a JOIN jobs j ON j.id=a.job_id
        WHERE a.event_id=%s AND j.status IN ('queued','running') LIMIT 1""", (event_id,)).fetchone()
    if pending:
        return {"status": "promotion_pending", "event_id": event_id, "job_id": pending[0]}
    pointer = conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s", (event_id,)).fetchone()
    if not pointer:
        raise ValueError("auto_public_pointer_required")
    predecessor = pointer[0]
    prior = _bundle(conn, event_id, predecessor)
    if prior["scope"] != root["scope"]:
        raise ValueError("auto_scope_changed")
    dsn = os.environ["SV_DB_URL"]
    aliases = sorted({f["quote"].strip() for f in root["scope"]["focus"] if f["role"] != "entity"})
    packet = snapshot(lambda: postgres_reader(dsn), event_id=event_id,
                      aliases=aliases, scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
    scope = validate_scope(root["scope"], packet)
    previous_docs = {d["article_id"]: d for d in prior["packet"]["documents"]}
    current_docs = {d["article_id"]: d for d in packet["documents"]}
    quotes = prior["qualification"]["quotes"]
    represented = {q["article_id"] for q in quotes}
    if any(previous_docs[i] != current_docs.get(i) for i in represented):
        raise ValueError("auto_quoted_source_changed")
    if len(represented) != len(quotes) or any(len(q["quote"].split()) > 25 for q in quotes):
        raise ValueError("auto_prior_quote_budget")
    eligible = candidates(packet, scope, represented)
    if not eligible:
        return {"status": "unchanged", "event_id": event_id, "reason": "no_eligible_new_excerpt"}
    # At most one review is admitted globally per scheduler pass; completed holds
    # are reusable decisions, never repeatedly retried to get an include.
    held_sources = []
    for article_id in sorted({p["article_id"] for p in eligible}):
        try:
            request_for(packet, scope=scope, article_id=article_id, paired=True)
        except ValueError as exc:
            if str(exc) not in {"assessment_pair_over_budget", "assessment_pair_candidate_omitted"}:
                raise
            held_sources.append({"article_id": article_id, "reason": str(exc)})
            continue
        payload = payload_for(event_id, packet["aliases"], scope=scope, article_id=article_id, paired=True)
        rows = conn.execute("""SELECT id FROM jobs WHERE job_type=%s
            AND payload_json::jsonb=%s::jsonb ORDER BY requested_at DESC LIMIT 10""",
            (JOB_TYPE, json.dumps(payload))).fetchall()
        matching = None
        for row in rows:
            job = get_job(conn, row[0])
            if job.status in {"queued", "running"}:
                return {"status": "review_pending", "event_id": event_id, "job_id": job.id}
            if job.status in {"failed", "canceled"}:
                return {"status": "held", "event_id": event_id, "reason": "review_failed", "job_id": job.id}
            if job.status != "succeeded":
                continue
            reviewed, receipt = read_material(job)
            if source_version(reviewed) == source_version(packet):
                matching = (job, reviewed, receipt)
                break
        if matching is None:
            busy = conn.execute("SELECT 1 FROM jobs WHERE queue_name='llm_local' AND status IN ('queued','running') LIMIT 1").fetchone()
            if busy:
                return {"status": "deferred", "event_id": event_id, "reason": "llm_busy"}
            job_id = submit(lambda: psycopg.connect(dsn, connect_timeout=3), event_id=event_id,
                            aliases=packet["aliases"], scope=scope, article_id=article_id, paired=True)
            return {"status": "review_queued", "event_id": event_id, "job_id": job_id}
        job, reviewed, receipt = matching
        if receipt["generation_version"] is None:
            raise ValueError("auto_generation_required")
        # Passage IDs include timestamps; select against the receipt's own packet.
        quote = select_quote(reviewed, scope, receipt["assessment"], article_id, represented)
        if quote is None:
            continue
        qualification = {"workflow": QUALIFICATION, "event_id": event_id,
            "source_version": source_version(packet), "scope_version": scope["scope_version"],
            "reviewer": {"kind": "policy", "id": "scoped-auto-quotes", "version": _version(POLICY)},
            "quotes": sorted(quotes + [quote], key=lambda q: (q["article_id"], q["start"]))}
        prepare(packet, scope, qualification, trusted_qualification_ids=frozenset({_version(qualification)}),
                predecessor=predecessor)
        approval = {"workflow": "event-auto-approval-v1", "policy": POLICY,
            "root_revision": root_revision, "review_job_id": job.id, "receipt": receipt,
            "packet": packet, "scope": scope, "qualification": qualification, "predecessor": predecessor}
        def authority_check(locked):
            if enrollments().get(event_id) != root_revision:
                raise ValueError("auto_enrollment_changed")
            _bundle(locked, event_id, root_revision)
            _bundle(locked, event_id, predecessor)
        result = record_approval(connection_factory("SV_EVENT_APPROVAL_DB_URL"), approval,
                                 authority_check=authority_check)
        return {**result, "event_id": event_id, "added_article_id": article_id}
    return {"status": "held", "event_id": event_id, "reason": "no_qualified_excerpt",
            "held_sources": held_sources}


def tick(conn) -> list[dict]:
    results = []
    for event_id, root in sorted(enrollments().items()):
        try:
            result = advance(conn, event_id, root)
        except (ValueError, PermissionError, OSError, psycopg.Error) as exc:
            conn.rollback()
            result = {"status": "held", "event_id": event_id,
                      "reason": str(exc)[:160] if isinstance(exc, ValueError) else type(exc).__name__}
        results.append(result)
        if result["status"] in {"queued", "review_queued", "review_pending"}:
            break
    return results
