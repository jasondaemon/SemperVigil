"""Review-gated publication of accepted Event ledger compositions.

The admission role records authority and queues one promotion. The promotion and
activation roles independently recheck the accepted ledger, composition, source
evidence, article availability, and Event membership before a revision can go live.
"""
import json
from urllib.parse import urlparse

from . import event_composition
from .event_approval import JOB_TYPE, MAX_APPROVAL_BYTES, connection_factory
from .investigation import _version
from .storage import create_event, enqueue_job
from .utils import utc_now_iso

APPROVAL_WORKFLOW = "event-composition-publication-approval-v1"
QUALIFICATION_WORKFLOW = "event-composition-publication-qualification-v1"
PUBLIC_WORKFLOW = "event-composition-public-revision-v1"
CONFIRMATION = "PUBLISH_ACCEPTED_EVENT"
POLICY = {"workflow": QUALIFICATION_WORKFLOW, "review": "accepted-composition",
          "source_freshness": "activation-rechecked", "generated_prose": True}
COMPATIBILITY_FAILURES = {"event_composition_publication_integrity_failure"}


def _publisher_key(source: dict) -> str:
    host = (urlparse(str(source.get("url") or "")).hostname or "").lower()
    return host.removeprefix("www.")


def _independent_source_count(sources: list[dict]) -> int:
    return len({key for source in sources if (key := _publisher_key(source))})


def event_identity(ledger_id: str) -> str:
    if not isinstance(ledger_id, str) or not ledger_id.startswith("eld_"):
        raise ValueError("event_composition_publication_ledger_invalid")
    return "evt_" + _version({"workflow": PUBLIC_WORKFLOW, "ledger_id": ledger_id})[:12]


def _publication_target(conn, ledger_id: str, *, lock: bool) -> dict | None:
    from .event_reassessment import assert_target_current, target
    bound = assert_target_current(conn, ledger_id) if lock else target(conn, ledger_id)
    if not bound:
        return None
    return {"kind": "confirmed-event-reassessment-v1",
            "event_id": bound["event_id"],
            "snapshot_version": bound["snapshot_version"],
            "active": bound["reassessment_status"] == "active"}


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("event_composition_publication_record_invalid")
    return value


def _source_rows(conn, revision_id: str, *, lock: bool) -> list[dict]:
    # Revision-source rows are immutable. Lock only mutable admission/evidence/article
    # rows so the restricted roles need no write privilege on the linkage table.
    suffix = " FOR SHARE OF c,e,a NOWAIT" if lock else ""
    rows = conn.execute(
        """SELECT s.candidate_id,s.evidence_revision_id,s.article_id,
                  c.status,c.evidence_revision_id,e.status,a.title,a.original_url,
                  a.brief_day,a.meta_json
             FROM event_ledger_revision_sources s
             JOIN incident_candidates c ON c.candidate_id=s.candidate_id
             JOIN article_evidence_revisions e ON e.revision_id=s.evidence_revision_id
             JOIN articles a ON a.id=s.article_id
            WHERE s.revision_id=%s ORDER BY s.candidate_id""" + suffix,
        (revision_id,),
    ).fetchall()
    result = []
    for row in rows:
        meta = _decode(row[9]) if row[9] else {}
        if row[3] != "enrolled" or row[4] != row[1] or row[5] != "accepted" or meta.get("suppressed"):
            raise ValueError("event_composition_publication_source_stale")
        result.append({"candidate_id": row[0], "evidence_revision_id": row[1],
                       "article_id": int(row[2]), "title": row[6] or "Source article",
                       "url": row[7], "brief_day": row[8]})
    if not result or any(not row["url"] for row in result):
        raise ValueError("event_composition_publication_source_unavailable")
    return result


def current_material(conn, composition_id: str, *, event_id: str | None = None,
                     lock: bool = False) -> dict:
    if not isinstance(composition_id, str) or not composition_id.startswith("elc_"):
        raise ValueError("event_composition_publication_composition_invalid")
    suffix = " FOR SHARE NOWAIT" if lock else ""
    row = conn.execute(
        """SELECT c.ledger_id,c.ledger_revision_id,c.status,c.composition_json,
                  c.reviewed_at,c.reviewed_by,r.status,r.predecessor_revision_id,
                  r.ledger_json,r.change_json
             FROM event_ledger_compositions c
             JOIN event_ledger_revisions r ON r.revision_id=c.ledger_revision_id
            WHERE c.composition_id=%s""" + suffix,
        (composition_id,),
    ).fetchone()
    if not row or row[2] != "accepted" or row[6] != "accepted":
        raise ValueError("event_composition_publication_not_accepted")
    from .event_ledger import _lineage_current
    if not _lineage_current(conn, row[1]):
        raise ValueError("event_composition_publication_source_stale")
    composition = _decode(row[3])
    ledger, change = _decode(row[8]), _decode(row[9])
    if (composition_id != "elc_" + _version(composition)
            or composition.get("ledger_id") != row[0]
            or composition.get("ledger_revision_id") != row[1]
            or ledger.get("ledger_id") != row[0]
            or row[1] != "elr_" + _version({"ledger": ledger, "change": change,
                                             "predecessor_revision_id": row[7]})):
        raise ValueError("event_composition_publication_integrity_failure")
    publication_target = _publication_target(conn, row[0], lock=False)
    identity = publication_target["event_id"] if publication_target else event_identity(row[0])
    if event_id is not None and event_id != identity:
        raise ValueError("event_composition_publication_event_mismatch")
    if lock:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                     ("event-reassessment:" + identity,))
        locked = conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE NOWAIT",
                              (identity,)).fetchone()
        if not locked:
            raise ValueError("event_composition_publication_event_unavailable")
        if publication_target and publication_target["active"]:
            checked = _publication_target(conn, row[0], lock=True)
            if checked != publication_target:
                raise ValueError("event_reassessment_snapshot_stale")
    sources = _source_rows(conn, row[1], lock=lock)
    expected = sorted((s["candidate_id"], s["evidence_revision_id"], int(s["article_id"]))
                      for s in ledger.get("sources", []))
    actual = sorted((s["candidate_id"], s["evidence_revision_id"], s["article_id"])
                    for s in sources)
    if expected != actual:
        raise ValueError("event_composition_publication_source_stale")
    if lock and not (publication_target and publication_target["active"]):
        links = conn.execute("SELECT article_id FROM event_articles WHERE event_id=%s "
                             "ORDER BY article_id FOR SHARE NOWAIT", (identity,)).fetchall()
        if [int(item[0]) for item in links] != sorted(source["article_id"] for source in sources):
            raise ValueError("event_composition_publication_membership_stale")
    return {"event_id": identity, "ledger_id": row[0], "ledger_revision_id": row[1],
            "ledger_record": {"ledger": ledger, "change": change,
                              "predecessor_revision_id": row[7]},
            "composition_id": composition_id, "composition": composition,
            "sources": sources, "reviewed_at": row[4], "reviewed_by": row[5],
            "publication_target": publication_target}


def validate_bundle(bundle: dict, *, event_id: str, expected_revision: str | None = None) -> dict:
    fields = {"workflow", "event_id", "ledger_revision_id", "composition_id",
              "ledger_record", "composition", "sources", "qualification", "predecessor"}
    if (not isinstance(bundle, dict) or frozenset(bundle) not in {frozenset(fields), frozenset(fields | {"event_target"})}
            or bundle.get("workflow") != PUBLIC_WORKFLOW):
        raise ValueError("invalid_event_composition_publication_bundle")
    material = {key: bundle[key] for key in
                ("event_id", "ledger_revision_id", "composition_id", "ledger_record",
                 "composition", "sources")}
    ledger_record, composition = bundle["ledger_record"], bundle["composition"]
    target = bundle.get("event_target")
    valid_identity = event_id == event_identity(ledger_record["ledger"]["ledger_id"])
    if target is not None:
        valid_identity = (isinstance(target, dict)
            and set(target) == {"kind", "event_id", "snapshot_version"}
            and target.get("kind") == "confirmed-event-reassessment-v1"
            and target.get("event_id") == event_id
            and isinstance(target.get("snapshot_version"), str)
            and len(target["snapshot_version"]) == 64
            and all(char in "0123456789abcdef" for char in target["snapshot_version"]))
    if (bundle["event_id"] != event_id or not valid_identity
            or bundle["ledger_revision_id"] != "elr_" + _version(ledger_record)
            or bundle["composition_id"] != "elc_" + _version(composition)
            or composition.get("ledger_revision_id") != bundle["ledger_revision_id"]
            or composition.get("workflow") not in {
                event_composition.WORKFLOW, event_composition.LEGACY_WORKFLOW}
            or composition.get("public_eligible") is not False
            or ledger_record["ledger"].get("workflow") not in {
                "accepted-evidence-event-ledger-v1", "accepted-evidence-event-ledger-v2"}
            or ledger_record["ledger"].get("public_eligible") is not False):
        raise ValueError("event_composition_publication_integrity_failure")
    if (not isinstance(bundle["sources"], list) or not bundle["sources"]
            or any(not isinstance(source, dict)
                   or set(source) != {"candidate_id", "evidence_revision_id", "article_id",
                                      "title", "url", "brief_day"}
                   or not isinstance(source["article_id"], int)
                   or not isinstance(source["title"], str)
                   or not isinstance(source["url"], str)
                   for source in bundle["sources"])):
        raise ValueError("event_composition_publication_source_invalid")
    qualification = bundle["qualification"]
    if (not isinstance(qualification, dict)
            or qualification.get("workflow") != QUALIFICATION_WORKFLOW
            or qualification.get("event_id") != event_id
            or qualification.get("ledger_revision_id") != bundle["ledger_revision_id"]
            or qualification.get("composition_id") != bundle["composition_id"]):
        raise ValueError("event_composition_publication_qualification_invalid")
    active = {fact["fact_id"]: fact for fact in ledger_record["ledger"].get("facts", [])
              if fact["fact_id"] not in set(ledger_record["ledger"].get("superseded_fact_ids", []))
              | set(ledger_record["ledger"].get("conflict_fact_ids", []))}
    sections = composition.get("sections")
    section_policy = composition.get("section_policy", event_composition.LEGACY_SECTION_POLICY)
    if not isinstance(sections, dict) or set(sections) != set(event_composition.SECTIONS):
        raise ValueError("event_composition_publication_sections_invalid")
    for section, items in sections.items():
        if not isinstance(items, list):
            raise ValueError("event_composition_publication_sections_invalid")
        for item in items:
            refs = item.get("fact_ids") if isinstance(item, dict) else None
            if (not isinstance(item.get("text"), str) or not item["text"].strip()
                    or not isinstance(refs, list) or not refs or len(refs) != len(set(refs))
                    or any(ref not in active for ref in refs)
                    or any(section not in event_composition._allowed_sections(
                        active[ref], section_policy) for ref in refs)):
                raise ValueError("event_composition_publication_citation_invalid")
            if section == "timeline":
                dated = [active[ref].get("date_text") for ref in refs if active[ref].get("date_text")]
                if len(dated) != 1 or item.get("date_text") != dated[0]:
                    raise ValueError("event_composition_publication_timeline_invalid")
    revision = _version({"workflow": PUBLIC_WORKFLOW, "bundle": bundle})
    if expected_revision is not None and revision != expected_revision:
        raise ValueError("event_publication_pointer_mismatch")
    return {**material, "ledger": ledger_record["ledger"], "sections": sections,
            "revision_id": revision}


def materialize_event(conn, composition_id: str) -> dict:
    material = current_material(conn, composition_id)
    event_id = material["event_id"]
    overview = " ".join(
        item["text"] for item in material["composition"]["sections"]["overview"]
    )
    existing = conn.execute("SELECT id,event_key FROM events WHERE id=%s OR event_key=%s FOR UPDATE",
                            (event_id, "event-ledger:" + material["ledger_id"])).fetchall()
    expected_key = "event-ledger:" + material["ledger_id"]
    target = material.get("publication_target")
    if target and target["active"]:
        if len(existing) != 1 or existing[0][0] != event_id:
            raise ValueError("event_composition_publication_event_conflict")
        conn.commit()
        return current_material(conn, composition_id, event_id=event_id)
    if existing and any(row != (event_id, expected_key) for row in existing):
        raise ValueError("event_composition_publication_event_conflict")
    if not existing:
        dates = sorted(source["brief_day"] for source in material["sources"] if source["brief_day"])
        stamp = (dates[0] if dates else utc_now_iso())
        create_event(conn, material["ledger_record"]["ledger"]["kind"],
                     material["ledger_record"]["ledger"]["title"], overview, stamp,
                     dates[-1] if dates else stamp, event_key=expected_key,
                     status="open", visibility="active", lifecycle="candidate",
                     publish_state="draft", site_slug=event_id, event_id=event_id)
    else:
        conn.execute(
            "UPDATE events SET summary=%s,updated_at=%s WHERE id=%s",
            (overview, utc_now_iso(), event_id),
        )
    for source in material["sources"]:
        conn.execute("""INSERT INTO event_articles(event_id,article_id,added_by,created_at)
            VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (event_id, source["article_id"], "event-ledger-publication", utc_now_iso()))
    conn.commit()
    return current_material(conn, composition_id, event_id=event_id)


def queue_research_if_needed(conn, material: dict) -> tuple[str, int] | None:
    """Hold one-source narratives at draft while corroborating coverage is sought."""
    from .config import get_events_settings

    settings = get_events_settings(conn)
    minimum = max(2, int(settings.get("publish_min_articles", 2) or 2))
    source_count = _independent_source_count(material["sources"])
    if source_count >= minimum:
        return None
    maximum = max(minimum, int(settings.get("enrich_min_articles_max_results", 12) or 12))
    job_id = enqueue_job(
        conn,
        "enrich_event_from_web",
        {"event_id": material["event_id"], "max_results": maximum,
         "replace_existing": False},
        debounce=True,
        dedupe=True,
    )
    return job_id, minimum


def _compatibility_recovery(authority, approval_id: str, prior_job_id: str) -> str | None:
    """Queue one replacement when an older worker rejected a current contract."""
    prior = authority.execute(
        "SELECT status,COALESCE(error,'') FROM jobs WHERE id=%s", (prior_job_id,),
    ).fetchone()
    if not prior or prior[0] != "failed" or prior[1] not in COMPATIBILITY_FAILURES:
        return None
    existing = authority.execute(
        """SELECT id FROM jobs WHERE job_type=%s AND parent_job_id=%s
            ORDER BY requested_at LIMIT 1""", (JOB_TYPE, prior_job_id),
    ).fetchone()
    if existing:
        return existing[0]
    return enqueue_job(
        authority, JOB_TYPE, {"approval_id": approval_id}, priority=-10,
        queue_name="fetch", max_attempts=1, parent_job_id=prior_job_id,
        dedupe_key="event-publication-compatibility:" + approval_id,
        commit=False,
    )


def _submit(conn, composition_id: str, *, reviewer_kind: str,
            required_reviewer: str | None = None) -> dict:
    if reviewer_kind not in {"human", "policy"}:
        raise ValueError("event_composition_publication_reviewer_invalid")
    material = materialize_event(conn, composition_id)
    if required_reviewer is not None and material.get("reviewed_by") != required_reviewer:
        raise ValueError("event_composition_publication_review_policy_required")
    event_id = material["event_id"]
    research = queue_research_if_needed(conn, material)
    if research:
        research_job_id, minimum_sources = research
        return {"event_id": event_id, "job_id": research_job_id,
                "status": "research_queued", "source_count":
                _independent_source_count(material["sources"]),
                "minimum_sources": minimum_sources,
                "public_eligible": False}
    with connection_factory("SV_EVENT_APPROVAL_DB_URL")() as authority:
        authority.execute("SET LOCAL statement_timeout='3s'")
        current = current_material(authority, composition_id, event_id=event_id, lock=True)
        pointer = authority.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s",
                                    (event_id,)).fetchone()
        predecessor = pointer[0] if pointer else None
        qualification = {"workflow": QUALIFICATION_WORKFLOW, "event_id": event_id,
            "ledger_revision_id": current["ledger_revision_id"], "composition_id": composition_id,
            "reviewer": {"kind": reviewer_kind,
                         "id": current["reviewed_by"] or "authenticated-admin",
                         "version": _version(POLICY)}, "reviewed_at": current["reviewed_at"]}
        approval = {"workflow": APPROVAL_WORKFLOW, "event_id": event_id,
                    "ledger_revision_id": current["ledger_revision_id"],
                    "composition_id": composition_id, "qualification": qualification,
                    "predecessor": predecessor}
        raw = json.dumps(approval, sort_keys=True, ensure_ascii=True)
        if len(raw.encode()) > MAX_APPROVAL_BYTES:
            raise ValueError("event_approval_too_large")
        approval_id, qualification_id = _version(approval), _version(qualification)
        prior = authority.execute("SELECT approval_json,job_id FROM event_review_approvals "
                                  "WHERE approval_id=%s", (approval_id,)).fetchone()
        if prior:
            if prior[0] != raw:
                raise ValueError("event_approval_integrity_failure")
            recovery_job_id = _compatibility_recovery(
                authority, approval_id, prior[1]
            )
            if recovery_job_id:
                return {"event_id": event_id, "approval_id": approval_id,
                        "job_id": recovery_job_id, "status": "queued",
                        "public_eligible": False}
            return {"event_id": event_id, "approval_id": approval_id, "job_id": prior[1],
                    "status": "reused", "public_eligible": False}
        writable = authority.execute("""SELECT
            has_table_privilege(current_user,'event_public_pointers','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_public_pointers','INSERT,UPDATE')
            OR has_table_privilege(current_user,'event_public_revisions','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_public_revisions','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("approval_admission_role_required")
        qraw = json.dumps(qualification, sort_keys=True, ensure_ascii=True)
        authority.execute("""INSERT INTO event_quote_qualifications
            (event_id,qualification_id,qualification_json,recorded_at) VALUES (%s,%s,%s,%s)
            ON CONFLICT(event_id,qualification_id) DO NOTHING""",
            (event_id, qualification_id, qraw, utc_now_iso()))
        stored = authority.execute("SELECT qualification_json,revoked_at FROM event_quote_qualifications "
                                   "WHERE event_id=%s AND qualification_id=%s",
                                   (event_id, qualification_id)).fetchone()
        if stored != (qraw, None):
            raise ValueError("qualification_unavailable")
        job_id = enqueue_job(authority, JOB_TYPE, {"approval_id": approval_id}, priority=-10,
                             queue_name="fetch", max_attempts=1, commit=False)
        authority.execute("""INSERT INTO event_review_approvals
            (approval_id,event_id,qualification_id,approval_json,job_id,recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (approval_id, event_id, qualification_id, raw, job_id, utc_now_iso()))
    return {"event_id": event_id, "approval_id": approval_id, "job_id": job_id,
            "status": "queued", "public_eligible": False}


def submit(conn, composition_id: str, *, confirmation: str) -> dict:
    if confirmation != CONFIRMATION:
        raise ValueError("event_composition_publication_confirmation_required")
    return _submit(conn, composition_id, reviewer_kind="human")


def submit_automated(conn, composition_id: str) -> dict:
    """Admit only a composition accepted by the versioned support-audit policy."""
    return _submit(conn, composition_id, reviewer_kind="policy",
                   required_reviewer="policy:event-composition-audit-v1")


def promote(factory, approval: dict, *, qualification_id: str) -> dict:
    event_id, composition_id = approval["event_id"], approval["composition_id"]
    with factory() as conn:
        conn.execute("SET LOCAL statement_timeout='3s'")
        writable = conn.execute("""SELECT
            has_table_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("qualification_read_only_role_required")
        guarded = conn.execute("""SELECT EXISTS(SELECT 1 FROM pg_trigger
            WHERE tgrelid='event_quote_qualifications'::regclass
              AND tgname='event_qualification_guard' AND tgenabled IN ('O','A')
              AND NOT tgisinternal)""").fetchone()[0]
        if not guarded:
            raise ValueError("qualification_revocation_guard_required")
        current = current_material(conn, composition_id, event_id=event_id, lock=True)
        qualification = approval["qualification"]
        qrow = conn.execute("SELECT qualification_json,revoked_at FROM event_quote_qualifications "
                            "WHERE event_id=%s AND qualification_id=%s",
                            (event_id, qualification_id)).fetchone()
        if not qrow or qrow[1] is not None or json.loads(qrow[0]) != qualification:
            raise ValueError("qualification_unavailable")
        bundle = {"workflow": PUBLIC_WORKFLOW, "event_id": event_id,
                  "ledger_revision_id": current["ledger_revision_id"],
                  "composition_id": composition_id, "ledger_record": current["ledger_record"],
                  "composition": current["composition"], "sources": current["sources"],
                  "qualification": qualification, "predecessor": approval["predecessor"]}
        target = current.get("publication_target")
        if target:
            bundle["event_target"] = {key: target[key] for key in
                                      ("kind", "event_id", "snapshot_version")}
        projection = validate_bundle(bundle, event_id=event_id)
        revision, encoded = projection["revision_id"], json.dumps(bundle, sort_keys=True, ensure_ascii=True)
        pointer = conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s "
                               "FOR UPDATE NOWAIT", (event_id,)).fetchone()
        prior = pointer[0] if pointer else None
        if prior not in {approval["predecessor"], revision}:
            raise ValueError("publication_predecessor_conflict")
        conn.execute("""INSERT INTO event_public_revisions
            (event_id,revision_id,qualification_id,predecessor,bundle_json,recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id,revision_id) DO NOTHING""",
            (event_id, revision, qualification_id, approval["predecessor"], encoded, utc_now_iso()))
        stored = conn.execute("""SELECT qualification_id,predecessor,bundle_json
            FROM event_public_revisions WHERE event_id=%s AND revision_id=%s""",
            (event_id, revision)).fetchone()
        if stored != (qualification_id, approval["predecessor"], encoded):
            raise ValueError("public_revision_conflict")
        conn.execute("""INSERT INTO event_public_pointers(event_id,revision_id,updated_at)
            VALUES (%s,%s,%s) ON CONFLICT(event_id) DO UPDATE
            SET revision_id=EXCLUDED.revision_id,updated_at=EXCLUDED.updated_at""",
            (event_id, revision, utc_now_iso()))
        if target and target["active"]:
            overview = " ".join(item["text"] for item in
                                current["composition"]["sections"]["overview"])
            conn.execute("DELETE FROM event_articles WHERE event_id=%s", (event_id,))
            for source in current["sources"]:
                conn.execute(
                    """INSERT INTO event_articles(event_id,article_id,added_by,created_at)
                       VALUES (%s,%s,'event-ledger-reassessment',%s)""",
                    (event_id, source["article_id"], utc_now_iso()),
                )
            conn.execute(
                """UPDATE events SET event_key=%s,title=%s,summary=%s,site_slug=%s,
                   lifecycle='confirmed',publish_state='published',
                   published_at=COALESCE(published_at,%s),updated_at=%s WHERE id=%s""",
                ("event-ledger:" + current["ledger_id"],
                 current["ledger_record"]["ledger"]["title"], overview, event_id,
                 utc_now_iso(), utc_now_iso(), event_id),
            )
            conn.execute(
                """UPDATE event_reassessment_cases
                   SET status='completed',completed_at=%s,updated_at=%s,
                       decision_reason='strict replacement promoted'
                   WHERE event_id=%s AND status='active'""",
                (utc_now_iso(), utc_now_iso(), event_id),
            )
        else:
            conn.execute("""UPDATE events SET lifecycle='confirmed',publish_state='published',
                published_at=COALESCE(published_at,%s),updated_at=%s WHERE id=%s""",
                (utc_now_iso(), utc_now_iso(), event_id))
    return {"event_id": event_id, "revision_id": revision,
            "status": "reused" if prior == revision else "promoted"}


def run_approval(approval: dict, *, qualification_id: str, factory=None) -> dict:
    if approval.get("workflow") != APPROVAL_WORKFLOW:
        raise ValueError("invalid_event_composition_publication_approval")
    factory = factory or connection_factory("SV_EVENT_PROMOTION_DB_URL")
    result = promote(factory, approval, qualification_id=qualification_id)
    return {**result, "publication_status": "awaiting_export", "public_eligible": False}
