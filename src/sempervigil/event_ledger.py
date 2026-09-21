"""Private, versioned Event fact ledger derived from accepted article evidence."""
import json

from .event_fact_roles import validate as validate_sections
from .investigation import _version
from .utils import utc_now_iso

WORKFLOW = "accepted-evidence-event-ledger-v2"
LEGACY_WORKFLOW = "accepted-evidence-event-ledger-v1"
CHANGE_KINDS = {"initial", "additive", "correction", "conflict"}
DECISIONS = {"accept", "hold", "reject", "withdraw"}


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("event_ledger_stored_shape_invalid")
    return value


def _source(conn, candidate_id: str) -> dict:
    row = conn.execute(
        """
        SELECT c.candidate_id, c.article_id, c.status, c.kind, c.title,
               c.signals_json, r.revision_id, r.status, r.evidence_json,
               c.selected_fact_ids_json, c.selected_fact_sections_json
        FROM incident_candidates c
        JOIN article_evidence_revisions r ON r.revision_id=c.evidence_revision_id
        WHERE c.candidate_id=%s
        """,
        (candidate_id,),
    ).fetchone()
    if not row:
        raise ValueError("event_ledger_candidate_missing")
    if row[2] != "enrolled":
        raise ValueError("event_ledger_candidate_not_enrolled")
    if row[7] != "accepted":
        raise ValueError("event_ledger_evidence_not_accepted")
    evidence = _decode(row[8])
    if evidence.get("status") != "unreviewed" or evidence.get("public_eligible") is not False:
        raise ValueError("event_ledger_evidence_shape_invalid")
    facts = evidence.get("facts")
    if not isinstance(facts, list) or not facts:
        raise ValueError("event_ledger_evidence_shape_invalid")
    if row[9]:
        selected = set(json.loads(row[9]))
        facts = [fact for fact in facts if fact.get("id") in selected]
        if not facts or {fact.get("id") for fact in facts} != selected:
            raise ValueError("event_ledger_fact_selection_invalid")
        if not row[10]:
            raise ValueError("event_ledger_fact_sections_missing")
        stored_sections = _decode(row[10])
        assignments = [{"fact_id": fact_id, "sections": sections}
                       for fact_id, sections in stored_sections.items()]
        fact_sections = validate_sections(
            {str(fact.get("id") or ""): fact for fact in facts}, selected, assignments)
    else:
        raise ValueError("event_ledger_fact_selection_missing")
    return {
        "candidate_id": row[0], "article_id": row[1], "kind": row[3], "title": row[4],
        "signals": _decode(row[5]), "evidence_revision_id": row[6], "facts": facts,
        "fact_sections": fact_sections,
    }


def _fact(source: dict, fact: dict) -> dict:
    required = {"id", "statement", "kind", "date_text", "date_role", "evidence_passages"}
    if not required <= set(fact) or not str(fact["id"]).strip() or not str(fact["statement"]).strip():
        raise ValueError("event_ledger_fact_invalid")
    passages = fact["evidence_passages"]
    if not isinstance(passages, list) or not passages:
        raise ValueError("event_ledger_fact_invalid")
    exact = []
    for passage in passages:
        if (not isinstance(passage, dict) or not {"id", "start", "end", "text"} <= set(passage)
                or not str(passage["text"]).strip()):
            raise ValueError("event_ledger_fact_invalid")
        exact.append({key: passage[key] for key in ("id", "start", "end", "text")})
    return {
        "fact_id": fact["id"], "candidate_id": source["candidate_id"],
        "evidence_revision_id": source["evidence_revision_id"], "article_id": source["article_id"],
        "statement": fact["statement"], "kind": fact["kind"],
        "date_text": fact["date_text"], "date_role": fact["date_role"],
        "sections": source["fact_sections"][fact["id"]],
        "exact_passages": exact,
    }


def _section_tags(fact: dict) -> list[str]:
    text = fact["statement"].lower()
    tags = []
    if fact["date_role"] == "incident" or fact["date_text"]:
        tags.append("timeline")
    if any(cue in text for cue in (
        "initial access", "unauthorized access", "gained access", "breached",
        "intercept", "redirect", "malicious manifest", "malicious installer",
        "malware", "package", "payload", "phishing email", "phishing message",
        "phishing campaign", "via phishing", "pivot", "execute", "infect",
    )):
        tags.append("attack_path")
    if fact["kind"] != "recommendation" and any(cue in text for cue in (
        "impact", "exfiltrat", "stole", "stolen", "exposed", "affected",
        "personal data", "devices", "records", "accounts", "wallet", "disrupt",
    )):
        tags.append("impact")
    if any(cue in text for cue in (
        "closed the attacker", "closed attackers", "implemented additional security",
        "notified", "reported the breach", "reported the incident", "patched",
        "remediation", "rotated", "restored", "containment", "contained",
        "investigation", "monitoring", "continues to monitor", "moved update",
        "signature verification", "certificate verification",
    )):
        tags.append("response_recovery")
    if fact["kind"] == "recommendation":
        tags.append("mitigation")
    if any(cue in text for cue in (
        "attributed", "assessed", "agency", "researcher", "fbi", "cisa",
        "threat actor", "behind the attack", "claimed responsibility", "state-sponsored",
    )):
        tags.append("attribution")
    if fact["kind"] in {"allegation", "uncertainty"} or any(cue in text for cue in (
        "no evidence", "not aware", "cannot rule out", "has not shared",
        "no known", "unknown", "unclear", "did not respond", "did not answer",
        "whether",
    )):
        tags.append("open_question")
    return list(dict.fromkeys(tags)) or ["context"]


def _accepted(conn, ledger_id: str) -> tuple | None:
    return conn.execute(
        """SELECT revision_id, ledger_json FROM event_ledger_revisions
           WHERE ledger_id=%s AND status='accepted' FOR UPDATE""", (ledger_id,)
    ).fetchone()


def propose(conn, candidate_id: str, *, ledger_id: str | None = None,
            change_kind: str = "initial", supersedes_fact_ids: list[str] | None = None,
            conflict_fact_ids: list[str] | None = None) -> dict:
    if change_kind not in CHANGE_KINDS:
        raise ValueError("event_ledger_change_kind_invalid")
    source = _source(conn, candidate_id)
    ledger_id = ledger_id or ("eld_" + _version({"candidate_id": candidate_id}))
    if not ledger_id.startswith("eld_"):
        raise ValueError("event_ledger_identity_invalid")
    prior_row = _accepted(conn, ledger_id)
    prior = _decode(prior_row[1]) if prior_row else None
    existing_sources = [] if prior is None else list(prior["sources"])
    if any(row["candidate_id"] == candidate_id for row in existing_sources):
        return {"ledger_id": ledger_id, "revision_id": prior_row[0], "status": "accepted",
                "reused": True, "public_eligible": False}
    if (prior is None) != (change_kind == "initial"):
        raise ValueError("event_ledger_change_kind_mismatch")
    sources = sorted(existing_sources + [{key: source[key] for key in
        ("candidate_id", "evidence_revision_id", "article_id", "title", "kind")}],
        key=lambda row: row["candidate_id"])
    facts = [] if prior is None else list(prior["facts"])
    known = {row["fact_id"] for row in facts}
    added = []
    for raw in source["facts"]:
        item = _fact(source, raw)
        if item["fact_id"] not in known:
            facts.append(item)
            known.add(item["fact_id"])
            added.append(item["fact_id"])
    supersedes = sorted(set(supersedes_fact_ids or []))
    conflicts = sorted(set(conflict_fact_ids or []))
    if not set(supersedes + conflicts) <= known:
        raise ValueError("event_ledger_unknown_fact_reference")
    if change_kind == "correction" and not supersedes:
        raise ValueError("event_ledger_correction_target_required")
    if change_kind == "conflict" and not conflicts:
        raise ValueError("event_ledger_conflict_target_required")
    prior_superseded = [] if prior is None else list(prior.get("superseded_fact_ids", []))
    prior_conflicts = [] if prior is None else list(prior.get("conflict_fact_ids", []))
    ledger = {
        "workflow": WORKFLOW, "ledger_id": ledger_id,
        "title": source["title"] if prior is None else prior["title"],
        "kind": source["kind"] if prior is None else prior["kind"],
        "sources": sources, "facts": sorted(facts, key=lambda row: row["fact_id"]),
        "superseded_fact_ids": sorted(set(prior_superseded + supersedes)),
        "conflict_fact_ids": sorted(set(prior_conflicts + conflicts)),
        "public_eligible": False,
    }
    change = {"kind": change_kind, "added_fact_ids": sorted(added),
              "supersedes_fact_ids": supersedes, "conflict_fact_ids": conflicts}
    identity = {"ledger": ledger, "change": change,
                "predecessor_revision_id": prior_row[0] if prior_row else None}
    revision_id = "elr_" + _version(identity)
    encoded = json.dumps(ledger, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    encoded_change = json.dumps(change, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    open_row = conn.execute(
        "SELECT revision_id, ledger_json, change_json, status FROM event_ledger_revisions "
        "WHERE ledger_id=%s AND status IN ('proposed','held') FOR UPDATE", (ledger_id,)
    ).fetchone()
    if open_row:
        if open_row[1] == encoded and open_row[2] == encoded_change:
            return {"ledger_id": ledger_id, "revision_id": open_row[0], "status": open_row[3],
                    "reused": True, "public_eligible": False}
        raise ValueError("event_ledger_open_revision_exists")
    conn.execute(
        """INSERT INTO event_ledger_revisions
           (revision_id, ledger_id, predecessor_revision_id, status, change_kind,
            ledger_json, change_json, created_at)
           VALUES (%s,%s,%s,'proposed',%s,%s,%s,%s)""",
        (revision_id, ledger_id, prior_row[0] if prior_row else None, change_kind,
         encoded, encoded_change, utc_now_iso()),
    )
    for row in sources:
        conn.execute(
            """INSERT INTO event_ledger_revision_sources
               (revision_id, candidate_id, evidence_revision_id, article_id)
               VALUES (%s,%s,%s,%s)""",
            (revision_id, row["candidate_id"], row["evidence_revision_id"], row["article_id"]),
        )
    conn.commit()
    return {"ledger_id": ledger_id, "revision_id": revision_id, "status": "proposed",
            "reused": False, "public_eligible": False}


def propose_initial_sources(conn, candidate_ids: list[str], *, ledger_id: str,
                            title: str, commit: bool = True,
                            replace_open: bool = False) -> dict:
    """Create one reviewable initial revision from a validated source cohort."""
    if (not ledger_id.startswith("eld_") or not title.strip() or len(title) > 512
            or not 2 <= len(candidate_ids) <= 50
            or len(set(candidate_ids)) != len(candidate_ids)):
        raise ValueError("event_ledger_initial_cohort_invalid")
    prior = conn.execute(
        """SELECT revision_id,status,ledger_json FROM event_ledger_revisions
            WHERE ledger_id=%s ORDER BY created_at DESC,revision_id DESC LIMIT 1
            FOR UPDATE""", (ledger_id,)
    ).fetchone()
    if prior and not replace_open:
        raise ValueError("event_ledger_initial_cohort_exists")
    if prior and prior[1] not in {"proposed", "held", "rejected", "withdrawn"}:
        raise ValueError("event_ledger_initial_cohort_not_replaceable")
    source_rows = sorted((_source(conn, candidate_id) for candidate_id in candidate_ids),
                         key=lambda row: row["candidate_id"])
    sources = [{key: source[key] for key in
                ("candidate_id", "evidence_revision_id", "article_id", "title", "kind")}
               for source in source_rows]
    facts, known = [], set()
    for source in source_rows:
        for raw in source["facts"]:
            item = _fact(source, raw)
            if item["fact_id"] in known:
                continue
            known.add(item["fact_id"])
            facts.append(item)
    if not facts:
        raise ValueError("event_ledger_initial_cohort_empty")
    ledger = {"workflow": WORKFLOW, "ledger_id": ledger_id, "title": title.strip(),
              "kind": source_rows[0]["kind"], "sources": sources,
              "facts": sorted(facts, key=lambda row: row["fact_id"]),
              "superseded_fact_ids": [], "conflict_fact_ids": [],
              "public_eligible": False}
    change = {"kind": "initial", "added_fact_ids": sorted(known),
              "supersedes_fact_ids": [], "conflict_fact_ids": []}
    predecessor = prior[0] if prior else None
    identity = {"ledger": ledger, "change": change,
                "predecessor_revision_id": predecessor}
    revision_id = "elr_" + _version(identity)
    if prior and prior[0] == revision_id:
        return {"ledger_id": ledger_id, "revision_id": revision_id,
                "status": prior[1], "reused": True, "public_eligible": False}
    if prior and prior[1] in {"proposed", "held"}:
        now = utc_now_iso()
        conn.execute(
            """UPDATE event_ledger_revisions SET status='rejected',reviewed_at=%s,
               reviewed_by='system:title-provenance',
               review_reason='replaced by evidence-source title'
               WHERE revision_id=%s""",
            (now, prior[0]),
        )
    conn.execute(
        """INSERT INTO event_ledger_revisions
           (revision_id,ledger_id,predecessor_revision_id,status,change_kind,
            ledger_json,change_json,created_at)
           VALUES (%s,%s,%s,'proposed','initial',%s,%s,%s)""",
        (revision_id, ledger_id, predecessor,
         json.dumps(ledger, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
         json.dumps(change, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
         utc_now_iso()),
    )
    if prior:
        conn.execute(
            """UPDATE event_ledger_revisions SET superseded_by_revision_id=%s
               WHERE revision_id=%s""", (revision_id, prior[0])
        )
    for source in sources:
        conn.execute(
            """INSERT INTO event_ledger_revision_sources
               (revision_id,candidate_id,evidence_revision_id,article_id)
               VALUES (%s,%s,%s,%s)""",
            (revision_id, source["candidate_id"], source["evidence_revision_id"],
             source["article_id"]),
        )
    if commit:
        conn.commit()
    return {"ledger_id": ledger_id, "revision_id": revision_id,
            "status": "proposed", "reused": False, "public_eligible": False}


def _lineage_current(conn, revision_id: str) -> bool:
    stale = conn.execute(
        """SELECT 1 FROM event_ledger_revision_sources s
           JOIN incident_candidates c ON c.candidate_id=s.candidate_id
           JOIN article_evidence_revisions e ON e.revision_id=s.evidence_revision_id
           WHERE s.revision_id=%s AND (c.status<>'enrolled' OR e.status<>'accepted'
                 OR c.evidence_revision_id<>s.evidence_revision_id) LIMIT 1""",
        (revision_id,),
    ).fetchone()
    if stale is not None:
        return False
    row = conn.execute(
        "SELECT ledger_json FROM event_ledger_revisions WHERE revision_id=%s",
        (revision_id,),
    ).fetchone()
    if not row:
        return False
    ledger = _decode(row[0])
    actual: dict[str, set[str]] = {}
    for fact in ledger.get("facts", []):
        actual.setdefault(str(fact.get("candidate_id") or ""), set()).add(
            str(fact.get("fact_id") or "")
        )
    rows = conn.execute(
        """SELECT s.candidate_id,c.selected_fact_ids_json,c.selected_fact_sections_json
             FROM event_ledger_revision_sources s JOIN incident_candidates c
               ON c.candidate_id=s.candidate_id
            WHERE s.revision_id=%s""", (revision_id,),
    ).fetchall()
    for candidate_id, selected_raw, sections_raw in rows:
        if selected_raw and actual.get(candidate_id, set()) != set(json.loads(selected_raw)):
            return False
        if sections_raw:
            expected = _decode(sections_raw)
            actual_sections = {fact["fact_id"]: fact.get("sections", [])
                               for fact in ledger.get("facts", [])
                               if fact.get("candidate_id") == candidate_id}
            if actual_sections != expected:
                return False
    return True


def review(conn, revision_id: str, decision: str, *, reason: str, reviewer: str) -> dict:
    if decision not in DECISIONS or not revision_id.startswith("elr_"):
        raise ValueError("event_ledger_review_invalid")
    if not reviewer.strip() or len(reviewer) > 80:
        raise ValueError("event_ledger_review_identity_invalid")
    reason = reason.strip()
    if decision != "accept" and not reason:
        raise ValueError("event_ledger_review_reason_required")
    if len(reason) > 1000:
        raise ValueError("event_ledger_review_reason_too_long")
    row = conn.execute(
        "SELECT ledger_id, status FROM event_ledger_revisions WHERE revision_id=%s FOR UPDATE",
        (revision_id,),
    ).fetchone()
    if not row:
        raise ValueError("event_ledger_revision_missing")
    if decision == "withdraw":
        if row[1] != "accepted":
            raise ValueError("event_ledger_withdraw_requires_accepted")
        next_status = "withdrawn"
    else:
        if row[1] not in {"proposed", "held"}:
            raise ValueError("event_ledger_revision_already_decided")
        next_status = {"accept": "accepted", "hold": "held", "reject": "rejected"}[decision]
    if decision == "accept" and not _lineage_current(conn, revision_id):
        raise ValueError("event_ledger_lineage_stale")
    now = utc_now_iso()
    if decision == "accept":
        conn.execute(
            """UPDATE event_ledger_revisions SET status='superseded', reviewed_at=%s,
               reviewed_by=%s, review_reason='superseded by accepted revision',
               superseded_by_revision_id=%s
               WHERE ledger_id=%s AND status='accepted' AND revision_id<>%s""",
            (now, reviewer, revision_id, row[0], revision_id),
        )
    conn.execute(
        """UPDATE event_ledger_revisions SET status=%s, reviewed_at=%s,
           reviewed_by=%s, review_reason=%s WHERE revision_id=%s""",
        (next_status, now, reviewer, reason or None, revision_id),
    )
    conn.commit()
    return {"ledger_id": row[0], "revision_id": revision_id, "status": next_status,
            "public_eligible": False}


def list_revisions(conn, *, status: str = "proposed", limit: int = 50) -> list[dict]:
    allowed = {"proposed", "accepted", "held", "rejected", "superseded", "withdrawn", "all"}
    if status not in allowed or not 1 <= limit <= 200:
        raise ValueError("event_ledger_list_invalid")
    where = "" if status == "all" else "WHERE r.status=%s"
    params = [] if status == "all" else [status]
    rows = conn.execute(
        f"""SELECT r.revision_id, r.ledger_id, r.predecessor_revision_id, r.status,
                   r.change_kind, r.ledger_json, r.change_json, r.created_at,
                   r.reviewed_at, r.reviewed_by, r.review_reason
            FROM event_ledger_revisions r {where}
            ORDER BY r.created_at DESC, r.revision_id DESC LIMIT %s""",
        (*params, limit),
    ).fetchall()
    result = []
    for row in rows:
        ledger = _decode(row[5])
        result.append({"revision_id": row[0], "ledger_id": row[1],
                       "predecessor_revision_id": row[2], "status": row[3],
                       "change_kind": row[4], "ledger": ledger, "change": _decode(row[6]),
                       "created_at": row[7], "reviewed_at": row[8], "reviewed_by": row[9],
                       "review_reason": row[10], "lineage_current": _lineage_current(conn, row[0]),
                       "public_eligible": False})
    return result


def get_revision(conn, revision_id: str, *, require_status: str | None = None) -> dict:
    if not isinstance(revision_id, str) or not revision_id.startswith("elr_"):
        raise ValueError("event_ledger_revision_invalid")
    row = conn.execute(
        """SELECT revision_id, ledger_id, predecessor_revision_id, status,
                  change_kind, ledger_json, change_json, created_at,
                  reviewed_at, reviewed_by, review_reason
           FROM event_ledger_revisions WHERE revision_id=%s""",
        (revision_id,),
    ).fetchone()
    if not row or (require_status is not None and row[3] != require_status):
        raise ValueError("event_ledger_revision_missing")
    return {"revision_id": row[0], "ledger_id": row[1],
            "predecessor_revision_id": row[2], "status": row[3],
            "change_kind": row[4], "ledger": _decode(row[5]), "change": _decode(row[6]),
            "created_at": row[7], "reviewed_at": row[8], "reviewed_by": row[9],
            "review_reason": row[10], "lineage_current": _lineage_current(conn, row[0]),
            "public_eligible": False}
