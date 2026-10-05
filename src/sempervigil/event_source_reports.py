"""Bounded, event-centered writer/reviewer jobs; default-disabled admission."""
import json
import os
import re

from . import event_report_contract as contract
from .investigation import _version
from .storage import enqueue_job
from .utils import utc_now_iso

JOB_TYPE = "event_source_report"
MODEL = "gpt-5.6-luna"
SCHEMA = """
CREATE TABLE IF NOT EXISTS event_source_report_runs (
 run_id TEXT PRIMARY KEY, event_id TEXT NOT NULL REFERENCES events(id),
 request_key TEXT NOT NULL UNIQUE, trigger_kind TEXT NOT NULL
 CHECK(trigger_kind IN ('evidence_change','generator_upgrade')),
 snapshot_json TEXT NOT NULL, source_version TEXT NOT NULL, generator_version TEXT NOT NULL,
 predecessor TEXT, status TEXT NOT NULL CHECK(status IN ('queued','running','held','accepted')),
 correction_enabled BOOLEAN NOT NULL DEFAULT FALSE,
 report_json TEXT, review_json TEXT, spans_json TEXT, reason TEXT,
 budget_tokens INTEGER NOT NULL CHECK(budget_tokens BETWEEN 1 AND 200000),
 charged_tokens INTEGER NOT NULL DEFAULT 0 CHECK(charged_tokens>=0),
 reserved_tokens INTEGER NOT NULL DEFAULT 0 CHECK(reserved_tokens>=0),
 created_at TEXT NOT NULL,
 CHECK(charged_tokens+reserved_tokens<=budget_tokens)
);
CREATE TABLE IF NOT EXISTS event_source_report_calls (
 run_id TEXT NOT NULL REFERENCES event_source_report_runs(run_id),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 4),
 phase TEXT NOT NULL CHECK(phase IN ('writer','review','correction','verification')),
 request_json TEXT NOT NULL, response_json TEXT, status TEXT NOT NULL
 CHECK(status IN ('started','completed','failed')),
 reservation INTEGER NOT NULL CHECK(reservation>0), usage_json TEXT,
 error TEXT, created_at TEXT NOT NULL,
 PRIMARY KEY(run_id,ordinal), UNIQUE(run_id,phase)
);
CREATE TABLE IF NOT EXISTS event_source_report_derivatives (
 run_id TEXT PRIMARY KEY REFERENCES event_source_report_runs(run_id),
 projection_json TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE OR REPLACE FUNCTION source_report_artifact_guard() RETURNS trigger AS $$
BEGIN
 IF TG_TABLE_NAME='event_source_report_derivatives' THEN
   RAISE EXCEPTION 'source report artifact is immutable' USING ERRCODE='23514';
 END IF;
 IF OLD.status='completed' THEN
   RAISE EXCEPTION 'source report artifact is immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS source_report_call_guard ON event_source_report_calls;
CREATE TRIGGER source_report_call_guard BEFORE UPDATE OR DELETE ON event_source_report_calls
 FOR EACH ROW EXECUTE FUNCTION source_report_artifact_guard();
DROP TRIGGER IF EXISTS source_report_derivative_guard ON event_source_report_derivatives;
CREATE TRIGGER source_report_derivative_guard BEFORE UPDATE OR DELETE ON event_source_report_derivatives
 FOR EACH ROW EXECUTE FUNCTION source_report_artifact_guard();
"""


def enabled():
    value = os.environ.get("SV_EVENT_SOURCE_REPORT_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("event_source_report_enablement_invalid")
    return value == "1"


def check_scope(event_id):
    scope = os.environ.get("SV_EVENT_SOURCE_REPORT_EVENT_IDS", "").strip()
    if scope and event_id not in {s.strip() for s in scope.split(",") if s.strip()}:
        raise PermissionError("event_source_report_event_outside_scope")


def configuration(conn):
    from .services.ai_service import get_model, get_provider
    row = conn.execute("""SELECT p.id,m.id FROM llm_providers p JOIN llm_models m ON m.provider_id=p.id
      WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
      AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s ORDER BY m.id LIMIT 1""",
      (MODEL,)).fetchone()
    if not row:
        raise ValueError("event_source_report_model_missing")
    provider, model = get_provider(conn, row[0]), get_model(conn, row[1])
    version = _version({"workflow": contract.WORKFLOW, "model": model["id"],
        "provider": provider["id"], "base_url": provider["base_url"],
        "writer": contract.WRITER, "reviewer": contract.REVIEWER,
        "schema": contract.schema(["S1"]), "reasoning": "low",
        "limits": [3200, 1600], "tokenizer": "o200k_base", "context_tokens": 24000,
        "update_context": "published-evidence-delta-v3", "cohort_policy":"serialized-reservation-v1",
        "projection_policy":contract.PROJECTION_WORKFLOW})
    return model, provider, version


def snapshot(conn, event_id, *, lock=False):
    suffix = " FOR SHARE NOWAIT" if lock else ""
    row = conn.execute("SELECT id,title,visibility,lifecycle FROM events WHERE id=%s"+suffix,
                       (event_id,)).fetchone()
    if not row or row[2:] != ("active", "confirmed"):
        raise ValueError("event_source_report_event_ineligible")
    # Membership is frozen separately from body deduplication. Suppressed sources
    # never enter model context. Article metadata is part of freshness identity.
    links = conn.execute("SELECT article_id FROM event_articles WHERE event_id=%s ORDER BY article_id"+suffix,
                         (event_id,)).fetchall()
    ids = [r[0] for r in links]
    articles = conn.execute("""SELECT id,title,original_url,content_text,published_at,ingested_at,meta_json
      FROM articles WHERE id=ANY(%s) ORDER BY id"""+suffix, (ids,)).fetchall()
    sources, excluded, membership, bodies = [], [], [], {}
    for aid, title, url, text, published, retrieved, rawmeta in articles:
        meta = json.loads(rawmeta) if isinstance(rawmeta, str) and rawmeta else (rawmeta or {})
        membership.append({"article_id": aid, "title": title, "url": url,
                           "text_hash": contract.digest(text or ""), "suppressed": bool(meta.get("suppressed"))})
        if meta.get("suppressed") or not text or not url:
            excluded.append(aid)
            continue
        from urllib.parse import urlparse
        if urlparse(url).scheme not in {"http", "https"}:
            raise ValueError("event_source_report_url_invalid")
        content_hash = contract.digest(" ".join(text.split()))
        if content_hash in bodies:
            bodies[content_hash]["duplicates"].append({"article_id": aid, "url": url})
            continue
        source = {"id": "S"+str(aid), "article_id": aid, "title": title or "Source article",
                  "url": url, "text": text, "content_hash": content_hash,
                  "published_at": str(published) if published else None,
                  "retrieved_at": str(retrieved) if retrieved else None, "duplicates": []}
        sources.append(source); bodies[content_hash] = source
    if not sources or len(articles) != len(ids):
        raise ValueError("event_source_report_sources_missing")
    material = {"event_id": event_id, "title": row[1], "sources": sources,
                "membership": membership, "excluded_article_ids": excluded}
    return {**material, "source_version": _version(material),
            "evidence_version": _version(sorted(bodies))}


def previous(conn, event_id):
    row = conn.execute("""SELECT p.revision_id,r.bundle_json FROM event_public_pointers p
       JOIN event_public_revisions r USING(event_id,revision_id) WHERE p.event_id=%s""", (event_id,)).fetchone()
    if not row:
        return None, None
    bundle = json.loads(row[1]) if isinstance(row[1], str) else row[1]
    # Explicitly remove evidence from the continuity field. Current original sources
    # are independently supplied; no report assertion is promoted into a premise.
    report = bundle.get("report")
    if report is None and bundle.get("composition"):
        sections=bundle["composition"].get("sections",{})
        report={"items":[{"section":section,"text":x["text"],
                          "claim_type":x.get("claim_type"),"confidence":x.get("confidence")}
                         for section,items in sections.items() for x in items]}
    return row[0], report


def meaningful_change(old, new):
    prior = {s["article_id"]:s for s in old["sources"]}
    if any(s["article_id"] in prior and any(s.get(k)!=prior[s["article_id"]].get(k)
           for k in ("title","url","published_at")) for s in new["sources"]):
        return True
    if old["evidence_version"] == new["evidence_version"]:
        return False
    by_article = {s["article_id"]: s for s in old["sources"]}
    if any(s["article_id"] in by_article and s["content_hash"] != by_article[s["article_id"]]["content_hash"]
           for s in new["sources"]):
        return True  # Corrections to existing sources must never be novelty-filtered.
    if {s["content_hash"] for s in old["sources"]} - {s["content_hash"] for s in new["sources"]}:
        return True  # Removal, suppression, and corrections also require reconciliation.
    old_text = " ".join(_novelty_body(s["text"]) for s in old["sources"])
    additions = [s for s in new["sources"] if s["content_hash"] not in
                 {x["content_hash"] for x in old["sources"]}]
    for source in additions:
        sentences = re.split(r"(?<=[.!?])\s+", _novelty_body(source["text"]))
        novel = [s for s in sentences if s not in old_text]
        # Conservative lexical gate: only skip exact contained statements. A local
        # semantic classifier can narrow this later; ambiguity currently proceeds.
        if novel:
            return True
    return False


def _novelty_body(text):
    """Ignore recognized syndicated-page chrome only in the addition gate.

    This never changes stored/model evidence or filters an existing-body correction.
    Ambiguous or unrecognized text still proceeds to whole-context review.
    """
    text = " ".join(text.split()).replace("Advertisement. Scroll to continue reading.", "")
    for marker in (" Related:", " Written By ", " Daily Briefing Newsletter ",
                   " More from ", " Latest News "):
        text = text.split(marker, 1)[0]
    return text.strip()


def published_baseline(conn, event_id, current):
    """Use the actual public revision, including qualified held derivatives.

    Legacy bodies can be reconstructed only when their immutable article evidence
    versions still match; otherwise report an unavailable baseline, never novelty.
    Unpublished accepted runs are not a replacement for a public predecessor.
    """
    row = conn.execute("""SELECT p.revision_id,r.bundle_json FROM event_public_pointers p
       JOIN event_public_revisions r USING(event_id,revision_id) WHERE p.event_id=%s""", (event_id,)).fetchone()
    if row:
        bundle = json.loads(row[1]) if isinstance(row[1], str) else row[1]
        # Each retained publication workflow owns its own revision-hash formula.
        # Legacy compositions hash a workflow envelope, not the bare bundle.
        from .event_render import resolve
        resolve(bundle,event_id=event_id,expected_revision=row[0])
        if bundle.get("workflow") == contract.PUBLIC_WORKFLOW:
            from .event_source_report_publication import validate_bundle
            validate_bundle(bundle,event_id=event_id,expected_revision=row[0])
            raw = _load(conn,bundle["run_id"])
            if raw["event_id"] != event_id or raw["source_version"] != bundle["qualification"]["source_version"]:
                raise ValueError("event_source_report_baseline_integrity")
            return raw["snapshot"], raw["generator_version"]
        from .article_evidence import source_for
        from .storage import get_article_by_id
        ids = []
        for source in bundle.get("sources", []):
            aid = source["article_id"]
            evidence = conn.execute("SELECT article_id,source_version FROM article_evidence_revisions WHERE revision_id=%s",
                                    (source.get("evidence_revision_id"),)).fetchone()
            article = get_article_by_id(conn,aid)
            if not article or not evidence or evidence != (aid,source_for(article)["source_version"]):
                return None, None
            ids.append(aid)
        retained = [s for s in current["sources"] if s["article_id"] in ids]
        if not ids or len(retained) != len(ids):
            return None, None
        return {"sources":retained,"evidence_version":_version(sorted(s["content_hash"] for s in retained))}, None
    latest = conn.execute("""SELECT snapshot_json,generator_version FROM event_source_report_runs
        WHERE event_id=%s AND status='accepted' ORDER BY created_at DESC LIMIT 1""", (event_id,)).fetchone()
    return (json.loads(latest[0]),latest[1]) if latest else (None,None)


def cohort_configuration():
    cohort = os.environ.get("SV_EVENT_SOURCE_REPORT_COHORT_ID", "").strip()
    limit = os.environ.get("SV_EVENT_SOURCE_REPORT_COHORT_TOKENS", "").strip()
    if not cohort and not limit:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}",cohort) or not limit.isdecimal() or not 1 <= int(limit) <= 2000000:
        raise ValueError("event_source_report_cohort_invalid")
    return {"id":cohort,"limit":int(limit)}


def _reserve_cohort(conn, cohort, reservation):
    if not cohort:
        return
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",("source-report-cohort:"+cohort["id"],))
    rows = conn.execute("""SELECT charged_tokens,reserved_tokens,snapshot_json::jsonb->'cohort'->>'limit'
       FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'=%s""",(cohort["id"],)).fetchall()
    if any(int(row[2]) != cohort["limit"] for row in rows):
        raise ValueError("event_source_report_cohort_limit_conflict")
    if sum(row[0]+row[1] for row in rows)+reservation > cohort["limit"]:
        raise ValueError("event_source_report_cohort_exhausted")


def submit(conn, event_id, *, trigger="evidence_change", budget_tokens=24000,
           debounce_seconds=300, allow_correction=False, analyst_question=None):
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    check_scope(event_id)
    if (trigger not in {"evidence_change", "generator_upgrade"} or not 1 <= budget_tokens <= 200000
            or type(allow_correction) is not bool):
        raise ValueError("event_source_report_admission_invalid")
    if not 0 <= debounce_seconds <= 86400:
        raise ValueError("event_source_report_debounce_invalid")
    if analyst_question is not None and (not isinstance(analyst_question,str)
            or not 1 <= len(analyst_question.strip()) <= 800):
        raise ValueError("event_source_report_analyst_question_invalid")
    _, _, generation = configuration(conn)
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("source-report:"+event_id,))
    snap = snapshot(conn, event_id)
    predecessor, prior = previous(conn, event_id)
    old, prior_generation = published_baseline(conn,event_id,snap)
    if old:
        if trigger == "evidence_change" and not meaningful_change(old, snap):
            conn.commit(); return {"status": "unchanged", "reason": "no_meaningful_evidence_change"}
        if trigger == "generator_upgrade" and prior_generation == generation:
            conn.commit(); return {"status": "unchanged", "reason": "generator_current"}
    # Identical snapshot/config requests, including held attempts, never regenerate.
    key = _version({"event_id": event_id, "evidence": snap["evidence_version"],
                    "source_version": snap["source_version"], "generator": generation,
                    "analyst_question":analyst_question})
    run_id = "esr_"+key
    existing = conn.execute("SELECT status FROM event_source_report_runs WHERE run_id=%s", (run_id,)).fetchone()
    if existing:
        conn.commit(); return {"run_id": run_id, "status": existing[0], "reused": True}
    snap.update(previous_report=prior, previous_evidence_hashes=[] if not old else
                [s["content_hash"] for s in old["sources"]],
                previous_cited_article_ids=[])
    snap = contract.update_context(snap,trigger,old)
    if analyst_question:
        snap["analyst_question"] = analyst_question.strip()
    cohort = cohort_configuration()
    if cohort:
        _reserve_cohort(conn,cohort,0)
        snap["cohort"] = cohort
    if old and prior:
        cited = {c["source_id"] for x in prior.get("items", []) for c in x.get("citations", [])}
        snap["previous_cited_article_ids"] = [s["article_id"] for s in old["sources"]
                                              if s["id"] in cited]
    contract.context(snap)  # Fail before reserving or admitting a paid request.
    conn.execute("""INSERT INTO event_source_report_runs(run_id,event_id,request_key,trigger_kind,
      snapshot_json,source_version,generator_version,predecessor,status,budget_tokens,created_at)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'queued',%s,%s)""",
      (run_id,event_id,key,trigger,contract.encode(snap),snap["source_version"],generation,
       predecessor,budget_tokens,utc_now_iso()))
    from datetime import datetime, timezone, timedelta
    not_before = (datetime.now(timezone.utc)+timedelta(seconds=debounce_seconds)).isoformat()
    job_id = enqueue_job(conn, JOB_TYPE, {"run_id": run_id}, queue_name="openai", priority=-10,
                         max_attempts=1, dedupe_key="source-report:"+key,
                         commit=False)
    conn.execute("UPDATE jobs SET available_at=%s WHERE id=%s", (not_before, job_id))
    conn.execute("UPDATE event_source_report_runs SET correction_enabled=%s WHERE run_id=%s",(allow_correction,run_id))
    conn.commit()
    return {"run_id": run_id, "job_id": job_id, "status": "queued"}


def _load(conn, run_id):
    row = conn.execute("""SELECT snapshot_json,source_version,generator_version,status,budget_tokens,
        charged_tokens,reserved_tokens,event_id,predecessor,report_json,review_json,spans_json
        FROM event_source_report_runs WHERE run_id=%s""", (run_id,)).fetchone()
    if not row:
        raise ValueError("event_source_report_missing")
    names = ("snapshot", "source_version", "generator_version", "status", "budget_tokens",
             "charged_tokens", "reserved_tokens", "event_id", "predecessor", "report", "review", "spans")
    result = dict(zip(names, row))
    for name in ("snapshot", "report", "review", "spans"):
        result[name] = json.loads(result[name]) if isinstance(result[name], str) else result[name]
    return result


def _fresh(conn, record):
    if snapshot(conn, record["event_id"])["source_version"] != record["source_version"]:
        raise ValueError("event_source_report_sources_changed")
    if previous(conn, record["event_id"])[0] != record["predecessor"]:
        raise ValueError("event_source_report_predecessor_changed")
    if configuration(conn)[2] != record["generator_version"]:
        raise ValueError("event_source_report_configuration_changed")


def _complete(conn, payload):
    import time
    from .services.ai_service import load_provider_secret
    from .llm.router import _http_request, _auth_headers, _join_url
    _, provider, _ = configuration(conn)
    started = time.monotonic()
    response = _http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
        _auth_headers(provider["type"], load_provider_secret(conn, provider["id"])),
        payload, provider, context={"stage": JOB_TYPE,"no_retry":True})
    response["transport_elapsed_ms"] = int((time.monotonic()-started)*1000)
    return response


def call(conn, run_id, phase, system, data, response_schema, *, complete=None):
    record = _load(conn, run_id); _fresh(conn, record)
    model, _, _ = configuration(conn)
    payload = {"model": model["model_name"], "reasoning_effort": "low",
        "max_completion_tokens": 3200 if phase in {"writer", "correction"} else 1600,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": contract.encode(data)}],
        "response_format": {"type": "json_schema", "json_schema":
            {"name": "event_source_report", "strict": True, "schema": response_schema}}}
    reservation = contract.tokens(contract.encode(payload)) + 512 + payload["max_completion_tokens"]
    _reserve_cohort(conn,record["snapshot"].get("cohort"),reservation)
    conn.execute("SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE", (run_id,))
    ordinal = conn.execute("SELECT count(*) FROM event_source_report_calls WHERE run_id=%s", (run_id,)).fetchone()[0]+1
    if ordinal > 4 or record["charged_tokens"]+record["reserved_tokens"]+reservation > record["budget_tokens"]:
        raise ValueError("event_source_report_budget_exhausted")
    conn.execute("UPDATE event_source_report_runs SET reserved_tokens=reserved_tokens+%s WHERE run_id=%s",
                 (reservation,run_id))
    conn.execute("""INSERT INTO event_source_report_calls(run_id,ordinal,phase,request_json,status,
       reservation,created_at) VALUES(%s,%s,%s,%s,'started',%s,%s)""",
       (run_id,ordinal,phase,contract.encode(payload),reservation,utc_now_iso()))
    conn.commit()  # Reserve attempt BEFORE sending; interrupted requests cannot replay.
    try:
        response = (complete or (lambda p: _complete(conn,p)))(payload)
    except Exception as exc:
        conn.execute("UPDATE event_source_report_calls SET status='failed',error=%s WHERE run_id=%s AND ordinal=%s",
                     (type(exc).__name__,run_id,ordinal)); conn.commit()
        raise
    usage = response.get("usage") or {}
    actual = usage.get("total_tokens", reservation)
    if type(actual) is not int or actual < 0:
        actual = reservation
    # Persist response even on unexpected provider overrun; retain reservation and
    # hold instead of losing the very evidence needed to account for that overrun.
    conn.execute("""UPDATE event_source_report_calls SET status='completed',response_json=%s,
       usage_json=%s WHERE run_id=%s AND ordinal=%s""",
       (contract.encode(response),contract.encode(usage),run_id,ordinal))
    if actual <= reservation:
        conn.execute("""UPDATE event_source_report_runs SET charged_tokens=charged_tokens+%s,
          reserved_tokens=reserved_tokens-%s WHERE run_id=%s""", (actual,reservation,run_id))
    conn.commit()
    if actual > reservation:
        raise ValueError("event_source_report_provider_budget_overrun")
    choice = response.get("choices", [{}])[0]
    if choice.get("finish_reason") != "stop" or choice.get("message", {}).get("refusal"):
        raise ValueError("event_source_report_incomplete_response")
    return json.loads(choice["message"]["content"])


def correct_and_verify(conn, run_id, packet, report, review, *, complete=None):
    """Exactly one scoped correction and one independent whole-report verification."""
    ids = [s["id"] for s in packet["sources"]]
    flagged = {x["item_id"] for x in review["issues"]}
    revised = call(conn,run_id,"correction",contract.WRITER,
        {"evidence":packet,"report":report,"substantive_issues":review["issues"],
         "instruction":"Change only flagged item IDs. Preserve title, kind and all other items exactly."},
        contract.generation_schema(ids,packet),complete=complete)
    before, after = {x["id"]:x for x in report["items"]},{x["id"]:x for x in revised["items"]}
    if (set(before)!=set(after) or revised["title"]!=report["title"] or revised["kind"]!=report["kind"]
        or any(before[k]!=after[k] for k in before.keys()-flagged)):
        raise ValueError("event_source_report_correction_scope_changed")
    spans = contract.validate(revised,packet)
    conn.execute("UPDATE event_source_report_runs SET report_json=%s,spans_json=%s WHERE run_id=%s",
                 (contract.encode(revised),contract.encode(spans),run_id));conn.commit()
    verification = call(conn,run_id,"verification",contract.REVIEWER,
        {"evidence":packet,"report":revised,"previous_issues":review["issues"]},
        contract.review_schema(revised,ids),complete=complete)
    contract.validate_review(verification,revised,packet)
    return revised, verification


def run(conn, job, *, complete=None):
    if not enabled() or job.job_type != JOB_TYPE or job.queue_name != "openai" or job.status != "running":
        raise PermissionError("event_source_report_job_not_authorized")
    if set(job.payload or {}) != {"run_id"} or job.max_attempts != 1:
        raise ValueError("event_source_report_job_invalid")
    run_id = job.payload["run_id"]
    conn.execute("SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE",(run_id,))
    record = _load(conn,run_id)
    check_scope(record["event_id"])
    if record["status"] != "queued":
        if record["status"] == "running":
            conn.execute("UPDATE event_source_report_runs SET status='held',reason='interrupted_request_not_replayed' WHERE run_id=%s",(run_id,))
            record["status"]="held"
        conn.commit()
        return {"run_id": run_id, "status": record["status"], "reused": True}
    try:
        _fresh(conn,record)
        conn.execute("UPDATE event_source_report_runs SET status='running' WHERE run_id=%s",(run_id,));conn.commit()
        packet = contract.context(record["snapshot"])
        ids = [s["id"] for s in packet["sources"]]
        report = call(conn,run_id,"writer",contract.WRITER,packet,contract.generation_schema(ids,packet),complete=complete)
        spans = contract.validate(report,packet)
        conn.execute("UPDATE event_source_report_runs SET report_json=%s,spans_json=%s WHERE run_id=%s",
                     (contract.encode(report),contract.encode(spans),run_id));conn.commit()
        review = call(conn,run_id,"review",contract.REVIEWER,{"evidence":packet,"report":report},
                      contract.review_schema(report,ids),complete=complete)
        contract.validate_review(review,report,packet)
        conn.execute("UPDATE event_source_report_runs SET review_json=%s WHERE run_id=%s",
                     (contract.encode(review),run_id));conn.commit()
        correction = conn.execute("SELECT correction_enabled FROM event_source_report_runs WHERE run_id=%s",(run_id,)).fetchone()[0]
        if not review["ready"] and correction:
            report, review = correct_and_verify(conn,run_id,packet,report,review,complete=complete)
        _fresh(conn,record)
        status = "accepted" if review["ready"] else "held"
        conn.execute("UPDATE event_source_report_runs SET status=%s,review_json=%s,reason=%s WHERE run_id=%s",
                     (status,contract.encode(review),None if review["ready"] else "substantive_review_issues",run_id))
        conn.commit()
        return {"run_id":run_id,"status":status,"public_eligible":False}
    except Exception as exc:
        conn.rollback()
        conn.execute("UPDATE event_source_report_runs SET status='held',reason=%s WHERE run_id=%s",
                     (str(exc)[:300],run_id));conn.commit()
        return {"run_id":run_id,"status":"held","reason":str(exc)[:300],"public_eligible":False}


def tick(conn):
    """Explicitly enrolled Events only; one admission per scheduler tick."""
    if not enabled():
        return []
    from .storage import get_setting
    approved = get_setting(conn,"event.source_report.approved",[])
    if not isinstance(approved,list) or len(approved)>100 or any(not isinstance(x,str) for x in approved):
        raise ValueError("event_source_report_approval_list_invalid")
    for run_id in approved:
        record = _load(conn,run_id)
        if record["status"] != "accepted":
            continue
        # Explicit approval is still subjected to immutable artifact, freshness,
        # separate admission/promotion authority and predecessor gates.
        from .event_source_report_publication import submit as publish
        if previous(conn,record["event_id"])[0] != record["predecessor"]:
            continue
        receipt = publish(conn,run_id)
        if receipt["status"] == "queued":
            return [receipt]
    enrolled = get_setting(conn,"event.source_report.enrolled",[])
    if not isinstance(enrolled,list) or len(enrolled)>100:
        raise ValueError("event_source_report_enrollment_invalid")
    if enrolled and cohort_configuration() is None:
        raise ValueError("event_source_report_scheduler_cohort_required")
    for event_id in enrolled:
        if not isinstance(event_id,str):
            raise ValueError("event_source_report_enrollment_invalid")
        # Suppress concurrent evidence/config requests even when articles arrive
        # during debounce. A stale queued snapshot holds before spending any tokens.
        pending = conn.execute("SELECT 1 FROM event_source_report_runs WHERE event_id=%s AND status IN ('queued','running') LIMIT 1",
                               (event_id,)).fetchone()
        if pending: continue
        result = submit(conn,event_id)
        if result["status"]=="queued": return [result]
    return []
