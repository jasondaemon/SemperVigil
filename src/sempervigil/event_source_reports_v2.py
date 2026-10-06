"""Bounded, event-centered writer/reviewer jobs; default-disabled admission."""
import json
import os
import re

from . import event_report_contract_v2 as contract
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
CREATE TABLE IF NOT EXISTS event_source_report_recoveries (
 parent_run_id TEXT PRIMARY KEY REFERENCES event_source_report_runs(run_id),
 child_run_id TEXT NOT NULL UNIQUE REFERENCES event_source_report_runs(run_id),
 audit_json TEXT NOT NULL, released_tokens INTEGER NOT NULL CHECK(released_tokens>0),
 recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_source_report_allowances (
 run_id TEXT PRIMARY KEY REFERENCES event_source_report_runs(run_id),
 tokens INTEGER NOT NULL CHECK(tokens BETWEEN 1 AND 200000),
 audit_json TEXT NOT NULL, recorded_at TEXT NOT NULL
);
CREATE OR REPLACE FUNCTION source_report_artifact_guard() RETURNS trigger AS $$
BEGIN
 IF TG_TABLE_NAME IN ('event_source_report_derivatives','event_source_report_recoveries','event_source_report_allowances') THEN
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
DROP TRIGGER IF EXISTS source_report_recovery_guard ON event_source_report_recoveries;
CREATE TRIGGER source_report_recovery_guard BEFORE UPDATE OR DELETE ON event_source_report_recoveries
 FOR EACH ROW EXECUTE FUNCTION source_report_artifact_guard();
DROP TRIGGER IF EXISTS source_report_allowance_guard ON event_source_report_allowances;
CREATE TRIGGER source_report_allowance_guard BEFORE UPDATE OR DELETE ON event_source_report_allowances
 FOR EACH ROW EXECUTE FUNCTION source_report_artifact_guard();
"""


def enabled():
    from .attack_catalog_runtime import scope
    return bool(scope())


def check_scope(event_id):
    from .attack_catalog_runtime import selected
    if not selected(event_id):raise PermissionError('event_report_v2_outside_scope')


def phase_settings():
    """Explicit bounded per-phase request options; model selection stays separate."""
    defaults={phase:{'reasoning_effort':'low','max_completion_tokens':3200 if phase in {'writer','correction'} else 1600}
              for phase in ('writer','review','correction','verification')}
    overrides=json.loads(os.environ.get('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG','{}'))
    if not isinstance(overrides,dict) or set(overrides)-set(defaults):
        raise ValueError('event_source_report_phase_config_invalid')
    for phase,values in overrides.items():
        if not isinstance(values,dict) or set(values)-{'reasoning_effort','max_completion_tokens'}:
            raise ValueError('event_source_report_phase_config_invalid')
        defaults[phase].update(values)
    for values in defaults.values():
        if (not isinstance(values['reasoning_effort'],str)
                or values['reasoning_effort'] not in {'none','low','medium','high','xhigh','max'}
                or type(values['max_completion_tokens']) is not int
                or not 1<=values['max_completion_tokens']<=128000):
            raise ValueError('event_source_report_phase_config_invalid')
    from .event_report_final_editor import configuration as editor_configuration
    editor = editor_configuration()
    if editor:
        defaults['review'] = {k: editor[k] for k in ('reasoning_effort', 'max_completion_tokens')}
    return defaults


def reviewer_model():
    from .event_report_final_editor import configuration as editor_configuration
    editor = editor_configuration()
    return editor['model'] if editor else MODEL


def configuration(conn):
    from .event_source_report_pilot import policy
    pilot=None
    from .services.ai_service import get_model, get_provider
    row = conn.execute("""SELECT p.id,m.id FROM llm_providers p JOIN llm_models m ON m.provider_id=p.id
      WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
      AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s ORDER BY m.id LIMIT 1""",
      (os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL') or MODEL,)).fetchone()
    if not row:
        raise ValueError("event_source_report_model_missing")
    provider, model = get_provider(conn, row[0]), get_model(conn, row[1])
    from .attack_catalog_runtime import settings
    from .attack_catalog import generation_schema
    from .event_report_final_editor import configuration as editor_configuration, PROMPT as editor_prompt, invocation_policy
    editor = editor_configuration()
    from .event_report_transport import policy as transport_policy
    code_identity = runtime_code_identity()
    version = _version({"attack": {'enabled': False} if editor else settings(),"code_identity":code_identity,"workflow": contract.WORKFLOW, "model": model["id"],
        "provider": provider["id"], "base_url": provider["base_url"],
        "writer": contract.WRITER if editor else contract.WRITER+contract.ATTACK_WRITER,
        "reviewer": editor_prompt if editor else contract.REVIEWER+contract.ATTACK_REVIEWER,
        "schema": contract.generation_schema(['S1'], {}) if editor else generation_schema(["S1"],["T1110.003"],{},contract_override=contract), "phase_settings":phase_settings(),
        "tokenizer": "o200k_base", "context_tokens": 24000,
        "update_context": "published-evidence-delta-v4-membership-baseline", "cohort_policy":"serialized-reservation-v1",
        "projection_policy":contract.PROJECTION_WORKFLOW,
        "fixed_reviewer_model":reviewer_model(),"review_schema":contract.review_schema({'items':[{'id':'P01'}]},['S1']),
        **({'pilot_policy':pilot} if pilot else {}),
        **({'final_editor': editor, 'final_editor_model_policy': invocation_policy(),
           'transport_policy': transport_policy()} if editor else {})})
    return model, provider, version


def runtime_code_identity():
    from pathlib import Path
    import hashlib
    return {name: hashlib.sha256((Path(__file__).parent/name).read_bytes()).hexdigest()
            for name in ('event_source_reports_v2.py', 'event_report_contract_v2.py',
                         'attack_catalog.py', 'attack_catalog_runtime.py',
                         'event_source_report_publication_v2.py', 'event_report_continuation_import.py',
                         'event_report_final_editor.py', 'event_report_transport.py', 'llm/router.py',
                         'event_report_generation_identity.py', 'data/event_report_v2_prompt_history.json',
                         'event_report_editorial.py', 'event_report_v2_policy.py', 'event_report_v2_integrity.py')}


def runtime_identity():
    """Code/catalog/options proof readable by the separate publication role."""
    from .attack_catalog_runtime import settings
    from .event_report_final_editor import configuration as editor_configuration, invocation_policy
    from .event_report_transport import policy as transport_policy
    return _version({'code': runtime_code_identity(), 'attack': {'enabled': False} if editor_configuration() else settings(),
                     'phase_settings': phase_settings(),
                     'writer_model': os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL') or MODEL,
                     'reviewer_model': reviewer_model(),
                     **({'final_editor': editor_configuration(), 'final_editor_model_policy': invocation_policy(),
                        'transport_policy': transport_policy()}
                        if editor_configuration() else {})})


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
    return _source_material(event_id, row[1], articles, ids)


def _source_material(event_id, title, articles, ids):
    """Freeze every membership; deduplicate only the bodies packed for the model."""
    if len(ids) != len(set(ids)) or {r[0] for r in articles} != set(ids):
        raise ValueError("event_source_report_sources_missing")
    sources, excluded, membership, bodies = [], [], [], {}
    for aid, article_title, url, text, published, retrieved, rawmeta in sorted(articles, key=lambda r: r[0]):
        meta = json.loads(rawmeta) if isinstance(rawmeta, str) and rawmeta else (rawmeta or {})
        membership.append({"article_id": aid, "title": article_title, "url": url,
                           "text_hash": contract.digest(text or ""), "suppressed": bool(meta.get("suppressed")),
                           "published_at": str(published) if published else None,
                           "retrieved_at": str(retrieved) if retrieved else None})
        if meta.get("suppressed") or not text or not url:
            excluded.append(aid)
            continue
        from urllib.parse import urlparse
        if urlparse(url).scheme not in {"http", "https"}:
            raise ValueError("event_source_report_url_invalid")
        content_hash = contract.digest(" ".join(text.split()))
        if content_hash in bodies:
            bodies[content_hash]["duplicates"].append(dict(membership[-1]))
            continue
        source = {"id": "S"+str(aid), "article_id": aid, "title": article_title or "Source article",
                  "url": url, "text": text, "content_hash": content_hash,
                  "published_at": str(published) if published else None,
                  "retrieved_at": str(retrieved) if retrieved else None, "duplicates": []}
        sources.append(source); bodies[content_hash] = source
    if not sources or len(articles) != len(ids):
        raise ValueError("event_source_report_sources_missing")
    material = {"event_id": event_id, "title": title, "sources": sources,
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


def require_qualified_predecessor(conn, event_id, expected):
    """A source baseline is not permission to revive withdrawn publication.

    Use the same native material checks as export for supported report workflows.
    No held run, old fact ledger, or continuity prose becomes fresh evidence.
    """
    row = conn.execute("""SELECT p.revision_id,r.bundle_json,r.qualification_id,
        q.qualification_json,q.revoked_at FROM event_public_pointers p
        JOIN event_public_revisions r USING(event_id,revision_id)
        JOIN event_quote_qualifications q USING(event_id,qualification_id)
        WHERE p.event_id=%s""", (event_id,)).fetchone()
    if not row or row[0] != expected or row[4] is not None:
        raise ValueError('event_report_v2_qualified_predecessor_required')
    bundle=json.loads(row[1]) if isinstance(row[1],str) else row[1]
    qualification=json.loads(row[3]) if isinstance(row[3],str) else row[3]
    if _version(qualification)!=row[2] or bundle.get('qualification')!=qualification:
        raise ValueError('event_report_v2_predecessor_integrity')
    from .event_render import resolve
    resolve(bundle,event_id=event_id,expected_revision=expected)
    try:
        if bundle.get('workflow')=='event-source-report-public-v2':
            from .event_source_report_publication_v2 import published_material,bundle_for
            material=published_material(conn,bundle)
            valid=bundle_for(material,bundle['run_id'],qualification,bundle['predecessor'])==bundle
        elif bundle.get('workflow')=='event-source-report-public-v1':
            from .event_source_report_publication import current_material,bundle_for
            material=current_material(conn,bundle['run_id'],published=True)
            valid=bundle_for(material,bundle['run_id'],qualification,bundle['predecessor'])==bundle
        elif bundle.get('workflow')=='event-composition-public-revision-v1':
            from .event_composition_publication import current_material
            material=current_material(conn,bundle['composition_id'],event_id=event_id,allow_current_public_superseded=True)
            valid=all(material[k]==bundle[k] for k in ('ledger_revision_id','ledger_record','composition','sources'))
        else:
            valid=False
    except ValueError as exc:
        raise ValueError('event_report_v2_qualified_predecessor_required') from exc
    if not valid:
        raise ValueError('event_report_v2_qualified_predecessor_required')


def meaningful_change(old, new):
    if "membership" in old and "membership" in new:
        prior_members = {m["article_id"]: m for m in old["membership"]}
        current_members = {m["article_id"]: m for m in new["membership"]}
        if prior_members.keys() - current_members.keys():
            return True
        for aid in prior_members.keys() & current_members.keys():
            before, after = prior_members[aid], current_members[aid]
            if any(before[k] != after.get(k) for k in
                   ("title", "url", "text_hash", "suppressed", "published_at") if k in before):
                return True
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
        if bundle.get("workflow") in {contract.PUBLIC_WORKFLOW,"event-source-report-public-v1"}:
            from .event_source_report_publication_v2 import validate_bundle
            if bundle["workflow"]=="event-source-report-public-v1":
                from .event_source_report_publication import validate_bundle
            validate_bundle(bundle,event_id=event_id,expected_revision=row[0])
            raw = _load(conn,bundle["run_id"])
            if raw["event_id"] != event_id or raw["source_version"] != bundle["qualification"]["source_version"]:
                raise ValueError("event_source_report_baseline_integrity")
            return raw["snapshot"], raw["generator_version"]
        from .article_evidence import source_for
        bindings = bundle.get("sources", [])
        if type(bindings) is not list or any(type(source) is not dict for source in bindings):
            return None, None
        ids = [source.get("article_id") for source in bindings]
        if (not ids or any(type(aid) is not int or aid <= 0 for aid in ids)
                or len(ids) != len(set(ids))):
            return None, None
        # A canonical packed body is not a source membership. In particular, an
        # original member may now be an alias of a newly linked lower-ID source.
        available = {m["article_id"] for m in current["membership"]}
        if not set(ids) <= available or set(ids) & set(current["excluded_article_ids"]):
            return None, None
        articles = conn.execute("""SELECT id,title,original_url,content_text,published_at,ingested_at,meta_json
          FROM articles WHERE id=ANY(%s) ORDER BY id""", (ids,)).fetchall()
        by_id = {a[0]: a for a in articles}
        retained_evidence = []
        for binding in bindings:
            aid = binding["article_id"]
            evidence = conn.execute("SELECT article_id,source_version FROM article_evidence_revisions WHERE revision_id=%s",
                                    (binding.get("evidence_revision_id"),)).fetchone()
            article = by_id.get(aid)
            if (not article or not evidence
                    or any(binding[k] != article[i] for k, i in (("title", 1), ("url", 2)) if k in binding)):
                return None, None
            try:
                version = source_for({"id": aid, "title": article[1], "content_text": article[3]})["source_version"]
            except ValueError:
                return None, None
            if evidence != (aid, version):
                return None, None
            retained_evidence.append({**binding, "source_version": version})
        try:
            baseline = _source_material(event_id, current["title"], articles, ids)
        except ValueError:
            return None, None
        if baseline["excluded_article_ids"]:
            return None, None
        # Keep immutable identities for all original members, even body aliases.
        return {**baseline, "legacy_evidence": sorted(retained_evidence, key=lambda b: b["article_id"])}, None
    latest = conn.execute("""SELECT snapshot_json,generator_version FROM event_source_report_runs
        WHERE event_id=%s AND status='accepted' ORDER BY created_at DESC LIMIT 1""", (event_id,)).fetchone()
    return (json.loads(latest[0]),latest[1]) if latest else (None,None)


def cohort_configuration():
    # Never inherit the legacy pilot's allowance or reconfigure its live policy.
    cohort = os.environ.get("SV_EVENT_REPORT_V2_COHORT_ID", "").strip()
    limit = os.environ.get("SV_EVENT_REPORT_V2_COHORT_TOKENS", "").strip()
    if not cohort and not limit:
        return None
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}",cohort) or not limit.isdecimal() or not 1 <= int(limit) <= 2000000:
        raise ValueError("event_report_v2_cohort_invalid")
    return {"id":cohort,"limit":int(limit)}


def _reserve_cohort(conn, cohort, reservation):
    if not cohort:
        return
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",("source-report-cohort:"+cohort["id"],))
    rows = conn.execute("""SELECT charged_tokens,reserved_tokens,snapshot_json::jsonb->'cohort'->>'limit'
       FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'=%s""",(cohort["id"],)).fetchall()
    if any(int(row[2]) != cohort["limit"] for row in rows):
        raise ValueError("event_source_report_cohort_limit_conflict")
    # Separately authorized correction/verification spending is not charged
    # against the immutable original writer/reviewer cohort allowance.
    extra = conn.execute("""SELECT COALESCE(SUM(CASE WHEN c.status='completed'
       AND (c.usage_json::jsonb->>'total_tokens') ~ '^[0-9]+$'
       THEN LEAST((c.usage_json::jsonb->>'total_tokens')::bigint,c.reservation) ELSE c.reservation END),0)
       FROM event_source_report_calls c JOIN event_source_report_allowances a USING(run_id)
       JOIN event_source_report_runs r USING(run_id)
       WHERE c.phase IN ('correction','verification') AND r.snapshot_json::jsonb->'cohort'->>'id'=%s""",
       (cohort["id"],)).fetchone()[0]
    if sum(row[0]+row[1] for row in rows)-extra+reservation > cohort["limit"]:
        raise ValueError("event_source_report_cohort_exhausted")


def submit(conn, event_id, *, trigger="evidence_change", budget_tokens=24000,
           debounce_seconds=300, allow_correction=False, analyst_question=None):
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    check_scope(event_id)
    if os.environ.get("SV_EVENT_REPORT_V2_GENERATION_ENABLED","0")!="1":
        raise PermissionError("event_report_v2_generation_disabled")
    if allow_correction:raise ValueError("event_report_v2_correction_disabled")
    from .event_source_report_pilot import policy,admit
    pilot=None
    from .event_report_v2_policy import policy as autonomous_policy, admit as autonomous_admit
    autonomous = autonomous_policy()
    if autonomous:
        if trigger != "evidence_change" or analyst_question is not None:
            raise ValueError("event_report_v2_successor_required")
        budget_tokens = autonomous["run_tokens"]
        debounce_seconds = autonomous["debounce_seconds"]
    if pilot:
        if allow_correction or analyst_question is not None:
            raise ValueError('event_source_report_pilot_repair_disabled')
        phases=phase_settings()
        if (os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL')!='gpt-5.6-sol'
            or phases['writer']!={'reasoning_effort':'none','max_completion_tokens':6000}
            or phases['review']!={'reasoning_effort':'low','max_completion_tokens':2400}):
            raise ValueError('event_source_report_pilot_models_invalid')
        budget_tokens=pilot['run_tokens']
    if (trigger not in {"evidence_change", "generator_upgrade"} or not 1 <= budget_tokens <= 200000
            or type(allow_correction) is not bool):
        raise ValueError("event_source_report_admission_invalid")
    if not 0 <= debounce_seconds <= 86400:
        raise ValueError("event_source_report_debounce_invalid")
    if analyst_question is not None and (not isinstance(analyst_question,str)
            or not 1 <= len(analyst_question.strip()) <= 800):
        raise ValueError("event_source_report_analyst_question_invalid")
    model, _, generation = configuration(conn)
    from .event_report_final_editor import configuration as editor_configuration, invocation_policy, SOURCE_CHECKING_PASSES
    editor = editor_configuration()
    if editor:
        if pilot or allow_correction:
            raise ValueError('event_final_editor_legacy_repair_incompatible')
        if model['model_name'] == editor['model'] and invocation_policy() != SOURCE_CHECKING_PASSES:
            raise ValueError('event_report_v2_independent_models_required')
        row = conn.execute('''SELECT m.id FROM llm_models m JOIN llm_providers p ON p.id=m.provider_id
            WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
              AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s ORDER BY m.id LIMIT 1''', (editor['model'],)).fetchone()
        if not row:
            raise ValueError('event_source_report_reviewer_model_missing')
    if autonomous:
        if (not os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL')
            or (model['model_name'] == reviewer_model() and not (editor and invocation_policy() == SOURCE_CHECKING_PASSES))):
            raise ValueError('event_report_v2_independent_models_required')
        reviewer = conn.execute('''SELECT m.id FROM llm_models m JOIN llm_providers p ON p.id=m.provider_id
            WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
              AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s ORDER BY m.id LIMIT 1''', (reviewer_model(),)).fetchone()
        if not reviewer:
            raise ValueError('event_source_report_reviewer_model_missing')
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("source-report:"+event_id,))
    snap = snapshot(conn, event_id)
    snap['review_contract']=contract.REVIEW_CONTRACT
    snap['report_contract']=contract.WORKFLOW
    if editor:
        snap['final_editor'] = editor
        snap['final_editor_model_policy'] = invocation_policy()
        from .event_report_transport import policy as transport_policy
        snap['transport_policy'] = transport_policy()
    from .event_report_generation_identity import identity as prompt_identity
    from .event_report_final_editor import PROMPT as editor_prompt
    snap['generation_prompt_identity'] = prompt_identity(
        contract.WRITER if editor else contract.WRITER + contract.ATTACK_WRITER,
        editor_prompt if editor else contract.REVIEWER + contract.ATTACK_REVIEWER, generation)
    predecessor, prior = previous(conn, event_id)
    old, prior_generation = published_baseline(conn,event_id,snap)
    if pilot:
        from .event_source_report_pilot import active
        active(pilot)
        if event_id not in pilot['events'] or trigger!='evidence_change' or not predecessor or old is None:
            raise ValueError('event_source_report_pilot_successor_required')
    if old:
        if trigger == "evidence_change" and not meaningful_change(old, snap):
            conn.commit(); return {"status": "unchanged", "reason": "no_meaningful_evidence_change"}
        if trigger == "generator_upgrade" and prior_generation == generation:
            conn.commit(); return {"status": "unchanged", "reason": "generator_current"}
    # Identical snapshot/config requests, including held attempts, never regenerate.
    key = _version({"event_id": event_id, "evidence": snap["evidence_version"],
                    "source_version": snap["source_version"], "generator": generation,
                    "analyst_question":analyst_question,
                    **({"autonomous_policy": _version(autonomous)} if autonomous else {})})
    run_id = "esr_"+key
    existing = conn.execute("SELECT status FROM event_source_report_runs WHERE run_id=%s", (run_id,)).fetchone()
    if existing:
        conn.commit(); return {"run_id": run_id, "status": existing[0], "reused": True}
    snap.update(previous_report=prior, previous_evidence_hashes=[] if not old else
                [s["content_hash"] for s in old["sources"]],
                previous_cited_article_ids=[])
    if editor and prior:
        from .event_report_final_editor import narrative
        snap['previous_report'] = narrative(prior)
    snap = contract.update_context(snap,trigger,old)
    if analyst_question:
        snap["analyst_question"] = analyst_question.strip()
    from .attack_catalog_runtime import reference
    if not editor:
        snap["attack_reference"]=reference(snap["sources"])
    if autonomous:
        autonomous_admit(conn, autonomous, event_id, generation, predecessor, old)
        snap["autonomous_policy"] = autonomous
        snap["autonomous_runtime_version"] = runtime_identity()
    cohort = ({"id": autonomous["id"], "limit": autonomous["limit"]} if autonomous else None)
    if cohort:
        _reserve_cohort(conn,cohort,0)
        snap["cohort"] = cohort
    if pilot:
        admit(conn,pilot,event_id,predecessor,old,trigger)
        snap['pilot']=pilot
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
    result = {"run_id": run_id, **dict(zip(names, row))}
    for name in ("snapshot", "report", "review", "spans"):
        result[name] = json.loads(result[name]) if isinstance(result[name], str) else result[name]
    return result


def _fresh(conn, record):
    if "autonomous_policy" in record["snapshot"]:
        from .event_report_v2_policy import check_run
        check_run(conn, record, record["run_id"])
        from .attack_catalog_runtime import settings
        if not record['snapshot'].get('final_editor') and settings()['catalog'] != record['snapshot']['attack_reference']['catalog']:
            raise ValueError('event_report_v2_catalog_changed')
    from .event_source_report_pilot import policy,active
    pilot=record['snapshot'].get('pilot')
    if pilot:
        if pilot!=policy():raise ValueError('event_source_report_pilot_policy_changed')
        active(pilot)
    if snapshot(conn, record["event_id"])["source_version"] != record["snapshot"].get("publication_freshness_source_version",record["source_version"]):
        raise ValueError("event_source_report_sources_changed")
    if previous(conn, record["event_id"])[0] != record["predecessor"]:
        raise ValueError("event_source_report_predecessor_changed")
    if "autonomous_policy" in record["snapshot"]:
        require_qualified_predecessor(conn,record["event_id"],record["predecessor"])
    if configuration(conn)[2] != record["snapshot"].get("runtime_generation_at_import",record["generator_version"]):
        raise ValueError("event_source_report_configuration_changed")


class PreTransportFailure(ValueError):
    """Only the local readiness boundary may assert that no HTTP was attempted."""
    def __init__(self):
        super().__init__("event_source_report_credentials_not_ready")
        self.proof = {"workflow":"event-source-report-pretransport-proof-v1",
                      "kind":"instrumented_readiness","http_attempted":False,
                      "failure_stage":"provider_credentials"}


def ready_client(conn):
    """Resolve credentials before any paid request; never expose headers in receipts."""
    from .services.ai_service import load_provider_secret
    from .llm.router import _auth_headers
    _, provider, _ = configuration(conn)
    try:
        secret = load_provider_secret(conn,provider["id"])
        if not isinstance(secret,str) or not secret.strip():
            raise ValueError("Missing provider secret")
        headers = _auth_headers(provider["type"],secret)
    except Exception as exc:
        raise PreTransportFailure() from exc
    return provider,headers


def _complete(conn, payload, *, before_transport=None, transport_seconds=None):
    import time
    from .llm.router import _http_request, _join_url
    provider,headers = ready_client(conn)
    authorized_until = None
    completion_until = None
    if before_transport is not None:
        window = before_transport()
        if transport_seconds is not None:
            transport_seconds = min(transport_seconds, window['seconds'])
            authorized_until = window['authorized_until']
            completion_until = window['completion_until']
    started = time.monotonic()
    if transport_seconds is not None:
        from .event_report_transport import complete as bounded_complete
        response = bounded_complete(_join_url(provider["base_url"], "/chat/completions"),
            headers, payload, provider, seconds=transport_seconds, stage=JOB_TYPE,
            authorized_until=authorized_until, completion_until=completion_until)
        response["transport_elapsed_ms"] = int((time.monotonic()-started)*1000)
        return response
    response = _http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
        headers,
        payload, provider, context={"stage": JOB_TYPE,"no_retry":True})
    response["transport_elapsed_ms"] = int((time.monotonic()-started)*1000)
    return response


def call(conn, run_id, phase, system, data, response_schema, *, complete=None, completion_cap=None):
    if completion_cap is not None and (phase!='correction' or type(completion_cap) is not int or not 1<=completion_cap<=3200):
        raise ValueError('event_source_report_completion_cap_invalid')
    record = _load(conn, run_id); _fresh(conn, record)
    if 'autonomous_policy' in record['snapshot']:
        prior_phases = [row[0] for row in conn.execute(
            'SELECT phase FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal', (run_id,)).fetchall()]
        if (phase not in {'writer', 'review'} or len(prior_phases) >= 2
            or phase != ('writer' if not prior_phases else 'review')
            or prior_phases not in ([], ['writer'])):
            raise ValueError('event_report_v2_two_call_limit')
    model, _, _ = configuration(conn)
    if phase in {'review','verification'} and (os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL') or record['snapshot'].get('final_editor')):
        from .services.ai_service import get_model
        row=conn.execute("""SELECT m.id FROM llm_models m JOIN llm_providers p ON p.id=m.provider_id
          WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
          AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s ORDER BY m.id LIMIT 1""",(reviewer_model(),)).fetchone()
        if not row:raise ValueError('event_source_report_reviewer_model_missing')
        model=get_model(conn,row[0])
    settings=phase_settings()[phase]
    allowance = conn.execute('SELECT tokens,audit_json FROM event_source_report_allowances WHERE run_id=%s',(run_id,)).fetchone()
    if allowance and phase in {'correction','verification'}:
        settings={**settings,**json.loads(allowance[1]).get('phase_options',{}).get(phase,{})}
    payload = {"model": model["model_name"], "reasoning_effort": settings['reasoning_effort'],
        "max_completion_tokens": completion_cap or settings['max_completion_tokens'],
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": contract.encode(data)}],
        "response_format": {"type": "json_schema", "json_schema":
            {"name": "event_source_report", "strict": True, "schema": response_schema}}}
    reservation = contract.tokens(contract.encode(payload)) + 512 + payload["max_completion_tokens"]
    if record['snapshot'].get('final_editor'):
        if (type(model.get('max_context')) is not int or reservation > model['max_context']):
            raise ValueError('event_final_editor_model_context_exceeded')
    transport_seconds = None
    if record['snapshot'].get('final_editor'):
        from .event_report_transport import policy as transport_policy, preflight
        pinned_transport = record['snapshot'].get('transport_policy')
        if pinned_transport != transport_policy():
            raise ValueError('event_report_transport_policy_changed')
        transport_seconds = preflight(payload, pinned_transport[
            'editor_seconds' if phase == 'review' else 'writer_seconds'])
    conn.execute("SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE", (run_id,))
    if allowance and phase in {'correction','verification'}:
        spent = conn.execute("SELECT COALESCE(SUM(reservation),0) FROM event_source_report_calls WHERE run_id=%s AND phase IN ('correction','verification')",(run_id,)).fetchone()[0]
        if spent+reservation > allowance[0]:
            raise ValueError('event_source_report_additional_allowance_exhausted')
    else:
        _reserve_cohort(conn,record['snapshot'].get('cohort'),reservation)
    if record["snapshot"].get("report_contract")==contract.WORKFLOW:
        reserved_calls=conn.execute("SELECT COALESCE(SUM(reservation),0) FROM event_source_report_calls WHERE run_id=%s",(run_id,)).fetchone()[0]
        if reserved_calls+reservation>record["budget_tokens"]:raise ValueError("event_report_v2_reservation_exhausted")
    # Locks may have waited across expiry or an evidence/authority change.
    record = _load(conn, run_id)
    _fresh(conn, record)
    if 'autonomous_policy' in record['snapshot']:
        prior_phases = [row[0] for row in conn.execute(
            'SELECT phase FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal', (run_id,)).fetchall()]
        if (phase not in {'writer', 'review'} or len(prior_phases) >= 2
            or phase != ('writer' if not prior_phases else 'review')
            or prior_phases not in ([], ['writer'])):
            raise ValueError('event_report_v2_two_call_limit')
    ordinal = conn.execute("SELECT count(*) FROM event_source_report_calls WHERE run_id=%s", (run_id,)).fetchone()[0]+1
    if record['snapshot'].get('pilot'):
        reserved_calls=conn.execute('SELECT COALESCE(SUM(reservation),0) FROM event_source_report_calls WHERE run_id=%s',(run_id,)).fetchone()[0]
        if reserved_calls+reservation>record['budget_tokens']:
            raise ValueError('event_source_report_pilot_reservation_exhausted')
    if ordinal > 2 or record["charged_tokens"]+record["reserved_tokens"]+reservation > record["budget_tokens"]:
        raise ValueError("event_source_report_budget_exhausted")
    conn.execute("UPDATE event_source_report_runs SET reserved_tokens=reserved_tokens+%s WHERE run_id=%s",
                 (reservation,run_id))
    conn.execute("""INSERT INTO event_source_report_calls(run_id,ordinal,phase,request_json,status,
       reservation,created_at) VALUES(%s,%s,%s,%s,'started',%s,%s)""",
       (run_id,ordinal,phase,contract.encode(payload),reservation,utc_now_iso()))
    conn.commit()  # Reserve attempt BEFORE sending; interrupted requests cannot replay.
    def before_transport():
        try:
            current = _load(conn, run_id)
            _fresh(conn, current)
            remaining = None
            authorized_until = None
            completion_until = None
            if transport_seconds is not None:
                from datetime import datetime, timezone
                from .event_report_transport import policy as transport_policy
                pinned = current['snapshot'].get('transport_policy')
                if pinned != transport_policy():
                    raise ValueError('event_report_transport_policy_changed')
                first = conn.execute('SELECT min(created_at) FROM event_source_report_calls WHERE run_id=%s',
                                     (run_id,)).fetchone()[0]
                origin = datetime.fromisoformat(str(first).replace('Z', '+00:00'))
                if origin.tzinfo is None:
                    origin = origin.replace(tzinfo=timezone.utc)
                completion_until = origin.timestamp() + pinned['overall_seconds']
                authorized_until = completion_until
                remaining = authorized_until - datetime.now(timezone.utc).timestamp()
                if remaining <= 0:
                    raise ValueError('event_report_transport_budget_exhausted')
                remaining = min(transport_seconds, remaining)
            if 'autonomous_policy' in current['snapshot']:
                # Freshness AND the overall-window query may cross expiry or
                # disablement. Cheap authority must be the last parent guard.
                from .event_report_v2_policy import policy, active, utc
                final_policy = policy()
                if final_policy != current['snapshot']['autonomous_policy']:
                    raise ValueError('event_report_v2_policy_changed')
                if authorized_until is not None:
                    authorized_until = min(authorized_until,
                        utc(final_policy['expires_at']).timestamp())
                active(final_policy)
            return None if remaining is None else {'seconds': remaining, 'authorized_until': authorized_until,
                'completion_until': completion_until}
        except Exception as cause:
            failure = PreTransportFailure()
            failure.proof.update(kind='instrumented_authority', failure_stage='authority_recheck')
            raise failure from cause
    try:
        before_transport()  # Journal is durable, but no transport has occurred.
        response = complete(payload) if complete else _complete(conn, payload, before_transport=before_transport,
            **({"transport_seconds": transport_seconds} if transport_seconds is not None else {}))
    except Exception as exc:
        conn.rollback()  # Journal was committed; recover from a failed guard query.
        error = type(exc).__name__
        if isinstance(exc,PreTransportFailure):
            error = contract.encode({"type":error,"proof":{**exc.proof,"request_version":_version(payload)}})
        if isinstance(exc, PreTransportFailure) and exc.proof['kind'] == 'instrumented_authority':
            # Only this local boundary proves zero HTTP. Lifetime admission and
            # call reservations stay consumed; only outstanding transport clears.
            conn.execute("UPDATE event_source_report_runs SET reserved_tokens=reserved_tokens-%s WHERE run_id=%s", (reservation, run_id))
        conn.execute("UPDATE event_source_report_calls SET status='failed',error=%s WHERE run_id=%s AND ordinal=%s",
                     (error,run_id,ordinal)); conn.commit()
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


def recover_pretransport(conn, parent_run_id, *, legacy_proof=None, debounce_seconds=300):
    """One explicitly requested, parent-linked retry of a proven zero-HTTP failure.

    Completed/unknown transport, paid calls, descendants and stale snapshots are
    ineligible. Original terminal run/job/call/journal remain intact. A separate
    immutable audit reconciles the unused reservation and links one normal job.
    No automatic scheduler or public endpoint invokes this operation.
    """
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    if not 0 <= debounce_seconds <= 86400:
        raise ValueError("event_source_report_debounce_invalid")
    parent = _load(conn,parent_run_id);check_scope(parent["event_id"])
    if parent['snapshot'].get('pilot') or parent['snapshot'].get('autonomous_policy'):raise ValueError('event_source_report_pilot_repair_disabled')
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",("source-report:"+parent["event_id"],))
    prior = conn.execute("SELECT child_run_id,audit_json FROM event_source_report_recoveries WHERE parent_run_id=%s",
                         (parent_run_id,)).fetchone()
    if prior:
        child = _load(conn,prior[0])
        job = conn.execute("SELECT id FROM jobs WHERE payload_json::jsonb->>'run_id'=%s AND job_type=%s",
                           (prior[0],JOB_TYPE)).fetchone()
        conn.commit()
        return {"run_id":prior[0],"job_id":job[0],"status":child["status"],"reused":True}
    if conn.execute("SELECT 1 FROM event_source_report_recoveries WHERE child_run_id=%s",(parent_run_id,)).fetchone():
        raise ValueError("event_source_report_recovery_already_used")
    conn.execute("SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE",(parent_run_id,))
    _fresh(conn,parent)
    if parent["status"] != "held" or parent["charged_tokens"] != 0 or any(parent[k] is not None for k in ("report","review","spans")):
        raise ValueError("event_source_report_recovery_ineligible")
    calls = conn.execute("""SELECT ordinal,phase,status,error,request_json,response_json,usage_json,reservation
       FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal""",(parent_run_id,)).fetchall()
    if len(calls)!=1 or calls[0][:3]!=(1,"writer","failed") or calls[0][5:7]!=(None,None):
        raise ValueError("event_source_report_transport_uncertain")
    call=calls[0];payload=json.loads(call[4])
    if legacy_proof is None:
        try:
            saved=json.loads(call[3]);proof=saved["proof"]
            assert saved["type"]=="PreTransportFailure" and proof["kind"]=="instrumented_readiness"
        except (ValueError,KeyError,TypeError,AssertionError):
            raise ValueError("event_source_report_pretransport_proof_required") from None
    else:
        proof=legacy_proof
        reason=conn.execute("SELECT reason FROM event_source_report_runs WHERE run_id=%s",(parent_run_id,)).fetchone()[0]
        if (call[3]!="ValueError" or proof.get("kind")!="legacy_missing_master_key"
                or proof.get("master_key_present") is not False or proof.get("failure_reproduced") is not True
                or proof.get("reason_code")!="missing_master_key"
                or reason!="Master key is not set. Set SEMPERVIGIL_MASTER_KEY (preferred) or legacy SEMPERIVGIL_MASTER_KEY."):
            raise ValueError("event_source_report_pretransport_proof_invalid")
        for key in ("complete_code_version","loader_code_version","executor_instance_version","journal_version"):
            if not re.fullmatch(r"[a-f0-9]{64}",str(proof.get(key,""))):
                raise ValueError("event_source_report_pretransport_proof_invalid")
        if not isinstance(proof.get("audit_reason"),str) or not 30<=len(proof["audit_reason"])<=2000:
            raise ValueError("event_source_report_pretransport_proof_invalid")
    if (proof.get("workflow")!="event-source-report-pretransport-proof-v1"
            or proof.get("http_attempted") is not False or proof.get("failure_stage")!="provider_credentials"
            or proof.get("request_version")!=_version(payload) or parent["reserved_tokens"]!=call[7]):
        raise ValueError("event_source_report_pretransport_proof_invalid")
    jobs=conn.execute("SELECT id,status FROM jobs WHERE job_type=%s AND payload_json::jsonb->>'run_id'=%s",
                      (JOB_TYPE,parent_run_id)).fetchall()
    if len(jobs)!=1 or jobs[0][1]!="failed":
        raise ValueError("event_source_report_recovery_job_ineligible")
    # Correct executor must be ready BEFORE reconciling/admitting anything.
    ready_client(conn)
    key=_version({"workflow":"event-source-report-pretransport-retry-v1","parent_run_id":parent_run_id})
    child="esr_"+key
    trigger=conn.execute("SELECT trigger_kind FROM event_source_report_runs WHERE run_id=%s",(parent_run_id,)).fetchone()[0]
    conn.execute("""INSERT INTO event_source_report_runs(run_id,event_id,request_key,trigger_kind,
      snapshot_json,source_version,generator_version,predecessor,status,budget_tokens,created_at)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'queued',%s,%s)""",
      (child,parent["event_id"],key,trigger,contract.encode(parent["snapshot"]),parent["source_version"],
       parent["generator_version"],parent["predecessor"],parent["budget_tokens"],utc_now_iso()))
    # No correction is admitted in a two-call recovery.
    from datetime import datetime,timezone,timedelta
    available=(datetime.now(timezone.utc)+timedelta(seconds=debounce_seconds)).isoformat()
    job=enqueue_job(conn,JOB_TYPE,{"run_id":child},queue_name="openai",priority=-10,max_attempts=1,
                    available_at=available,parent_job_id=jobs[0][0],dedupe_key="source-report:"+key,commit=False)
    audit={"workflow":"event-source-report-pretransport-recovery-v1","parent_run_id":parent_run_id,
           "child_run_id":child,"proof":proof,"released_tokens":call[7],"confirmed_provider_tokens":0,
           "parent_job_id":jobs[0][0],"child_job_id":job,"reason":"Confirmed local credential failure before HTTP; one explicit retry, not new evidence."}
    conn.execute("INSERT INTO event_source_report_recoveries VALUES(%s,%s,%s,%s,%s)",
                 (parent_run_id,child,contract.encode(audit),call[7],utc_now_iso()))
    conn.execute("UPDATE event_source_report_runs SET reserved_tokens=reserved_tokens-%s WHERE run_id=%s",(call[7],parent_run_id))
    conn.commit()
    return {"run_id":child,"job_id":job,"status":"queued","parent_run_id":parent_run_id,"released_tokens":call[7]}


def review_preserved(conn, run_id, *, complete=None, review_focus=()):
    """Operator-only single review of an immutable writer held on provenance.

    No writer replay, terminal job reset, correction or publication. A resolved
    source span grants provenance only; the full report still needs review.
    """
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    record = _load(conn, run_id)
    check_scope(record["event_id"])
    _fresh(conn, record)
    reason = conn.execute("SELECT reason FROM event_source_report_runs WHERE run_id=%s", (run_id,)).fetchone()[0]
    rows = conn.execute("SELECT phase,status,response_json,reservation FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal", (run_id,)).fetchall()
    if (record["status"] != "held" or reason != "event_report_quote_not_in_source"
            or record["reserved_tokens"] or record["review"] is not None or record["report"] is not None
            or len(rows) != 1 or rows[0][0:2] != ("writer", "completed")):
        raise ValueError("event_source_report_preserved_review_not_eligible")
    response = json.loads(rows[0][2])
    choice = response["choices"][0]
    if choice.get("finish_reason") != "stop" or choice["message"].get("refusal"):
        raise ValueError("event_source_report_incomplete_response")
    report = json.loads(choice["message"]["content"])
    packet = contract.context(record["snapshot"])
    spans = contract.validate(report, packet)
    review = call(conn, run_id, "review", contract.REVIEWER,
                  {"evidence": packet, "report": report, "citation_provenance": spans,
                   "operator_review_focus": list(review_focus)},
                  contract.review_schema(report, [s["id"] for s in packet["sources"]]), complete=complete)
    contract.validate_review(review, report, packet)
    # This diagnostic continuation never autonomously promotes a held candidate.
    conn.execute("UPDATE event_source_report_runs SET report_json=%s,spans_json=%s,review_json=%s,reason=%s WHERE run_id=%s",
                 (contract.encode(report), contract.encode(spans), contract.encode(review),
                  "substantive_review_issues" if not review["ready"] else "operator_review_required", run_id))
    conn.commit()
    return {"status": "held", "report": report, "review": review, "spans": spans}


def grant_correction_allowance(conn, run_id, tokens, *, authority, manual_issues=None, phase_options=None):
    """Audited operator grant only for the final two phases; never resets jobs."""
    if not enabled() or not authority or type(tokens) is not int or not 1<=tokens<=200000:
        raise PermissionError('event_source_report_allowance_not_authorized')
    record=_load(conn,run_id);check_scope(record['event_id']);_fresh(conn,record)
    if record['snapshot'].get('pilot') or record['snapshot'].get('autonomous_policy'):raise ValueError('event_source_report_pilot_repair_disabled')
    conn.execute('SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE',(run_id,))
    rows=conn.execute('SELECT phase,status FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(run_id,)).fetchall()
    if manual_issues:
        reason=conn.execute('SELECT reason FROM event_source_report_runs WHERE run_id=%s',(run_id,)).fetchone()[0]
        if not record['review'] or not record['review']['ready'] or not isinstance(reason,str) or not reason.startswith('manual_quality_'):
            raise ValueError('event_source_report_manual_correction_not_eligible')
        contract.validate_review({'ready':False,'issues':manual_issues,'locator_warnings':[],
                                  **({'editorial_warnings':[]} if record['snapshot'].get('review_contract')==contract.REVIEW_CONTRACT else {})},
                                 record['report'],contract.context(record['snapshot']))
    if (record['status']!='held' or record['reserved_tokens'] or not record['review']
            or (record['review']['ready'] and not manual_issues) or rows!=[('writer','completed'),('review','completed')]):
        raise ValueError('event_source_report_correction_not_eligible')
    phase_options=phase_options or {}
    if not isinstance(phase_options,dict) or set(phase_options)-{'correction','verification'}:
        raise ValueError('event_source_report_phase_config_invalid')
    for values in phase_options.values():
        if (not isinstance(values,dict) or set(values)!={'reasoning_effort','max_completion_tokens'}
                or not isinstance(values['reasoning_effort'],str)
                or values['reasoning_effort'] not in {'none','low','medium','high','xhigh','max'}
                or type(values['max_completion_tokens']) is not int or not 1<=values['max_completion_tokens']<=128000):
            raise ValueError('event_source_report_phase_config_invalid')
    audit={'authority':authority,'phases':['correction','verification'],'max_calls':4,
           'original_budget':record['budget_tokens'],'prior_charged_tokens':record['charged_tokens'],
           'report_version':_version(record['report']),'review_version':_version(record['review'])}
    audit.update(manual_issues=manual_issues or [],phase_options=phase_options,
                 final_generation_version=_version({'base_generator_version':record['generator_version'],'phase_options':phase_options}))
    prior=conn.execute('SELECT tokens,audit_json FROM event_source_report_allowances WHERE run_id=%s',(run_id,)).fetchone()
    if prior:
        saved=json.loads(prior[1])
        if (prior[0]!=tokens or saved['authority']!=authority
                or saved.get('manual_issues',[])!=(manual_issues or []) or saved.get('phase_options',{})!=phase_options):
            raise ValueError('event_source_report_allowance_conflict')
        return
    conn.execute('INSERT INTO event_source_report_allowances(run_id,tokens,audit_json,recorded_at) VALUES(%s,%s,%s,%s)',
                 (run_id,tokens,contract.encode(audit),utc_now_iso()))
    conn.execute('UPDATE event_source_report_runs SET budget_tokens=%s WHERE run_id=%s',
                 (max(record['budget_tokens'],record['charged_tokens']+tokens),run_id))
    conn.commit()


def correct_and_verify(conn, run_id, packet, report, review, *, complete=None, compact=False, field_scope=None):
    """Exactly one scoped correction and one independent whole-report verification."""
    ids = [s["id"] for s in packet["sources"]]
    flagged = {x["item_id"] for x in review["issues"]}
    if field_scope and set(field_scope)-flagged:
        raise ValueError('event_source_report_correction_scope_changed')
    if compact:
        patch = call(conn,run_id,'correction',contract.CORRECTOR,
            {'evidence':packet,'report':report,'substantive_issues':review['issues'],
             'instruction':'Correct only flagged items and associated rationale; retain all other items/title/kind exactly.'},
            contract.correction_schema(report,ids,flagged),complete=complete)
        revised = contract.apply_correction(report,patch,flagged)
    else:
        revised = call(conn,run_id,"correction",contract.WRITER,
            {"evidence":packet,"report":report,"substantive_issues":review["issues"],
             "instruction":"Change only flagged item IDs. Preserve title, kind and all other items exactly."},
            contract.generation_schema(ids,packet),complete=complete)
    before, after = {x["id"]:x for x in report["items"]},{x["id"]:x for x in revised["items"]}
    if (set(before)!=set(after) or revised["title"]!=report["title"] or revised["kind"]!=report["kind"]
        or any(before[k]!=after[k] for k in before.keys()-flagged)):
        raise ValueError("event_source_report_correction_scope_changed")
    for item,fields in (field_scope or {}).items():
        if ({k:v for k,v in before[item].items() if k not in fields}
                !={k:v for k,v in after[item].items() if k not in fields}):
            raise ValueError('event_source_report_correction_scope_changed')
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
        if record['snapshot'].get('final_editor'):
            from . import event_report_final_editor as editor
            raw = call(conn, run_id, 'writer', contract.WRITER, packet,
                       contract.generation_schema(ids, packet), complete=complete)
            spans = contract.validate(raw, packet)
            report = editor.empty_mappings(raw)
        else:
            from .attack_catalog_runtime import catalog
            from .attack_catalog import generation_schema,project_optional_mappings
            cat=catalog(packet['attack_reference']['catalog']['domain'])
            raw = call(conn,run_id,"writer",contract.WRITER+contract.ATTACK_WRITER,packet,
                       generation_schema(ids,[t['id'] for t in packet['attack_reference']['candidates']],packet,contract_override=contract),complete=complete)
            report,review_packet,spans,resolved,removed=project_optional_mappings(raw,packet,cat,contract_override=contract)
            projection={'workflow':'optional-mapping-projection-v1','input_version':_version(raw),'snapshot_version':_version(record['snapshot']),
                        'projection':{'report':report,'evidence':review_packet,'spans':spans,'removed':removed}}
        if not record['snapshot'].get('final_editor'):
            conn.execute("INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s)",
                         (run_id,contract.encode(projection),utc_now_iso()))
        conn.execute("UPDATE event_source_report_runs SET report_json=%s,spans_json=%s WHERE run_id=%s",
                     (contract.encode(report),contract.encode(spans),run_id));conn.commit()
        if record['snapshot'].get('final_editor'):
            from . import event_report_final_editor as editor
            call(conn, run_id, 'review', editor.PROMPT, editor.editor_input(packet, report, spans),
                 editor.schema(packet), complete=complete)
            rows = conn.execute('SELECT request_json,response_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal', (run_id,)).fetchall()
            final = editor.derive(*(json.loads(v) for row in rows for v in row), record['snapshot'])
            report, review, spans = final['report'], final['review'], final['spans']
            projection = {'workflow': editor.WORKFLOW, 'final': final}
            conn.execute('INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s)', (run_id, contract.encode(projection), utc_now_iso()))
            conn.execute('UPDATE event_source_report_runs SET report_json=%s,spans_json=%s WHERE run_id=%s', (contract.encode(report), contract.encode(spans), run_id))
        else:
            review = call(conn,run_id,"review",contract.REVIEWER+contract.ATTACK_REVIEWER,{"evidence":review_packet,"report":report,"citation_provenance":spans},
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


def _legacy_tick(conn):
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
        from .event_source_report_publication_v2 import submit as publish
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


def tick(conn):
    """Bounded v2 scheduling, isolated from ingestion and the legacy pilot."""
    from .event_report_v2_policy import policy, active, locked_rows
    from .storage import set_setting
    results = []
    try:
        p = policy()
        if not p:
            return []
        active(p)
        locked_rows(conn, p, lock=False)
        from .event_source_report_publication_v2 import submit as publish
        rows = conn.execute("""SELECT run_id,event_id FROM event_source_report_runs
            WHERE status='accepted' AND snapshot_json::jsonb->'cohort'->>'id'=%s
              AND snapshot_json::jsonb->>'report_contract'=%s AND event_id=ANY(%s)
            ORDER BY created_at,run_id""", (p['id'], contract.WORKFLOW, p['events'])).fetchall()
        for rid, event_id in rows:
            record = _load(conn, rid)
            if previous(conn, event_id)[0] != record['predecessor']:
                continue
            try:
                results.append(publish(conn, rid, automatic=True))
            except Exception as exc:
                conn.rollback()
                reason = safe_reason(exc)
                # Publication failure does not invalidate accepted generation.
                # A concurrent promoter may already have published this run.
                # Keep content state intact; expose the isolated admission hold
                # through the tick receipt rather than withdrawing public data.
                results.append({'run_id': rid, 'status': 'publication_held', 'reason': reason})
        for event_id in p['events']:
            try:
                receipt = submit(conn, event_id)
                results.append({'event_id': event_id, **receipt})
                if receipt['status'] == 'queued':
                    break
            except Exception as exc:
                conn.rollback()
                results.append({'event_id': event_id, 'status': 'held', 'reason': safe_reason(exc)})
    except Exception as exc:
        conn.rollback()
        results.append({'status': 'held', 'reason': safe_reason(exc)})
    try:
        set_setting(conn, 'event.report_v2.last_tick', {'checked_at': utc_now_iso(), 'results': results})
        conn.commit()
    except Exception:
        conn.rollback()
    return results


def safe_reason(value):
    match = re.match(r'event_(?:source_report|report_v2)_[a-z_]+', str(value or ''))
    return match.group(0) if match else 'unclassified_failure'
