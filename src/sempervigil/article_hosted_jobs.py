"""Production hosted briefing/classification with a local-worker fallback caller."""

import json
import time

from . import article_applicability as applicability
from . import article_hosted_analysis as contract
from .article_evidence import source_for
from .investigation import _version
from .llm.router import _auth_headers, _http_request, _join_url
from .services.ai_service import list_providers, load_provider_secret
from .storage import get_article_by_id
from .utils import utc_now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS article_hosted_analyses (
    article_id BIGINT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    source_version TEXT NOT NULL,
    generation TEXT NOT NULL,
    status TEXT NOT NULL,
    brief_json JSONB,
    applicability_json JSONB,
    usage_json JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (article_id, source_version, generation)
);
CREATE INDEX IF NOT EXISTS idx_article_hosted_analyses_regulatory
    ON article_hosted_analyses(status, created_at DESC);
"""

REGULATORY_KINDS = frozenset({
    "regulatory_proposal", "regulatory_change", "regulatory_guidance",
    "regulatory_enforcement",
})


def generation() -> str:
    return _version({"workflow": contract.WORKFLOW, "brief_model": contract.BRIEF_MODEL,
                     "brief_prompt": contract.BRIEF_PROMPT, "brief_schema": contract.brief_schema(),
                     "classifier_model": contract.CLASSIFIER_MODEL,
                     "classifier_prompt": applicability.PROMPT,
                     "classifier_schema": applicability.schema()})


def _provider(conn) -> tuple[dict, str]:
    candidates = [provider for provider in list_providers(conn)
                  if provider["is_enabled"] and provider["type"] == "openai_compatible"
                  and str(provider["base_url"]).rstrip("/") == "https://api.openai.com/v1"]
    if len(candidates) != 1:
        raise ValueError("hosted_article_official_provider_required")
    secret = load_provider_secret(conn, candidates[0]["id"])
    if not secret:
        raise ValueError("hosted_article_provider_secret_missing")
    return candidates[0], secret


def _complete(provider: dict, secret: str, model: str, prompt: str, data: dict,
              schema: dict, max_tokens: int, stage: str) -> tuple[dict, dict]:
    response = _http_request(
        "POST", _join_url(provider["base_url"], "/chat/completions"),
        _auth_headers(provider["type"], secret),
        {"model": model,
         "messages": [{"role": "system", "content": prompt},
                      {"role": "user", "content": json.dumps(data, ensure_ascii=False)}],
         "response_format": {"type": "json_schema", "json_schema": {
             "name": "article_hosted_" + stage, "strict": True, "schema": schema}},
         "reasoning_effort": "none", "max_completion_tokens": max_tokens},
        provider, context={"stage": stage, "no_retry": True},
    )
    choices = response.get("choices") or []
    if (len(choices) != 1 or choices[0].get("finish_reason") != "stop"
            or choices[0].get("message", {}).get("refusal")):
        raise ValueError("hosted_article_incomplete_or_refused")
    value = json.loads(choices[0]["message"]["content"])
    return value, response.get("usage") or {}


def _store(conn, article_id: int, source_version: str, status: str, brief: dict | None,
           classification: dict | None, usage: dict, error: str | None) -> None:
    conn.execute("""INSERT INTO article_hosted_analyses
        (article_id,source_version,generation,status,brief_json,applicability_json,usage_json,error)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(article_id,source_version,generation) DO UPDATE SET
        status=EXCLUDED.status,brief_json=EXCLUDED.brief_json,
        applicability_json=EXCLUDED.applicability_json,
        usage_json=EXCLUDED.usage_json,error=EXCLUDED.error""",
        (article_id, source_version, generation(), status,
         json.dumps(brief) if brief is not None else None,
         json.dumps(classification) if classification is not None else None,
         json.dumps(usage), error))


def analyze(conn, article_id: int) -> dict:
    from . import openai_budget

    if openai_budget.configured_cap() is None:
        raise ValueError("hosted_article_budget_guard_required")
    article = get_article_by_id(conn, article_id)
    if not article:
        raise ValueError("hosted_article_source_missing")
    snapshot = {field: article.get(field) for field in ("id", "title", "content_text")}
    payload, valid_ids = contract.source_payload(snapshot)
    source_version = payload["source_version"]
    existing = conn.execute("""SELECT status,applicability_json FROM article_hosted_analyses
        WHERE article_id=%s AND source_version=%s AND generation=%s""",
        (article_id, source_version, generation())).fetchone()
    if existing and existing[0] == "published":
        return {"status": "already_published", "article_id": article_id,
                "kind": existing[1]["kinds"][0]["type"]}
    provider, secret = _provider(conn)
    usage = {}
    brief = None
    classification = None
    started = time.monotonic()
    conn.execute("SAVEPOINT hosted_article_analysis")
    try:
        brief, usage["luna"] = _complete(
            provider, secret, contract.BRIEF_MODEL, contract.BRIEF_PROMPT,
            payload, contract.brief_schema(), 5000, "article_brief")
        contract.validate_brief(brief, valid_ids)
        focused = applicability.request(snapshot, generation())
        classification, usage["terra"] = _complete(
            provider, secret, contract.CLASSIFIER_MODEL, applicability.PROMPT,
            json.loads(focused["input"]), focused["schema"], 2048,
            "article_applicability")
        validated = applicability.validate(json.dumps(classification), snapshot, generation())
        kinds = classification["kinds"]
        if len(kinds) != 1 or kinds[0]["type"] != brief["classification"]["kind"]:
            raise ValueError("hosted_article_primary_kind_disagreement")
        if {field: get_article_by_id(conn, article_id).get(field) for field in snapshot} != snapshot:
            raise ValueError("hosted_article_source_changed")
        summary = {"summary": brief["summary"]["narrative"],
                   "bullets": brief["summary"]["bullets"]}
        context = {"facts": [row["text"] for row in brief["context"]["facts"]],
                   "timeline": [f"{row['date_text']}: {row['text']}" for row in brief["context"]["timeline"]]}
        conn.execute("""UPDATE articles SET summary_llm=%s,summary_model=%s,
            summary_generated_at=%s,summary_error=NULL,context_llm=%s,context_model=%s,
            context_generated_at=%s,context_error=NULL,updated_at=%s WHERE id=%s""",
            (json.dumps(summary), contract.BRIEF_MODEL, utc_now_iso(),
             json.dumps(context), contract.BRIEF_MODEL, utc_now_iso(), utc_now_iso(), article_id))
        status = "published"
        _store(conn, article_id, source_version, status, brief, validated["result"], usage, None)
        conn.execute("RELEASE SAVEPOINT hosted_article_analysis")
        return {"status": status, "article_id": article_id, "kind": kinds[0]["type"],
                "elapsed_s": round(time.monotonic() - started, 2)}
    except Exception as exc:
        conn.execute("ROLLBACK TO SAVEPOINT hosted_article_analysis")
        conn.execute("RELEASE SAVEPOINT hosted_article_analysis")
        _store(conn, article_id, source_version, "held", brief, classification, usage,
               str(exc)[:200])
        raise


def regulatory_rows(conn, limit: int = 200, article_ids: list[int] | None = None) -> list[dict]:
    if article_ids is not None and not article_ids:
        return []
    if article_ids is not None:
        limit = len(article_ids)
    scope_sql = "AND h.article_id=ANY(%s)" if article_ids is not None else ""
    params = (generation(), article_ids, limit) if article_ids is not None else (generation(), limit)
    rows = conn.execute("""SELECT h.article_id,h.source_version,h.brief_json,h.applicability_json,
        a.title,a.original_url,a.published_at,s.name,s.id
        FROM article_hosted_analyses h
        JOIN articles a ON a.id=h.article_id JOIN sources s ON s.id=a.source_id
        WHERE h.status='published' AND h.generation=%s
          AND h.applicability_json->'kinds'->0->>'type' IN
              ('regulatory_proposal','regulatory_change','regulatory_guidance','regulatory_enforcement')
        """ + scope_sql + " ORDER BY a.published_at DESC NULLS LAST LIMIT %s",
        params).fetchall()
    result = []
    for article_id, version, brief, classification, title, url, published, source, source_id in rows:
        article = get_article_by_id(conn, article_id)
        if not article or source_for(article)["source_version"] != version:
            continue
        if source_id in {"web_enrich", "src_test"}:
            continue
        result.append({"article_id": article_id, "title": title, "url": url,
                       "published_at": published, "source": source,
                       "summary": brief["summary"]["narrative"],
                       "kind": classification["kinds"][0]["type"],
                       "jurisdictions": classification["jurisdictions"]})
    return result
