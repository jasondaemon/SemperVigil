import json

import pytest

from sempervigil import article_hosted_jobs as jobs
from sempervigil import openai_budget

pytestmark = pytest.mark.offline


class Cursor:
    def fetchone(self):
        return None


class Conn:
    def __init__(self):
        self.queries = []

    def execute(self, sql, params=()):
        self.queries.append((sql, params))
        return Cursor()


def _brief(kind):
    return {
        "summary": {"narrative": "The regulator proposed a new reporting rule.",
                    "bullets": ["A new reporting rule was proposed."],
                    "passage_ids": ["p001"]},
        "context": {"facts": [{"text": "The rule is proposed.", "passage_ids": ["p001"]}],
                    "timeline": [], "uncertainties": []},
        "classification": {"kind": kind, "passage_ids": ["p001"]},
        "event_seed": {"is_event": False, "event_type": "none", "victim": None,
                       "incident_date": None, "compromised_scope": None,
                       "passage_ids": []},
    }


def _focused(kind):
    return {"kinds": [{"type": kind, "passage_ids": ["p001"]}],
            "jurisdictions": [{"code": "AU", "role": "legal_scope", "passage_ids": ["p001"]}],
            "organizations": [], "products": []}


def _setup(monkeypatch, *, focused_kind="regulatory_proposal"):
    article = {"id": 99, "title": "New reporting proposal",
               "content_text": "The regulator proposed a new reporting rule for Australia."}
    monkeypatch.setattr(openai_budget, "configured_cap", lambda: 90)
    monkeypatch.setattr(jobs, "get_article_by_id", lambda *a: article)
    monkeypatch.setattr(jobs, "_provider", lambda *a: ({"id": "p"}, "secret"))
    calls = iter([(_brief("regulatory_proposal"), {"prompt_tokens": 100}),
                  (_focused(focused_kind), {"prompt_tokens": 100})])
    monkeypatch.setattr(jobs, "_complete", lambda *a: next(calls))
    return Conn()


def test_hosted_analysis_writes_existing_summary_and_context_contract(monkeypatch):
    conn = _setup(monkeypatch)
    result = jobs.analyze(conn, 99)
    assert result["status"] == "published"
    update = next(params for sql, params in conn.queries if "UPDATE articles SET" in sql)
    assert json.loads(update[0])["summary"] == "The regulator proposed a new reporting rule."
    assert json.loads(update[3])["facts"] == ["The rule is proposed."]
    stored = next(params for sql, params in conn.queries if "INSERT INTO article_hosted_analyses" in sql)
    assert stored[3] == "published"
    assert json.loads(stored[5])["kinds"][0]["type"] == "regulatory_proposal"


def test_primary_kind_disagreement_holds_without_public_write(monkeypatch):
    conn = _setup(monkeypatch, focused_kind="incident_report")
    with pytest.raises(ValueError, match="primary_kind_disagreement"):
        jobs.analyze(conn, 99)
    assert not any("UPDATE articles SET" in sql for sql, _ in conn.queries)
    stored = next(params for sql, params in conn.queries if "INSERT INTO article_hosted_analyses" in sql)
    assert stored[3] == "held"


def test_budget_pricing_and_unknown_model_are_fail_closed(monkeypatch):
    monkeypatch.setenv("SV_OPENAI_MONTHLY_BUDGET_USD", "90")
    assert openai_budget.configured_cap() == 90
    cost = openai_budget._reserved_cost({"model": "gpt-5.6-terra",
                                         "messages": [{"content": "policy"}],
                                         "max_completion_tokens": 2048})
    assert 0 < cost < 0.1
    with pytest.raises(ValueError, match="unknown_model"):
        openai_budget._reserved_cost({"model": "unknown", "max_completion_tokens": 10})
