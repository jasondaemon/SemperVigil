import json

import pytest

from sempervigil import migrations_pg, worker

pytestmark = pytest.mark.offline


def test_validator_migration_routes_dedicated_schema_without_replacing_existing_route(monkeypatch):
    monkeypatch.setattr(migrations_pg, "_table_exists", lambda *_args: True)
    captured = {}
    monkeypatch.setattr(migrations_pg, "_upsert_llm_schema",
                        lambda _conn, *args: captured.update(schema=args))
    monkeypatch.setattr(migrations_pg, "_upsert_llm_prompt",
                        lambda _conn, *args: captured.update(prompt=args))

    class Conn:
        def execute(self, sql, params=()):
            if "FROM pipeline_stage_config s JOIN llm_profiles p" in sql:
                return self
            captured.setdefault("writes", []).append((sql, params))
            return self

        def fetchone(self):
            return ("provider-qwen", "model-qwen")

    migrations_pg._migrate_event_web_validator_profile(Conn())
    schema = json.loads(captured["schema"][3])
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "related", "confidence", "matched_facts", "contradictions", "rationale"}
    assert "Do not summarize" in captured["prompt"][3]
    assert "ON CONFLICT(stage_name) DO NOTHING" in captured["writes"][1][0]
    assert captured["writes"][0][1][2:4] == ("provider-qwen", "model-qwen")


def test_event_validator_does_not_fall_back_to_article_summarizer(monkeypatch):
    stages = []
    monkeypatch.delenv("SV_EVENT_ENRICH_VALIDATION_PROFILE_ID", raising=False)
    monkeypatch.setattr(worker, "get_active_profile_for_stage",
                        lambda _conn, stage: stages.append(stage) or (None, "missing"))
    assert worker._event_source_validation_profile(None) is None
    assert stages == ["event_web_validate"]
