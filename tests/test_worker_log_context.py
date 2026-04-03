from types import SimpleNamespace

from sempervigil.services.ai_service import (
    create_model,
    create_profile,
    create_provider,
    create_prompt,
    set_pipeline_routing,
)
from sempervigil.storage import init_db
from sempervigil.worker import _job_context_fields


def test_llm_job_context_includes_profile_provider_and_model():
    conn = init_db()
    provider = create_provider(
        conn,
        {
            "name": "ollama",
            "type": "openai_compatible",
            "base_url": "http://example.invalid/v1",
        },
    )
    model = create_model(
        conn,
        {
            "provider_id": provider["id"],
            "model_name": "ollama/llama3",
        },
    )
    prompt = create_prompt(
        conn,
        {
            "name": "Summarize",
            "system_template": "system {{input}}",
            "user_template": "user {{input}}",
        },
    )
    profile = create_profile(
        conn,
        {
            "name": "Article Summary",
            "primary_provider_id": provider["id"],
            "primary_model_id": model["id"],
            "prompt_id": prompt["id"],
        },
    )
    set_pipeline_routing(conn, "summarize_article_llm", profile["id"])

    job = SimpleNamespace(
        id="job_123",
        job_type="summarize_article_llm",
        payload={"article_id": 42},
    )

    fields = _job_context_fields(conn, job)

    assert fields["job_type"] == "summarize_article_llm"
    assert fields["profile_name"] == "Article Summary"
    assert fields["provider_name"] == "ollama"
    assert fields["model_name"] == "ollama/llama3"
