import json

import pytest

from sempervigil.llm import router


pytestmark = pytest.mark.offline


def _provider():
    return {"id": "local", "name": "Ollama native", "type": "ollama_native", "timeout_s": 30}


def test_native_transport_disables_thinking_and_maps_options(monkeypatch):
    captured = {}

    def request(method, url, headers, payload, provider, context=None):
        captured.update(method=method, url=url, headers=headers, payload=payload, context=context)
        return {"message": {"content": '{"items":[]}'}, "prompt_eval_count": 7, "eval_count": 3}

    monkeypatch.setattr(router, "_http_request", request)
    output = router._call_provider(
        "ollama_native",
        "http://ollama:11434",
        None,
        "ollama/qwen3.5:9b-q4_K_M",
        [{"role": "user", "content": "input"}],
        {"max_tokens": 256, "temperature": 0, "ignored": "value"},
        _provider(),
        {"stage": "cve_enrich_products", "json_response_format_enabled": True},
    )

    assert json.loads(output) == {"items": []}
    assert captured["method"] == "POST"
    assert captured["url"] == "http://ollama:11434/api/chat"
    assert captured["headers"] == {}
    assert captured["payload"] == {
        "model": "qwen3.5:9b-q4_K_M",
        "messages": [{"role": "user", "content": "input"}],
        "stream": False,
        "think": False,
        "options": {"num_predict": 256, "temperature": 0},
        "format": "json",
    }


def test_native_private_transport_uses_strict_schema(monkeypatch):
    captured = {}
    schema = {
        "type": "object",
        "properties": {"decision": {"type": "string"}},
        "required": ["decision"],
    }

    monkeypatch.setattr(
        router,
        "_http_request",
        lambda method, url, headers, payload, provider, context=None: (
            captured.update(payload=payload) or {"message": {"content": '{"decision":"include"}'}}
        ),
    )
    output = router._call_ollama_native(
        "http://ollama:11434",
        "ollama/qwen3.5:9b-q4_K_M",
        [{"role": "user", "content": "input"}],
        {},
        _provider(),
        response_format={"type": "json_schema", "json_schema": {"schema": schema}},
    )

    assert json.loads(output) == {"decision": "include"}
    assert captured["payload"]["format"] == schema
    assert captured["payload"]["think"] is False


def test_profile_schema_is_forwarded_to_native_ollama(monkeypatch):
    schema = {
        "type": "object",
        "properties": {"items": {"type": "array"}},
        "required": ["items"],
        "additionalProperties": False,
    }
    profile = {
        "name": "Products",
        "primary_provider_id": "local",
        "primary_model_id": "qwen",
        "prompt_id": "prompt",
        "schema_id": "schema",
        "params": {},
        "fallback": [],
    }
    captured = {}
    monkeypatch.setattr(router, "get_profile", lambda conn, profile_id: profile)
    monkeypatch.setattr(router, "get_provider", lambda conn, provider_id: _provider())
    monkeypatch.setattr(router, "get_model", lambda conn, model_id: {"model_name": "ollama/qwen3.5:9b-q4_K_M"})
    monkeypatch.setattr(router, "get_prompt", lambda conn, prompt_id: {
        "name": "products", "system_template": "system", "user_template": "{{input}}"})
    monkeypatch.setattr(router, "get_schema", lambda conn, schema_id: {"json_schema": schema})
    monkeypatch.setattr(router, "load_provider_secret", lambda conn, provider_id: None)
    monkeypatch.setattr(router, "load_runtime_config", lambda conn: type("C", (), {"llm": {}})())
    monkeypatch.setattr(router, "_http_request", lambda method, url, headers, payload, provider,
                        context=None: captured.update(payload=payload) or {
                            "message": {"content": '{"items":[]}'}})

    result = router.run_profile(object(), "profile", "input", router.logging.getLogger("test"))

    assert result["schema_valid"] is True
    assert captured["payload"]["format"] == schema


def test_native_transport_rejects_empty_visible_content(monkeypatch):
    monkeypatch.setattr(
        router,
        "_http_request",
        lambda *args, **kwargs: {"message": {"content": "", "thinking": "hidden"}},
    )
    with pytest.raises(ValueError, match="ollama_native_missing_content"):
        router._call_ollama_native(
            "http://ollama:11434", "ollama/qwen3.5:9b-q4_K_M", [], {}, _provider()
        )


def test_native_token_metrics_are_recorded():
    assert router._extract_response_metrics(
        json.dumps({"prompt_eval_count": 11, "eval_count": 5})
    ) == {"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16}
