from sempervigil import orchestrator


def test_desired_fetch_launches_scales_with_queue_depth(monkeypatch):
    monkeypatch.delenv("SV_ORCH_LLM_HIGH_WATERMARK", raising=False)
    monkeypatch.delenv("SV_ORCH_OPENAI_HIGH_WATERMARK", raising=False)
    policy = orchestrator._launch_policy()

    queues = {
        "fetch": {"queued": 60},
        "llm_local": {"queued": 0},
        "openai": {"queued": 0},
    }

    desired = orchestrator._desired_fetch_launches(queues, policy)

    assert desired == 2


def test_desired_fetch_launches_suppresses_under_llm_pressure(monkeypatch):
    monkeypatch.setenv("SV_ORCH_LLM_HIGH_WATERMARK", "10")
    monkeypatch.setenv("SV_ORCH_FETCH_SUPPRESS_TO", "1")
    policy = orchestrator._launch_policy()

    queues = {
        "fetch": {"queued": 200},
        "llm_local": {"queued": 12},
        "openai": {"queued": 0},
    }

    desired = orchestrator._desired_fetch_launches(queues, policy)

    assert desired == 1
