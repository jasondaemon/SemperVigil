from sempervigil import orchestrator
from sempervigil.storage import enqueue_job, init_db


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


def test_tick_runner_launches_enqueues_fetch_launch_job(monkeypatch):
    conn = init_db()
    logger = orchestrator._setup_logging()
    monkeypatch.setenv("SV_FETCH_SCALE_STEP", "10")
    monkeypatch.setenv("SV_FETCH_MAX_ACTIVE", "2")
    enqueue_job(conn, "fetch_article_content", {"article_id": 1})
    enqueue_job(conn, "fetch_article_content", {"article_id": 2})
    enqueue_job(conn, "fetch_article_content", {"article_id": 3})
    enqueue_job(conn, "fetch_article_content", {"article_id": 4})
    enqueue_job(conn, "fetch_article_content", {"article_id": 5})
    enqueue_job(conn, "fetch_article_content", {"article_id": 6})
    enqueue_job(conn, "fetch_article_content", {"article_id": 7})
    enqueue_job(conn, "fetch_article_content", {"article_id": 8})
    enqueue_job(conn, "fetch_article_content", {"article_id": 9})
    enqueue_job(conn, "fetch_article_content", {"article_id": 10})
    enqueue_job(conn, "fetch_article_content", {"article_id": 11})

    launched = orchestrator._tick_runner_launches(conn, logger)

    assert launched == 2
    rows = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE job_type = %s AND status IN ('queued', 'running')",
        ("launch_fetch_worker",),
    ).fetchone()
    assert rows is not None
    assert int(rows[0] or 0) == 2


def test_tick_runner_launches_enforces_single_openai_launch(monkeypatch):
    conn = init_db()
    logger = orchestrator._setup_logging()
    enqueue_job(conn, "build_daily_brief", {"day": "2026-03-23"})
    enqueue_job(conn, "launch_openai_worker", {"queue_name": "openai", "max_jobs": 1})

    launched = orchestrator._tick_runner_launches(conn, logger)

    assert launched == 0
