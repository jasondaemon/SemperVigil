from sempervigil.admin import _render_metrics_text
from sempervigil.storage import claim_next_job, enqueue_job, heartbeat_job, init_db, mark_build_dirty
from sempervigil.admin import app
from fastapi.testclient import TestClient


def test_render_metrics_text_exposes_queue_runner_and_build_metrics(tmp_path):
    conn = init_db()
    mark_build_dirty(conn, reason="test")
    enqueue_job(conn, "fetch_article_content", {"article_id": 1})
    enqueue_job(conn, "launch_fetch_worker", {"queue_name": "fetch"})

    payload = _render_metrics_text(conn)

    assert "sempervigil_build_dirty 1" in payload
    assert 'sempervigil_queue_jobs{queue_name="control",status="queued"} 1' in payload
    assert 'sempervigil_queue_jobs{queue_name="fetch",status="queued"} 1' in payload
    assert 'sempervigil_jobs{job_type="launch_fetch_worker",queue_name="control",status="queued"} 1' in payload
    assert 'sempervigil_runner_launch_jobs{runner_type="fetch",status="queued"} 1' in payload
    assert 'sempervigil_sources_ingest_state{state="queued"} 0' in payload
    assert 'sempervigil_dashboard_jobs{job_type="fetch_article_content",status="queued",worker_group="fetch"} 1' in payload
    assert 'sempervigil_dashboard_need{job_type="fetch_article_content",worker_group="fetch"}' in payload
    assert 'sempervigil_dashboard_current{column="queued",job_type="fetch_article_content",worker_group="fetch"} 1' in payload
    assert 'sempervigil_dashboard_current{column="need",job_type="fetch_article_content",worker_group="fetch"}' in payload
    assert 'sempervigil_dashboard_order{job_type="fetch_article_content",worker_group="fetch"}' in payload


def test_render_metrics_text_exposes_runner_health_and_queue_worker_health(tmp_path):
    conn = init_db()
    launch_id = enqueue_job(conn, "launch_fetch_worker", {"queue_name": "fetch"})
    work_id = enqueue_job(conn, "fetch_article_content", {"article_id": 1})
    launch_job = claim_next_job(
        conn,
        "runner-a",
        allowed_types=["launch_fetch_worker"],
        allowed_queues=["control"],
        lease_seconds=300,
        lock_timeout_seconds=300,
    )
    assert launch_job and launch_job.id == launch_id
    work_job = claim_next_job(
        conn,
        "runner-a",
        allowed_queues=["fetch"],
        lease_seconds=300,
        lock_timeout_seconds=300,
    )
    assert work_job and work_job.id == work_id
    heartbeat_job(conn, launch_id, "runner-a", 300)

    payload = _render_metrics_text(conn)

    assert 'sempervigil_runner_health{health="active",runner_type="fetch"} 1' in payload
    assert 'sempervigil_runner_health{health="idle",runner_type="fetch"} 0' in payload
    assert 'sempervigil_runner_health{health="stale",runner_type="fetch"} 0' in payload
    assert 'sempervigil_runner_health{health="active",runner_type="llm_local"} 0' in payload
    assert 'sempervigil_queue_worker_health{metric="active_runners",queue_name="fetch"} 1' in payload
    assert 'sempervigil_queue_worker_health{metric="running_jobs",queue_name="fetch"} 1' in payload


def test_render_metrics_text_exposes_legacy_public_daily_metrics(monkeypatch):
    conn = init_db()

    monkeypatch.setattr(
        "sempervigil.admin.get_public_metrics_daily_counts",
        lambda _conn, days=14: [
            {"day": "2026-03-10", "articles": 106, "cves_high": 209, "cves_critical": 34},
            {"day": "2026-03-11", "articles": 87, "cves_high": 111, "cves_critical": 12},
        ],
    )

    payload = _render_metrics_text(conn)

    assert 'sv_articles_daily_count{day="2026-03-10"} 106' in payload
    assert 'sv_cves_high_daily_count{day="2026-03-10"} 209' in payload
    assert 'sv_cves_critical_daily_count{day="2026-03-10"} 34' in payload
    assert 'sv_articles_daily_count{day="2026-03-11"} 87' in payload


def test_public_metrics_daily_api_exposes_daily_rows(monkeypatch):
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    monkeypatch.setattr(
        "sempervigil.admin.get_public_metrics_daily_counts",
        lambda _conn, days=14: [
            {"day": "2026-03-10", "articles": 106, "cves_high": 209, "cves_critical": 34},
            {"day": "2026-03-11", "articles": 87, "cves_high": 111, "cves_critical": 12},
        ],
    )

    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.get("/admin/api/public-metrics/daily?days=14")
    assert response.status_code == 200
    payload = response.json()
    assert payload["days"] == 14
    assert payload["rows"][0]["day"] == "2026-03-10"
    assert payload["rows"][1]["cves_critical"] == 12
