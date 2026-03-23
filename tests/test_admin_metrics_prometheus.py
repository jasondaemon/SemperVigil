from sempervigil.admin import _render_metrics_text
from sempervigil.storage import enqueue_job, init_db, mark_build_dirty


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

