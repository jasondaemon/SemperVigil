from sempervigil.storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    get_build_state,
    init_db,
    list_jobs,
    mark_build_dirty,
)
from sempervigil.utils import utc_now_iso_offset


def test_enqueue_and_claim_job(tmp_path):
    conn = init_db()
    conn2 = init_db()

    job_id = enqueue_job(conn, "build_site", {"reason": "test"})
    claimed = claim_next_job(conn, "worker-1")

    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"

    second = claim_next_job(conn2, "worker-2")
    assert second is None


def test_debounce_build_job(tmp_path):
    conn = init_db()

    first = enqueue_job(conn, "build_site", None, debounce=True)
    claimed = claim_next_job(conn, "worker-1")
    assert claimed is not None
    assert claimed.id == first
    second = enqueue_job(conn, "build_site", None, debounce=True)

    jobs = list_jobs(conn, limit=10)
    assert any(job.id == first for job in jobs)
    assert first == second


def test_job_lifecycle_records_result(tmp_path):
    conn = init_db()

    job_id = enqueue_job(conn, "test_source", {"source_id": "cisa-alerts"})
    claimed = claim_next_job(conn, "worker-1")

    assert claimed is not None
    assert claimed.id == job_id

    result = {"status": "ok", "found_count": 5}
    complete_job(conn, job_id, result=result)

    jobs = list_jobs(conn, limit=1)
    assert jobs[0].status == "succeeded"
    assert jobs[0].result == result


def test_stale_lock_requeues_job(tmp_path):
    conn = init_db()

    job_id = enqueue_job(conn, "build_site", None)
    claimed = claim_next_job(conn, "worker-1")
    assert claimed is not None

    stale_time = utc_now_iso_offset(seconds=-3600)
    conn.execute(
        "UPDATE jobs SET locked_at = ?, status = 'running' WHERE id = ?",
        (stale_time, job_id),
    )
    conn.commit()

    reclaimed = claim_next_job(conn, "worker-2", lock_timeout_seconds=10)
    assert reclaimed is not None
    assert reclaimed.id == job_id


def test_enqueue_job_sets_queue_name(tmp_path):
    conn = init_db()

    job_id = enqueue_job(conn, "build_daily_brief", {"day": "2026-03-23"})
    row = conn.execute("SELECT queue_name FROM jobs WHERE id = %s", (job_id,)).fetchone()

    assert row is not None
    assert row[0] == "openai"


def test_enqueue_launch_job_sets_control_queue(tmp_path):
    conn = init_db()

    job_id = enqueue_job(conn, "launch_fetch_worker", {"queue_name": "fetch"})
    row = conn.execute("SELECT queue_name FROM jobs WHERE id = %s", (job_id,)).fetchone()

    assert row is not None
    assert row[0] == "control"


def test_claim_next_job_can_filter_by_queue(tmp_path):
    conn = init_db()

    fetch_id = enqueue_job(conn, "fetch_article_content", {"article_id": 1})
    llm_id = enqueue_job(conn, "summarize_article_llm", {"article_id": 1})

    claimed = claim_next_job(conn, "worker-1", allowed_queues=["llm_local"])

    assert claimed is not None
    assert claimed.id == llm_id
    assert claimed.queue_name == "llm_local"
    assert claimed.id != fetch_id


def test_mark_build_dirty_tracks_state(tmp_path):
    conn = init_db()

    mark_build_dirty(conn, reason="article_updated")
    mark_build_dirty(conn, reason="daily_brief")

    state = get_build_state(conn)

    assert state["dirty"] is True
    assert state["requested_at"]
    assert state["last_dirty_at"]
    assert state["reasons"] == ["article_updated", "daily_brief"]
