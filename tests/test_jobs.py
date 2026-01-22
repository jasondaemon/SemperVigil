from sempervigil.storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    init_db,
    list_jobs,
)
from sempervigil.utils import utc_now_iso_offset


def test_enqueue_and_claim_job(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))
    conn2 = init_db(str(db_path))

    job_id = enqueue_job(conn, "build_site", {"reason": "test"})
    claimed = claim_next_job(conn, "worker-1")

    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"

    second = claim_next_job(conn2, "worker-2")
    assert second is None


def test_debounce_build_job(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    first = enqueue_job(conn, "build_site", None, debounce=True)
    claimed = claim_next_job(conn, "worker-1")
    assert claimed is not None
    assert claimed.id == first
    second = enqueue_job(conn, "build_site", None, debounce=True)

    jobs = list_jobs(conn, limit=10)
    assert any(job.id == first for job in jobs)
    assert first == second


def test_job_lifecycle_records_result(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

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
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

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
