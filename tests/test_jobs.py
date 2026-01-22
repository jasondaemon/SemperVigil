from sempervigil.storage import claim_next_job, enqueue_job, init_db, list_jobs


def test_enqueue_and_claim_job(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    job_id = enqueue_job(conn, "build_site", {"reason": "test"})
    claimed = claim_next_job(conn, "worker-1")

    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"

    second = claim_next_job(conn, "worker-2")
    assert second is None


def test_debounce_build_job(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    first = enqueue_job(conn, "build_site", None, debounce=True)
    second = enqueue_job(conn, "build_site", None, debounce=True)

    jobs = list_jobs(conn, limit=10)
    assert any(job.id == first for job in jobs)
    assert first == second
