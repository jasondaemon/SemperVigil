from sempervigil.storage import claim_next_job, enqueue_job, init_db, list_jobs


def test_job_runner_claims_one_job_fifo(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    first = enqueue_job(conn, "test_source", {"source_id": "a"})
    second = enqueue_job(conn, "test_source", {"source_id": "b"})

    job = claim_next_job(conn, "worker-1")
    assert job is not None
    assert job.id == first

    remaining = list_jobs(conn, limit=10)
    remaining_ids = [item.id for item in remaining if item.status == "queued"]
    assert second in remaining_ids
