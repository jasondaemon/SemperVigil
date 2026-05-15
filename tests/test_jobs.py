from sempervigil.storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    enqueue_source_ingest_job,
    get_build_state,
    get_source,
    init_db,
    list_due_sources,
    list_jobs,
    mark_build_dirty,
    record_source_run,
    finalize_source_ingest_state,
)
from sempervigil.utils import utc_now_iso, utc_now_iso_offset


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


def test_stale_control_launch_requeues_with_queue_filter(tmp_path):
    conn = init_db()

    job_id = enqueue_job(conn, "launch_fetch_worker", {"queue_name": "fetch"})
    claimed = claim_next_job(
        conn,
        "runner-1",
        allowed_types=["launch_fetch_worker"],
        allowed_queues=["control"],
        lock_timeout_seconds=30,
        lease_seconds=30,
    )
    assert claimed is not None
    assert claimed.id == job_id

    stale_time = utc_now_iso_offset(seconds=-3600)
    conn.execute(
        """
        UPDATE jobs
        SET locked_at = %s,
            lease_expires_at = %s,
            status = 'running'
        WHERE id = %s
        """,
        (stale_time, stale_time, job_id),
    )
    conn.commit()

    reclaimed = claim_next_job(
        conn,
        "runner-2",
        allowed_types=["launch_fetch_worker"],
        allowed_queues=["control"],
        lock_timeout_seconds=30,
        lease_seconds=30,
    )
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

    mark_build_dirty(conn, reason="article_updated", metadata={"site_data_refresh": {"requested_by": "admin"}})
    mark_build_dirty(conn, reason="daily_brief")
    mark_build_dirty(conn, metadata={"feed_archive_refresh": {"mode": "missing_only", "requested_by": "admin"}})

    state = get_build_state(conn)

    assert state["dirty"] is True
    assert state["requested_at"]
    assert state["last_dirty_at"]
    assert state["reasons"] == ["article_updated", "daily_brief"]
    assert state["metadata"] == {
        "site_data_refresh": {"requested_by": "admin"},
        "feed_archive_refresh": {"mode": "missing_only", "requested_by": "admin"},
    }


def test_due_source_enqueue_sets_pending_state_and_blocks_duplicate_schedule(tmp_path):
    conn = init_db()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO sources
            (id, name, enabled, interval_minutes, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        ("source-a", "Source A", 1, 60, now, now),
    )
    conn.commit()

    due = list_due_sources(conn, now)
    assert [source.id for source in due] == ["source-a"]

    first_job_id = enqueue_source_ingest_job(conn, "source-a", now_iso=now)
    assert first_job_id is not None

    source = get_source(conn, "source-a")
    assert source is not None
    assert source.ingest_job_id == first_job_id
    assert source.last_enqueued_at == now

    due_after_enqueue = list_due_sources(conn, now)
    assert due_after_enqueue == []

    second_job_id = enqueue_source_ingest_job(conn, "source-a", now_iso=now)
    assert second_job_id == first_job_id


def test_source_not_due_again_until_interval_after_completion(tmp_path):
    conn = init_db()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO sources
            (id, name, enabled, interval_minutes, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        ("source-b", "Source B", 1, 60, now, now),
    )
    conn.commit()

    job_id = enqueue_source_ingest_job(conn, "source-b", now_iso=now)
    assert job_id is not None

    finished_at = utc_now_iso_offset(seconds=300)
    record_source_run(
        conn,
        source_id="source-b",
        started_at=now,
        finished_at=finished_at,
        status="ok",
        http_status=200,
        items_found=5,
        items_accepted=1,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        error=None,
        notes=None,
    )
    finalize_source_ingest_state(
        conn,
        source_id="source-b",
        job_id=job_id,
        finished_at=finished_at,
        next_due_at=utc_now_iso_offset(seconds=3900),
    )

    before_due = list_due_sources(conn, utc_now_iso_offset(seconds=3599))
    assert before_due == []

    after_due = list_due_sources(conn, utc_now_iso_offset(seconds=3901))
    assert [source.id for source in after_due] == ["source-b"]
