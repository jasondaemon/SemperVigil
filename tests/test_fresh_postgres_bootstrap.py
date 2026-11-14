from sempervigil.storage import init_db


def test_fresh_database_reaches_current_queue_schema():
    conn = init_db()
    columns = {
        row[0]
        for row in conn.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'jobs'
            """
        ).fetchall()
    }
    assert {"queue_name", "available_at", "lease_expires_at"} <= columns
    assert conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = %s",
        ("pg_jobs_stage_035",),
    ).fetchone()
