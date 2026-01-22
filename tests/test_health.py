from sempervigil.storage import get_source, init_db, record_source_run, upsert_source
from sempervigil.worker import _maybe_pause_source
from sempervigil.utils import utc_now_iso, utc_now_iso_offset


def _seed_source(conn, source_id: str) -> None:
    upsert_source(
        conn,
        {
            "id": source_id,
            "name": "Test Source",
            "enabled": True,
            "base_url": "https://example.com",
            "default_frequency_minutes": 60,
        },
    )


def test_auto_pause_on_error_streak(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))
    _seed_source(conn, "source-1")

    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        ("alerts.pause_on_failure.error_streak", "2", utc_now_iso()),
    )
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        ("alerts.pause_on_failure.pause_minutes", "60", utc_now_iso()),
    )
    conn.commit()

    record_source_run(
        conn,
        source_id="source-1",
        started_at=utc_now_iso_offset(seconds=-10),
        finished_at=utc_now_iso_offset(seconds=-9),
        status="error",
        http_status=None,
        items_found=0,
        items_accepted=0,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        error="boom",
        notes=None,
    )
    record_source_run(
        conn,
        source_id="source-1",
        started_at=utc_now_iso_offset(seconds=-5),
        finished_at=utc_now_iso_offset(seconds=-4),
        status="error",
        http_status=None,
        items_found=0,
        items_accepted=0,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        error="boom",
        notes=None,
    )

    _maybe_pause_source(conn, "source-1", logger=None)
    source = get_source(conn, "source-1")
    assert source is not None
    assert source.enabled is False
    assert source.pause_until is not None
    assert "error_streak" in (source.paused_reason or "")
    alert = conn.execute(
        "SELECT alert_type FROM health_alerts WHERE source_id = ?",
        ("source-1",),
    ).fetchone()
    assert alert is not None


def test_auto_pause_on_zero_streak(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))
    _seed_source(conn, "source-2")

    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        ("alerts.pause_on_failure.zero_streak", "2", utc_now_iso()),
    )
    conn.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        ("alerts.pause_on_failure.pause_minutes", "60", utc_now_iso()),
    )
    conn.commit()

    record_source_run(
        conn,
        source_id="source-2",
        started_at=utc_now_iso_offset(seconds=-10),
        finished_at=utc_now_iso_offset(seconds=-9),
        status="ok",
        http_status=200,
        items_found=5,
        items_accepted=0,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        error=None,
        notes=None,
    )
    record_source_run(
        conn,
        source_id="source-2",
        started_at=utc_now_iso_offset(seconds=-5),
        finished_at=utc_now_iso_offset(seconds=-4),
        status="ok",
        http_status=200,
        items_found=3,
        items_accepted=0,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        error=None,
        notes=None,
    )

    _maybe_pause_source(conn, "source-2", logger=None)
    source = get_source(conn, "source-2")
    assert source is not None
    assert source.enabled is False
    assert "zero_streak" in (source.paused_reason or "")
    alert = conn.execute(
        "SELECT alert_type FROM health_alerts WHERE source_id = ?",
        ("source-2",),
    ).fetchone()
    assert alert is not None
