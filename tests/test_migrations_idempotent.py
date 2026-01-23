import sqlite3

from sempervigil.migrations import _get_migrations, apply_migrations


def test_apply_migrations_idempotent(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(str(db_path))
    apply_migrations(conn)
    apply_migrations(conn)

    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    versions = [row[0] for row in rows]
    expected = [version for version, _ in _get_migrations()]
    assert sorted(versions) == sorted(expected)
    assert len(versions) == len(set(versions))
