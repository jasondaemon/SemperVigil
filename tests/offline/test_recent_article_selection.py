from unittest.mock import Mock

import pytest

from sempervigil import storage

pytestmark = pytest.mark.offline


def test_recent_selection_preserves_stored_brief_day(monkeypatch):
    monkeypatch.setattr(storage, "_table_exists", lambda *args: True)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [
        (7, "Title", "https://example.com/news", "2026-09-09T01:00:00Z",
         "2026-09-09T02:00:00Z", "example", "Example", None, None,
         "security", "2026-09-08")
    ]
    rows = storage.list_recent_articles(conn, limit=200)
    assert rows[0]["brief_day"] == "2026-09-08"
    assert rows[0]["tags"] == "security"
    assert "a.brief_day" in conn.execute.call_args.args[0]
    assert conn.execute.call_args.args[1] == (200,)
