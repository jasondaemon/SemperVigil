from sempervigil.storage import get_dashboard_metrics


class FakeCursor:
    def __init__(self, value=0):
        self._value = value

    def fetchone(self):
        return (self._value,)


class FakeConn:
    def execute(self, sql, params=()):
        if "FROM cves" in sql and "WHERE c." in sql:
            assert "FROM cves c" in sql, f"Bad aliasing: {sql}"
        return FakeCursor(0)


def test_dashboard_metrics_sql_has_valid_cves_aliasing():
    metrics = get_dashboard_metrics(FakeConn())
    assert "articles_missing_content_count" in metrics
    assert "cves_missing_description_count" in metrics
