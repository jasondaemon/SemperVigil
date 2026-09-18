from unittest.mock import Mock

import pytest

from sempervigil import feed_inventory as inventory

pytestmark = pytest.mark.offline


@pytest.fixture
def query_case(monkeypatch):
    monkeypatch.setattr(inventory, "_table_exists", lambda *a: True)
    monkeypatch.setattr(inventory, "_table_columns", lambda conn, table:
                        inventory.ARTICLE_FIELDS if table == "articles" else inventory.CVE_FIELDS)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = []
    return conn


@pytest.mark.parametrize("dependency", [
    "article_products", "cve_products", "cve_product_versions", "vendors",
    "threat_actor_aliases", "event_articles", "article_tags", "cve_kev", "sources",
])
def test_export_dependencies_are_included(query_case, dependency):
    sql, params = inventory.feed_inventory_query(query_case)
    assert dependency in sql
    assert "ORDER BY kind, owner, dependency, digest" in sql


def test_check_timestamps_are_not_substituted_for_content(query_case):
    sql, _ = inventory.feed_inventory_query(query_case)
    assert "b.epss_score" in sql
    assert "b.epss_checked_at" in sql  # This timestamp is itself a public field.
    assert "b.updated_at" not in sql
    assert "cve_products_checked_at" not in sql
    assert "k.due_date" in sql


def test_missing_brief_day_uses_application_timezone(query_case, monkeypatch):
    monkeypatch.setenv("SV_APP_TIMEZONE", "America/New_York")
    query_case.execute.return_value.fetchall.return_value = [(7, "2026-09-09T01:00:00Z")]
    _, params = inventory.feed_inventory_query(query_case)
    assert params == ([7], ["2026-09-08"], [])


def test_empty_schema_returns_empty_inventory(monkeypatch):
    monkeypatch.setattr(inventory, "_table_exists", lambda *a: False)
    sql, params = inventory.feed_inventory_query(Mock())
    assert "WHERE FALSE" in sql
    assert params == ()


def test_optional_dependencies_can_be_absent(query_case, monkeypatch):
    monkeypatch.setattr(inventory, "_table_exists", lambda conn, table: table == "cves")
    sql, params = inventory.feed_inventory_query(query_case)
    assert "cve_base" in sql
    assert "article_base" not in sql
    assert "JOIN" not in sql
    assert params == ()
