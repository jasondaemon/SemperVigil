from unittest.mock import Mock

import pytest

from sempervigil import feed_inventory as inventory
from sempervigil import storage

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


def test_event_research_articles_do_not_dirty_public_feed_days(query_case):
    sql, _ = inventory.feed_inventory_query(query_case)
    assert "COALESCE(b.source_id, '') NOT IN ('web_enrich')" in sql


def test_missing_brief_day_uses_application_timezone(query_case, monkeypatch):
    monkeypatch.setenv("SV_APP_TIMEZONE", "America/New_York")
    query_case.execute.return_value.fetchall.return_value = [(7, "2026-09-09T01:00:00Z")]
    sql, params = inventory.feed_inventory_query(query_case)
    assert params == ([7], ["2026-09-08"], "America/New_York", [])
    assert "timezone(%s" in sql
    assert "2026-11-15" in sql


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
    assert params == ("America/New_York",)


def test_materialized_bases_retain_hash_not_large_payload(query_case):
    sql, _ = inventory.feed_inventory_query(query_case)
    assert "md5(jsonb_build_array(b.id" in sql
    assert "md5(jsonb_build_array(b.cve_id" in sql
    assert "AS payload" not in sql
    assert "md5(payload)" not in sql
    assert "AS digest FROM articles" in sql


def test_cve_editorial_day_boundary(monkeypatch):
    monkeypatch.setenv("SV_APP_TIMEZONE", "America/New_York")
    assert storage._brief_day_from("2026-11-15T00:16:51.120") == "2026-11-14"
    assert storage._brief_day_from("2026-11-15T05:00:00.000") == "2026-11-15"
    sql, timezone_name = storage._cve_feed_day_sql("c.published_at")
    assert timezone_name == "America/New_York"
    assert "2026-11-15" in sql
    assert "AT TIME ZONE 'UTC'" in sql


def test_cve_export_and_inventory_share_day_expression(query_case, monkeypatch):
    monkeypatch.setattr(storage, "_table_exists", lambda *a: False)
    monkeypatch.setattr(storage, "_table_columns", lambda *a: set())
    monkeypatch.setattr(storage, "_table_exists", lambda conn, table: table == "cves")
    storage.list_cves_for_day(query_case, "2026-11-14")
    export_sql, export_params = query_case.execute.call_args.args
    inventory_sql, inventory_params = inventory.feed_inventory_query(query_case)
    assert "timezone(%s" in export_sql
    assert "timezone(%s" in inventory_sql
    assert export_params == ("America/New_York", "2026-11-14", 200)
    assert inventory_params[-2] == "America/New_York"
