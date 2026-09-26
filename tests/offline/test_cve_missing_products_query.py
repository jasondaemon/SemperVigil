import sqlite3

import pytest

from sempervigil import storage

pytestmark = pytest.mark.offline


class _CompatConn:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=()):
        return self.conn.execute(sql.replace("%s", "?"), params)


def test_missing_products_count_and_list_keep_actionable_rules(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.create_function("btrim", 1, lambda value: value.strip() if value is not None else None)
    conn.executescript(
        """
        CREATE TABLE cves (
            cve_id TEXT PRIMARY KEY,
            description_text TEXT,
            reference_domains_json TEXT,
            cve_products_checked_at TEXT,
            published_at TEXT
        );
        CREATE TABLE cve_products (cve_id TEXT);
        CREATE TABLE cve_product_versions (cve_id TEXT);
        INSERT INTO cves VALUES
            ('CVE-A', 'description', NULL, NULL, '2026-09-01'),
            ('CVE-B', 'description', NULL, NULL, '2026-09-02'),
            ('CVE-C', 'description', NULL, NULL, '2026-09-03'),
            ('CVE-D', 'description', NULL, NULL, '2026-09-04'),
            ('CVE-E', '', '[]', NULL, '2026-09-05'),
            ('CVE-F', 'description', NULL, '2026-09-06', '2026-09-06'),
            ('CVE-G', '', '["example.com"]', NULL, '2026-09-07');
        INSERT INTO cve_products VALUES ('CVE-B'), ('CVE-D');
        INSERT INTO cve_product_versions VALUES ('CVE-C'), ('CVE-D');
        """
    )
    monkeypatch.setattr(storage, "_table_exists", lambda _conn, table: table == "cves")
    monkeypatch.setattr(
        storage, "_table_columns", lambda _conn, _table: {"cve_products_checked_at"}
    )
    conn = _CompatConn(conn)

    assert storage.count_cve_ids_missing_products(conn) == 4
    assert storage.list_cve_ids_missing_products(conn) == [
        "CVE-G", "CVE-C", "CVE-B", "CVE-A"
    ]
    assert storage.list_cve_ids_missing_products(conn, limit=2) == ["CVE-G", "CVE-C"]
