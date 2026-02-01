from uuid import uuid4

from sempervigil.storage import (
    init_db,
    link_cve_products_from_items,
    list_cve_tags,
    upsert_cve,
)
from sempervigil.utils import utc_now_iso


def test_llm_vendor_product_persistence_no_tags():
    conn = init_db()
    cve_id = f"CVE-2099-{uuid4().hex[:6]}"
    now = utc_now_iso()
    upsert_cve(
        conn,
        cve_id=cve_id,
        published_at=now,
        last_modified_at=now,
        preferred_cvss_version=None,
        preferred_base_score=None,
        preferred_base_severity=None,
        preferred_vector=None,
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="test",
    )
    stats = link_cve_products_from_items(
        conn,
        cve_id=cve_id,
        items=[{"vendor": "unknown", "product": "Frigate Professional", "versions": ["<=1.3.1"]}],
        source="llm",
    )
    assert stats["links_created"] == 0
    row = conn.execute(
        "SELECT COUNT(*) FROM cve_products WHERE cve_id = %s",
        (cve_id,),
    ).fetchone()
    assert int(row[0]) == 0
    row = conn.execute(
        "SELECT COUNT(*) FROM cve_product_versions WHERE cve_id = %s AND version = %s",
        (cve_id, "<=1.3.1"),
    ).fetchone()
    assert int(row[0]) == 0
    assert list_cve_tags(conn, cve_id) == []
