from uuid import uuid4

from sempervigil.storage import (
    get_product_id_by_vendor_name,
    get_vendor_id_by_name,
    init_db,
    link_cve_products_from_items,
    list_cve_tags,
    upsert_product,
    upsert_vendor,
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


def test_llm_vendor_product_persistence_accepts_non_latin_names():
    conn = init_db()
    vendor_display = "百度"
    product_display = "站长合集"
    vendor_id = upsert_vendor(conn, vendor_display)
    product_id, product_key = upsert_product(conn, vendor_id, product_display)

    assert vendor_id > 0
    assert product_id > 0
    assert product_key
    assert get_vendor_id_by_name(conn, vendor_display) == vendor_id
    assert get_product_id_by_vendor_name(conn, vendor_id, product_display) == product_id

    vendor_row = conn.execute(
        "SELECT display_name, name_norm FROM vendors WHERE id = %s",
        (vendor_id,),
    ).fetchone()
    product_row = conn.execute(
        "SELECT display_name, name_norm, product_key FROM products WHERE id = %s",
        (product_id,),
    ).fetchone()
    assert vendor_row is not None
    assert product_row is not None
    assert vendor_row[0] == vendor_display
    assert product_row[0] == product_display
    assert str(vendor_row[1]).startswith("u_")
    assert str(product_row[1]).startswith("u_")
