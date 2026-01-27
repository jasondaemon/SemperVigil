from sempervigil.storage import (
    init_db,
    link_cve_product,
    upsert_cve,
    upsert_event_for_cve,
    upsert_product,
    upsert_vendor,
)
from sempervigil.utils import utc_now_iso, utc_now_iso_offset


def _seed_cve(conn, cve_id: str, published_at: str) -> None:
    upsert_cve(
        conn,
        cve_id=cve_id,
        published_at=published_at,
        last_modified_at=published_at,
        preferred_cvss_version="3.1",
        preferred_base_score=9.8,
        preferred_base_severity="CRITICAL",
        preferred_vector=None,
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Test CVE",
        affected_products=None,
        affected_cpes=None,
        reference_domains=None,
    )


def test_event_merge_shared_product(tmp_path):
    conn = init_db()
    vendor_id = upsert_vendor(conn, "Acme")
    product_id, _ = upsert_product(conn, vendor_id, "Widget")
    now = utc_now_iso()
    _seed_cve(conn, "CVE-2025-0001", now)
    _seed_cve(conn, "CVE-2025-0002", now)
    link_cve_product(conn, "CVE-2025-0001", product_id)
    link_cve_product(conn, "CVE-2025-0002", product_id)

    event_id_one, _ = upsert_event_for_cve(
        conn,
        cve_id="CVE-2025-0001",
        published_at=now,
        window_days=14,
        min_shared_products=1,
    )
    event_id_two, _ = upsert_event_for_cve(
        conn,
        cve_id="CVE-2025-0002",
        published_at=now,
        window_days=14,
        min_shared_products=1,
    )

    assert event_id_one == event_id_two


def test_event_separate_outside_window(tmp_path):
    conn = init_db()
    vendor_id = upsert_vendor(conn, "Acme")
    product_id, _ = upsert_product(conn, vendor_id, "Widget")
    now = utc_now_iso()
    _seed_cve(conn, "CVE-2025-1000", now)
    _seed_cve(conn, "CVE-2025-2000", now)
    link_cve_product(conn, "CVE-2025-1000", product_id)
    link_cve_product(conn, "CVE-2025-2000", product_id)

    event_id_one, _ = upsert_event_for_cve(
        conn,
        cve_id="CVE-2025-1000",
        published_at=now,
        window_days=7,
        min_shared_products=1,
    )
    old_seen = utc_now_iso_offset(seconds=-(15 * 86400))
    conn.execute("UPDATE events SET last_seen_at = ? WHERE id = ?", (old_seen, event_id_one))
    conn.commit()
    event_id_two, _ = upsert_event_for_cve(
        conn,
        cve_id="CVE-2025-2000",
        published_at=now,
        window_days=7,
        min_shared_products=1,
    )

    assert event_id_one != event_id_two
