import copy

from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import (
    add_watchlist_vendor,
    compute_scope_for_cves,
    get_cve,
    init_db,
    link_cve_product,
    upsert_cve,
    upsert_product,
    upsert_vendor,
)


def _seed_runtime_config(tmp_path):
    data_dir = tmp_path / "data"
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["paths"]["data_dir"] = str(data_dir)
    config["paths"]["state_db"] = str(data_dir / "state.sqlite3")
    config["paths"]["output_dir"] = str(tmp_path / "site" / "content" / "posts")
    config["paths"]["run_reports_dir"] = str(data_dir / "reports")
    config["publishing"]["json_index_path"] = str(
        tmp_path / "site" / "static" / "sempervigil" / "index.json"
    )
    set_runtime_config(conn, config)
    return conn


def test_watchlist_vendor_marks_cve_in_scope(tmp_path):
    conn = _seed_runtime_config(tmp_path)
    cve_id = "CVE-2025-0001"
    upsert_cve(
        conn,
        cve_id=cve_id,
        published_at="2025-01-01T00:00:00Z",
        last_modified_at="2025-01-02T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=9.0,
        preferred_base_severity="CRITICAL",
        preferred_vector="AV:N/AC:L",
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Test CVE description",
        affected_products=["Exchange"],
        affected_cpes=["cpe:2.3:a:microsoft:exchange:*:*:*:*:*:*:*:*"],
        reference_domains=["example.com"],
    )
    vendor_id = upsert_vendor(conn, "Microsoft")
    product_id, _ = upsert_product(conn, vendor_id, "Exchange")
    link_cve_product(conn, cve_id, product_id)

    add_watchlist_vendor(conn, "Microsoft")
    compute_scope_for_cves(conn, [cve_id], min_cvss=None)

    detail = get_cve(conn, cve_id)
    assert detail["in_scope"] is True
    assert any("matched_vendor:" in reason for reason in detail["scope_reasons"])
