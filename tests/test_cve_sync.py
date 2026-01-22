from sempervigil.cve_sync import process_cve_item
from sempervigil.storage import init_db


def _make_cve_item(
    cve_id: str,
    v31_score: float,
    v31_severity: str,
    v31_vector: str,
    v40_score: float | None = None,
    v40_severity: str | None = None,
    v40_vector: str | None = None,
) -> dict:
    metrics = {
        "cvssMetricV31": [
            {
                "cvssData": {
                    "baseScore": v31_score,
                    "baseSeverity": v31_severity,
                    "vectorString": v31_vector,
                },
                "exploitabilityScore": 1.0,
                "impactScore": 1.0,
            }
        ]
    }
    if v40_score is not None:
        metrics["cvssMetricV40"] = [
            {
                "cvssData": {
                    "baseScore": v40_score,
                    "baseSeverity": v40_severity,
                    "vectorString": v40_vector,
                },
                "exploitabilityScore": 1.0,
                "impactScore": 1.0,
            }
        ]
    return {
        "id": cve_id,
        "published": "2025-01-01T00:00:00Z",
        "lastModified": "2025-01-02T00:00:00Z",
        "descriptions": [{"lang": "en", "value": "desc"}],
        "metrics": metrics,
    }


def test_snapshot_insert_creates_no_changes(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    item = _make_cve_item("CVE-2025-1111", 5.0, "MEDIUM", "AV:N/AC:L")
    result = process_cve_item(conn, item, prefer_v4=True)
    assert result.new_snapshot is True
    assert result.change_count == 0
    assert conn.execute("SELECT COUNT(*) FROM cves").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM cve_snapshots").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM cve_changes").fetchone()[0] == 0


def test_severity_upgrade_detection(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    first = _make_cve_item("CVE-2025-2222", 5.0, "MEDIUM", "AV:N/AC:L")
    second = _make_cve_item("CVE-2025-2222", 7.5, "HIGH", "AV:N/AC:L")
    process_cve_item(conn, first, prefer_v4=True)
    result = process_cve_item(conn, second, prefer_v4=True)
    assert result.change_count >= 1
    change = conn.execute(
        "SELECT change_type, from_severity, to_severity FROM cve_changes"
    ).fetchone()
    assert change[0] == "severity_upgrade"
    assert change[1] == "MEDIUM"
    assert change[2] == "HIGH"


def test_vector_change_detection(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    first = _make_cve_item("CVE-2025-3333", 5.0, "MEDIUM", "AV:N/AC:L")
    second = _make_cve_item("CVE-2025-3333", 5.0, "MEDIUM", "AV:L/AC:L")
    process_cve_item(conn, first, prefer_v4=True)
    process_cve_item(conn, second, prefer_v4=True)
    change = conn.execute(
        "SELECT change_type, vector_from, vector_to FROM cve_changes WHERE change_type = 'vector_change'"
    ).fetchone()
    assert change is not None
    assert change[1] == "AV:N/AC:L"
    assert change[2] == "AV:L/AC:L"


def test_preferred_severity_diff_on_v4_added(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    first = _make_cve_item("CVE-2025-4444", 7.0, "HIGH", "AV:N/AC:L")
    second = _make_cve_item(
        "CVE-2025-4444",
        7.0,
        "HIGH",
        "AV:N/AC:L",
        v40_score=9.0,
        v40_severity="CRITICAL",
        v40_vector="AV:N/AC:L/AT:N",
    )
    process_cve_item(conn, first, prefer_v4=True)
    process_cve_item(conn, second, prefer_v4=True)
    change_types = {
        row[0]
        for row in conn.execute("SELECT change_type FROM cve_changes").fetchall()
    }
    assert "cvss_version_added" in change_types
    assert "preferred_severity_diff" in change_types


def test_idempotent_rerun(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    item = _make_cve_item("CVE-2025-5555", 4.0, "LOW", "AV:N/AC:L")
    process_cve_item(conn, item, prefer_v4=True)
    process_cve_item(conn, item, prefer_v4=True)
    assert conn.execute("SELECT COUNT(*) FROM cve_snapshots").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM cve_changes").fetchone()[0] == 0
