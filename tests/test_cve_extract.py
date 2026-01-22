from sempervigil.models import Article
from sempervigil.signals import build_cve_evidence, extract_cve_ids


def test_extract_cve_ids_normalizes_case_and_dedupes():
    text = "Patch for cve-2024-1234 and CVE-2024-1234 plus CVE-2025-99999."
    cves = extract_cve_ids([text])
    assert cves == ["CVE-2024-1234", "CVE-2025-99999"]


def test_extract_cve_ids_multiple_fields():
    cves = extract_cve_ids(["CVE-2023-1111", "See cve-2023-2222"])
    assert cves == ["CVE-2023-1111", "CVE-2023-2222"]


def test_build_cve_evidence_structure():
    article = Article(
        id=None,
        stable_id="abc",
        original_url="https://example.com/article",
        normalized_url="https://example.com/article",
        title="CVE-2025-12345 in the wild",
        source_id="source-1",
        published_at=None,
        published_at_source=None,
        ingested_at="2025-01-01T00:00:00Z",
        summary="Details about CVE-2025-12345",
        tags=[],
    )
    evidence = build_cve_evidence(article, ["CVE-2025-12345"])
    assert evidence["extracted_signals"]["cve_ids"] == ["CVE-2025-12345"]
    assert evidence["final_decision"]["rule_ids"] == ["rule.cve.explicit"]
    assert evidence["citations"]["urls"] == ["https://example.com/article"]
