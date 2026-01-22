from sempervigil.models import Article
from sempervigil.signals import build_cve_evidence, extract_cve_ids
from sempervigil.storage import get_article_id, init_db, insert_articles, upsert_cve_links


def test_upsert_cve_links_idempotent(tmp_path):
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))
    conn.execute(
        """
        CREATE TABLE article_cves (
            article_id INTEGER NOT NULL,
            cve_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            confidence_band TEXT NOT NULL,
            matched_by TEXT NOT NULL,
            inference_level TEXT NOT NULL,
            reasons_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(article_id, cve_id)
        )
        """
    )
    conn.commit()

    article = Article(
        id=None,
        stable_id="stable-1",
        original_url="https://example.com/cve-2025-1111",
        normalized_url="https://example.com/cve-2025-1111",
        title="CVE-2025-1111 issue",
        source_id="source-1",
        published_at=None,
        published_at_source=None,
        ingested_at="2025-01-01T00:00:00Z",
        summary="Summary mentions CVE-2025-1111",
        tags=[],
    )
    insert_articles(conn, [article])
    article_id = get_article_id(conn, "source-1", "stable-1")
    assert article_id is not None

    cve_ids = extract_cve_ids([article.title, article.summary or "", article.original_url])
    evidence = build_cve_evidence(article, cve_ids)

    upsert_cve_links(conn, article_id, cve_ids, evidence)
    upsert_cve_links(conn, article_id, cve_ids, evidence)

    cursor = conn.execute("SELECT COUNT(*) FROM article_cves")
    assert cursor.fetchone()[0] == 1

    row = conn.execute(
        """
        SELECT confidence, confidence_band, matched_by, inference_level, reasons_json, evidence_json
        FROM article_cves
        """
    ).fetchone()
    assert row is not None
    confidence, band, matched_by, inference_level, reasons_json, evidence_json = row
    assert confidence == 1.0
    assert band == "linked"
    assert matched_by == "explicit"
    assert inference_level == "explicit"
    assert "rule.cve.explicit" in reasons_json
    assert "CVE-2025-1111" in evidence_json
    assert "https://example.com/cve-2025-1111" in evidence_json
