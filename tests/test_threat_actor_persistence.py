from __future__ import annotations

import uuid

from sempervigil.models import Article
from sempervigil.storage import (
    add_threat_actor_alias,
    get_article_threat_actors,
    get_cve_threat_actors,
    init_db,
    insert_articles,
    link_article_threat_actor,
    link_cve_threat_actor,
    upsert_cve,
    upsert_threat_actor,
)
from sempervigil.utils import utc_now_iso


def test_threat_actor_links_article_and_cve():
    conn = init_db()
    unique = uuid.uuid4().hex[:8]
    cve_id = f"CVE-2099-{unique}"
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
    article = Article(
        id=None,
        stable_id=f"stable-{unique}",
        original_url=f"https://example.com/{unique}",
        normalized_url=f"https://example.com/{unique}",
        title=f"Test Article {unique}",
        source_id=f"source-{unique}",
        published_at=now,
        published_at_source=None,
        ingested_at=now,
        summary=None,
        tags=[],
    )
    insert_articles(conn, [article])
    article_id = conn.execute(
        "SELECT id FROM articles WHERE stable_id = %s",
        (article.stable_id,),
    ).fetchone()[0]

    actor_id = upsert_threat_actor(
        conn,
        actor_key=f"actor-{unique}",
        display_name=f"APT {unique}",
        actor_type="apt",
        country="RU",
        confidence=85,
    )
    add_threat_actor_alias(conn, actor_id, f"Alias {unique}")
    link_article_threat_actor(conn, int(article_id), actor_id)
    link_cve_threat_actor(conn, cve_id, actor_id)

    article_actors = get_article_threat_actors(conn, int(article_id))
    cve_actors = get_cve_threat_actors(conn, cve_id)
    assert any(actor.get("actor_key") == f"actor-{unique}" for actor in article_actors)
    assert any(actor.get("actor_key") == f"actor-{unique}" for actor in cve_actors)
    assert any(
        (actor.get("aliases") and f"Alias {unique}" in actor.get("aliases", []))
        for actor in article_actors
    )
