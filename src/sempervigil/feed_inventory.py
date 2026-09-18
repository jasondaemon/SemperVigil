"""Read-only dependency fingerprints for incremental daily feed exports.

Hash export inputs, not check/job timestamps: a completed enrichment check is not
necessarily a content change. SQL returns one small fingerprint per date rather
than materializing all historical feed payloads in the application.
"""
from __future__ import annotations

from typing import Any

from .storage import _brief_day_from, _table_columns, _table_exists


ARTICLE_FIELDS = (
    "id", "source_id", "title", "original_url", "published_at", "ingested_at",
    "summary_llm", "meta_json",
)
CVE_FIELDS = (
    "cve_id", "description_text", "published_at", "last_modified_at",
    "preferred_cvss_version", "preferred_base_score", "preferred_base_severity",
    "preferred_vector", "epss_score", "epss_percentile", "epss_date",
    "epss_checked_at", "cvss_v31_json", "cvss_v40_json", "cvss_v31_list_json",
    "cvss_v40_list_json", "affected_products_json", "affected_cpes_json",
    "reference_domains_json",
)


def feed_inventory_query(conn: Any, source_icons: tuple[str, ...] = ()) -> tuple[str, tuple]:
    """Build SQL using only fixed identifiers; all fallback dates are parameters."""
    ctes = []
    parts = []
    params: list[object] = []

    def projection(alias: str, fields: tuple[str, ...], columns: set[str]) -> str:
        return "jsonb_build_array(" + ", ".join(
            f"{alias}.{field}" if field in columns else "NULL" for field in fields
        ) + ")::text"

    for kind, table, key, fields in (
        ("article", "articles", "id", ARTICLE_FIELDS),
        ("cve", "cves", "cve_id", CVE_FIELDS),
    ):
        if not _table_exists(conn, table):
            continue
        columns = set(_table_columns(conn, table))
        if kind == "article":
            # Match list_articles_for_day's application-timezone fallback.
            fallback = conn.execute(
                "SELECT id, COALESCE(NULLIF(published_at, ''), ingested_at) "
                "FROM articles WHERE NULLIF(brief_day, '') IS NULL"
            ).fetchall()
            ids, days = [], []
            for article_id, timestamp in fallback:
                if timestamp:
                    ids.append(article_id)
                    days.append(_brief_day_from(str(timestamp)))
            ctes.append("fallback_days AS (SELECT * FROM unnest(%s::bigint[], %s::text[]) AS f(id, day))")
            params.extend([ids, days])
            day = "COALESCE(NULLIF(b.brief_day, ''), f.day)"
            extra = "LEFT JOIN fallback_days f ON f.id = b.id"
        else:
            day = "substr(COALESCE(b.published_at, b.last_modified_at), 1, 10)"
            extra = ""
        ctes.append(
            f"{kind}_base AS MATERIALIZED (SELECT b.{key} AS owner, {day} AS day, "
            f"{projection('b', fields, columns)} AS payload FROM {table} b {extra})"
        )
        parts.append(
            f"SELECT day, '{kind}' AS kind, owner::text, 'base' AS dependency, "
            f"md5(payload) AS digest FROM {kind}_base"
        )

        def dependency(name: str, tables: tuple[str, ...], joins: str, payload: str) -> None:
            if all(_table_exists(conn, t) for t in tables):
                parts.append(
                    f"SELECT b.day, '{kind}', b.owner::text, '{name}', "
                    f"md5(jsonb_build_array({payload})::text) FROM {kind}_base b {joins}"
                )

        links = f"{kind}_products"
        owner_key = "article_id" if kind == "article" else "cve_id"
        dependency("products", (links, "products", "vendors"),
                   f"JOIN {links} l ON l.{owner_key}=b.owner JOIN products p ON p.id=l.product_id "
                   "JOIN vendors v ON v.id=p.vendor_id",
                   "p.id, p.product_key, p.name_norm, p.display_name, v.name_norm, v.display_name")
        actors = f"{kind}_threat_actors"
        actor_join = (f"JOIN {actors} l ON l.{owner_key}=b.owner "
                      "JOIN threat_actors t ON t.id=l.actor_id")
        dependency("actors", (actors, "threat_actors"), actor_join,
                   "t.id, t.actor_key, t.display_name, t.actor_type, t.country, t.confidence")
        dependency("actor_aliases", (actors, "threat_actors", "threat_actor_aliases"),
                   actor_join + " JOIN threat_actor_aliases a ON a.actor_id=t.id", "t.id, a.alias")
        if kind == "article":
            has_sources = _table_exists(conn, "sources")
            dependency("source", ("articles", "sources"),
                       "JOIN articles a ON a.id=b.owner LEFT JOIN sources s ON s.id=a.source_id",
                       "s.name, a.source_id = ANY(%s::text[])")
            if has_sources:
                params.append(list(source_icons))
            dependency("tags", ("article_tags",),
                       "JOIN article_tags t ON t.article_id=b.owner", "t.tag")
            dependency("events", ("event_articles", "events"),
                       "JOIN event_articles l ON l.article_id=b.owner JOIN events e ON e.id=l.event_id",
                       "e.event_key")
        else:
            dependency("versions", ("cve_product_versions", "products"),
                       "JOIN cve_product_versions l ON l.cve_id=b.owner JOIN products p ON p.id=l.product_id",
                       "p.display_name, p.name_norm, p.product_key, l.version, l.source")
            dependency("kev", ("cve_kev",), "JOIN cve_kev k ON k.cve_id=b.owner", "k.cve_id, k.due_date")
    if not parts:
        return "SELECT NULL::text, NULL::text, 0, 0 WHERE FALSE", ()
    sql = "WITH " + ",\n".join(ctes) + ", fragments AS (" + "\nUNION ALL\n".join(parts) + ")\n"
    sql += """
        SELECT day,
               md5(string_agg(kind || ':' || owner || ':' || dependency || ':' || digest,
                              ',' ORDER BY kind, owner, dependency, digest)),
               count(*) FILTER (WHERE kind='article' AND dependency='base'),
               count(*) FILTER (WHERE kind='cve' AND dependency='base')
        FROM fragments WHERE day IS NOT NULL AND day <> ''
        GROUP BY day ORDER BY day
    """
    return sql, tuple(params)


def list_feed_content_inventory(conn: Any, source_icons: tuple[str, ...] = ()) -> list[dict[str, object]]:
    sql, params = feed_inventory_query(conn, source_icons)
    return [
        {"day": str(day), "content_signature": signature,
         "article_count": articles, "cve_count": cves}
        for day, signature, articles, cves in conn.execute(sql, params).fetchall()
    ]
