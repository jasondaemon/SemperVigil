"""Builder-owned qualified Events export and rendered-output verification."""
import hashlib
import json
import os
from pathlib import Path
import re
from contextlib import contextmanager

from bs4 import BeautifulSoup, NavigableString, Tag
import psycopg

from .event_activation import MANIFEST, MAX_EVENTS
from .event_publication_store import load_export
from .event_render import index_entry, render
from .utils import atomic_write_json

INDEX_PATH = "sempervigil/index/events.json"


def check_current(conn, packet: dict) -> None:
    """Short activation-time source lock window; the event row is already locked."""
    from psycopg.rows import dict_row
    from .event_review import snapshot, MAX_DOCUMENTS
    from .event_revision_store import source_version
    from .investigation import READ_SCOPE, EVIDENCE_SCOPE
    guarded = conn.execute("""SELECT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attname='event_id'
        JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attname='id'
        WHERE c.conrelid='event_articles'::regclass AND c.confrelid='events'::regclass
          AND c.contype='f' AND c.convalidated AND NOT c.condeferrable
          AND c.conkey=ARRAY[a.attnum] AND c.confkey=ARRAY[b.attnum])""").fetchone()[0]
    if not guarded:
        raise ValueError("revision_membership_constraint_required")
    links = conn.execute("""SELECT article_id FROM event_articles WHERE event_id=%s
        ORDER BY article_id LIMIT %s FOR SHARE NOWAIT""", (packet["event"]["id"], MAX_DOCUMENTS + 1)).fetchall()
    ids = [row[0] for row in links]
    if ids != sorted(d["article_id"] for d in packet["documents"]):
        raise ValueError("stale_revision_snapshot")
    conn.execute("SELECT id FROM articles WHERE id=ANY(%s) ORDER BY id FOR SHARE NOWAIT", (ids,)).fetchall()
    @contextmanager
    def session():
        with conn.cursor(row_factory=dict_row) as cursor:
            yield cursor
    current = snapshot(session, event_id=packet["event"]["id"], aliases=packet["aliases"],
                       scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
    if source_version(current) != source_version(packet):
        raise ValueError("stale_revision_snapshot")


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_PUBLICATION_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_publication_enablement")
    return value == "1"


def database():
    dsn = os.environ.get("SV_EVENT_ACTIVATION_DB_URL", "")
    if not dsn:
        raise ValueError("event_publication_database_required")
    return psycopg.connect(dsn, connect_timeout=3)


def fragment_identity(html: str) -> str:
    """Compare the complete qualified subtree despite harmless HTML minification."""
    soup = BeautifulSoup(html, "html.parser")
    fragments = soup.find_all(id="sv-event-coverage")
    if len(fragments) != 1:
        raise ValueError("event_release_fragment_missing_or_duplicate")
    def node(value):
        if isinstance(value, NavigableString):
            return " ".join(str(value).split()) or None
        if not isinstance(value, Tag):
            raise ValueError("invalid_event_release_fragment")
        children = [node(child) for child in value.children]
        return [value.name, sorted(value.attrs.items()), [child for child in children if child is not None]]
    return hashlib.sha256(json.dumps(node(fragments[0]), ensure_ascii=True, sort_keys=True).encode()).hexdigest()


def prepare_site(conn, config, logger) -> dict:
    """Called by the builder only. No Hugo invocation or live-release writes."""
    from .worker import _collect_published_events
    from .storage import get_event
    from .publish import write_events_authorized_snapshot
    with database() as read:
        read.execute("SET TRANSACTION READ ONLY")
        read.execute("SET LOCAL statement_timeout='3s'")
        pointers = dict(read.execute("SELECT event_id,revision_id FROM event_public_pointers ORDER BY event_id LIMIT %s",
                                     (MAX_EVENTS + 1,)).fetchall())
    if len(pointers) > MAX_EVENTS:
        raise ValueError("event_publication_inventory_too_large")
    authorization = load_export(database, list(pointers))
    if set(authorization["managed_event_ids"]) != set(pointers):
        raise ValueError("event_publication_inventory_changed")
    # A stale managed report must not block unrelated daily news publication.
    # Remove only that report until a fresh qualified revision is available.
    authorization["withdrawn"].update(authorization["withheld"])
    authorization["withheld"] = {}
    events, _ = _collect_published_events(conn)
    by_id = {event["id"]: event for event in events}
    slugs = {}
    for event_id in pointers:
        event = get_event(conn, event_id)
        if event is None:
            raise ValueError("managed_event_record_unavailable")
        slug = str(event.get("site_slug") or event_id)
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,200}", slug) or slug.lower() == "_index":
            raise ValueError("invalid_event_release_slug")
        slugs[event_id] = slug
        by_id[event_id] = event
    if len({s.casefold() for s in slugs.values()}) != len(slugs):
        raise ValueError("duplicate_event_release_slug")
    content = Path(config.paths.output_dir).parent
    static_root = content.parent / "static"
    static = static_root / "sempervigil"
    pages, index_path = write_events_authorized_snapshot(list(by_id.values()), str(content), str(static),
                                                       authorization=authorization)
    active = authorization["promoted_revision_ids"]
    manifest = {"workflow": "event-release-authorization-v2", "revisions": active,
        "withdrawn": {key: pointers[key] for key in authorization["withdrawn"]},
        "pages": slugs, "index_sha256": hashlib.sha256(Path(index_path).read_bytes()).hexdigest(),
        "fragments": {key: fragment_identity(render(authorization["qualified_revisions"][key],
                      event_id=key, expected_revision=value)[1]) for key, value in active.items()}}
    target = static_root / MANIFEST
    if target.is_symlink():
        raise ValueError("event_manifest_symlink")
    if not target.exists() or json.loads(target.read_bytes()) != manifest:
        atomic_write_json(str(target), manifest, indent=2)
    return {"published": len(active), "withdrawn": authorization["withdrawn"], "pages": len(pages)}


def verify_release(release: Path, manifest: dict, bundles: dict) -> None:
    """Bind the actual candidate page and index to database-approved quotations."""
    if manifest.get("workflow") != "event-release-authorization-v2":
        raise ValueError("event_release_bound_manifest_required")
    def read(relative: str, limit: int) -> bytes:
        path = release / relative
        if any(p.is_symlink() for p in [path, *path.parents] if p != release.parent):
            raise ValueError("event_release_symlink")
        with path.open("rb") as handle:
            raw = handle.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("event_release_file_too_large")
        return raw
    raw = read(INDEX_PATH, 8 * 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != manifest["index_sha256"]:
        raise ValueError("event_release_index_changed")
    entries = json.loads(raw)
    if type(entries) is not list or any(type(row) is not dict for row in entries):
        raise ValueError("invalid_event_release_index")
    ids = [row.get("event_id") for row in entries]
    if any(type(key) is not str for key in ids) or len(set(ids)) != len(ids):
        raise ValueError("invalid_event_release_index")
    by_id = {row["event_id"]: row for row in entries}
    for key, revision in manifest["revisions"].items():
        bundle = bundles[key]
        if by_id.get(key) != index_entry(bundle, event_id=key, expected_revision=revision):
            raise ValueError("event_release_index_projection_mismatch")
        expected = fragment_identity(render(bundle, event_id=key, expected_revision=revision)[1])
        page = read(f"events/{manifest['pages'][key]}/index.html", 2 * 1024 * 1024)
        if manifest["fragments"].get(key) != expected or fragment_identity(page.decode()) != expected:
            raise ValueError("event_release_page_projection_mismatch")
    for key in manifest["withdrawn"]:
        if key in by_id or (release / "events" / manifest["pages"][key] / "index.html").exists():
            raise ValueError("event_release_withdrawal_incomplete")
    if any(row.get("event_revision") and key not in manifest["revisions"] for key, row in by_id.items()):
        raise ValueError("event_release_unmanaged_revision")
