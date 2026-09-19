"""Explicit qualified revision transactions. No migrations or runtime admission.

Qualification rows are a separate authority boundary: the promotion principal
must have SELECT only on them. No code here creates or approves qualifications.
"""
import json
import re
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from .event_projection import prepare
from .event_revision_store import locked_current_snapshot
from .investigation import _version
from .utils import utc_now_iso

SCHEMA = """
CREATE TABLE event_quote_qualifications (
    event_id TEXT NOT NULL REFERENCES events(id),
    qualification_id TEXT NOT NULL CHECK (qualification_id ~ '^[0-9a-f]{64}$'),
    qualification_json TEXT NOT NULL CHECK (octet_length(qualification_json) <= 32768),
    recorded_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY(event_id, qualification_id)
);
CREATE TABLE event_public_revisions (
    event_id TEXT NOT NULL REFERENCES events(id),
    revision_id TEXT NOT NULL CHECK (revision_id ~ '^[0-9a-f]{64}$'),
    qualification_id TEXT NOT NULL,
    predecessor TEXT,
    bundle_json TEXT NOT NULL CHECK (octet_length(bundle_json) <= 3100000),
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(event_id, revision_id),
    FOREIGN KEY(event_id, qualification_id)
        REFERENCES event_quote_qualifications(event_id, qualification_id),
    FOREIGN KEY(event_id, predecessor) REFERENCES event_public_revisions(event_id, revision_id)
);
CREATE TABLE event_public_pointers (
    event_id TEXT PRIMARY KEY REFERENCES events(id),
    revision_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(event_id, revision_id) REFERENCES event_public_revisions(event_id, revision_id)
);
CREATE FUNCTION guard_event_qualification() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'qualification records are immutable';
    END IF;
    IF ROW(NEW.event_id,NEW.qualification_id,NEW.qualification_json,NEW.recorded_at)
       IS DISTINCT FROM ROW(OLD.event_id,OLD.qualification_id,OLD.qualification_json,OLD.recorded_at)
       OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL THEN
        RAISE EXCEPTION 'qualification records are immutable except one-way revocation';
    END IF;
    PERFORM id FROM events WHERE id=OLD.event_id FOR UPDATE;
    RETURN NEW;
END;
$$;
CREATE TRIGGER event_qualification_guard BEFORE UPDATE OR DELETE ON event_quote_qualifications
    FOR EACH ROW EXECUTE FUNCTION guard_event_qualification()
"""


def promote(connection_factory, packet: dict, scope: dict, *, qualification_id: str,
            expected_predecessor: str | None) -> dict:
    """Commit an immutable revision and pointer against locked current sources.

Caller must separately authorize publication. This is not an API handler or a
semantic reviewer. Conflicts abort; no automatic retries or builds are performed.
"""
    identities = [qualification_id] + ([expected_predecessor] if expected_predecessor is not None else [])
    for identity in identities:
        if type(identity) is not str or not re.fullmatch(r"[0-9a-f]{64}", identity):
            raise ValueError("invalid_publication_identity")
    with locked_current_snapshot(connection_factory, packet) as conn:
        # A queue/model worker must not be able to mint its own trusted records.
        writable = conn.execute("""SELECT
            has_table_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("qualification_read_only_role_required")
        guarded = conn.execute("""SELECT EXISTS(SELECT 1 FROM pg_trigger
            WHERE tgrelid='event_quote_qualifications'::regclass
              AND tgname='event_qualification_guard' AND tgenabled IN ('O','A')
              AND NOT tgisinternal)""").fetchone()[0]
        if not guarded:
            raise ValueError("qualification_revocation_guard_required")
        event_id = packet["event"]["id"]
        row = conn.execute("""SELECT qualification_json,revoked_at FROM event_quote_qualifications
            WHERE event_id=%s AND qualification_id=%s""",
            (event_id, qualification_id)).fetchone()
        if row is None or row[1] is not None:
            raise ValueError("qualification_unavailable")
        qualification = json.loads(row[0])
        if _version(qualification) != qualification_id:
            raise ValueError("qualification_integrity_failure")
        projection = prepare(packet, scope, qualification,
                             trusted_qualification_ids=frozenset({qualification_id}),
                             predecessor=expected_predecessor)
        revision = projection["revision_id"]
        bundle = {"packet": packet, "scope": scope, "qualification": qualification,
                  "predecessor": expected_predecessor}
        encoded = json.dumps(bundle, sort_keys=True, ensure_ascii=True)
        if len(encoded.encode()) > 3100000:
            raise ValueError("public_revision_too_large")
        pointer = conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s FOR UPDATE NOWAIT",
                               (event_id,)).fetchone()
        current = pointer[0] if pointer else None
        if current not in (expected_predecessor, revision):
            raise ValueError("publication_predecessor_conflict")
        conn.execute("""INSERT INTO event_public_revisions
            (event_id,revision_id,qualification_id,predecessor,bundle_json,recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (event_id,revision_id) DO NOTHING""",
            (event_id, revision, qualification_id, expected_predecessor, encoded, utc_now_iso()))
        existing = conn.execute("""SELECT qualification_id,predecessor,bundle_json
            FROM event_public_revisions WHERE event_id=%s AND revision_id=%s""",
            (event_id, revision)).fetchone()
        if existing is None or existing[:2] != (qualification_id, expected_predecessor):
            raise ValueError("public_revision_conflict")
        from .event_render import resolve
        resolve(json.loads(existing[2]), event_id=event_id, expected_revision=revision)
        if current != revision:
            conn.execute("""INSERT INTO event_public_pointers(event_id,revision_id,updated_at)
                VALUES (%s,%s,%s) ON CONFLICT(event_id) DO UPDATE
                SET revision_id=EXCLUDED.revision_id,updated_at=EXCLUDED.updated_at""",
                (event_id, revision, utc_now_iso()))
    return {"event_id": event_id, "revision_id": revision,
            "status": "reused" if current == revision else "promoted"}


def load_export(connection_factory, event_ids: list[str]) -> dict:
    """Read one bounded, consistent authorization snapshot, never legacy fallback.

Withheld and withdrawn identities remain managed. A future coordinator must handle
them explicitly, not drop these IDs from the maps and export legacy narratives.
This snapshot is not a lease: build activation still needs revocation coordination.
"""
    if (type(event_ids) is not list or len(event_ids) > 20
            or any(type(v) is not str or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", v) for v in event_ids)
            or len(set(event_ids)) != len(event_ids)):
        raise ValueError("invalid_publication_export_ids")
    result = {"qualified_revisions": {}, "promoted_revision_ids": {},
              "managed_event_ids": [], "withheld": {}, "withdrawn": {}}
    if not event_ids:
        return result
    from .event_review import snapshot
    from .event_revision_store import source_version
    from .event_render import resolve
    from .investigation import READ_SCOPE, EVIDENCE_SCOPE
    with connection_factory() as conn:
        if conn.autocommit or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            raise ValueError("dedicated_export_transaction_required")
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET LOCAL statement_timeout='3s'")
        rows = conn.execute("""SELECT p.event_id,p.revision_id,r.qualification_id,r.bundle_json,
                   q.qualification_json,q.revoked_at
            FROM event_public_pointers p
            LEFT JOIN event_public_revisions r ON r.event_id=p.event_id AND r.revision_id=p.revision_id
            LEFT JOIN event_quote_qualifications q ON q.event_id=r.event_id AND q.qualification_id=r.qualification_id
            WHERE p.event_id=ANY(%s) ORDER BY p.event_id""", (event_ids,)).fetchall()

        @contextmanager
        def session():
            with conn.cursor(row_factory=dict_row) as cursor:
                yield cursor

        for event_id, revision, qid, raw, qraw, revoked in rows:
            result["managed_event_ids"].append(event_id)
            if raw is None or qraw is None:
                raise ValueError("broken_publication_reference")
            if revoked is not None:
                result["withdrawn"][event_id] = "qualification_revoked"
                continue
            bundle, qualification = json.loads(raw), json.loads(qraw)
            if _version(qualification) != qid or bundle.get("qualification") != qualification:
                raise ValueError("qualification_integrity_failure")
            _, projection = resolve(bundle, event_id=event_id, expected_revision=revision)
            try:
                current = snapshot(session, event_id=event_id, aliases=bundle["packet"]["aliases"],
                                   scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
            except ValueError as exc:
                if str(exc) != "event_unavailable":
                    raise
                result["withdrawn"][event_id] = "event_unavailable"
                continue
            cited = {entry["article_id"] for entry in projection["entries"]}
            present = {document["article_id"] for document in current["documents"]}
            if cited - present or any(o["reason"] == "unavailable" for o in current["omissions"]):
                result["withdrawn"][event_id] = "evidence_unavailable"
            elif source_version(current) != source_version(bundle["packet"]):
                result["withheld"][event_id] = "evidence_changed"
            else:
                result["qualified_revisions"][event_id] = bundle
                result["promoted_revision_ids"][event_id] = revision
    return result
