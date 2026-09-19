"""Explicit qualified revision transactions. No migrations or runtime admission.

Qualification rows are a separate authority boundary: the promotion principal
must have SELECT only on them. No code here creates or approves qualifications.
"""
import json
import re

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
