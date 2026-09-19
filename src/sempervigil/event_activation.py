"""Default-disabled, bounded Events authorization at the release switch.

The publication coordinator must bind this manifest to its prepared page/JSON
output. This guard checks authority, not prose quality or rendered-file integrity.
It is not a replacement for export preflight or an autonomous publication gate.
"""
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

import psycopg

from .event_render import resolve
from .investigation import _version

MANIFEST = ".event-publication.json"
MAX_BYTES = 16384
MAX_EVENTS = 20


def validate_manifest(value: dict) -> dict:
    """Require complete managed-event inventory, including deliberate removals."""
    if (type(value) is not dict or value.keys() != {"workflow", "revisions", "withdrawn"}
            or value["workflow"] != "event-release-authorization-v1"):
        raise ValueError("invalid_event_activation_manifest")
    for field in ("revisions", "withdrawn"):
        entries = value[field]
        if (type(entries) is not dict or len(entries) > MAX_EVENTS
                or any(type(k) is not str or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", k)
                       or type(v) is not str or not re.fullmatch(r"[0-9a-f]{64}", v)
                       for k, v in entries.items())):
            raise ValueError("invalid_event_activation_manifest")
    identities = set(value["revisions"]) | set(value["withdrawn"])
    if (len(identities) > MAX_EVENTS
            or set(value["revisions"]) & set(value["withdrawn"])):
        raise ValueError("invalid_event_activation_manifest")
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_event_activation_key")
        result[key] = value
    return result


def read_manifest(release: Path) -> dict:
    """Do not follow manifest or release-directory symlinks or read unbounded data."""
    directory = os.open(release, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(MANIFEST, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES:
                raise ValueError("invalid_event_activation_file")
            raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError("invalid_event_activation_file")
    finally:
        os.close(directory)
    return validate_manifest(json.loads(raw, object_pairs_hook=_unique_object))


def authorize_and_activate(connection_factory, manifest: dict, activate) -> None:
    """Hold authority locks through a short release-switch callback, never Hugo.

Pointer-table SHARE prevents inventory changes (including new managed events).
Event row locks coordinate with promotion and the one-way revocation trigger.
NOWAIT refuses contention rather than waiting behind ingestion or promotion.
The caller must supply a dedicated connection and a bounded local switch only.
"""
    manifest = validate_manifest(manifest)
    expected = {**manifest["revisions"], **manifest["withdrawn"]}
    with connection_factory() as conn:
        if conn.autocommit or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            raise ValueError("dedicated_activation_transaction_required")
        conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        conn.execute("SET LOCAL statement_timeout='3s'")
        conn.execute("SET LOCAL idle_in_transaction_session_timeout='5s'")
        writable = conn.execute("""SELECT
            has_table_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("qualification_read_only_role_required")
        conn.execute("LOCK TABLE event_public_pointers IN SHARE MODE NOWAIT")
        pointers = conn.execute("""SELECT event_id,revision_id FROM event_public_pointers
            ORDER BY event_id LIMIT %s""", (MAX_EVENTS + 1,)).fetchall()
        if len(pointers) > MAX_EVENTS or dict(pointers) != expected:
            raise ValueError("event_activation_inventory_changed")
        guarded = conn.execute("""SELECT EXISTS(SELECT 1 FROM pg_trigger
            WHERE tgrelid='event_quote_qualifications'::regclass
              AND tgname='event_qualification_guard' AND tgenabled IN ('O','A')
              AND NOT tgisinternal)""").fetchone()[0]
        if not guarded:
            raise ValueError("qualification_revocation_guard_required")
        locked = conn.execute("""SELECT id FROM events WHERE id=ANY(%s)
            ORDER BY id FOR UPDATE NOWAIT""", (sorted(expected),)).fetchall()
        if [row[0] for row in locked] != sorted(expected):
            raise ValueError("event_activation_event_unavailable")
        rows = conn.execute("""SELECT p.event_id,p.revision_id,r.bundle_json,
                   r.qualification_id,q.qualification_json,q.revoked_at
            FROM event_public_pointers p
            LEFT JOIN event_public_revisions r ON r.event_id=p.event_id AND r.revision_id=p.revision_id
            LEFT JOIN event_quote_qualifications q ON q.event_id=r.event_id AND q.qualification_id=r.qualification_id
            ORDER BY p.event_id LIMIT %s""", (MAX_EVENTS + 1,)).fetchall()
        if {row[0]: row[1] for row in rows} != expected:
            raise ValueError("event_activation_inventory_changed")
        for event_id, revision, raw, qid, qraw, revoked in rows:
            if raw is None or qraw is None:
                raise ValueError("broken_publication_reference")
            if event_id in manifest["withdrawn"]:
                continue
            if revoked is not None:
                raise ValueError("event_activation_qualification_revoked")
            bundle, qualification = json.loads(raw), json.loads(qraw)
            if _version(qualification) != qid or bundle.get("qualification") != qualification:
                raise ValueError("qualification_integrity_failure")
            resolve(bundle, event_id=event_id, expected_revision=revision)
        # Reproduction above is local CPU work. Refuse if the server expired the
        # idle transaction, and renew its short window before the bounded switch.
        conn.execute("SELECT 1").fetchone()
        activate()


def activate_release(release: Path, current: Path, *, connection_factory=None) -> None:
    """Switch only a sibling releases directory, using the existing ln operation."""
    release, current = release.absolute(), current.absolute()
    if (current.name != "current" or release.parent != current.parent / "releases"
            or release.is_symlink() or not (release / "index.html").is_file()):
        raise ValueError("invalid_event_activation_release")
    manifest = read_manifest(release)
    if connection_factory is None:
        dsn = os.environ.get("SV_EVENT_ACTIVATION_DB_URL", "")
        if not dsn:
            raise ValueError("event_activation_database_required")
        connection_factory = lambda: psycopg.connect(dsn, connect_timeout=3)
    authorize_and_activate(connection_factory, manifest,
                           lambda: subprocess.run(["ln", "-sfn", "releases/" + release.name,
                                                   str(current)], check=True, timeout=2))


def main() -> int:
    try:
        if os.environ.get("SV_EVENT_ACTIVATION_CHECK", "0") != "1" or len(sys.argv) != 3:
            raise ValueError("event_activation_check_not_enabled")
        activate_release(Path(sys.argv[1]), Path(sys.argv[2]))
    except Exception as exc:
        # Do not expose database URLs, SQL parameters, or private source text.
        safe_reasons = {"event_activation_check_not_enabled", "event_activation_database_required",
            "event_activation_inventory_changed", "event_activation_qualification_revoked",
            "event_activation_event_unavailable", "qualification_revocation_guard_required",
            "qualification_read_only_role_required", "qualification_integrity_failure",
            "broken_publication_reference", "invalid_event_activation_release",
            "invalid_event_activation_manifest", "invalid_event_activation_file",
            "duplicate_event_activation_key", "dedicated_activation_transaction_required"}
        reason = str(exc) if str(exc) in safe_reasons else type(exc).__name__
        print("Events release activation failed (" + reason + ").", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
