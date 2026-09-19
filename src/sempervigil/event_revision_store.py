"""Opt-in immutable private snapshots. No publication or schema initialization."""
import json
import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from .event_review import MAX_DOCUMENTS, MAX_PACKET_BYTES, snapshot, validate_packet
from .event_revision import MAX_BYTES, validate_receipt
from .utils import utc_now_iso
from .investigation import EVIDENCE_SCOPE, READ_SCOPE, _version

SCHEMA = """
CREATE TABLE IF NOT EXISTS event_private_revisions (
    event_id TEXT NOT NULL REFERENCES events(id),
    revision_id TEXT NOT NULL CHECK (revision_id ~ '^[0-9a-f]{64}$'),
    packet_version TEXT NOT NULL CHECK (packet_version ~ '^[0-9a-f]{64}$'),
    packet_json TEXT NOT NULL CHECK (octet_length(packet_json) <= 3000000),
    receipt_json TEXT NOT NULL CHECK (octet_length(receipt_json) <= 65536),
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (event_id, revision_id)
)
"""


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_PRIVATE_REVISION_STORE", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_private_revision_store_enablement")
    return value == "1"


def persist(connection_factory, packet: dict, raw: bytes, descriptor: dict, *,
            artifact: str, html: bytes) -> dict:
    """Own an insert-only transaction; an existing revision must match exactly.

    A snapshot can become stale immediately after collection. This stores history,
    not a current-input assertion, qualification result or publication pointer.
    """
    packet_raw = json.dumps(packet, sort_keys=True, ensure_ascii=True).encode()
    packet = validate_packet(packet_raw)
    receipt = validate_receipt(raw, descriptor, packet_version=packet["packet_version"],
                               event_id=packet["event"]["id"], artifact=artifact, html=html)
    from .event_assessment import validate_assessment
    validate_assessment(receipt["assessment"], packet)
    if receipt["generation_version"] is None:
        raise ValueError("revision_generation_required")
    # Canonical bytes make duplicate comparison independent of JSON formatting.
    receipt_raw = json.dumps(receipt, sort_keys=True, ensure_ascii=True).encode()
    if len(packet_raw) > MAX_PACKET_BYTES or len(receipt_raw) > MAX_BYTES:
        raise ValueError("oversized_private_revision")
    event_id, revision_id = packet["event"]["id"], descriptor["version"]
    with connection_factory() as conn:
        if conn.autocommit or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            raise ValueError("dedicated_revision_transaction_required")
        conn.execute("SET LOCAL lock_timeout = '2s'")
        conn.execute("SET LOCAL statement_timeout = '3s'")
        row = conn.execute("""INSERT INTO event_private_revisions
            (event_id,revision_id,packet_version,packet_json,receipt_json,recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (event_id,revision_id) DO NOTHING
            RETURNING revision_id""", (event_id, revision_id, packet["packet_version"],
                packet_raw.decode(), receipt_raw.decode(), utc_now_iso())).fetchone()
        stored = conn.execute("""SELECT packet_version,packet_json,receipt_json
            FROM event_private_revisions WHERE event_id=%s AND revision_id=%s""",
            (event_id, revision_id)).fetchone()
        if stored != (packet["packet_version"], packet_raw.decode(), receipt_raw.decode()):
            raise ValueError("private_revision_conflict")
    return {"status": "stored" if row else "reused", "version": revision_id,
            "public_eligible": False}


def persist_if_enabled(dsn: str, packet: dict, raw: bytes, descriptor: dict, *,
                       artifact: str, html: bytes) -> dict | None:
    if not enabled():
        return None
    return persist(lambda: psycopg.connect(dsn, connect_timeout=5), packet, raw, descriptor,
                   artifact=artifact, html=html)


def source_version(packet: dict) -> str:
    """Exclude only bookkeeping timestamps, not source text or coverage."""
    packet = validate_packet(json.dumps(packet).encode())
    body = {k: v for k, v in packet.items() if k != "packet_version"}
    body["event"] = {k: v for k, v in body["event"].items() if k != "updated_at"}
    return _version(body)


@contextmanager
def locked_current_snapshot(connection_factory, packet: dict):
    """Yield a short caller-owned promotion window, not publication approval.

    Requires the production event_articles foreign keys. The event FOR UPDATE
    lock blocks FK-backed membership inserts; existing memberships and all cited
    articles are also locked. NOWAIT refuses contention instead of stalling ingest.
    Consumers must perform no inference, network calls or file build in this block.
    """
    packet = validate_packet(json.dumps(packet).encode())
    if packet["omissions"] or packet["links_truncated"]:
        raise ValueError("incomplete_revision_snapshot")
    with connection_factory() as conn:
        if conn.autocommit or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            raise ValueError("dedicated_revision_transaction_required")
        conn.execute("SET LOCAL statement_timeout = '3s'")
        conn.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'")
        guarded = conn.execute("""SELECT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attname='event_id'
            JOIN pg_attribute b ON b.attrelid=c.confrelid AND b.attname='id'
            WHERE c.conrelid='event_articles'::regclass AND c.confrelid='events'::regclass
              AND c.contype='f' AND c.convalidated AND NOT c.condeferrable
              AND c.conkey=ARRAY[a.attnum] AND c.confkey=ARRAY[b.attnum])""").fetchone()[0]
        if not guarded:
            raise ValueError("revision_membership_constraint_required")
        event = conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE NOWAIT",
                             (packet["event"]["id"],)).fetchone()
        if event is None:
            raise ValueError("revision_event_unavailable")
        links = conn.execute("""SELECT article_id FROM event_articles WHERE event_id=%s
            ORDER BY article_id LIMIT %s FOR SHARE NOWAIT""",
            (packet["event"]["id"], MAX_DOCUMENTS + 1)).fetchall()
        if len(links) > MAX_DOCUMENTS:
            raise ValueError("incomplete_revision_snapshot")
        identities = [row[0] for row in links]
        if identities != sorted(d["article_id"] for d in packet["documents"]):
            raise ValueError("stale_revision_snapshot")
        conn.execute("SELECT id FROM articles WHERE id=ANY(%s) ORDER BY id FOR SHARE NOWAIT",
                     (identities,)).fetchall()

        @contextmanager
        def read_session():
            with conn.cursor(row_factory=dict_row) as cursor:
                yield cursor

        current = snapshot(read_session, event_id=packet["event"]["id"], aliases=packet["aliases"],
                           scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
        if source_version(current) != source_version(packet):
            raise ValueError("stale_revision_snapshot")
        yield conn
