"""Opt-in immutable private snapshots. No publication or schema initialization."""
import json
import os

import psycopg

from .event_review import MAX_PACKET_BYTES, validate_packet
from .event_revision import MAX_BYTES, validate_receipt
from .utils import utc_now_iso

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
