"""Persistence contract for immutable Event composition derivatives."""
import json
import os

import psycopg

from sempervigil import event_composition
from sempervigil.migrations_pg import _migrate_event_ledger_compositions


def _record(revision_id="elr_one", text="Acme reported an incident."):
    sections = {section: [] for section in event_composition.SECTIONS}
    sections["overview"] = [{"text": text, "fact_ids": ["fact_one"],
                             "claim_type": "sourced_finding", "confidence": None}]
    return {"workflow": event_composition.WORKFLOW, "ledger_id": "eld_one",
            "ledger_revision_id": revision_id, "generation_version": "a" * 64,
            "request_version": "b" * 64, "section_policy": event_composition.SECTION_POLICY,
            "sections": sections, "change": {}, "status": "unreviewed",
            "public_eligible": False}


def test_derivative_storage_is_distinct_idempotent_and_ledger_scoped():
    conn = psycopg.connect(os.environ["SV_TEST_DB_URL"])
    try:
        conn.execute("DROP TABLE IF EXISTS event_ledger_compositions")
        conn.execute("DROP TABLE IF EXISTS event_ledger_revisions")
        conn.execute("CREATE TABLE event_ledger_revisions (revision_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO event_ledger_revisions VALUES ('elr_one'),('elr_two')")
        _migrate_event_ledger_compositions(conn)
        source = _record()
        source_id = event_composition.store_unreviewed(conn, source)
        child = event_composition.derive(source, source_id, "test-derivative-v1", {"audit": "one"})
        child_id = event_composition.store_unreviewed(conn, child)
        assert child_id != source_id
        assert event_composition.store_unreviewed(conn, child) == child_id
        changed = event_composition.derive(
            {**source, "ledger_revision_id": "elr_two",
             "sections": {**source["sections"], "overview": [
                 {**source["sections"]["overview"][0], "text": "Acme updated the incident."}]}},
            source_id, "test-derivative-v1", {"audit": "two"})
        changed_id = event_composition.store_unreviewed(conn, changed)
        assert changed_id not in {source_id, child_id}
        rows = conn.execute(
            "SELECT composition_id,composition_json FROM event_ledger_compositions ORDER BY composition_id"
        ).fetchall()
        assert len(rows) == 3
        assert json.loads(next(value for key, value in rows if key == source_id)) == source
    finally:
        conn.rollback()
        conn.execute("DROP TABLE IF EXISTS event_ledger_compositions")
        conn.execute("DROP TABLE IF EXISTS event_ledger_revisions")
        conn.commit()
        conn.close()
