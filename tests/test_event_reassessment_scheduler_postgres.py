"""Actual scheduler SQL and durable receipts; no model or search transport."""
import json
import pytest
from test_event_source_reports_postgres import database
from sempervigil import event_reassessment_automation as scheduler


@pytest.fixture
def queue(database, monkeypatch):
    conn, factory, namespace = database
    conn.execute("""CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT);
        CREATE TABLE event_reassessment_cases(event_id TEXT PRIMARY KEY,ledger_id TEXT,status TEXT,
          priority INTEGER,updated_at TEXT,decision_reason TEXT);
        CREATE TABLE event_ledger_revisions(ledger_id TEXT,revision_id TEXT,status TEXT);
        CREATE TABLE event_ledger_compositions(ledger_id TEXT,composition_id TEXT PRIMARY KEY,
          status TEXT,composition_json TEXT);""")
    conn.commit()
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED', '1')
    monkeypatch.delenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', raising=False)
    for name in ('_resume_curator_version_hold', '_resume_detail_filter_hold', '_resume_transient_composition_hold'):
        monkeypatch.setattr(scheduler, name, lambda _: None)
    return conn, factory


def test_terminal_audited_fallback_is_durable_and_does_not_starve_viable_case(queue, monkeypatch):
    conn, factory = queue
    raw = json.dumps({'ledger_revision_id': 'elr_fbi', 'fallback': {'retained': True}})
    decision = json.dumps({'audit': {'ready': False, 'reason': 'nonconvergent'}})
    conn.execute("""INSERT INTO event_reassessment_cases VALUES
        ('evt_fbi','ledger_fbi','held',0,'2026-10-01','old hold'),
        ('evt_active_hold',NULL,'active',1,'2026-10-01',NULL),
        ('evt_viable',NULL,'active',2,'2026-10-01',NULL),
        ('evt_later',NULL,'active',3,'2026-10-01',NULL)""")
    conn.execute("INSERT INTO event_ledger_compositions VALUES('ledger_fbi','elc_fbi','held',%s)", (raw,))
    conn.execute("""INSERT INTO jobs(id,job_type,status,payload_json,result_json,finished_at)
        VALUES('audit_original','event_composition_audit','succeeded',%s,%s,'2026-10-01')""",
        (json.dumps({'composition_id': 'elc_fbi'}), decision))
    conn.commit()
    attempts = []
    monkeypatch.setattr('sempervigil.event_ledger.get_revision', lambda *_a, **_k: {'ledger': {'original': True}})
    def remediate(*args):
        attempts.append(args[1]); return {'status': 'held', 'reason': 'audited fallback nonconvergent'}
    monkeypatch.setattr('sempervigil.event_composition_audit_jobs.remediate_fallback', remediate)
    seen = []
    def advance(c, eid):
        seen.append(eid)
        if eid == 'evt_active_hold': return scheduler._hold(c, eid, 'terminal active hold')
        c.execute("INSERT INTO jobs(id,job_type,status,payload_json) VALUES('viable_job','synthetic_advance','queued','{}')")
        c.commit()
        return {'status': 'queued', 'event_id': eid}
    monkeypatch.setattr(scheduler, 'advance', advance)
    results = scheduler.tick(conn)
    assert [r['status'] for r in results] == ['held', 'held', 'queued']
    assert seen == ['evt_active_hold', 'evt_viable'] and attempts == ['elc_fbi']
    assert conn.execute("SELECT count(*) FROM jobs WHERE status='queued'").fetchone()[0] == 1
    key = scheduler.TERMINAL_PREFIX+'elc_fbi:audit_original'
    receipt = json.loads(conn.execute('SELECT value FROM settings WHERE key=%s', (key,)).fetchone()[0])
    assert receipt['composition_id'] == 'elc_fbi' and receipt['audit_job_id'] == 'audit_original'
    # A fresh process/connection skips the unchanged artifact; it is not a cache.
    with factory() as reopened:
        assert scheduler._resume_audited_fallback(reopened) is None
    assert attempts == ['elc_fbi']
    assert conn.execute("SELECT composition_json FROM event_ledger_compositions WHERE composition_id='elc_fbi'").fetchone()[0] == raw
    assert conn.execute("SELECT result_json FROM jobs WHERE id='audit_original'").fetchone()[0] == decision
    # Only an explicit fresh audit identity makes this recovery eligible again.
    conn.execute("""INSERT INTO jobs(id,job_type,status,payload_json,result_json,finished_at)
        VALUES('audit_fresh','event_composition_audit','succeeded',%s,%s,'2026-10-02')""",
        (json.dumps({'composition_id': 'elc_fbi'}), json.dumps({'audit': {'ready': False, 'reason': 'fresh audit'}})))
    conn.commit()
    assert scheduler._resume_audited_fallback(conn)['status'] == 'held'
    assert attempts == ['elc_fbi', 'elc_fbi']
    assert scheduler._resume_audited_fallback(conn) is None


def test_pending_queue_larger_than_batch_rotates_without_mutating_case_identity(queue, monkeypatch):
    conn, _ = queue
    cases = [('evt_%02d'%i, None, 'active', i, '2026-10-01', None) for i in range(30)]
    with conn.cursor() as cur:
        cur.executemany('INSERT INTO event_reassessment_cases VALUES(%s,%s,%s,%s,%s,%s)', cases)
    conn.commit()
    seen = []
    def advance(c, eid):
        seen.append(eid); return {'status': 'pending', 'event_id': eid}
    monkeypatch.setattr(scheduler, 'advance', advance)
    assert len(scheduler.tick(conn)) == 25
    assert seen == [c[0] for c in cases[:25]]
    scheduler.tick(conn)
    assert seen[25:30] == [c[0] for c in cases[25:]]
    assert set(seen) == {c[0] for c in cases}
    assert conn.execute("SELECT count(*) FROM settings WHERE key LIKE 'event.reassessment.checked.v1:%'").fetchone()[0] == 30
    assert conn.execute('SELECT event_id,ledger_id,status,priority,updated_at,decision_reason FROM event_reassessment_cases ORDER BY priority').fetchall() == cases
    assert conn.execute('SELECT count(*) FROM jobs').fetchone()[0] == 0


def test_explicit_pause_skips_historical_recovery_and_all_paid_admission(queue, monkeypatch):
    conn, _ = queue
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', '[]')
    monkeypatch.setattr(scheduler, '_resume_audited_fallback', lambda _: pytest.fail('must not recover'))
    assert scheduler.tick(conn) == []
    assert conn.execute('SELECT count(*) FROM settings').fetchone()[0] == 0
    assert conn.execute('SELECT count(*) FROM jobs').fetchone()[0] == 0
