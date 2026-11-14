"""Actual scheduler SQL and durable receipts; no model or search transport."""
import json
import pytest
from test_event_source_reports_postgres import database
from sempervigil import event_reassessment_automation as scheduler
DETAIL_RECOVERY = scheduler._resume_detail_filter_hold


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


def seed_fallback(conn, monkeypatch):
    conn.execute("INSERT INTO event_reassessment_cases VALUES('evt_fbi','ledger_fbi','held',0,'2026-10-01','old hold')")
    conn.execute("INSERT INTO event_ledger_compositions VALUES('ledger_fbi','elc_fbi','held',%s)",
                 (json.dumps({'ledger_revision_id': 'elr_fbi', 'fallback': {'retained': True}}),))
    conn.execute("""INSERT INTO jobs(id,job_type,status,payload_json,result_json,finished_at)
        VALUES('audit_original','event_composition_audit','succeeded',%s,%s,'2026-10-01')""",
        (json.dumps({'composition_id': 'elc_fbi'}), json.dumps({'audit': {'ready': False}})))
    conn.commit()
    monkeypatch.setattr('sempervigil.event_ledger.get_revision', lambda *_a, **_k: {'ledger': {}})


def test_held_only_tick_commits_terminal_and_case_receipts(queue, monkeypatch):
    conn, factory = queue; seed_fallback(conn, monkeypatch)
    monkeypatch.setattr('sempervigil.event_composition_audit_jobs.remediate_fallback',
                        lambda *_: {'status': 'held', 'reason': 'nonconvergent'})
    assert scheduler.tick(conn)[0]['status'] == 'held'
    with factory() as reopened:
        assert reopened.execute('SELECT count(*) FROM settings WHERE key=%s',
            (scheduler.TERMINAL_PREFIX+'elc_fbi:audit_original',)).fetchone()[0] == 1
        assert scheduler._resume_audited_fallback(reopened) is None
    conn.execute("INSERT INTO event_reassessment_cases VALUES('evt_active',NULL,'active',1,'2026-10-01',NULL)")
    conn.commit()
    monkeypatch.setattr(scheduler, 'advance', lambda c, eid: scheduler._hold(c, eid, 'terminal'))
    assert scheduler.tick(conn)[0]['status'] == 'held'
    with factory() as reopened:
        assert reopened.execute('SELECT status FROM event_reassessment_cases WHERE event_id=%s',
                                ('evt_active',)).fetchone()[0] == 'held'
        assert reopened.execute('SELECT value FROM settings WHERE key=%s',
                                (scheduler.CHECKED_PREFIX+'evt_active',)).fetchone()
        assert reopened.execute("SELECT count(*) FROM jobs WHERE status='queued'").fetchone()[0] == 0


def test_existing_orchestrator_lease_serializes_recovery_across_helper_commit(queue, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from uuid import uuid4
    from sempervigil.storage import try_acquire_lease, release_lease
    conn, factory = queue; seed_fallback(conn, monkeypatch)
    lease_name = 'fixture-orchestrator-'+uuid4().hex
    entered, release = Event(), Event(); attempts = []
    def remediate(c, composition_id, *_):
        attempts.append(composition_id)
        c.commit()  # Actual remediation stores immutable child artifacts this way.
        entered.set()
        assert release.wait(5)
        return {'status': 'held', 'reason': 'nonconvergent'}
    monkeypatch.setattr('sempervigil.event_composition_audit_jobs.remediate_fallback', remediate)
    def recover():
        with factory() as session:
            if not try_acquire_lease(session, lease_name, 'fixture', 120):
                return {'status': 'lease_busy'}
            try: return scheduler.tick(session)
            finally: release_lease(session, lease_name, 'fixture')
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(recover)
        try:
            assert entered.wait(5)
            second = pool.submit(recover).result(timeout=5)
            assert second == {'status': 'lease_busy'}
        finally: release.set()
        assert first.result(timeout=5)[0]['status'] == 'held'
    assert attempts == ['elc_fbi'] and recover() == []


@pytest.mark.parametrize('worker_result', ['failed', 'succeeded', 'submit_exception'])
def test_fresh_curation_admission_stops_turn_even_if_worker_finishes_immediately(queue, monkeypatch, worker_result):
    from sempervigil.storage import enqueue_job
    conn, factory = queue
    conn.execute('''ALTER TABLE event_reassessment_cases ADD COLUMN snapshot_version TEXT,
        ADD COLUMN snapshot_json TEXT;
        CREATE TABLE article_evidence_revisions(article_id INTEGER,source_version TEXT,
          generation_version TEXT,revision_id TEXT,status TEXT,created_at TEXT);
        CREATE TABLE incident_candidates(event_id TEXT,evidence_revision_id TEXT,status TEXT,
          selected_fact_ids_json TEXT,selected_fact_sections_json TEXT);''')
    for eid, priority in [('evt_first', 0), ('evt_later', 1)]:
        packet = {'event': {'title': 'Synthetic incident'}, 'articles': [
            {'article_id': 1, 'source_version': 'source'}, {'article_id': 2, 'source_version': 'source'}]}
        conn.execute('''INSERT INTO event_reassessment_cases
            (event_id,status,priority,updated_at,snapshot_version,snapshot_json)
            VALUES(%s,'active',%s,'2026-10-01','snapshot',%s)''', (eid, priority, json.dumps(packet)))
    conn.execute("INSERT INTO article_evidence_revisions VALUES(1,'source','generation','revision1','accepted','2026-10-01'),(2,'source','generation','revision2','accepted','2026-10-01')")
    conn.commit()
    monkeypatch.setattr('sempervigil.event_reassessment.snapshot', lambda *_: {'snapshot_version': 'snapshot'})
    monkeypatch.setattr('sempervigil.event_reassessment._pending_articles', lambda *_: set())
    monkeypatch.setattr('sempervigil.event_reassessment.queue_next_evidence', lambda *_a, **_k: {'status': 'unchanged'})
    monkeypatch.setattr(scheduler, '_accepted_ledger_source_rows', lambda *_: [])
    monkeypatch.setattr('sempervigil.article_review_jobs.configuration', lambda *_: ({}, {}, {}, 'generation'))
    calls = []
    def submit(c, eid, revision):
        calls.append((eid, revision))
        job = enqueue_job(c, 'event_fact_curate', {'event_id': eid}, queue_name='openai', max_attempts=1)
        # Independent worker commits after submit, before advance reads status.
        with factory() as worker:
            worker.execute('UPDATE jobs SET status=%s,error=%s WHERE id=%s',
                           ('succeeded' if worker_result == 'succeeded' else 'failed', 'synthetic worker error', job))
        if worker_result == 'submit_exception': raise ValueError('synthetic post-commit submit error')
        return job
    monkeypatch.setattr('sempervigil.event_fact_curation_jobs.submit', submit)
    result = scheduler.tick(conn)
    assert len(result) == 1 and calls == [('evt_first', 'revision1')]
    assert result[0]['status'] == ('succeeded' if worker_result == 'succeeded' else 'held')
    assert len(result[0]['admitted_job_ids']) == 1
    assert conn.execute('SELECT count(*) FROM jobs').fetchone()[0] == 1
    assert conn.execute("SELECT status FROM event_reassessment_cases WHERE event_id='evt_later'").fetchone()[0] == 'active'


@pytest.mark.parametrize('fresh', [True, False])
def test_recovery_admission_stops_turn_on_immediate_worker_failure_but_reused_hold_yields(queue, monkeypatch, fresh):
    from sempervigil.storage import enqueue_job
    conn, factory = queue
    conn.execute('''ALTER TABLE event_ledger_compositions ADD COLUMN ledger_revision_id TEXT,
        ADD COLUMN reviewed_by TEXT,ADD COLUMN created_at TEXT;''')
    conn.execute("INSERT INTO event_reassessment_cases VALUES('evt_recovery','ledger','held',0,'2026-10-01','event_composition_filter_audit_invalid'),('evt_viable',NULL,'active',1,'2026-10-01',NULL)")
    conn.execute("INSERT INTO event_ledger_revisions VALUES('ledger','revision','accepted')")
    conn.execute("INSERT INTO event_ledger_compositions VALUES('ledger','composition','held','{}','revision','policy:event-composition-audit-v1','2026-10-01')")
    conn.commit()
    monkeypatch.setattr(scheduler, '_resume_detail_filter_hold', DETAIL_RECOVERY)
    monkeypatch.setattr(scheduler, '_filter_detail_failures', lambda *_: None)
    prior = None
    if not fresh:
        prior = enqueue_job(conn, 'event_composition_audit', {'composition_id': 'composition'}, queue_name='openai')
        conn.execute("UPDATE jobs SET status='failed' WHERE id=%s", (prior,)); conn.commit()
    def submit(c, composition):
        if prior: return prior
        job = enqueue_job(c, 'event_composition_audit', {'composition_id': composition}, queue_name='openai')
        with factory() as worker: worker.execute("UPDATE jobs SET status='failed',error='synthetic immediate failure' WHERE id=%s", (job,))
        return job
    monkeypatch.setattr('sempervigil.event_composition_audit_jobs.submit', submit)
    advanced = []
    def advance(c, eid):
        advanced.append(eid)
        job = enqueue_job(c, 'synthetic_advance', {'event_id': eid})
        return {'status': 'queued', 'job_id': job}
    monkeypatch.setattr(scheduler, 'advance', advance)
    result = scheduler.tick(conn)
    assert result[0]['status'] == 'held'
    assert bool(result[0].get('admitted_job_ids')) is fresh
    assert advanced == ([] if fresh else ['evt_viable'])
    assert len(result) == (1 if fresh else 2)
    assert conn.execute('SELECT count(*) FROM jobs').fetchone()[0] == (1 if fresh else 2)


@pytest.mark.parametrize('phase', ['composition', 'audit', 'repair'])
def test_composition_phase_immediate_failure_cannot_admit_second_work_in_same_turn(queue, monkeypatch, phase):
    from sempervigil.storage import enqueue_job
    conn, factory = queue
    conn.execute('''ALTER TABLE event_reassessment_cases ADD COLUMN snapshot_version TEXT,ADD COLUMN snapshot_json TEXT;
        ALTER TABLE event_ledger_revisions ADD COLUMN created_at TEXT,ADD COLUMN ledger_json TEXT;
        ALTER TABLE event_ledger_compositions ADD COLUMN ledger_revision_id TEXT,ADD COLUMN generation_version TEXT,
          ADD COLUMN reviewed_by TEXT,ADD COLUMN created_at TEXT;''')
    packet = json.dumps({'event': {'title': 'Synthetic incident'}, 'articles': []})
    for eid, priority in [('evt_first', 0), ('evt_later', 1)]:
        conn.execute('''INSERT INTO event_reassessment_cases
            (event_id,status,ledger_id,priority,updated_at,snapshot_version,snapshot_json)
            VALUES(%s,'active','ledger',%s,'2026-10-01','snapshot',%s)''', (eid, priority, packet))
    conn.execute("INSERT INTO event_ledger_revisions VALUES('ledger','revision','accepted','2026-10-01','{}')")
    if phase != 'composition':
        conn.execute('''INSERT INTO event_ledger_compositions VALUES
            ('ledger','composition',%s,'{}','revision','generation','policy:event-composition-audit-v1','2026-10-01')''',
            ('held' if phase == 'repair' else 'unreviewed',))
    if phase == 'repair':
        conn.execute("""INSERT INTO jobs(id,job_type,status,payload_json,result_json,finished_at)
            VALUES('seed_audit','event_composition_audit','succeeded','{"composition_id":"composition"}',
              '{"audit":{"ready":false}}','2026-10-01')""")
    conn.commit()
    monkeypatch.setattr('sempervigil.event_reassessment.snapshot', lambda *_: {'snapshot_version': 'snapshot'})
    monkeypatch.setattr('sempervigil.event_reassessment._pending_articles', lambda *_: set())
    monkeypatch.setattr('sempervigil.event_reassessment.queue_next_evidence', lambda *_a, **_k: {'status': 'unchanged'})
    monkeypatch.setattr(scheduler, '_accepted_ledger_source_rows', lambda *_: [(1, 'https://one.test/a'), (2, 'https://two.test/b')])
    monkeypatch.setattr('sempervigil.article_review_jobs.configuration', lambda *_: ({}, {}, {}, 'generation'))
    monkeypatch.setattr(scheduler, '_candidate_rows', lambda *_: [])
    monkeypatch.setattr('sempervigil.event_ledger._lineage_current', lambda *_: True)
    monkeypatch.setattr('sempervigil.event_ledger.get_revision', lambda *_a, **_k: {'ledger': {}})
    monkeypatch.setattr('sempervigil.event_composition_jobs.configuration', lambda *_: ({}, {}, 'generation'))
    monkeypatch.setattr(scheduler, '_repaired_derivative', lambda *_: None)
    monkeypatch.setattr(scheduler, '_filter_detail_failures', lambda *_: None)
    monkeypatch.setattr(scheduler, '_is_repaired_composition', lambda *_: False)
    admitted = []
    def submit(c, *_):
        if admitted: return admitted[0]  # Existing failed artifact, no fresh retry.
        job = enqueue_job(c, 'event_composition_'+phase, {}, queue_name='openai')
        admitted.append(job)
        with factory() as worker: worker.execute("UPDATE jobs SET status='failed',error='input_size' WHERE id=%s", (job,))
        return job
    module = {'composition': 'event_composition_jobs', 'audit': 'event_composition_audit_jobs',
              'repair': 'event_composition_repair_jobs'}[phase]
    monkeypatch.setattr('sempervigil.'+module+'.submit', submit)
    fallbacks = []
    def fallback(c, *_):
        fallbacks.append(True)
        job = enqueue_job(c, 'event_composition_audit', {'composition_id': 'fallback'}, queue_name='openai')
        return {'status': 'queued', 'job_id': job}
    monkeypatch.setattr(scheduler, '_zero_output_repair_fallback', fallback)
    result = scheduler.tick(conn)
    assert len(result) == 1 and result[0]['admitted_job_ids'] == admitted
    assert result[0]['status'] == ('pending' if phase == 'repair' else 'held')
    assert not fallbacks
    if phase == 'repair':
        monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', '["evt_first"]')
        resumed = scheduler.tick(conn)
        assert resumed[0]['action'] == 'composition_repair_extractive_fallback'
        assert len(resumed[0]['admitted_job_ids']) == 1 and len(admitted) == 1
        assert fallbacks == [True]
