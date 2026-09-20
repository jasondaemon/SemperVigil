"""Disposable PostgreSQL admission/claim/result test; inference is mocked."""
import json
import os
import psycopg

from sempervigil import (article_review_jobs as review, article_evidence as evidence,
                         article_evidence_store as evidence_store, event_composition,
                         event_ledger, incident_candidates, migrations_pg, storage)


def test_private_article_queue_lifecycle_and_no_content_writes(monkeypatch):
    monkeypatch.setenv('SV_ARTICLE_REVIEW_ENABLED', '1')
    monkeypatch.setattr(review, 'configuration', lambda conn: ({}, {}, {}, 'a'*64))
    article = {'id': 7, 'title': 'Acme', 'content_text': 'Acme reported a ransomware attack.',
               'summary_llm': '{"summary":"baseline"}', 'context_llm': '{}'}
    monkeypatch.setattr(review, 'snapshot', lambda conn, n: dict(article))
    conn = psycopg.connect(os.environ['SV_TEST_DB_URL'])
    created = []
    try:
        assert conn.execute('SELECT current_database()').fetchone()[0] == 'sempervigil_test'
        # Match queue columns; no production schema initialization/migrations.
        conn.execute('''CREATE TABLE jobs (
            id TEXT PRIMARY KEY, job_type TEXT, status TEXT, priority INTEGER,
            payload_json TEXT, result_json TEXT, requested_at TEXT, started_at TEXT,
            finished_at TEXT, locked_by TEXT, locked_at TEXT, error TEXT, queue_name TEXT,
            attempt_count INTEGER, max_attempts INTEGER, available_at TEXT,
            heartbeat_at TEXT, lease_expires_at TEXT, parent_job_id TEXT, dedupe_key TEXT)''')
        created.append('jobs')
        conn.execute('CREATE TABLE articles (id BIGINT PRIMARY KEY, title TEXT NOT NULL)')
        fixture_articles = {
            7: article,
            8: {'id': 8, 'title': 'Beta follow-up', 'content_text': 'Beta reported that the actors exfiltrated account records.'},
            9: {'id': 9, 'title': 'Beta correction', 'content_text': 'Beta corrected the count of accounts compromised after its investigation.'},
            10: {'id': 10, 'title': 'Independent conflict', 'content_text': 'A researcher disputed the reported count of compromised accounts.'},
        }
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO articles (id, title) VALUES (%s, %s)',
                               [(item['id'], item['title']) for item in fixture_articles.values()])
        created.append('articles')
        for table in ('events', 'llm_runs'):
            conn.execute('CREATE TABLE '+table+' (id INTEGER)')
            created.append(table)
        migrations_pg._migrate_article_evidence_revisions(conn)
        created.append('article_evidence_revisions')
        migrations_pg._migrate_incident_candidates(conn)
        created.append('incident_candidates')
        migrations_pg._migrate_event_ledger_revisions(conn)
        created.extend(['event_ledger_revisions', 'event_ledger_revision_sources'])
        migrations_pg._migrate_event_ledger_compositions(conn)
        created.append('event_ledger_compositions')
        conn.commit()
        watched = ('articles', 'events', 'llm_runs')
        before = {table: conn.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in watched}
        first = review.submit(conn, [7])
        assert review.submit(conn, [7]) == first
        claimed = storage.claim_next_job(conn, 'private-pg-test', allowed_types=[review.JOB_TYPE],
                                        allowed_queues=['llm_local'])
        assert claimed.id == first
        assert claimed.attempt_count == 0 and claimed.max_attempts == 1
        def generate(request):
            persisted = storage.get_job(conn, first)
            assert persisted.result['attempts'] > 0
            return json.dumps({'facts':[{'passage_ids':['p001'],
                'statement':article['content_text'], 'kind':'reported_fact',
                'date_text':None, 'date_role':'none'}]})
        result = review.run(conn, claimed, generate=generate)
        storage.complete_job(conn, first, result=result)
        saved = storage.get_job(conn, first).result
        assert saved['attempts'] == 1
        revision_id = saved['articles'][0]['phases'][0]['revision_id']
        assert conn.execute("SELECT status FROM article_evidence_revisions WHERE revision_id=%s",
                            (revision_id,)).fetchone()[0] == 'unreviewed'
        monkeypatch.setattr(evidence_store, 'get_article_by_id',
                            lambda conn, article_id: dict(fixture_articles[article_id]))
        accepted = evidence_store.review(conn, revision_id, 'accept', reason='', reviewer='test')
        assert accepted['status'] == 'accepted'
        assert evidence_store.list_revisions(conn, status='accepted')[0]['revision_id'] == revision_id
        candidate = incident_candidates.project(conn, revision_id)
        assert candidate['status'] == 'suggested'
        assert candidate['signals']['kind'] == 'ransomware'
        old_id = 'ic_' + ('0' * 64)
        conn.execute('UPDATE incident_candidates SET candidate_id=%s, signals_json=%s WHERE evidence_revision_id=%s',
                     (old_id, '{"projection_version":"old"}', revision_id))
        conn.commit()
        candidate = incident_candidates.project(conn, revision_id)
        assert candidate['candidate_id'] != old_id
        assert candidate['signals']['projection_version'] == incident_candidates.PROJECTION_VERSION
        assert incident_candidates.project(conn, revision_id)['candidate_id'] == candidate['candidate_id']
        enrolled = incident_candidates.review(
            conn, candidate['candidate_id'], 'enroll', reason='', reviewer='test')
        assert enrolled['status'] == 'enrolled'
        assert incident_candidates.list_candidates(conn, status='enrolled')[0]['candidate_id'] == candidate['candidate_id']
        ledger = event_ledger.propose(conn, candidate['candidate_id'])
        assert ledger['status'] == 'proposed' and ledger['public_eligible'] is False
        assert event_ledger.propose(conn, candidate['candidate_id']) == {**ledger, 'reused': True}
        accepted_ledger = event_ledger.review(
            conn, ledger['revision_id'], 'accept', reason='', reviewer='test')
        assert accepted_ledger['status'] == 'accepted'
        replay = event_ledger.propose(conn, candidate['candidate_id'], ledger_id=ledger['ledger_id'])
        assert replay['revision_id'] == ledger['revision_id'] and replay['reused'] is True
        listed = event_ledger.list_revisions(conn, status='accepted')[0]
        assert listed['lineage_current'] is True
        assert listed['ledger']['facts'][0]['exact_passages'][0]['text'] == article['content_text']
        assert listed['ledger']['public_eligible'] is False
        active_fact = listed['ledger']['facts'][0]
        output = {'items': [{'section': 'overview', 'text': active_fact['statement'],
                             'fact_refs': ['F01'], 'date_text': ''}]}
        record = event_composition.validate(
            json.dumps(output).encode(), listed, '9' * 64)
        composition_id = event_composition.store_unreviewed(conn, record)
        accepted_composition = event_composition.review(
            conn, composition_id, 'accept', reason='', reviewer='test')
        assert accepted_composition['status'] == 'accepted'
        assert event_composition.list_compositions(
            conn, status='accepted')[0]['ledger_current'] is True

        def enrolled_candidate(article_id, generation):
            item = fixture_articles[article_id]
            generated = json.dumps({'facts': [{'passage_ids': ['p001'],
                'statement': item['content_text'], 'kind': 'reported_fact',
                'date_text': None, 'date_role': 'none'}]})
            record = evidence.validate_context(generated.encode(), item, generation * 64)
            evidence_id = evidence_store.store_unreviewed(conn, item, record)
            evidence_store.review(conn, evidence_id, 'accept', reason='', reviewer='test')
            projected = incident_candidates.project(conn, evidence_id)
            incident_candidates.review(conn, projected['candidate_id'], 'enroll', reason='', reviewer='test')
            return projected['candidate_id']

        additive_candidate = enrolled_candidate(8, 'd')
        additive = event_ledger.propose(
            conn, additive_candidate, ledger_id=ledger['ledger_id'], change_kind='additive')
        assert len(event_ledger.list_revisions(conn, status='proposed')[0]['change']['added_fact_ids']) == 1
        event_ledger.review(conn, additive['revision_id'], 'accept', reason='', reviewer='test')
        assert event_composition.list_compositions(
            conn, status='accepted')[0]['ledger_current'] is False

        current = event_ledger.list_revisions(conn, status='accepted')[0]
        correction_target = current['ledger']['facts'][0]['fact_id']
        correction_candidate = enrolled_candidate(9, 'e')
        correction = event_ledger.propose(
            conn, correction_candidate, ledger_id=ledger['ledger_id'], change_kind='correction',
            supersedes_fact_ids=[correction_target])
        event_ledger.review(conn, correction['revision_id'], 'accept', reason='', reviewer='test')
        corrected = event_ledger.list_revisions(conn, status='accepted')[0]
        assert correction_target in corrected['ledger']['superseded_fact_ids']

        conflict_candidate = enrolled_candidate(10, 'f')
        conflict = event_ledger.propose(
            conn, conflict_candidate, ledger_id=ledger['ledger_id'], change_kind='conflict',
            conflict_fact_ids=[correction_target])
        event_ledger.review(conn, conflict['revision_id'], 'hold', reason='requires adjudication', reviewer='test')
        event_ledger.review(conn, conflict['revision_id'], 'accept', reason='adjudicated', reviewer='test')
        conflicted = event_ledger.list_revisions(conn, status='accepted')[0]
        assert correction_target in conflicted['ledger']['conflict_fact_ids']
        raw = generate({'phase': 'context'})
        replacement = evidence.validate_context(raw.encode(), article, 'b'*64)
        replacement_id = evidence_store.store_unreviewed(conn, article, replacement)
        evidence_store.review(conn, replacement_id, 'hold', reason='needs comparison', reviewer='test')
        evidence_store.review(conn, replacement_id, 'accept', reason='compared', reviewer='test')
        statuses = dict(conn.execute(
            'SELECT revision_id, status FROM article_evidence_revisions').fetchall())
        assert statuses[revision_id] == 'superseded'
        assert statuses[replacement_id] == 'accepted'
        stale_candidate = next(item for item in incident_candidates.list_candidates(
            conn, status='enrolled') if item['candidate_id'] == candidate['candidate_id'])
        assert stale_candidate['evidence_status'] == 'superseded'
        assert stale_candidate['eligible_for_curation'] is False
        stale_ledger = event_ledger.list_revisions(conn, status='accepted')[0]
        assert stale_ledger['lineage_current'] is False
        event_ledger.review(conn, conflict['revision_id'], 'withdraw',
                            reason='test withdrawal', reviewer='test')
        assert event_ledger.list_revisions(conn, status='withdrawn')[0]['revision_id'] == conflict['revision_id']
        rejected = evidence.validate_context(raw.encode(), article, 'c'*64)
        rejected_id = evidence_store.store_unreviewed(conn, article, rejected)
        evidence_store.review(conn, rejected_id, 'reject', reason='omits impact', reviewer='test')
        assert conn.execute('SELECT status FROM article_evidence_revisions WHERE revision_id=%s',
                            (rejected_id,)).fetchone()[0] == 'rejected'
        assert review.submit(conn, [7]) == first
        assert before == {table: conn.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in watched}
    finally:
        conn.rollback()
        for table in reversed(created):
            conn.execute('DROP TABLE IF EXISTS '+table)
        conn.commit()
        conn.close()
