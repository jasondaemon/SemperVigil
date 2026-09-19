"""Disposable PostgreSQL admission/claim/result test; inference is mocked."""
import json
import os
import psycopg

from sempervigil import article_review_jobs as review, storage


def test_private_article_queue_lifecycle_and_no_content_writes(monkeypatch):
    monkeypatch.setenv('SV_ARTICLE_REVIEW_ENABLED', '1')
    monkeypatch.setattr(review, 'configuration', lambda conn: ({}, {}, {}, 'a'*64))
    article = {'id': 7, 'title': 'Acme', 'content_text': 'Acme reported an outage.',
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
        for table in ('articles', 'events', 'llm_runs'):
            conn.execute('CREATE TABLE '+table+' (id INTEGER)')
            created.append(table)
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
            if request['phase'] == 'context':
                return json.dumps({'facts':[{'passage_ids':['p001'],
                    'statement':article['content_text'], 'kind':'reported_fact',
                    'date_text':None, 'date_role':'none'}]})
            return json.dumps({'summary_sentences':[{'text':article['content_text'], 'fact_ids':['f1']}], 'bullets':[]})
        result = review.run(conn, claimed, generate=generate)
        storage.complete_job(conn, first, result=result)
        assert storage.get_job(conn, first).result['attempts'] == 2
        assert review.submit(conn, [7]) == first
        assert before == {table: conn.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in watched}
    finally:
        conn.rollback()
        for table in reversed(created):
            conn.execute('DROP TABLE IF EXISTS '+table)
        conn.commit()
        conn.close()
