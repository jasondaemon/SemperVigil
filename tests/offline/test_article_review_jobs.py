import copy
import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
from sempervigil import article_review_jobs as review, article_evidence as evidence, worker

pytestmark = pytest.mark.offline
GEN = 'a' * 64
ARTICLE = {'id': 7, 'title': 'Acme', 'content_text': 'Acme said recovery remains unconfirmed.',
           'summary_llm': '{"summary":"Old"}', 'context_llm': '{}'}


@pytest.fixture
def harness(monkeypatch):
    monkeypatch.setenv('SV_ARTICLE_REVIEW_ENABLED', '1')
    monkeypatch.setattr(review, 'configuration', lambda conn: ({}, {}, {}, GEN))
    monkeypatch.setattr(review, 'snapshot', lambda conn, n: copy.deepcopy({**ARTICLE, 'id': n}))
    saved = []
    monkeypatch.setattr(review, 'update_job_result', lambda conn, jid, result: saved.append(copy.deepcopy(result)) or True)
    return saved


def job(count=1):
    return SimpleNamespace(id='test', job_type=review.JOB_TYPE, result=None, attempt_count=1,
        max_attempts=1, queue_name='llm_local', status='running', payload={
            'workflow': evidence.WORKFLOW, 'generation': GEN,
            'articles': [{**ARTICLE, 'id': 7+i} for i in range(count)]})


def response(request):
    if request['phase'] == 'context':
        return json.dumps({'facts': [{'evidence_quote': ARTICLE['content_text'],
            'statement': ARTICLE['content_text'], 'kind': 'reported_fact', 'attribution_quote': 'Acme',
            'uncertainty_quote': 'unconfirmed', 'date_quote': None, 'date_role': 'none'}],
            'uncertainties': ['Recovery remains unconfirmed.']})
    return json.dumps({'summary_sentences': [{'text': ARTICLE['content_text'], 'fact_ids': ['f1']}], 'bullets': []})


def test_three_articles_use_six_serial_calls_and_only_private_results(harness):
    current = job(3); before = copy.deepcopy(current.payload); phases = []
    def generate(request):
        assert harness[-1]['attempts'] == len(phases)+1
        assert harness[-1]['articles'][-1]['phases'][-1]['status'] == 'started'
        phases.append(request['phase'])
        return response(request)
    result = review.run(object(), current, generate=generate)
    assert phases == ['context', 'summary']*3
    assert result['attempts'] == 6 and not result['public_eligible']
    assert current.payload == before
    assert all(r['feed_preview']['summary'] == ARTICLE['content_text'] for r in result['articles'])


def test_invalid_context_skips_summary_without_repair(harness):
    result = review.run(object(), job(), generate=lambda request: '{}')
    assert result['attempts'] == 1
    assert result['articles'][0]['phases'][0]['status'] == 'invalid'
    assert 'feed_preview' not in result['articles'][0]


def test_transport_failure_stops_cohort_without_retry(harness):
    def fail(request): raise TimeoutError()
    result = review.run(object(), job(3), generate=fail)
    assert result['attempts'] == 1 and result['status'] == 'stopped'


@pytest.mark.parametrize('change', [{'attempt_count': 2}, {'max_attempts': 0},
    {'result': {'attempts': 1}}, {'queue_name': 'fetch'}, {'status': 'queued'}])
def test_replays_and_wrong_lane_rejected(harness, change):
    current = job()
    for key, value in change.items(): setattr(current, key, value)
    with pytest.raises(ValueError): review.run(object(), current, generate=lambda r: pytest.fail('inference'))
    assert not harness


def test_changed_baseline_or_generation_rejected_before_calls(harness, monkeypatch):
    current = job(); current.payload['generation'] = 'b'*64
    with pytest.raises(ValueError): review.run(object(), current)
    current = job(); current.payload['articles'][0]['summary_llm'] = 'changed'
    with pytest.raises(ValueError): review.run(object(), current)
    assert not harness


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv('SV_ARTICLE_REVIEW_ENABLED', raising=False)
    with pytest.raises(PermissionError): review.run(object(), job())


@pytest.mark.parametrize('kind', ['summary', 'context'])
@pytest.mark.parametrize('parsed', [{'summary': 'old'}, ['old'], None])
def test_normal_handlers_preserve_legacy_payloads_by_default(monkeypatch, kind, parsed):
    monkeypatch.delenv('SV_ARTICLE_STRICT_VALIDATION', raising=False)
    payload, text = worker._article_result_payload({'parsed': parsed, 'raw': 'raw', 'schema_valid': False}, kind)
    expected = parsed if parsed is not None else {'summary' if kind=='summary' else 'context_pack': 'raw'}
    assert json.loads(payload) == expected


def test_private_http_attempt_is_not_retried(monkeypatch):
    from sempervigil.llm import router
    calls = []
    def fail(*args, **kwargs):
        calls.append(1)
        raise HTTPError('https://example.test',503,'unavailable',{},io.BytesIO(b'busy'))
    monkeypatch.setattr(router.urllib.request, 'urlopen', fail)
    with pytest.raises(Exception):
        router._http_request('POST','https://example.test',{}, {}, {'type':'openai_compatible'},
                             context={'stage':review.JOB_TYPE})
    assert len(calls) == 1


def test_registry_and_model_admission():
    from sempervigil.storage import get_queue_name_for_job_type
    assert get_queue_name_for_job_type(review.JOB_TYPE) == 'llm_local'
    assert review.JOB_TYPE in worker._LLM_JOB_TYPES
    assert review.JOB_TYPE in worker.QUEUE_WORKER_TYPES['llm_local']
    assert review.JOB_TYPE in worker.HANDLED_JOB_TYPES


def test_admission_pins_baseline_and_deduplicates_completed_jobs(harness, monkeypatch):
    class Connection:
        existing = None
        def execute(self, sql, params):
            assert sql.startswith(('SELECT pg_advisory_xact_lock', 'SELECT id FROM jobs'))
            return SimpleNamespace(fetchone=lambda: self.existing)
        def commit(self): pass
    conn = Connection(); calls = []
    def enqueue(conn, kind, payload, **kwargs):
        calls.append((kind, payload, kwargs))
        return 'one-job'
    monkeypatch.setattr(review, 'enqueue_job', enqueue)
    assert review.submit(conn, [9,7,8]) == 'one-job'
    kind, payload, options = calls[0]
    assert kind == review.JOB_TYPE
    assert [a['id'] for a in payload['articles']] == [7,8,9]
    assert payload['generation'] == GEN
    assert options['max_attempts'] == 1 and options['priority'] == -10
    assert options['queue_name'] == 'llm_local'
    conn.existing = ('one-job',)
    assert review.submit(conn, [7,8,9]) == 'one-job'
    assert len(calls) == 1


@pytest.mark.parametrize('ids', [[], [True], [0], [7,7], [1,2,3,4], ['7']])
def test_bad_admission_never_queues(harness, ids):
    with pytest.raises(ValueError): review.submit(object(), ids)


def test_admin_request_rejects_payload_injection_and_inference(harness, monkeypatch):
    from pydantic import ValidationError
    from sempervigil import admin
    with pytest.raises(ValidationError):
        admin.ArticlePrivateReviewRequest(article_ids=[7], prompt='override')
    with pytest.raises(ValidationError):
        admin.ArticlePrivateReviewRequest(article_ids=[True])
    monkeypatch.delenv('SV_ADMIN_TOKEN', raising=False)
    with pytest.raises(admin.HTTPException) as caught:
        admin.api_article_private_review(admin.ArticlePrivateReviewRequest(article_ids=[7]))
    assert caught.value.status_code == 503


def test_source_change_between_phases_prevents_second_call(harness, monkeypatch):
    def generate(request):
        monkeypatch.setattr(review, 'snapshot', lambda conn, n: {**ARTICLE, 'content_text': 'Edited'})
        return response(request)
    with pytest.raises(ValueError, match='baseline_changed'):
        review.run(object(), job(), generate=generate)
    assert harness[-1]['attempts'] == 1


def test_reservation_failure_never_sends_request(harness, monkeypatch):
    monkeypatch.setattr(review, 'update_job_result', lambda *a: False)
    with pytest.raises(ValueError, match='not_running'):
        review.run(object(), job(), generate=lambda r: pytest.fail('inference'))


def test_default_error_path_preserves_deployed_behavior(monkeypatch):
    monkeypatch.delenv('SV_ARTICLE_STRICT_VALIDATION', raising=False)
    writes = []
    monkeypatch.setattr(worker, 'update_article_summary', lambda *a, **kw: writes.append(kw))
    monkeypatch.setattr(worker, 'record_article_enrichment_error', lambda *a, **kw: pytest.fail('strict enabled'))
    worker._article_enrichment_error(object(), 7, kind='summary', error='missing_content')
    assert writes[0]['summary_llm'] is None and writes[0]['summary_error'] == 'missing_content'
