"""Local HTTP only: enforce wall deadlines in worker threads and never retry."""
import concurrent.futures
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time

import pytest
from sempervigil import event_report_transport as transport
pytestmark = pytest.mark.offline


@pytest.mark.parametrize('seconds', [60, 120, 179, 241, float('inf'), True])
def test_actual_strong_editor_timeout_rejected(seconds):
    with pytest.raises(ValueError):
        transport.preflight({'model': 'gpt-5.6-sol', 'reasoning_effort': 'high',
                             'max_completion_tokens': 12000}, seconds)


def test_versioned_policy_bounds(monkeypatch):
    monkeypatch.delenv(transport.POLICY_ENV, raising=False)
    assert transport.policy() == transport.DEFAULT
    value = {**transport.DEFAULT, 'editor_seconds': 60}
    monkeypatch.setenv(transport.POLICY_ENV, json.dumps(value))
    with pytest.raises(ValueError): transport.policy()
    monkeypatch.setenv(transport.POLICY_ENV, json.dumps({**transport.DEFAULT, 'overall_seconds': 601}))
    with pytest.raises(ValueError): transport.policy()


@pytest.mark.parametrize('behavior', ['success', 'headers_stall', 'trickle', 'inflight_authority'])
def test_transport_process_enforces_absolute_deadline_in_thread(behavior, monkeypatch):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            if behavior == 'headers_stall':
                time.sleep(4)
            if behavior == 'inflight_authority':
                time.sleep(2)
            try:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                if behavior == 'trickle':
                    for _ in range(40):
                        self.wfile.write(b' '); self.wfile.flush(); time.sleep(0.1)
                self.wfile.write(b'{"choices": [], "usage": {"total_tokens": 9}}')
            except (BrokenPipeError, ConnectionResetError): pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    monkeypatch.delenv('SV_OPENAI_LOG_FILE', raising=False)
    provider = {'type': 'openai_compatible', 'name': 'local-fixture',
                'base_url': f'http://127.0.0.1:{server.server_port}', 'timeout_s': 60}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            started = time.monotonic()
            authorized_until = time.time() + 1.5 if behavior == 'inflight_authority' else None
            task = pool.submit(transport.complete, provider['base_url'] + '/chat/completions',
                               {'Authorization': 'Bearer synthetic-test-only'},
                               {'model': 'fixture', 'messages': []}, provider,
                               seconds=5 if behavior == 'inflight_authority' else 2, stage='fixture',
                               authorized_until=authorized_until)
            if behavior in ('success', 'inflight_authority'):
                assert task.result(timeout=5)['usage']['total_tokens'] == 9
                if behavior == 'inflight_authority':
                    assert time.time() > authorized_until
            else:
                with pytest.raises(TimeoutError, match='unknown_usage'): task.result(timeout=5)
                assert time.monotonic() - started < 3.5
        assert len(calls) == 1
        assert provider['timeout_s'] == 60  # scoped copy, no global provider mutation
    finally:
        server.shutdown(); server.server_close()


def test_native_boundary_uses_remaining_window_and_preserves_provider(monkeypatch):
    from sempervigil import event_source_reports_v2 as reports
    provider = {'base_url': 'https://api.openai.com/v1', 'timeout_s': 60}
    monkeypatch.setattr(reports, 'ready_client', lambda _: (provider, {'Authorization': 'synthetic'}))
    seen = []
    def bounded(url, headers, payload, actual_provider, *, seconds, stage, authorized_until, completion_until):
        seen.append((url, seconds, actual_provider, authorized_until, completion_until))
        return {'choices': [], 'usage': {'total_tokens': 3}}
    monkeypatch.setattr(transport, 'complete', bounded)
    response = reports._complete(None, {'model': 'fixture'}, before_transport=lambda: {'seconds': 12.5, 'authorized_until': 12345, 'completion_until': 67890},
                                 transport_seconds=240)
    assert seen == [('https://api.openai.com/v1/chat/completions', 12.5, provider, 12345, 67890)]
    assert provider['timeout_s'] == 60 and response['transport_elapsed_ms'] >= 0


def test_unserializable_metadata_never_spawns_or_sends_http(monkeypatch):
    monkeypatch.setattr(transport.subprocess, 'Popen', lambda *a, **k: pytest.fail('child must not spawn'))
    with pytest.raises(TypeError):
        transport.complete('https://example.invalid', {}, {'model': 'fixture'},
                           {'metadata': object()}, seconds=180, stage='fixture')


def test_expired_child_authorization_is_instrumented_zero_http():
    from sempervigil.event_source_reports_v2 import PreTransportFailure
    with pytest.raises(PreTransportFailure) as failure:
        transport.complete('http://127.0.0.1:1/chat/completions', {},
                           {'model': 'fixture', 'messages': []},
                           {'type': 'openai_compatible', 'name': 'fixture', 'base_url': 'http://127.0.0.1:1'},
                           seconds=5, stage='fixture', authorized_until=time.time()-1)
    assert failure.value.proof['kind'] == 'instrumented_authority'
    assert failure.value.proof['failure_stage'] == 'child_authorization_deadline'


def test_router_startup_work_cannot_cross_authorized_http_start(monkeypatch):
    from sempervigil.llm import router
    monkeypatch.setattr(router.urllib.request, 'urlopen', lambda *a, **k: pytest.fail('HTTP after deadline'))
    deadline = time.time() + 100
    def slow_startup(*args):
        monkeypatch.setattr(router.time, 'time', lambda: deadline+1)
        return {'request_chars': 0, 'request_tokens_estimate': 0}
    monkeypatch.setattr(router, '_estimate_request_metrics', slow_startup)
    with pytest.raises(router.PreHTTPDeadlineExpired):
        router._http_request('POST', 'http://127.0.0.1:1', {}, {'messages': []},
                             {'type': 'fixture'}, context={'no_retry': True, 'authorized_start_deadline': deadline})



def test_child_environment_excludes_unrelated_secrets_and_logs(monkeypatch):
    for key in ('SV_DB_URL', 'SV_MASTER_KEY', 'SV_OPENAI_LOG_FILE', 'UNRELATED_SECRET', 'HTTP_PROXY'):
        monkeypatch.setenv(key, 'synthetic-sensitive-test-value')
    monkeypatch.setenv('SSL_CERT_FILE', '/synthetic/certs.pem')
    seen = []
    class Child:
        returncode = 0
        args = []
        def communicate(self, input=None, timeout=None):
            return (b'{"response":{"usage":{"total_tokens":1}}}', b'')
        def poll(self): return 0
    def spawn(*args, **kwargs):
        seen.append(kwargs['env']); return Child()
    monkeypatch.setattr(transport.subprocess, 'Popen', spawn)
    transport.complete('https://example.invalid', {}, {'model': 'fixture'}, {}, seconds=180, stage='fixture')
    assert set(seen[0]) <= set(transport.CHILD_ENV_KEYS) | {'PYTHONPATH', 'PYTHONIOENCODING', 'PYTHONUTF8'}
    assert seen[0]['SSL_CERT_FILE'] == '/synthetic/certs.pem'
    assert not any('synthetic-sensitive-test-value' in str(v) for v in seen[0].values())


def test_expired_completion_window_never_spawns(monkeypatch):
    from sempervigil.event_source_reports_v2 import PreTransportFailure
    monkeypatch.setattr(transport.subprocess, 'Popen', lambda *a, **k: pytest.fail('spawn after completion window'))
    with pytest.raises(PreTransportFailure):
        transport.complete('https://example.invalid', {}, {}, {}, seconds=180, stage='fixture',
                           completion_until=time.time()-1)
