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


@pytest.mark.parametrize('behavior', ['success', 'headers_stall', 'trickle'])
def test_transport_process_enforces_absolute_deadline_in_thread(behavior, monkeypatch):
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            calls.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
            if behavior == 'headers_stall':
                time.sleep(4)
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
            task = pool.submit(transport.complete, provider['base_url'] + '/chat/completions',
                               {'Authorization': 'Bearer synthetic-test-only'},
                               {'model': 'fixture', 'messages': []}, provider,
                               seconds=2, stage='fixture')
            if behavior == 'success':
                assert task.result(timeout=5)['usage']['total_tokens'] == 9
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
    def bounded(url, headers, payload, actual_provider, *, seconds, stage):
        seen.append((url, seconds, actual_provider))
        return {'choices': [], 'usage': {'total_tokens': 3}}
    monkeypatch.setattr(transport, 'complete', bounded)
    response = reports._complete(None, {'model': 'fixture'}, before_transport=lambda: 12.5,
                                 transport_seconds=240)
    assert seen == [('https://api.openai.com/v1/chat/completions', 12.5, provider)]
    assert provider['timeout_s'] == 60 and response['transport_elapsed_ms'] >= 0
