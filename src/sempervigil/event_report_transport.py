"""Bounded transport for the opt-in narrative workflow; no automatic retries."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

POLICY_ENV = 'SV_EVENT_REPORT_V2_TRANSPORT_POLICY'
WORKFLOW = 'narrative-hard-transport-deadline-v1'
DEFAULT = {'workflow': WORKFLOW, 'writer_seconds': 180, 'editor_seconds': 240,
           'overall_seconds': 600}


def policy():
    value = json.loads(os.environ[POLICY_ENV]) if os.environ.get(POLICY_ENV) else dict(DEFAULT)
    if (not isinstance(value, dict) or set(value) != set(DEFAULT)
        or value['workflow'] != WORKFLOW
        or any(type(value[k]) is not int for k in ('writer_seconds', 'editor_seconds', 'overall_seconds'))
        or not 180 <= value['writer_seconds'] <= 240
        or not 180 <= value['editor_seconds'] <= 240
        or not 360 <= value['overall_seconds'] <= 600):
        raise ValueError('event_report_transport_policy_invalid')
    return value


def preflight(payload, seconds):
    """Actual strong editor profile must never silently inherit provider 60s."""
    if (not isinstance(seconds, (int, float)) or isinstance(seconds, bool)
        or not math.isfinite(seconds) or not 180 <= seconds <= 240):
        raise ValueError('event_report_transport_deadline_invalid')
    if (payload.get('reasoning_effort') == 'high'
        and payload.get('max_completion_tokens', 0) >= 6000 and seconds < 180):
        raise ValueError('event_report_editor_deadline_too_short')
    return seconds


# Secrets travel only through stdin, never argv, logs or a temporary file.
# The child uses the existing router/authenticated request, exactly one POST.
# Its provider timeout bounds socket inactivity; communicate bounds wall time,
# including a trickling response, and works in worker threads on macOS/Linux.
_CHILD = """
import contextlib, io, json, sys
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    try:
        from sempervigil.llm.router import _http_request
        q = json.load(sys.stdin)
        response = _http_request('POST', q['url'], q['headers'], q['payload'],
                                 q['provider'], context=q['context'])
        result = {'response': response}
    except Exception as exc:
        result = {'error_type': type(exc).__name__}
sys.stdout.write(json.dumps(result))
"""


def complete(url, headers, payload, provider, *, seconds, stage):
    """Kill/reap a timed-out transport; provider completion and usage stay unknown."""
    if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or not 0 < seconds <= 240:
        raise ValueError('event_report_transport_budget_exhausted')
    started = time.monotonic()
    env = dict(os.environ)
    env['PYTHONPATH'] = str(Path(__file__).resolve().parent.parent) + os.pathsep + env.get('PYTHONPATH', '')
    child = subprocess.Popen([sys.executable, '-c', _CHILD], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    request = json.dumps({'url': url, 'headers': headers, 'payload': payload,
                          'provider': {**provider, 'timeout_s': math.ceil(seconds)},
                          'context': {'stage': stage, 'no_retry': True}}).encode()
    try:
        remaining = seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise subprocess.TimeoutExpired(child.args, seconds)
        raw, _ = child.communicate(input=request, timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        child.kill()
        child.communicate()
        raise TimeoutError('event_report_transport_deadline_unknown_usage') from exc
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    if child.returncode:
        raise ValueError('event_report_transport_process_failed_unknown_usage')
    result = json.loads(raw)
    if 'response' not in result:
        # Once the child starts, never claim zero HTTP or free an unknown charge.
        raise ValueError('event_report_transport_failed_unknown_usage')
    return result['response']
