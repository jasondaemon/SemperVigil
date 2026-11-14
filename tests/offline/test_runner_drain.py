import subprocess
import sys
import threading

import pytest

from sempervigil import runner

pytestmark = pytest.mark.offline


def test_shutdown_refuses_new_claim_before_opening_database(monkeypatch):
    monkeypatch.setattr(runner, "init_db", lambda: pytest.fail("database opened"))
    assert runner.run_once("local", "llm_local", lambda: True) == runner._RUNNER_IDLE


def test_shutdown_waits_for_current_job_then_stops(monkeypatch):
    stopped = threading.Event()
    calls = []
    monkeypatch.setattr(runner, "_setup_logging", lambda: object())
    monkeypatch.setattr(runner, "log_event", lambda *args, **kwargs: None)

    def complete_current(runner_id, runner_type, shutdown_requested):
        calls.append((runner_id, runner_type, shutdown_requested()))
        stopped.set()
        return 0

    monkeypatch.setattr(runner, "run_once", complete_current)
    assert runner.run_loop("local", "llm_local", 5, stopped) == 0
    assert calls == [("local", "llm_local", False)]


def test_sigterm_finishes_inflight_local_job_before_exit():
    script = """
import sys
import time
from sempervigil import runner
runner._setup_logging = lambda: object()
runner.log_event = lambda *args, **kwargs: None
def current_job(runner_id, runner_type, shutdown_requested):
    print('started', flush=True)
    time.sleep(0.2)
    print('finished', flush=True)
    return 0
runner.run_once = current_job
sys.argv = ['runner', '--runner', 'llm_local', '--sleep', '1']
raise SystemExit(runner.main())
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True,
    )
    try:
        assert process.stdout.readline().strip() == "started"
        process.terminate()
        output, _ = process.communicate(timeout=5)
        assert process.returncode == 0
        assert output.strip() == "finished"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
