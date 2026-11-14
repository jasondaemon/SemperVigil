"""Observe fresh committed queue inserts within one synchronous coordinator turn."""
from contextlib import contextmanager
from contextvars import ContextVar

_current = ContextVar('committed_job_admissions', default=None)


@contextmanager
def observe_committed_jobs():
    jobs = []
    token = _current.set(jobs)
    try:
        yield jobs
    finally:
        _current.reset(token)


def record_committed_job(job_id):
    jobs = _current.get()
    if jobs is not None: jobs.append(job_id)


def was_job_admitted(job_id):
    jobs = _current.get()
    return jobs is not None and job_id in jobs
