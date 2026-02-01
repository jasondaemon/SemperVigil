import threading
from concurrent.futures import ThreadPoolExecutor

from sempervigil.storage import claim_next_job, complete_job, enqueue_job, init_db


def test_claim_next_job_atomic_single_winner():
    conn = init_db()
    job_id = enqueue_job(conn, "smoke_test", {})

    barrier = threading.Barrier(2)

    def _claim(worker_id: str):
        local = init_db()
        barrier.wait()
        job = claim_next_job(local, worker_id, allowed_types=["smoke_test"])
        return job.id if job else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(_claim, ["worker_a", "worker_b"]))

    winners = [job for job in results if job]
    assert len(winners) == 1
    assert winners[0] == job_id
    complete_job(conn, winners[0], result={"ok": True})
