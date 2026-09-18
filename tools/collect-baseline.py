#!/usr/bin/env python3
"""Collect aggregate production timings without importing application startup code."""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone


REMOTE = r'''
import json, os, sys
import psycopg

hours = int(sys.argv[1])
queries = {
    "queue": ("""
        SELECT queue_name, job_type, status, count(*) AS count,
               min(requested_at) AS oldest_requested_at
        FROM jobs WHERE status IN ('queued', 'running')
        GROUP BY queue_name, job_type, status ORDER BY queue_name, job_type, status
    """, ()),
    "completed_jobs": ("""
        WITH timed AS (
            SELECT queue_name, job_type, status,
              CASE WHEN pg_input_is_valid(finished_at, 'timestamp with time zone')
                THEN finished_at::timestamptz END AS finished,
              CASE WHEN pg_input_is_valid(started_at, 'timestamp with time zone')
                THEN started_at::timestamptz END AS started
            FROM jobs
        )
        SELECT queue_name, job_type, status, count(*) AS count,
               count(*) FILTER (WHERE started IS NULL) AS missing_or_invalid_start,
               round(avg(extract(epoch FROM
                 (finished - started)))::numeric, 2)
                 AS mean_seconds,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM
                 (finished - started))) AS p95_seconds
        FROM timed
        WHERE finished > now() - (%s * interval '1 hour')
        GROUP BY queue_name, job_type, status ORDER BY queue_name, job_type, status
    """, (hours,)),
    "inference": ("""
        SELECT j.job_type, l.ok, count(*) AS count,
               count(*) FILTER (WHERE j.id IS NULL) AS unlinked_runs,
               round(avg(l.latency_ms) / 1000.0, 2) AS mean_seconds,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY l.latency_ms) / 1000.0
                 AS p95_seconds,
               round(avg(l.input_chars), 0) AS mean_input_chars,
               round(avg(l.output_chars), 0) AS mean_output_chars,
               min(l.ts) AS first_sample_at, max(l.ts) AS last_sample_at
        FROM llm_runs l LEFT JOIN jobs j ON j.id = l.job_id
        WHERE CASE WHEN pg_input_is_valid(l.ts, 'timestamp with time zone')
          THEN l.ts::timestamptz END > now() - (%s * interval '1 hour')
        GROUP BY j.job_type, l.ok ORDER BY j.job_type, l.ok
    """, (hours,)),
    "latest_builds": ("""
        SELECT id, status, requested_at, started_at, finished_at,
               CASE WHEN pg_input_is_valid(finished_at, 'timestamp with time zone')
                     AND pg_input_is_valid(started_at, 'timestamp with time zone')
                 THEN extract(epoch FROM
                   (finished_at::timestamptz - started_at::timestamptz)) END AS seconds
        FROM jobs WHERE job_type = 'build_site'
        ORDER BY CASE WHEN pg_input_is_valid(requested_at, 'timestamp with time zone')
          THEN requested_at::timestamptz END DESC NULLS LAST LIMIT 10
    """, ()),
    "timestamp_quality": ("""
        SELECT count(*) FILTER (WHERE started_at IS NOT NULL AND
          NOT pg_input_is_valid(started_at, 'timestamp with time zone')) AS invalid_starts,
          count(*) FILTER (WHERE finished_at IS NOT NULL AND
          NOT pg_input_is_valid(finished_at, 'timestamp with time zone')) AS invalid_finishes
        FROM jobs
    """, ()),
}
out = {}
with psycopg.connect(
    os.environ['SV_DB_URL'],
    options='-c default_transaction_read_only=on -c statement_timeout=10000',
    connect_timeout=10,
) as conn:
    for name, (sql, params) in queries.items():
        with conn.transaction():
            cur = conn.execute(sql, params)
            names = [column.name for column in cur.description]
            out[name] = [dict(zip(names, row)) for row in cur.fetchall()]
print(json.dumps(out, default=str))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="sempervigil")
    parser.add_argument("--hours", type=int, default=72)
    args = parser.parse_args()
    if not 1 <= args.hours <= 168:
        parser.error("--hours must be between 1 and 168")
    command = [
        "kubectl", "-n", args.namespace, "exec", "-i",
        "deploy/sempervigil-worker-llm", "-c", "sempervigil-worker-llm",
        "--", "python", "-", str(args.hours),
    ]
    try:
        result = subprocess.run(
            command, input=REMOTE, text=True, capture_output=True, timeout=55, check=True,
        )
        measurements = json.loads(result.stdout)
    except (subprocess.SubprocessError, ValueError):
        # Raw remote exceptions can contain connection information; do not export them.
        parser.exit(1, "Baseline collection failed; inspect cluster access and database schema.\n")
    print(json.dumps({
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": args.hours,
        "note": "A query window is not evidence of continuous observation; inspect sample timestamps.",
        "measurements": measurements,
    }, indent=2))


if __name__ == "__main__":
    main()
