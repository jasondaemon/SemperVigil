from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from .utils import json_dumps, utc_now_iso


POLICY_VERSION = "legacy-unpublished-drafts-v2"
_STATE_FIELDS = (
    "visibility",
    "lifecycle",
    "status",
    "candidate",
    "publish_state",
    "published_at",
    "site_slug",
    "updated_at",
)


def _table_exists(conn: Any, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (table,),
        ).fetchone()
    )


def _fingerprint(state: dict[str, object]) -> str:
    material = {key: state.get(key) for key in _STATE_FIELDS}
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _row_state(row: tuple[object, ...]) -> dict[str, object]:
    return dict(zip(("id", *_STATE_FIELDS), row, strict=True))


def _eligible(state: dict[str, object]) -> bool:
    return (
        str(state.get("visibility") or "active") == "active"
        and str(state.get("lifecycle") or "candidate") in {"candidate", "archived"}
        and str(state.get("publish_state") or "draft") == "draft"
        and bool(state.get("candidate"))
        and not state.get("published_at")
        and not state.get("site_slug")
    )


def create_preview(conn: Any, *, requested_by: str = "admin") -> dict[str, object]:
    exclusions = [
        "COALESCE(e.visibility, 'active') = 'active'",
        "COALESCE(e.lifecycle, 'candidate') IN ('candidate', 'archived')",
        "COALESCE(e.publish_state, 'draft') = 'draft'",
        "COALESCE(e.candidate, false) = true",
        "COALESCE(e.manual, 0) = 0",
        "COALESCE(e.is_manual, 0) = 0",
        "e.published_at IS NULL",
        "NULLIF(e.site_slug, '') IS NULL",
        "COALESCE(e.event_key, '') NOT LIKE 'event-ledger:%%'",
    ]
    if _table_exists(conn, "event_public_pointers"):
        exclusions.append(
            "NOT EXISTS (SELECT 1 FROM event_public_pointers p WHERE p.event_id = e.id)"
        )
    if _table_exists(conn, "event_public_revisions"):
        exclusions.append(
            "NOT EXISTS (SELECT 1 FROM event_public_revisions r WHERE r.event_id = e.id)"
        )
    if _table_exists(conn, "event_review_approvals"):
        exclusions.append(
            "NOT EXISTS (SELECT 1 FROM event_review_approvals a WHERE a.event_id = e.id)"
        )
    if _table_exists(conn, "jobs"):
        exclusions.append(
            "NOT EXISTS (SELECT 1 FROM jobs j WHERE j.status IN ('queued', 'running') "
            "AND COALESCE(NULLIF(j.payload_json, ''), '{}')::jsonb ->> 'event_id' = e.id)"
        )

    rows = conn.execute(
        f"""
        SELECT e.id, e.visibility, e.lifecycle, e.status, e.candidate,
               e.publish_state, e.published_at, e.site_slug, e.updated_at
        FROM events e
        WHERE {' AND '.join(exclusions)}
        ORDER BY e.created_at, e.id
        """
    ).fetchall()
    run_id = f"lerr_{uuid.uuid4().hex}"
    counts = {
        "eligible": len(rows),
        "applied": 0,
        "skipped": 0,
        "restored": 0,
        "policy": POLICY_VERSION,
    }
    conn.execute(
        """
        INSERT INTO legacy_event_retirement_runs
            (run_id, policy_version, status, counts_json, created_at, requested_by)
        VALUES (%s, %s, 'previewed', %s, %s, %s)
        """,
        (run_id, POLICY_VERSION, json_dumps(counts), utc_now_iso(), requested_by),
    )
    if rows:
        conn.executemany(
            """
            INSERT INTO legacy_event_retirement_items
                (run_id, event_id, before_json, preview_fingerprint, status)
            VALUES (%s, %s, %s, %s, 'pending')
            """,
            [
                (
                    run_id,
                    str(row[0]),
                    json_dumps(_row_state(row)),
                    _fingerprint(_row_state(row)),
                )
                for row in rows
            ],
        )
    conn.commit()
    return {
        "run_id": run_id,
        "status": "previewed",
        "counts": counts,
        "sample_event_ids": [str(row[0]) for row in rows[:10]],
    }


def get_run(conn: Any, run_id: str) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT run_id, policy_version, status, counts_json, created_at,
               applied_at, restored_at, requested_by
        FROM legacy_event_retirement_runs
        WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return None
    counts = row[3]
    if isinstance(counts, str):
        counts = json.loads(counts) if counts else {}
    return {
        "run_id": row[0],
        "policy_version": row[1],
        "status": row[2],
        "counts": counts or {},
        "created_at": row[4],
        "applied_at": row[5],
        "restored_at": row[6],
        "requested_by": row[7],
    }


def list_runs(conn: Any, *, limit: int = 10) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT run_id FROM legacy_event_retirement_runs
        ORDER BY created_at DESC LIMIT %s
        """,
        (max(1, min(int(limit), 50)),),
    ).fetchall()
    return [run for row in rows if (run := get_run(conn, str(row[0]))) is not None]


def apply_run(conn: Any, run_id: str, *, batch_size: int = 200) -> dict[str, object]:
    run = get_run(conn, run_id)
    if not run:
        raise ValueError("retirement_run_not_found")
    if run["status"] not in {"previewed", "applying"}:
        raise ValueError(f"retirement_run_not_applicable:{run['status']}")
    conn.execute(
        "UPDATE legacy_event_retirement_runs SET status = 'applying' WHERE run_id = %s",
        (run_id,),
    )
    conn.commit()
    applied = 0
    skipped = 0
    while True:
        items = conn.execute(
            """
            SELECT event_id, preview_fingerprint
            FROM legacy_event_retirement_items
            WHERE run_id = %s AND status = 'pending'
            ORDER BY event_id LIMIT %s
            """,
            (run_id, max(1, min(int(batch_size), 500))),
        ).fetchall()
        if not items:
            break
        for event_id, preview_fingerprint in items:
            row = conn.execute(
                """
                SELECT id, visibility, lifecycle, status, candidate, publish_state,
                       published_at, site_slug, updated_at
                FROM events WHERE id = %s FOR UPDATE
                """,
                (event_id,),
            ).fetchone()
            state = _row_state(row) if row else {}
            if not row or not _eligible(state) or _fingerprint(state) != preview_fingerprint:
                conn.execute(
                    """
                    UPDATE legacy_event_retirement_items
                    SET status = 'skipped', reason = 'state_changed_or_ineligible'
                    WHERE run_id = %s AND event_id = %s
                    """,
                    (run_id, event_id),
                )
                skipped += 1
                continue
            now = utc_now_iso()
            conn.execute(
                """
                UPDATE events
                SET visibility = 'suppressed', lifecycle = 'archived', updated_at = %s
                WHERE id = %s
                """,
                (now, event_id),
            )
            applied_state = dict(state)
            applied_state.update(
                {"visibility": "suppressed", "lifecycle": "archived", "updated_at": now}
            )
            conn.execute(
                """
                UPDATE legacy_event_retirement_items
                SET status = 'applied', applied_fingerprint = %s, reason = NULL
                WHERE run_id = %s AND event_id = %s
                """,
                (_fingerprint(applied_state), run_id, event_id),
            )
            applied += 1
        conn.commit()
    status_counts = dict(
        conn.execute(
            """
            SELECT status, COUNT(*)
            FROM legacy_event_retirement_items
            WHERE run_id = %s GROUP BY status
            """,
            (run_id,),
        ).fetchall()
    )
    counts = dict(run.get("counts") or {})
    counts.update(
        {
            "applied": int(status_counts.get("applied", 0)),
            "skipped": int(status_counts.get("skipped", 0)),
        }
    )
    conn.execute(
        """
        UPDATE legacy_event_retirement_runs
        SET status = 'applied', counts_json = %s, applied_at = %s
        WHERE run_id = %s
        """,
        (json_dumps(counts), utc_now_iso(), run_id),
    )
    conn.commit()
    return get_run(conn, run_id) or {}


def restore_run(conn: Any, run_id: str, *, batch_size: int = 200) -> dict[str, object]:
    run = get_run(conn, run_id)
    if not run:
        raise ValueError("retirement_run_not_found")
    if run["status"] not in {"applied", "restoring"}:
        raise ValueError(f"retirement_run_not_restorable:{run['status']}")
    conn.execute(
        "UPDATE legacy_event_retirement_runs SET status = 'restoring' WHERE run_id = %s",
        (run_id,),
    )
    conn.commit()
    restored = 0
    skipped = 0
    while True:
        items = conn.execute(
            """
            SELECT event_id, before_json, applied_fingerprint
            FROM legacy_event_retirement_items
            WHERE run_id = %s AND status = 'applied'
            ORDER BY event_id LIMIT %s
            """,
            (run_id, max(1, min(int(batch_size), 500))),
        ).fetchall()
        if not items:
            break
        for event_id, before_json, applied_fingerprint in items:
            before = json.loads(before_json) if isinstance(before_json, str) else dict(before_json)
            row = conn.execute(
                """
                SELECT id, visibility, lifecycle, status, candidate, publish_state,
                       published_at, site_slug, updated_at
                FROM events WHERE id = %s FOR UPDATE
                """,
                (event_id,),
            ).fetchone()
            current = _row_state(row) if row else {}
            if not row or _fingerprint(current) != applied_fingerprint:
                conn.execute(
                    """
                    UPDATE legacy_event_retirement_items
                    SET status = 'restore_skipped', reason = 'state_changed_after_apply'
                    WHERE run_id = %s AND event_id = %s
                    """,
                    (run_id, event_id),
                )
                skipped += 1
                continue
            conn.execute(
                """
                UPDATE events
                SET visibility = %s, lifecycle = %s, status = %s, candidate = %s,
                    publish_state = %s, published_at = %s, site_slug = %s, updated_at = %s
                WHERE id = %s
                """,
                (
                    before.get("visibility"),
                    before.get("lifecycle"),
                    before.get("status"),
                    before.get("candidate"),
                    before.get("publish_state"),
                    before.get("published_at"),
                    before.get("site_slug"),
                    before.get("updated_at"),
                    event_id,
                ),
            )
            conn.execute(
                """
                UPDATE legacy_event_retirement_items
                SET status = 'restored', reason = NULL
                WHERE run_id = %s AND event_id = %s
                """,
                (run_id, event_id),
            )
            restored += 1
        conn.commit()
    status_counts = dict(
        conn.execute(
            """
            SELECT status, COUNT(*)
            FROM legacy_event_retirement_items
            WHERE run_id = %s GROUP BY status
            """,
            (run_id,),
        ).fetchall()
    )
    counts = dict(run.get("counts") or {})
    counts.update(
        {
            "restored": int(status_counts.get("restored", 0)),
            "restore_skipped": int(status_counts.get("restore_skipped", 0)),
        }
    )
    conn.execute(
        """
        UPDATE legacy_event_retirement_runs
        SET status = 'restored', counts_json = %s, restored_at = %s
        WHERE run_id = %s
        """,
        (json_dumps(counts), utc_now_iso(), run_id),
    )
    conn.commit()
    return get_run(conn, run_id) or {}
