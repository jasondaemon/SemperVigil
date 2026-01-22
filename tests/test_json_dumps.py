from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from uuid import UUID

import json
from pydantic import BaseModel

from sempervigil.cve_sync import PreferredMetrics
from sempervigil.storage import claim_next_job, complete_job, enqueue_job, init_db
from sempervigil.utils import json_dumps


class Color(Enum):
    RED = "red"


@dataclass
class Payload:
    value: str


class PayloadModel(BaseModel):
    name: str


def test_json_dumps_handles_supported_types():
    payload = {
        "dataclass": Payload(value="ok"),
        "preferred": PreferredMetrics(
            version="3.1", base_score=7.5, base_severity="HIGH", vector="AV:N/AC:L"
        ),
        "enum": Color.RED,
        "datetime": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "date": date(2025, 1, 2),
        "path": Path("/tmp/sempervigil"),
        "uuid": UUID("12345678-1234-5678-1234-567812345678"),
        "model": PayloadModel(name="example"),
        "set": {"a", "b"},
        "tuple": ("x", "y"),
    }
    encoded = json_dumps(payload)
    decoded = json.loads(encoded)
    assert decoded["dataclass"]["value"] == "ok"
    assert decoded["preferred"]["version"] == "3.1"
    assert decoded["enum"] == "red"
    assert decoded["datetime"].startswith("2025-01-01T00:00:00")
    assert decoded["date"] == "2025-01-02"
    assert decoded["path"] == "/tmp/sempervigil"
    assert decoded["uuid"] == "12345678-1234-5678-1234-567812345678"
    assert decoded["model"]["name"] == "example"
    assert sorted(decoded["set"]) == ["a", "b"]
    assert decoded["tuple"] == ["x", "y"]


def test_job_result_serialization_handles_complex_types(tmp_path):
    conn = init_db(str(tmp_path / "state.sqlite3"))
    job_id = enqueue_job(conn, "cve_sync", None)
    job = claim_next_job(conn, "worker-1")
    assert job is not None
    result = {
        "preferred": Payload(value="ok"),
        "preferred_metrics": PreferredMetrics(
            version="4.0", base_score=9.8, base_severity="CRITICAL", vector="AV:N/AC:L"
        ),
        "status": Color.RED,
        "when": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "path": Path("/tmp/sempervigil"),
        "model": PayloadModel(name="example"),
        "tuple": ("x", "y"),
    }
    assert complete_job(conn, job_id, result=result) is True
