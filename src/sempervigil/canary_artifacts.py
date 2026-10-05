"""Durable, content-addressed artifacts for private LLM canaries."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable


class CallNotResumable(ValueError):
    """A prepared or failed call must be reviewed, never silently reissued."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True,
                      separators=(",", ":")).encode()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    data = _canonical(value)
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class Journal:
    def __init__(self, root: str | Path, identity: dict[str, Any]):
        self.root = Path(root)
        self.identity = identity
        manifest = self.root / "manifest.json"
        if manifest.exists():
            current = json.loads(manifest.read_text(encoding="utf-8"))
            if current != identity:
                raise ValueError("canary_artifact_identity_mismatch")
        else:
            _atomic_json(manifest, identity)
        self.write_summary()

    @staticmethod
    def call_key(request: dict[str, Any]) -> str:
        return hashlib.sha256(_canonical(request)).hexdigest()

    def run_call(self, request: dict[str, Any], invoke: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        key = self.call_key(request)
        path = self.root / "calls" / f"{key}.json"
        if path.exists():
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("status") == "completed":
                return record["response"]
            raise CallNotResumable("canary_call_already_attempted")
        _atomic_json(path, {"key": key, "status": "prepared", "request": request})
        try:
            response = invoke()
        except Exception as exc:
            _atomic_json(path, {"key": key, "status": "failed", "request": request,
                                "error": {"type": type(exc).__name__, "message": str(exc)}})
            self.write_summary(terminal_error={"type": type(exc).__name__,
                                               "message": str(exc)})
            raise
        usage = response.get("usage") if isinstance(response, dict) else None
        _atomic_json(path, {"key": key, "status": "completed", "request": request,
                            "response": response, "usage": usage or {}})
        self.write_summary()
        return response

    def record_pass(self, number: int, value: dict[str, Any]) -> None:
        _atomic_json(self.root / "passes" / f"pass-{number:02d}.json", value)
        self.write_summary()

    def record_checkpoint(self, name: str, value: dict[str, Any]) -> None:
        _atomic_json(self.root / "checkpoints" / f"{name}.json", value)
        self.write_summary()

    def checkpoint(self, name: str) -> dict[str, Any] | None:
        path = self.root / "checkpoints" / f"{name}.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def run_batches(self, *, stage: str, pass_number: int,
                    batches: list[dict[str, Any]],
                    invoke: Callable[[dict[str, Any]], dict[str, Any]]) -> list[dict[str, Any]]:
        responses = []
        for number, batch in enumerate(batches, 1):
            envelope = {"identity": self.identity, "stage": stage,
                        "pass": pass_number, "batch": number, "request": batch}
            responses.append(self.run_call(envelope, lambda batch=batch: invoke(batch)))
        self.record_checkpoint(
            f"pass-{pass_number:02d}-{stage}",
            {"stage": stage, "pass": pass_number,
             "call_keys": [self.call_key({"identity": self.identity, "stage": stage,
                                           "pass": pass_number, "batch": number,
                                           "request": batch})
                           for number, batch in enumerate(batches, 1)]},
        )
        return responses

    def require_budget(self, *, estimated_next_tokens: int, max_total_tokens: int,
                       checkpoint: dict[str, Any]) -> None:
        summary = self.write_summary()
        exact = int(summary["usage"]["total_tokens"])
        if exact + int(estimated_next_tokens) > int(max_total_tokens):
            self.record_checkpoint("terminal-composition", checkpoint)
            error = {"type": "BudgetExceeded",
                     "message": "canary_token_budget_exhausted",
                     "exact_consumed_tokens": exact,
                     "reserved_next_tokens": int(estimated_next_tokens),
                     "max_total_tokens": int(max_total_tokens)}
            self.write_summary(terminal_error=error)
            raise ValueError("canary_token_budget_exhausted")

    def write_summary(self, *, terminal_error: dict[str, str] | None = None) -> dict[str, Any]:
        summary_path = self.root / "summary.json"
        if terminal_error is None and summary_path.exists():
            terminal_error = json.loads(
                summary_path.read_text(encoding="utf-8")
            ).get("terminal_error")
        calls = []
        for path in sorted((self.root / "calls").glob("*.json")):
            calls.append(json.loads(path.read_text(encoding="utf-8")))
        totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        for record in calls:
            if record.get("status") != "completed":
                continue
            usage = record.get("usage") or {}
            for field in totals:
                totals[field] += int(usage.get(field) or 0)
        summary = {
            "identity": self.identity,
            "calls": len(calls),
            "completed_calls": sum(row.get("status") == "completed" for row in calls),
            "failed_calls": sum(row.get("status") == "failed" for row in calls),
            "prepared_calls": sum(row.get("status") == "prepared" for row in calls),
            "usage": totals,
            "passes": len(list((self.root / "passes").glob("*.json"))),
            "checkpoints": len(list((self.root / "checkpoints").glob("*.json"))),
            "terminal_error": terminal_error,
        }
        _atomic_json(summary_path, summary)
        return summary
