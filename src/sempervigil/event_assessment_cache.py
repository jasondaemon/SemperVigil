"""Private immutable assessment reuse; a cache hit is never factual approval."""
import json
import os
import re
import secrets
import stat
from collections.abc import Callable
from pathlib import Path

from .event_assessment import assess, request_for, validate_assessment, validate_response
from .event_review import _json
from .investigation import _version

MAX_ENTRY_BYTES = 32768


def _scoped_request_version(packet: dict, request: dict) -> str:
    # Report writes touch event.updated_at without changing source evidence.
    # Keep all other snapshot fields, including omitted/truncated coverage.
    snapshot = {k: v for k, v in packet.items() if k != "packet_version"}
    snapshot["event"] = {k: v for k, v in packet["event"].items() if k != "updated_at"}
    identity = {"workflow": "event-scoped-reuse-v1", "snapshot": snapshot,
                     "scope_version": request["scope_version"],
                     "system": request["system"], "input": request["input"]}
    if "article_id" in request:
        identity.update(workflow="event-source-reuse-v1", article_id=request["article_id"])
    if request.get("paired") is True:
        identity.update(workflow="event-paired-source-reuse-v1")
    return _version(identity)


def _read(folder: int, name: str, packet: dict, request: str, generation: str,
          *, scope: dict | None = None, article_id: int | None = None, paired: bool = False) -> dict | None:
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_ENTRY_BYTES:
            raise ValueError("invalid_assessment_cache")
        entry = _json(stream.read(MAX_ENTRY_BYTES + 1), MAX_ENTRY_BYTES)
    expected = {"request_version", "generation_version", "assessment"}
    if scope is not None:
        expected.add("event_updated_at")
    if (entry.keys() != expected
            or entry["request_version"] != request or entry["generation_version"] != generation):
        raise ValueError("stale_assessment_cache")
    if scope is not None:
        original = {**packet, "event": {**packet["event"], "updated_at": entry["event_updated_at"]}}
        original["packet_version"] = _version({k: v for k, v in original.items() if k != "packet_version"})
        prior_request = request_for(original, scope=scope, article_id=article_id, paired=paired)
        assessment = validate_assessment(entry["assessment"], original)
        if (assessment["request_version"] != prior_request["request_version"]
                or _scoped_request_version(original, prior_request) != request):
            raise ValueError("stale_assessment_cache")
        # Rebind exact validated decisions to the current packet's passage IDs.
        response = {"decisions": [{"id": key, **assessment["suggestions"][identity]}
                                  for key, identity in prior_request["mapping"].items()]}
        return validate_response(json.dumps(response).encode(), packet, scope=scope, article_id=article_id, paired=paired)
    assessment = validate_assessment(entry["assessment"], packet)
    if assessment["request_version"] != request:
        raise ValueError("stale_assessment_cache")
    return assessment


def reuse(packet: dict, complete: Callable[[str], dict], root: Path, *, scope: dict | None = None,
          article_id: int | None = None, paired: bool = False) -> tuple[dict, bool]:
    """Only a guarded completion with a versioned configuration may use cache."""
    generation = getattr(complete, "cache_identity", None)
    if generation is None:
        return assess(packet, complete, scope=scope, article_id=article_id, paired=paired), False
    if type(generation) is not str or not re.fullmatch(r"[0-9a-f]{64}", generation):
        raise ValueError("invalid_assessment_cache_identity")
    full_request = request_for(packet, scope=scope, article_id=article_id, paired=paired)
    request = (_scoped_request_version(packet, full_request) if scope is not None
               else full_request["request_version"])
    name = _version({"request": request, "generation": generation}) + ".json"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        try:
            os.mkdir("assessment-cache", mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        folder = os.open("assessment-cache", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        try:
            cached = _read(folder, name, packet, request, generation, scope=scope, article_id=article_id, paired=paired)
            if cached is not None:
                return cached, True
            value = None
            if scope is not None:
                # Import a valid exact-snapshot v1 entry without another model call.
                legacy_request = full_request["request_version"]
                legacy_name = _version({"request": legacy_request, "generation": generation}) + ".json"
                value = _read(folder, legacy_name, packet, legacy_request, generation)
            imported = value is not None
            if value is None:
                value = assess(packet, complete, scope=scope, article_id=article_id, paired=paired)
            entry = {"request_version": request, "generation_version": generation, "assessment": value}
            if scope is not None:
                entry["event_updated_at"] = packet["event"]["updated_at"]
            encoded = json.dumps(entry, sort_keys=True, ensure_ascii=True).encode()
            if len(encoded) > MAX_ENTRY_BYTES:
                raise ValueError("oversized_assessment_cache")
            temporary = ".pending-" + secrets.token_hex(16)
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=folder)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                try:
                    os.link(temporary, name, src_dir_fd=folder, dst_dir_fd=folder, follow_symlinks=False)
                except FileExistsError:
                    # Another complete immutable result wins; never overwrite it.
                    winner = _read(folder, name, packet, request, generation, scope=scope, article_id=article_id, paired=paired)
                    if winner is None:
                        raise ValueError("assessment_cache_race")
                    return winner, False
            finally:
                os.unlink(temporary, dir_fd=folder)
            return value, imported
        finally:
            os.close(folder)
    finally:
        os.close(root_fd)
