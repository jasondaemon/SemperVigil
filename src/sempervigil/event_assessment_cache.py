"""Private immutable assessment reuse; a cache hit is never factual approval."""
import json
import os
import re
import secrets
import stat
from collections.abc import Callable
from pathlib import Path

from .event_assessment import assess, request_for, validate_assessment
from .event_review import _json
from .investigation import _version

MAX_ENTRY_BYTES = 32768


def _read(folder: int, name: str, packet: dict, request: str, generation: str) -> dict | None:
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_ENTRY_BYTES:
            raise ValueError("invalid_assessment_cache")
        entry = _json(stream.read(MAX_ENTRY_BYTES + 1), MAX_ENTRY_BYTES)
    if (entry.keys() != {"request_version", "generation_version", "assessment"}
            or entry["request_version"] != request or entry["generation_version"] != generation):
        raise ValueError("stale_assessment_cache")
    assessment = validate_assessment(entry["assessment"], packet)
    if assessment["request_version"] != request:
        raise ValueError("stale_assessment_cache")
    return assessment


def reuse(packet: dict, complete: Callable[[str], dict], root: Path, *, scope: dict | None = None) -> tuple[dict, bool]:
    """Only a guarded completion with a versioned configuration may use cache."""
    generation = getattr(complete, "cache_identity", None)
    if generation is None:
        return assess(packet, complete, scope=scope), False
    if type(generation) is not str or not re.fullmatch(r"[0-9a-f]{64}", generation):
        raise ValueError("invalid_assessment_cache_identity")
    request = request_for(packet, scope=scope)["request_version"]
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
            cached = _read(folder, name, packet, request, generation)
            if cached is not None:
                return cached, True
            value = assess(packet, complete, scope=scope)
            encoded = json.dumps({"request_version": request, "generation_version": generation,
                                  "assessment": value}, sort_keys=True, ensure_ascii=True).encode()
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
                    winner = _read(folder, name, packet, request, generation)
                    if winner is None:
                        raise ValueError("assessment_cache_race")
                    return winner, False
            finally:
                os.unlink(temporary, dir_fd=folder)
            return value, False
        finally:
            os.close(folder)
    finally:
        os.close(root_fd)
