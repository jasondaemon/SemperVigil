from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def set_umask_from_env() -> None:
    _apply_umask()


def ensure_runtime_dirs(paths: Iterable[str]) -> None:
    for path in paths:
        if not path:
            continue
        _ensure_dir(Path(path))


def build_default_paths(data_dir: str, output_dir: str) -> list[str]:
    paths = [data_dir, "/site", "/site/public", "/site/static/sempervigil"]
    output_path = Path(output_dir)
    content_root = output_path.parent if output_path.name == "posts" else output_path
    paths.extend(
        [
            str(content_root),
            str(content_root / "posts"),
            str(content_root / "events"),
            str(content_root / "cves"),
        ]
    )
    return paths


def _apply_umask() -> None:
    umask_value = os.environ.get("SV_UMASK", "002")
    try:
        os.umask(int(umask_value, 8))
    except (ValueError, TypeError):
        os.umask(0o002)


def _ensure_dir(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        return
    _safe_chmod(path, 0o775)


def _safe_chmod(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except PermissionError:
        return
