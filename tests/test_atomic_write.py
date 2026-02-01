from __future__ import annotations

import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from sempervigil.utils import atomic_write_json, atomic_write_text


def test_atomic_write_text_no_partial_reads() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "index.md"
        contents = [f"version:{idx}\n" + ("x" * 1024) for idx in range(20)]
        expected = set(contents)
        stop = threading.Event()
        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def writer() -> None:
            for payload in contents:
                atomic_write_text(path, payload)
                time.sleep(0.005)
            stop.set()

        def reader() -> None:
            try:
                while not stop.is_set():
                    if path.exists():
                        data = path.read_text(encoding="utf-8")
                        assert data in expected
                    time.sleep(0.002)
            except BaseException as exc:  # pragma: no cover - surfacing thread errors
                with errors_lock:
                    errors.append(exc)
                stop.set()

        t_write = threading.Thread(target=writer)
        t_read = threading.Thread(target=reader)
        t_read.start()
        t_write.start()
        t_write.join()
        stop.set()
        t_read.join()
        assert not errors, f"reader errors: {errors}"


def test_atomic_write_json_no_partial_reads() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "index.json"
        payloads = [{"version": idx, "items": list(range(100))} for idx in range(10)]
        expected = {str(p["version"]) for p in payloads}
        stop = threading.Event()
        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def writer() -> None:
            for payload in payloads:
                atomic_write_json(path, payload, indent=2)
                time.sleep(0.005)
            stop.set()

        def reader() -> None:
            try:
                while not stop.is_set():
                    if path.exists():
                        data = path.read_text(encoding="utf-8")
                        assert '"version"' in data
                        # Should contain a whole payload, not a partial write.
                        assert any(f"\"version\": {version}" in data for version in expected)
                    time.sleep(0.002)
            except BaseException as exc:  # pragma: no cover - surfacing thread errors
                with errors_lock:
                    errors.append(exc)
                stop.set()

        t_write = threading.Thread(target=writer)
        t_read = threading.Thread(target=reader)
        t_read.start()
        t_write.start()
        t_write.join()
        stop.set()
        t_read.join()
        assert not errors, f"reader errors: {errors}"
