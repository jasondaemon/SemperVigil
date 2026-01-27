import logging
import os
import sys

from sempervigil.utils import configure_logging


def test_configure_logging_idempotent(tmp_path, monkeypatch):
    log_file = tmp_path / "app.log"
    monkeypatch.setenv("SV_LOG_LEVEL", "INFO")
    monkeypatch.setenv("SV_LOG_FILE", str(log_file))

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        root.handlers = []
        configure_logging("sempervigil.hugo")
        configure_logging("sempervigil.hugo")

        stream_handlers = [
            handler
            for handler in root.handlers
            if isinstance(handler, logging.StreamHandler)
        ]
        file_handlers = [
            handler for handler in root.handlers if isinstance(handler, logging.FileHandler)
        ]

        assert len(stream_handlers) == 1
        assert stream_handlers[0].stream is sys.stdout
        assert len(file_handlers) == 1
        assert os.path.abspath(file_handlers[0].baseFilename) == os.path.abspath(
            str(log_file)
        )
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)
