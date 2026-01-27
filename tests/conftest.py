from __future__ import annotations

import os
import pytest


def pytest_sessionstart(session) -> None:
    if not os.environ.get("SV_DB_URL"):
        pytest.skip("SV_DB_URL is required for Postgres-only tests", allow_module_level=True)
