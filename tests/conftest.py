from __future__ import annotations

import os
import pytest


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--run-db-tests", action="store_true",
        help="Run integration tests against the disposable SV_TEST_DB_URL database.",
    )


def pytest_configure(config) -> None:
    if config.getoption("--run-db-tests"):
        test_url = os.environ.get("SV_TEST_DB_URL", "").strip()
        if not test_url:
            raise pytest.UsageError("--run-db-tests requires a disposable SV_TEST_DB_URL")
        os.environ["SV_DB_URL"] = test_url


def pytest_collection_modifyitems(config, items) -> None:
    if config.getoption("--run-db-tests"):
        return
    skip = pytest.mark.skip(reason="Requires --run-db-tests and disposable SV_TEST_DB_URL")
    for item in items:
        if item.get_closest_marker("offline") is None:
            item.add_marker(skip)


def pytest_ignore_collect(collection_path, config):
    if config.getoption("--run-db-tests"):
        return None
    if collection_path.name.startswith("test_") and collection_path.suffix == ".py":
        return "offline" not in collection_path.parts
    return None


def pytest_terminal_summary(terminalreporter, config) -> None:
    if not config.getoption("--run-db-tests"):
        terminalreporter.write_line(
            "Offline scope only: integration tests were not collected."
        )


@pytest.fixture(autouse=True)
def protect_offline_database(request, monkeypatch):
    if request.node.get_closest_marker("offline") is not None:
        import psycopg

        def reject_connection(*args, **kwargs):
            raise AssertionError("Offline tests must not connect to a database")

        monkeypatch.delenv("SV_DB_URL", raising=False)
        monkeypatch.setattr(psycopg, "connect", reject_connection)
