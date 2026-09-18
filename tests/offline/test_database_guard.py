import os

import psycopg
import pytest

pytestmark = pytest.mark.offline


def test_offline_environment_has_no_database_url():
    assert "SV_DB_URL" not in os.environ


def test_offline_connection_is_rejected():
    with pytest.raises(AssertionError, match="must not connect"):
        psycopg.connect("postgresql://localhost/never_connect")
