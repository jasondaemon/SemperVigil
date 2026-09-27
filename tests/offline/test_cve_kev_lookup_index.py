from unittest.mock import Mock

import pytest

from sempervigil import migrations_pg

pytestmark = pytest.mark.offline


def test_kev_lookup_index_matches_unchecked_query():
    conn = Mock()

    migrations_pg._migrate_cve_kev_lookup_index(conn)

    sql = conn.execute.call_args.args[0]
    assert "COALESCE(last_modified_at, published_at)" in sql
    assert "WHERE kev_checked_at IS NULL" in sql
