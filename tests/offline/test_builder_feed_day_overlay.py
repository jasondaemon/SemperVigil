import runpy
import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.offline


ROOT = Path(__file__).resolve().parents[2]
OVERLAY = runpy.run_path(str(ROOT / "docker/builder-feed-day-overlay/overlay.py"))


def test_feed_day_overlay_preserves_unrelated_live_storage_code():
    source = (ROOT / "src/sempervigil/storage.py").read_text()
    base = source.replace("a.title ILIKE %s", "a.title LIKE %s")
    base = base.replace("cve_id ILIKE %s", "cve_id LIKE %s")
    base = base.replace("WHERE {cve_day_sql} = %s", "WHERE DATE(COALESCE(c.published_at, c.last_modified_at)) = %s")
    start = base.index("def _editorial_timezone_name()")
    end = base.index("def get_article_by_id(", start)
    base = base[:start] + "def _brief_day_from(value: str) -> str:\n    return value[:10]\n\n\n" + base[end:]
    result = OVERLAY["apply_overlay"](base, source)
    assert "a.title LIKE %s" in result
    assert "cve_id LIKE %s" in result
    assert "WHERE {cve_day_sql} = %s" in result
    assert "def _cve_feed_day_sql" in result
    names = [node.name for node in ast.parse(result).body if isinstance(node, ast.FunctionDef)]
    assert names.count("_cve_feed_day_sql") == 1
    assert names.count("_brief_day_from") == 1
