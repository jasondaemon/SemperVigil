import re
from pathlib import Path


SQL_ALIAS_DOT_RE = re.compile(r"AS\s+\w+\.\w+", re.IGNORECASE)


def test_storage_sql_aliases_no_dotted_names():
    storage_path = Path(__file__).resolve().parents[1] / "src" / "sempervigil" / "storage.py"
    text = storage_path.read_text()
    assert not SQL_ALIAS_DOT_RE.search(text), "Found dotted SQL alias (AS a.b) in storage.py"


def test_search_articles_unpack_no_dotted_names():
    storage_path = Path(__file__).resolve().parents[1] / "src" / "sempervigil" / "storage.py"
    text = storage_path.read_text()
    block_start = text.find("def search_articles")
    assert block_start != -1
    block_end = text.find("def get_cve", block_start)
    assert block_end != -1
    block = text[block_start:block_end]
    assert ", c." not in block and " c." not in block, "Found dotted unpack target in search_articles"


def test_sql_c_alias_references_have_from():
    storage_path = Path(__file__).resolve().parents[1] / "src" / "sempervigil" / "storage.py"
    text = storage_path.read_text()
    sql_blocks = []
    sql_blocks += re.findall(r""""(.*?)"""", text, flags=re.DOTALL)
    sql_blocks += re.findall(r"'''(.*?)'''", text, flags=re.DOTALL)
    requires_alias = ("c.published_at", "c.description_text", "c.preferred_", "c.last_modified_at")
    allowed_alias = (
        "FROM cves c",
        "JOIN cves c",
        "FROM cve_snapshots c",
        "JOIN cve_snapshots c",
        "FROM articles c",
        "JOIN articles c",
    )
    for sql in sql_blocks:
        if not any(token in sql for token in requires_alias):
            continue
        if not any(alias in sql for alias in allowed_alias):
            raise AssertionError(f"Found c.* reference without alias in SQL: {sql}")
