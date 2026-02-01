from sempervigil.worker import (
    _article_vendor_product_select_cols,
    _cve_vendor_product_select_cols,
)


def test_article_vendor_product_select_cols_respects_columns():
    assert _article_vendor_product_select_cols(set()) == [
        "a.id",
        "a.title",
        "s.name AS source_name",
        "a.original_url",
        "a.published_at",
        "a.ingested_at",
    ]
    assert _article_vendor_product_select_cols({"summary_llm"})[-1] == "a.summary_llm"


def test_cve_vendor_product_select_cols_respects_columns():
    cols = _cve_vendor_product_select_cols(set())
    assert "summary" not in cols
    assert "description_text" not in cols
    cols_with_desc = _cve_vendor_product_select_cols({"description_text"})
    assert "description_text" in cols_with_desc
