from sempervigil.worker import _parse_product_items


def test_parse_product_items_article():
    result = {"parsed": {"items": [{"vendor": "Acme", "product": "Widget"}]}}
    items, error = _parse_product_items(result, allow_versions=False)
    assert error is None
    assert items == [{"vendor": "Acme", "product": "Widget"}]


def test_parse_product_items_cve_versions():
    result = {"parsed": [{"vendor": "Acme", "product": "Widget", "versions": ["<=1.2"]}]}
    items, error = _parse_product_items(result, allow_versions=True)
    assert error is None
    assert items == [{"vendor": "Acme", "product": "Widget", "versions": ["<=1.2"]}]


def test_parse_product_items_invalid_json():
    result = {"raw": "not json"}
    items, error = _parse_product_items(result, allow_versions=False)
    assert items == []
    assert error in {"invalid_json", "no_items"}
