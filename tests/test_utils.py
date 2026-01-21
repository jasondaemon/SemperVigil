from sempervigil.utils import normalize_url


def test_normalize_url_strips_tracking_and_sorts():
    url = "https://Example.com/path?utm_source=news&b=2&a=1"
    normalized = normalize_url(url, strip_tracking_params=True, tracking_params=["utm_source"])
    assert normalized == "https://example.com/path?a=1&b=2"


def test_normalize_url_keeps_tracking_when_disabled():
    url = "https://example.com/path?utm_source=news&b=2"
    normalized = normalize_url(url, strip_tracking_params=False, tracking_params=["utm_source"])
    assert normalized == "https://example.com/path?b=2&utm_source=news"
