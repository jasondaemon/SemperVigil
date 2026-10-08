"""Bounded discovery decisions, without fetched bodies or private headers."""
from urllib.parse import urlsplit, urlunsplit

LIMIT = 50


def result(rank, item):
    raw = str(item.get('url') or '')
    try:
        parsed = urlsplit(raw)
        # Search URLs are public evidence locators. Strip accidental userinfo and
        # query strings from diagnostic copies; original ingestion is unchanged.
        locator = urlunsplit((parsed.scheme, parsed.hostname or '', parsed.path, '', ''))
    except ValueError:
        locator = ''
    return {'rank': rank, 'url': locator[:2048],
            'title': str(item.get('title') or '')[:512], 'score': None,
            'score_reasons': {}, 'outcome': 'discarded', 'reason': 'missing_url'}
