"""Public presentation metadata; never changes report prose or evidence identity."""
from urllib.parse import urlparse

from .enrichment.url import normalize_url


def quotation_report_metadata(projection):
    """Older quote reports include only represented public entries, never linked extras."""
    by_article = {entry['article_id']: entry for entry in projection['entries']}
    entries, by_url = [], {}
    for number, article_id in enumerate(sorted(by_article), 1):
        source = by_article[article_id]
        key = normalize_url(source['url'])
        if key in by_url:
            by_url[key]['numbers'].append(number)
            continue
        entry = {'numbers': [number], 'title': source['source_title'], 'url': source['url'],
                 'publisher': (urlparse(source['url']).hostname or '').removeprefix('www.')}
        # feed_day is not publication date; it must never fill published_at.
        entries.append(entry)
        by_url[key] = entry
    return {'event_report_sources': entries, 'event_cited_source_count': len(entries)}


def source_report_metadata(bundle):
    """Preserve citation numbers, omit uncited packet sources, collapse URL aliases."""
    sources = bundle['sources']
    used = {citation['source_id'] for item in bundle['report']['items']
            for citation in item['citations']}
    if not used <= {source['id'] for source in sources}:
        raise ValueError('event_report_presentation_source_missing')
    entries, by_url = [], {}
    for number, source in enumerate(sources, 1):
        if source['id'] not in used:
            continue
        key = normalize_url(source['url'])
        if key in by_url:
            by_url[key]['numbers'].append(number)
            continue
        entry = {'numbers': [number], 'title': source['title'], 'url': source['url'],
                 'publisher': (urlparse(source['url']).hostname or '').removeprefix('www.')}
        if source.get('published_at'):
            entry['published_at'] = source['published_at']
        entries.append(entry)
        by_url[key] = entry
    chronology = [{'label': item['date_label'], 'date': item['date_sort']}
                  for item in bundle['report']['items'] if item['section'] == 'timeline']
    return {'event_report_sources': entries, 'event_report_timeline': chronology,
            'event_cited_source_count': len(entries)}
