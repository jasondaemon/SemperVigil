import pytest

from sempervigil.event_report_presentation import source_report_metadata, quotation_report_metadata
pytestmark = pytest.mark.offline


def packet(sources, citations, timeline=()):
    return {'sources': sources, 'report': {'items': [
        {'section': 'overview', 'citations': [{'source_id': sid} for sid in citations]},
        *[{'section': 'timeline', 'date_label': label, 'date_sort': date,
           'citations': [{'source_id': citations[0]}]} for label, date in timeline]]}}


def source(sid, url, **extra):
    return {'id': sid, 'title': 'Genuine article title '+sid, 'url': url, **extra}


def test_only_cited_sources_preserve_original_numbers_and_genuine_dates():
    result = source_report_metadata(packet([
        source('S1', 'https://publisher.example/one', published_at='2026-09-24T09:56:04Z'),
        source('S2', 'https://publisher.example/unused'),
        source('S3', 'https://www.sec.gov/filing')], ['S3', 'S1']))
    assert [s['numbers'] for s in result['event_report_sources']] == [[1], [3]]
    assert result['event_cited_source_count'] == 2
    assert result['event_report_sources'][0]['published_at'] == '2026-09-24T09:56:04Z'
    assert result['event_report_sources'][1]['publisher'] == 'sec.gov'
    assert 'published_at' not in result['event_report_sources'][1]


def test_duplicate_urls_keep_all_citation_numbers_without_duplicate_entries():
    result = source_report_metadata(packet([
        source('S1', 'https://publisher.example/story/'),
        source('S2', 'https://publisher.example/story')], ['S1', 'S2']))
    assert len(result['event_report_sources']) == 1
    assert result['event_report_sources'][0]['numbers'] == [1, 2]


def test_missing_reference_fails_instead_of_silently_dropping_a_source():
    with pytest.raises(ValueError, match='source_missing'):
        source_report_metadata(packet([source('S1', 'https://publisher.example/one')], ['S9']))


def test_chronology_never_invents_dates_or_reorders_approved_items():
    result = source_report_metadata(packet([source('S1', 'https://publisher.example/one')],
        ['S1'], [('Undated', None), ('September 22, 2026', '2026-09-22')]))
    assert result['event_report_timeline'] == [
        {'label': 'Undated', 'date': None}, {'label': 'September 22, 2026', 'date': '2026-09-22'}]


def test_older_quotations_deduplicate_and_do_not_invent_publication_dates():
    entry = {'article_id': 8, 'source_title': 'Published title',
             'url': 'https://publisher.example/story', 'feed_day': '2026-10-01'}
    result = quotation_report_metadata({'entries': [entry, entry],
        'unrepresented_article_ids': [9]})
    assert result == {'event_report_sources': [{'numbers': [1], 'title': 'Published title',
        'url': 'https://publisher.example/story', 'publisher': 'publisher.example'}],
        'event_cited_source_count': 1}
