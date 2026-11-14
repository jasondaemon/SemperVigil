"""Publisher captures and research identity are deterministic, original-only inputs."""
import copy
import pytest
from sempervigil import event_source_reports_v2 as reports, event_report_contract_v2 as contract
from sempervigil.event_report_initial import validate, require_source_identity
from sempervigil.enrichment.query import build_event_enrich_query

pytestmark = pytest.mark.offline


def rows():
    return [(1, 'Original', 'https://publisher.test/story/', 'The legacy EHR incident occurred in January 2025.',
             '2026-10-01', '2026-10-01', '{}', '2026-10-01', '1', None),
            (2, 'Updated', 'https://publisher.test/story?utm_source=feed',
             'The legacy EHR incident occurred in January 2025. New confirmed detail.',
             '2026-10-01', '2026-10-02', '{}', '2026-10-02', '1', None)]


def test_same_publisher_url_is_one_full_model_source_with_reversible_capture_history():
    packet = reports._source_material('evt_test', 'Legacy EHR incident', rows(), [1, 2])
    assert len(packet['sources']) == 1 and packet['sources'][0]['article_id'] == 2
    assert [m['article_id'] for m in packet['membership']] == [1, 2]
    assert [c['text'] for c in packet['capture_history'][0]['captures']] == [r[3] for r in rows()]
    model = contract.context(packet)
    assert 'capture_history' not in model
    assert len(model['sources']) == 1 and model['sources'][0]['text'] == rows()[1][3]
    assert model['coverage']['omitted_source_ids'] == []
    # The discarded model duplicate remains part of immutable capture integrity.
    changed = rows(); changed[0] = (*changed[0][:3], 'Corrected original capture.', *changed[0][4:])
    assert reports._source_material('evt_test', 'Legacy EHR incident', changed, [1, 2])['source_version'] != packet['source_version']


def test_latest_failed_capture_does_not_replace_last_complete_body():
    values = rows(); values[1] = (*values[1][:8], '0', 'fetch failed')
    packet = reports._source_material('evt_test', 'Legacy EHR incident', values, [1, 2])
    assert packet['sources'][0]['article_id'] == 1
    assert len(packet['capture_history'][0]['captures']) == 2
    values[0] = (*values[0][:8], '0', 'fetch failed')
    with pytest.raises(ValueError, match='full_capture_unavailable'):
        reports._source_material('evt_test', 'Legacy EHR incident', values, [1, 2])


def test_new_capture_of_same_publisher_has_changed_current_citation_id():
    old = reports._source_material('evt_test', 'Legacy EHR incident', rows()[:1], [1])
    current = reports._source_material('evt_test', 'Legacy EHR incident', rows(), [1, 2])
    original = copy.deepcopy(old)
    result = contract.update_context(current, 'evidence_change', old)
    assert result['evidence_delta'] == {'baseline': 'known', 'new': [], 'changed': ['S2'], 'removed': []}
    assert result['sources'][0]['id'] == 'S2'
    assert result['capture_history'] == current['capture_history']
    assert old == original
    same = rows(); same[1] = (*same[1][:3], same[0][3], *same[1][4:])
    unchanged = reports._source_material('evt_test', 'Legacy EHR incident', same, [1, 2])
    assert contract.update_context(unchanged, 'evidence_change', old)['evidence_delta']['changed'] == []


@pytest.mark.parametrize('urls', [
    ('https://publisher.test/story;id=1', 'https://publisher.test/story;id=2'),
    ('https://publisher.test/story?id=1&id=2', 'https://publisher.test/story?id=2&id=1'),
    ('https://publisher.test/story#first', 'https://publisher.test/story#second'),
])
def test_resource_significant_url_components_preserve_distinct_evidence(urls):
    values = rows()
    values = [(*r[:2], url, *r[3:]) for r, url in zip(values, urls)]
    packet = reports._source_material('evt_test', 'Legacy EHR incident', values, [1, 2])
    assert [s['id'] for s in packet['sources']] == ['S1', 'S2']
    assert packet.get('capture_history', []) == []
    assert [s['text'] for s in packet['sources']] == [r[3] for r in values]


def identity():
    return {'event_id': 'evt_test', 'entity': 'Acme', 'system': 'legacy EHR',
            'incident_window': 'January 2025', 'incident_year': 2025, 'query_terms': ['legacy', 'EHR'],
            'source_anchors': [{'source_id': 'S1', 'quote': rows()[0][3]}]}


def test_query_uses_verified_incident_year_and_terms_not_publication_or_first_seen():
    event = {'id': 'evt_test', 'entity': 'Acme', 'kind': 'breach', 'first_seen': '2026-10-01',
             'incident_date': '2024-01-01'}
    plain = build_event_enrich_query(event)
    assert '2026' not in plain and '2024' not in plain
    trusted = validate(identity(), ['evt_test'])
    require_source_identity(trusted, [{'id': 'S1', 'text': rows()[0][3]}])
    assert build_event_enrich_query(event, incident_identity=trusted).endswith('"legacy" "EHR" 2025')
    bad = copy.deepcopy(trusted); bad['query_terms'] = ['UnknownVendor']
    with pytest.raises(ValueError, match='query_identity_source_changed'):
        require_source_identity(bad, [{'id': 'S1', 'text': rows()[0][3]}])
    bad = copy.deepcopy(trusted); bad['incident_year'] = 2026; bad['incident_window'] = 'January 2026'
    with pytest.raises(ValueError, match='query_identity_source_changed'):
        require_source_identity(bad, [{'id': 'S1', 'text': rows()[0][3]}])
    with pytest.raises(ValueError, match='query_identity_mismatch'):
        build_event_enrich_query({**event, 'entity': 'Other'}, incident_identity=trusted)


def test_broken_qualified_identity_reference_cannot_fall_back_to_generic_query():
    from types import SimpleNamespace
    from sempervigil.event_report_initial import research_identity
    conn = SimpleNamespace(execute=lambda *_: SimpleNamespace(fetchone=lambda: ('a'*64, None)))
    with pytest.raises(ValueError, match='identity_history_unavailable'):
        research_identity(conn, 'evt_test')
