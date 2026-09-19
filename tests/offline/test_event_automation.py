import copy
import json
from unittest.mock import Mock

import pytest

from sempervigil import event_automation as auto, event_assessment as assessment
from test_event_review import database, get_packet, resign
from test_event_scope import proposal

pytestmark = pytest.mark.offline


@pytest.fixture
def material(database):
    packet = get_packet(database)
    scope = proposal(packet)
    packet['documents'].append({**packet['documents'][0], 'article_id': 2,
        'url': 'https://example.net/story',
        'text': 'Acme confirmed that its contact system was breached by an attacker.'})
    resign(packet)
    return packet, scope


def test_default_disabled_never_reads_database(monkeypatch):
    monkeypatch.delenv('SV_EVENT_AUTO_SCOPES', raising=False)
    conn = Mock()
    assert auto.tick(conn) == []
    conn.execute.assert_not_called()


@pytest.mark.parametrize('value', ['[]', 'null', '{"event":"no"}', '{"event":true}',
    json.dumps({str(i): 'a'*64 for i in range(4)}), 'x'*2049])
def test_invalid_enrollment_rejected(monkeypatch, value):
    monkeypatch.setenv('SV_EVENT_AUTO_SCOPES', value)
    with pytest.raises(ValueError): auto.enrollments()


def test_enrollment_pins_exact_revision(monkeypatch):
    monkeypatch.setenv('SV_EVENT_AUTO_SCOPES', json.dumps({'event': 'a'*64}))
    assert auto.enrollments() == {'event': 'a'*64}


def test_policy_requires_both_scope_terms_and_short_exact_excerpt(material):
    packet, scope = material
    values = auto.candidates(packet, scope, {1})
    assert len(values) == 1 and values[0]['article_id'] == 2
    p = values[0]
    assert packet['documents'][1]['text'][p['start']:p['end']] == p['quote']


@pytest.mark.parametrize('text', [
    'Acme makes software for many important customers around the world.',
    'The contact system suffered a breach affecting another organization.',
    'Acme used a contact systems provider for its software platform.',
    'Acme contact system ' + 'word '*30 + '.',
])
def test_weak_or_oversized_candidates_not_admitted(material, text):
    packet, scope = material
    packet['documents'][1]['text'] = text
    resign(packet)
    assert auto.candidates(packet, scope, {1}) == []


def test_duplicate_source_not_quoted_twice(material):
    packet, scope = material
    packet['documents'][1]['url'] = packet['documents'][0]['url']
    resign(packet)
    assert auto.candidates(packet, scope, {1}) == []


@pytest.mark.parametrize('fault', ['anchor', 'omission', 'truncated'])
def test_changed_anchor_and_incomplete_evidence_fail_closed(material, fault):
    packet, scope = material
    if fault == 'anchor': packet['documents'][0]['text'] += ' Changed.'
    if fault == 'omission': packet['omissions'] = [{'reason':'unavailable'}]
    if fault == 'truncated': packet['links_truncated'] = True
    resign(packet)
    with pytest.raises(ValueError): auto.candidates(packet, scope, {1})


@pytest.mark.parametrize('decision,reason,accepted', [
    ('include','same_incident',True), ('include','explicit_update',True),
    ('hold','insufficient_context',False), ('hold','conflicting_evidence',False),
    ('exclude','different_incident',False), ('exclude','unrelated_context',False)])
def test_model_include_is_necessary_but_policy_also_required(material, decision, reason, accepted):
    packet, scope = material
    request = assessment.request_for(packet, scope=scope, article_id=2, paired=True)
    result = assessment.validate_response(json.dumps({'decisions':[
        {'id': key, 'decision':decision, 'reason':reason} for key in request['mapping']]}).encode(),
        packet, scope=scope, article_id=2, paired=True)
    assert bool(auto.select_quote(packet, scope, result, 2, {1})) == accepted
    assert auto.select_quote(packet, scope, result, 2, {1,2}) is None


def test_nonpaired_and_corrupted_assessments_rejected(material):
    packet, scope = material
    request = assessment.request_for(packet, scope=scope, article_id=2)
    result = assessment.validate_response(json.dumps({'decisions':[
        {'id': key, 'decision':'include', 'reason':'same_incident'} for key in request['mapping']]}).encode(),
        packet, scope=scope, article_id=2)
    with pytest.raises(ValueError): auto.select_quote(packet, scope, result, 2, {1})
    result['paired'] = True
    with pytest.raises(ValueError): auto.select_quote(packet, scope, result, 2, {1})


def test_tick_bounds_new_admission_and_isolates_holds(monkeypatch):
    monkeypatch.setenv('SV_EVENT_AUTO_SCOPES', json.dumps({'one':'a'*64,'two':'b'*64}))
    run = Mock(return_value={'status':'review_queued'})
    monkeypatch.setattr(auto,'advance',run)
    assert auto.tick(Mock()) == [{'status':'review_queued'}]
    assert run.call_count == 1
    run.side_effect = [ValueError('changed_source'), {'status':'unchanged'}]
    result = auto.tick(Mock())
    assert result[0]['status'] == 'held' and result[1]['status'] == 'unchanged'
