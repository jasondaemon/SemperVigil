"""Offline, provenance-preserving split proposals; no publication or approval writes.

The automated review remains bound to the original report. A separate independent
editorial review must bind the derivative and complete evidence. This workbench
does not authenticate a reviewer or grant publication authority.
"""
import copy
from .investigation import _version
from . import event_report_contract_v2 as contract
from .attack_catalog import validate_report

WORKFLOW = 'event-report-editorial-split-v1'
CHECKS = {'complete_sources', 'all_findings_supported', 'inference_premises_and_limits',
          'correct_epistemic_types', 'no_added_facts', 'provenance_preserved',
          'whole_derivative_reviewed'}


def prepare(report, evidence, review, operations, *, editor, catalog):
    """Split exact prose into typed parts without changing sources or other items."""
    if not isinstance(editor, str) or not editor.strip():
        raise ValueError('editorial_editor_required')
    validate_report(report, evidence, catalog, contract_override=contract)
    contract.validate_review(review, report, evidence)
    if review.get('ready') is not True:
        raise ValueError('editorial_original_review_not_ready')
    if not isinstance(operations, list) or not operations:
        raise ValueError('editorial_operations_required')
    result = copy.deepcopy(report)
    original_ids = {item['id'] for item in report['items']}
    used_ids = set(original_ids)
    changed = set()
    for operation in operations:
        if set(operation) != {'item_id', 'parts'} or operation['item_id'] in changed:
            raise ValueError('editorial_operation_invalid')
        matches = [i for i in result['items'] if i['id'] == operation['item_id']]
        if len(matches) != 1:
            raise ValueError('editorial_item_missing')
        original = matches[0]
        parts = copy.deepcopy(operation['parts'])
        if not isinstance(parts, list) or len(parts) < 2:
            raise ValueError('editorial_split_required')
        if original.get('attack_mappings') or any(p.get('attack_mappings') != [] for p in parts):
            raise ValueError('editorial_mapped_split_requires_separate_semantic_path')
        if parts[0]['id'] != original['id']:
            raise ValueError('editorial_original_id_required')
        if ' '.join(p['text'] for p in parts) != original['text']:
            raise ValueError('editorial_prose_changed')
        for part in parts:
            if set(part) != set(original):
                raise ValueError('editorial_fields_changed')
            for citation in part['citations']:
                if citation not in original['citations']:
                    raise ValueError('editorial_citation_added_or_changed')
        if any(not any(c in p['citations'] for p in parts) for c in original['citations']):
            raise ValueError('editorial_citation_removed')
        for part in parts[1:]:
            if part['id'] in used_ids:
                raise ValueError('editorial_item_id_collision')
            used_ids.add(part['id'])
        index = result['items'].index(original)
        result['items'][index:index+1] = parts
        changed.add(original['id'])
    spans, resolved = validate_report(result, evidence, catalog, contract_override=contract)
    artifact = {'workflow': WORKFLOW, 'editor': editor,
                'original_report': copy.deepcopy(report), 'original_review': copy.deepcopy(review),
                'original_report_version': _version(report), 'original_review_version': _version(review),
                'evidence_version': _version(evidence), 'operations': copy.deepcopy(operations),
                'report': result, 'report_version': _version(result), 'spans': spans,
                'resolved_mappings': resolved,
                'automated_review_scope': 'original_report_only',
                'editorial_review_status': 'pending_independent_review'}
    return artifact


def verify(artifact, evidence, *, catalog):
    expected = prepare(artifact['original_report'], evidence, artifact['original_review'],
                       artifact['operations'], editor=artifact['editor'], catalog=catalog)
    if artifact != expected:
        raise ValueError('editorial_derivative_integrity')
    return artifact


def validate_manual_review(artifact, review, evidence, *, catalog):
    """Validate a receipt's binding; callers must authenticate its independent role."""
    verify(artifact, evidence, catalog=catalog)
    required = {'workflow', 'ready', 'reviewer', 'report_version', 'evidence_version',
                'original_report_version', 'original_review_version', 'checks', 'source_review'}
    if (not isinstance(review, dict) or set(review) != required or
        review['workflow'] != 'event-report-independent-editorial-review-v1' or
        review['ready'] is not True or not isinstance(review['reviewer'], str) or
        not review['reviewer'].strip() or review['reviewer'] == artifact['editor'] or
        not isinstance(review['source_review'], str) or not review['source_review'].strip() or
        set(review['checks']) != CHECKS or any(v is not True for v in review['checks'].values()) or
        any(review[k] != artifact[k] for k in
            ['report_version', 'evidence_version', 'original_report_version', 'original_review_version'])):
        raise ValueError('editorial_independent_review_required')
    return {'workflow': review['workflow'], 'review_version': _version(review),
            'derivative_version': _version(artifact), 'report_version': artifact['report_version'],
            'automated_review_scope': 'original_report_only'}
