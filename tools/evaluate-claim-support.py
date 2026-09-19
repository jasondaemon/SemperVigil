#!/usr/bin/env python3
"""Evaluate saved private job results against frozen expectations, without inference."""
import argparse
import json
from pathlib import Path


def evaluate(expected: dict, jobs: list[dict]) -> dict:
    cases = expected['cases']
    by_id = {case['id']: case for case in cases}
    if len(by_id) != len(cases):
        raise ValueError('duplicate_expected_claim')
    observed, errors = {}, []
    for job in jobs:
        if job.get('status') != 'succeeded':
            errors.append({'job_id': job.get('id'), 'error': 'job_not_succeeded'})
            continue
        result = (job.get('result') or {}).get('claim_support') or {}
        if result.get('event_id') != expected['event_id'] or result.get('public_eligible') is not False:
            raise ValueError('unexpected_audit_identity_or_authority')
        for suggestion in result['suggestions']:
            key = suggestion['claim_id']
            if key in observed:
                raise ValueError('duplicate_observed_claim')
            if suggestion['decision'] not in {'model_supported', 'reject', 'hold'}:
                raise ValueError('invalid_observed_decision')
            observed[key] = suggestion['decision']
    extra = sorted(set(observed) - set(by_id))
    matrix, mismatches = {}, []
    for key, case in by_id.items():
        actual = observed.get(key, 'unassessed')
        pair = case['expected'] + ' -> ' + actual
        matrix[pair] = matrix.get(pair, 0) + 1
        if actual != case['expected']:
            mismatches.append({'id': key, 'expected': case['expected'], 'actual': actual,
                               'reason': case['reason']})
    return {'passed': not (mismatches or errors or extra), 'cases': len(cases),
            'assessed': len(set(observed) & set(by_id)), 'matrix': matrix,
            'mismatches': mismatches, 'job_errors': errors, 'unexpected_claims': extra}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected', type=Path, required=True)
    parser.add_argument('--results', type=Path, required=True)
    args = parser.parse_args()
    report = evaluate(json.loads(args.expected.read_text()), json.loads(args.results.read_text()))
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
