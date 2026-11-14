import pytest

from sempervigil import article_applicability_eval as evaluator

pytestmark = pytest.mark.offline

VERSION = "a" * 64


def _casebook():
    return {"version": 1, "cases": [{
        "article_id": 1, "source_version": VERSION,
        "audited_groups": ["kinds", "jurisdictions", "organizations", "products"],
        "expected": {
            "kinds": [{"type": "regulatory_enforcement"}],
            "jurisdictions": [{"code": "GB", "role": "legal_scope"}],
            "organizations": [{"name": "Information Commissioner's Office", "role": "regulator"}],
            "products": [],
        },
    }]}


def _result():
    return {"workflow": evaluator.WORKFLOW, "public_eligible": False, "articles": [{
        "article_id": 1, "source_version": VERSION, "status": "private_comparison",
        "public_eligible": False,
        "result": {
            "kinds": [{"type": "regulatory_enforcement", "passage_ids": ["p001"]}],
            "jurisdictions": [{"code": "AU", "role": "legal_scope", "passage_ids": ["p001"]}],
            "organizations": [{"name": "Information  Commissioner's Office",
                               "role": "regulator", "passage_ids": ["p001"]}],
            "products": [{"name": "generic platform", "vendor_name": None,
                          "role": "referenced", "passage_ids": ["p001"]}],
        },
    }]}


def test_scores_false_scope_and_product_without_regrading_unreviewed_fields():
    report = evaluator.evaluate(_casebook(), [_result()])
    assert report["graded"] == 1
    assert report["scores"]["jurisdictions"] == {"tp": 0, "fp": 1, "fn": 1}
    assert report["scores"]["organizations"] == {"tp": 1, "fp": 0, "fn": 0}
    assert report["scores"]["products"] == {"tp": 0, "fp": 1, "fn": 0}


def test_stale_and_missing_results_are_not_graded():
    stale = _result()
    stale["articles"][0]["source_version"] = "b" * 64
    report = evaluator.evaluate(_casebook(), [stale])
    assert report["cases"][0]["status"] == "stale_source"
    assert report["graded"] == 0
    assert evaluator.evaluate(_casebook(), [])["cases"][0]["status"] == "missing"


def test_only_explicitly_audited_fields_are_scored():
    casebook = _casebook()
    case = casebook["cases"][0]
    case["audited_groups"] = ["kinds"]
    case["expected"] = {"kinds": case["expected"]["kinds"]}
    report = evaluator.evaluate(casebook, [_result()])
    assert report["scores"]["jurisdictions"] == {"tp": 0, "fp": 0, "fn": 0}


def test_duplicate_or_public_results_rejected():
    with pytest.raises(ValueError, match="not_private_result"):
        evaluator.evaluate(_casebook(), [{**_result(), "public_eligible": True}])
    with pytest.raises(ValueError, match="duplicate_result"):
        evaluator.evaluate(_casebook(), [_result(), _result()])
