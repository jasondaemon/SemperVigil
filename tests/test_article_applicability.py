import json

import pytest

from sempervigil import article_applicability as applicability

pytestmark = pytest.mark.offline


ARTICLE = {
    "id": 42,
    "title": "Example decision and incident",
    "content_text": (
        "The UK regulator fined Acme after a data breach affecting customers in Germany.\n\n"
        "Acme said its Widget service was compromised by attackers."
    ),
}
GENERATION = "a" * 64


def test_private_result_preserves_distinct_roles_and_multi_region_scope():
    result = applicability.validate(json.dumps({
        "kinds": [
            {"type": "incident_report", "passage_ids": ["p001", "p002"]},
            {"type": "regulatory_enforcement", "passage_ids": ["p001"]},
        ],
        "jurisdictions": [
            {"code": "GB", "role": "reporting_authority", "passage_ids": ["p001"]},
            {"code": "DE", "role": "affected", "passage_ids": ["p001"]},
        ],
        "organizations": [
            {"name": "Acme", "role": "victim", "passage_ids": ["p002"]},
            {"name": "Acme", "role": "product_vendor", "passage_ids": ["p002"]},
        ],
        "products": [
            {"name": "Widget", "vendor_name": "Acme", "role": "affected",
             "passage_ids": ["p002"]},
        ],
    }), ARTICLE, GENERATION)
    assert result["public_eligible"] is False
    assert result["result"]["jurisdictions"][1]["code"] == "DE"
    assert result["result"]["products"][0]["vendor_name"] == "Acme"
    assert result["source_version"] == applicability.request(ARTICLE, GENERATION)["source_version"]


def test_unknown_scope_is_empty_not_source_country_guess():
    result = applicability.validate(
        '{"kinds":[],"jurisdictions":[],"organizations":[],"products":[]}',
        ARTICLE, GENERATION
    )
    assert result["result"]["jurisdictions"] == []


@pytest.mark.parametrize("passage_ids", [["p999"], ["p001", "p001"]])
def test_rejects_invalid_evidence_references(passage_ids):
    raw = json.dumps({"kinds": [{"type": "analysis", "passage_ids": passage_ids}],
                      "jurisdictions": [], "organizations": [], "products": []})
    with pytest.raises(ValueError, match="invalid_evidence"):
        applicability.validate(raw, ARTICLE, GENERATION)


def test_rejects_extra_fields_and_replayed_article_version():
    with pytest.raises(ValueError, match="invalid_shape"):
        applicability.validate(
            '{"kinds":[],"jurisdictions":[],"organizations":[],"products":[],"approved":true}',
            ARTICLE, GENERATION,
        )
    changed = {**ARTICLE, "content_text": ARTICLE["content_text"] + " Updated."}
    assert applicability.request(changed, GENERATION)["source_version"] != applicability.request(
        ARTICLE, GENERATION
    )["source_version"]
