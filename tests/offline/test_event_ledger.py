import pytest

from sempervigil import event_ledger

pytestmark = pytest.mark.offline


def test_section_tags_are_transparent_and_preserve_fact_roles():
    impact = {"statement": "The actors stole credentials from 30,000 devices.",
              "kind": "reported_fact", "date_role": "incident", "date_text": "July 2026"}
    assert event_ledger._section_tags(impact) == ["timeline", "impact"]
    recommendation = {"statement": "CISA recommends resetting exposed credentials.",
                      "kind": "recommendation", "date_role": "none", "date_text": None}
    assert event_ledger._section_tags(recommendation) == ["mitigation", "attribution"]
    allegation = {"statement": "The source alleged that an unknown actor gained access.",
                  "kind": "allegation", "date_role": "none", "date_text": None}
    assert event_ledger._section_tags(allegation) == ["open_question"]


@pytest.mark.parametrize("kind", ["daily-summary", "rewrite", "publish"])
def test_unknown_change_kinds_are_not_admitted(kind):
    assert kind not in event_ledger.CHANGE_KINDS


def test_publication_is_not_a_ledger_review_decision():
    assert "publish" not in event_ledger.DECISIONS
