import pytest

from sempervigil import incident_candidates as candidates

pytestmark = pytest.mark.offline


def record(*facts):
    return {"status": "unreviewed", "public_eligible": False, "facts": list(facts)}


def fact(identifier, statement, *, date_role="none", date_text=None):
    return {"id": identifier, "statement": statement, "kind": "reported_fact",
            "date_role": date_role, "date_text": date_text}


def test_projection_requires_incident_signal_not_shared_cve_or_vendor():
    evidence = record(
        fact("f1", "SolarWinds patched CVE-2026-28326 in Access Rights Manager."),
        fact("f2", "The vulnerability has a CVSS score of 8.8."),
    )
    assert candidates.projection(evidence, "SolarWinds patch") is None


def test_projection_retains_explicit_campaign_date_and_supporting_facts():
    evidence = record(
        fact("f1", "WaterPlum compromised 30,000 devices in the campaign known as \"Contagious Interview\".",
             date_role="incident", date_text="December 2025 through July 2026"),
        fact("f2", "The actors exfiltrated credentials from cryptocurrency wallets."),
        fact("f3", "The advisory recommends verifying applicant identities."),
    )
    result = candidates.projection(evidence, "WaterPlum campaign")
    assert result == {
        "projection_version": candidates.PROJECTION_VERSION,
        "title": "WaterPlum campaign",
        "kind": "campaign",
        "incident_dates": ["December 2025 through July 2026"],
        "campaigns": ["Contagious Interview"],
        "cves": [],
        "supporting_fact_ids": ["f1", "f2"],
    }


def test_projection_rejects_non_private_or_malformed_evidence():
    with pytest.raises(ValueError, match="shape_invalid"):
        candidates.projection({"status": "accepted", "public_eligible": False, "facts": []}, "x")
