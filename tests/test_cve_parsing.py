from sempervigil.cve_sync import _extract_description, _extract_cvss, _select_preferred_metrics
from sempervigil.cve_filters import extract_signals


def test_extract_description_variants():
    assert _extract_description("hello") == "hello"
    assert _extract_description({"value": "hi"}) == "hi"
    assert _extract_description([{"lang": "en", "value": "english"}]) == "english"
    assert _extract_description([{"lang": "fr", "value": "bonjour"}]) == "bonjour"


def test_extract_signals_configurations_list():
    cve_item = {
        "configurations": [
            {
                "nodes": [
                    {
                        "cpeMatch": [
                            {"criteria": "cpe:2.3:a:vendor:product:1.0:*:*:*:*:*:*:*"}
                        ]
                    }
                ]
            }
        ],
        "references": [{"url": "https://example.com/advisory"}],
    }
    signals = extract_signals(cve_item)
    assert "vendor" in signals.vendors
    assert "product" in signals.products
    assert signals.cpes
    assert "vendor:product:1.0" in signals.product_versions
    assert "example.com" in signals.reference_domains


def test_extract_cvss_list_and_preferred():
    entries = [
        {
            "type": "Secondary",
            "source": "nvd",
            "cvssData": {"version": "3.1", "baseScore": 5.0, "baseSeverity": "MEDIUM", "vectorString": "AV:N"},
        },
        {
            "type": "Primary",
            "source": "nvd",
            "cvssData": {"version": "3.1", "baseScore": 9.8, "baseSeverity": "CRITICAL", "vectorString": "AV:N"},
        },
    ]
    v31_list = _extract_cvss(entries, "3.1")
    assert len(v31_list) == 2
    preferred = _select_preferred_metrics(v31_list, [], prefer_v4=False)
    assert preferred.version == "3.1"
    assert preferred.base_score == 9.8
