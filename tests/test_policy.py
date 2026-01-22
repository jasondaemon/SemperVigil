import logging

from sempervigil.policy import resolve_policy


def test_policy_deep_merge_overrides_nested():
    overrides = {"canonical_url": {"strip_tracking_params": False}}
    merged = resolve_policy(overrides, logging.getLogger("test"))
    assert merged["canonical_url"]["strip_tracking_params"] is False
    assert "tracking_params" in merged["canonical_url"]
