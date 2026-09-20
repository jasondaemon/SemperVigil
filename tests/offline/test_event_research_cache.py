from __future__ import annotations

import pytest

from sempervigil import worker
from sempervigil.legacy_event_retirement import _eligible, _fingerprint


pytestmark = pytest.mark.offline


def test_validator_version_changes_with_event_scope(monkeypatch):
    monkeypatch.setattr(
        worker,
        "_event_source_validation_profile",
        lambda _conn: {
            "id": "profile-1",
            "primary_provider_id": "provider-1",
            "primary_model_id": "model-1",
            "prompt_id": "prompt-1",
            "schema_id": None,
            "params": {"temperature": 0},
            "updated_at": "2026-09-20T00:00:00+00:00",
        },
    )
    first = worker._event_source_validator_version(
        object(),
        {"title": "Incident", "entity": "Example", "kind": "breach"},
    )
    second = worker._event_source_validator_version(
        object(),
        {"title": "Incident", "entity": "Different", "kind": "breach"},
    )
    assert first != second


def test_validator_version_changes_with_profile(monkeypatch):
    profile = {
        "id": "profile-1",
        "primary_provider_id": "provider-1",
        "primary_model_id": "model-1",
        "prompt_id": "prompt-1",
        "updated_at": "2026-09-20T00:00:00+00:00",
    }
    monkeypatch.setattr(worker, "_event_source_validation_profile", lambda _conn: profile)
    event = {"title": "Incident", "entity": "Example", "kind": "breach"}
    first = worker._event_source_validator_version(object(), event)
    profile["primary_model_id"] = "model-2"
    second = worker._event_source_validator_version(object(), event)
    assert first != second


def test_legacy_retirement_only_accepts_unpublished_candidates():
    state = {
        "visibility": "active",
        "lifecycle": "candidate",
        "publish_state": "draft",
        "candidate": True,
        "published_at": None,
        "site_slug": None,
    }
    assert _eligible(state)
    assert not _eligible({**state, "publish_state": "published"})
    assert not _eligible({**state, "visibility": "suppressed"})
    assert not _eligible({**state, "candidate": False})


def test_retirement_fingerprint_detects_later_changes():
    state = {
        "visibility": "active",
        "lifecycle": "candidate",
        "status": "open",
        "candidate": True,
        "publish_state": "draft",
        "published_at": None,
        "site_slug": None,
        "updated_at": "2026-09-20T00:00:00+00:00",
    }
    assert _fingerprint(state) != _fingerprint({**state, "status": "closed"})
