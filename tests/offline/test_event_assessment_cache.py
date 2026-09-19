import json
import os
from types import SimpleNamespace

import pytest

from sempervigil import event_assessment_cache as cache
from test_event_review import database, get_packet, resign
from test_event_assessment import response, configured
from sempervigil import worker

pytestmark = pytest.mark.offline


def completion(packet, identity="a" * 64):
    calls = []
    def run(text):
        calls.append(text)
        return response(packet)
    run.cache_identity = identity
    return run, calls


def test_unchanged_inputs_reuse_one_inference(database, tmp_path):
    packet = get_packet(database)
    run, calls = completion(packet)
    first, hit = cache.reuse(packet, run, tmp_path)
    assert not hit and first["public_eligible"] is False
    files = list((tmp_path / "assessment-cache").glob("*.json"))
    assert len(files) == 1 and files[0].stat().st_mode & 0o777 == 0o600
    stamp = files[0].stat().st_mtime_ns
    second, hit = cache.reuse(packet, run, tmp_path)
    assert hit and first == second and len(calls) == 1
    assert files[0].stat().st_mtime_ns == stamp


@pytest.mark.parametrize("change", ["evidence", "configuration"])
def test_changed_inputs_invalidate(database, tmp_path, change):
    packet = get_packet(database)
    run, calls = completion(packet)
    cache.reuse(packet, run, tmp_path)
    if change == "evidence":
        packet["documents"][0]["text"] += " Acme disclosed another fact."
        resign(packet)
    else:
        run.cache_identity = "b" * 64
    assert cache.reuse(packet, run, tmp_path)[1] is False
    assert len(calls) == 2


@pytest.mark.parametrize("damage", ["json", "generation", "packet", "symlink", "fifo", "oversized"])
def test_damaged_cache_fails_without_inference(database, tmp_path, damage):
    packet = get_packet(database)
    run, calls = completion(packet)
    cache.reuse(packet, run, tmp_path)
    path = next((tmp_path / "assessment-cache").glob("*.json"))
    entry = json.loads(path.read_bytes())
    if damage == "json": path.write_bytes(b"not json")
    if damage == "oversized": path.write_bytes(b" " * (cache.MAX_ENTRY_BYTES + 1))
    if damage == "generation":
        entry["generation_version"] = "b" * 64
        path.write_text(json.dumps(entry))
    if damage == "packet":
        entry["assessment"]["packet_version"] = "b" * 64
        path.write_text(json.dumps(entry))
    if damage in {"symlink", "fifo"}:
        path.unlink()
        if damage == "fifo": os.mkfifo(path)
        else: path.symlink_to(tmp_path / "missing")
    with pytest.raises((ValueError, OSError)):
        cache.reuse(packet, run, tmp_path)
    assert len(calls) == 1


def test_symlink_cache_directory_is_refused(database, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "assessment-cache").symlink_to(outside, target_is_directory=True)
    packet = get_packet(database)
    run, calls = completion(packet)
    with pytest.raises(OSError): cache.reuse(packet, run, tmp_path)
    assert calls == [] and list(outside.iterdir()) == []


def test_unversioned_callback_is_not_cached(database, tmp_path):
    packet = get_packet(database)
    run, calls = completion(packet)
    del run.cache_identity
    assert cache.reuse(packet, run, tmp_path)[1] is False
    assert cache.reuse(packet, run, tmp_path)[1] is False
    assert len(calls) == 2 and list(tmp_path.iterdir()) == []


def test_profile_guard_still_runs_before_reuse_identity(monkeypatch):
    profile = configured(monkeypatch)
    first = worker._private_review_completion(None, None, None).cache_identity
    assert first == worker._private_review_completion(None, None, None).cache_identity
    profile["params"]["max_tokens"] = 512
    assert first != worker._private_review_completion(None, None, None).cache_identity
    profile["params"]["max_tokens"] = 9000
    with pytest.raises(ValueError): worker._private_review_completion(None, None, None)


def test_configuration_change_during_inference_cannot_be_cached(monkeypatch):
    profile = configured(monkeypatch)
    def infer(*args, **kwargs):
        profile["params"]["max_tokens"] = 512
        return {"decisions": []}
    monkeypatch.setattr(worker, "run_profile", infer)
    run = worker._private_review_completion(None, SimpleNamespace(id="test"), None)
    with pytest.raises(ValueError, match="configuration_changed"):
        run("input")
