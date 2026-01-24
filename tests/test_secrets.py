import base64

import pytest

from sempervigil.security.secrets import decrypt_secret, encrypt_secret


def _set_master_env(monkeypatch):
    key = base64.urlsafe_b64encode(b"a" * 32).decode("utf-8")
    monkeypatch.setenv("SEMPERIVGIL_MASTER_KEY", key)
    monkeypatch.setenv("SEMPERIVGIL_KEY_ID", "v1")


def test_encrypt_decrypt_roundtrip(monkeypatch):
    _set_master_env(monkeypatch)
    key_id, blob = encrypt_secret("supersecret", b"provider:test")
    assert key_id == "v1"
    assert decrypt_secret(blob, b"provider:test") == "supersecret"


def test_encrypt_decrypt_aad_mismatch(monkeypatch):
    _set_master_env(monkeypatch)
    _, blob = encrypt_secret("supersecret", b"provider:test")
    with pytest.raises(Exception):
        decrypt_secret(blob, b"provider:other")
