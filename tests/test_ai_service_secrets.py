import base64

from sempervigil.security.secrets import MASTER_KEY_ENV, KEY_ID_ENV
from sempervigil.services.ai_service import (
    create_provider,
    list_providers,
    load_provider_secret,
    set_provider_secret,
)
from sempervigil.storage import init_db


def _set_master_env(monkeypatch):
    key = base64.urlsafe_b64encode(b"b" * 32).decode("utf-8")
    monkeypatch.setenv(MASTER_KEY_ENV, key)
    monkeypatch.setenv(KEY_ID_ENV, "v1")


def test_provider_secret_is_encrypted(tmp_path, monkeypatch):
    _set_master_env(monkeypatch)
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    provider = create_provider(
        conn,
        {"name": "Test Provider", "type": "openai_compatible", "base_url": "https://example.com"},
    )
    set_provider_secret(conn, provider["id"], "supersecret")

    row = conn.execute(
        "SELECT api_key_enc, api_key_last4 FROM llm_provider_secrets WHERE provider_id = ?",
        (provider["id"],),
    ).fetchone()
    assert row is not None
    api_key_enc, api_key_last4 = row
    assert "supersecret" not in api_key_enc
    assert api_key_last4 == "cret"

    providers = list_providers(conn)
    assert providers[0]["key_last4"] == "cret"

    decrypted = load_provider_secret(conn, provider["id"])
    assert decrypted == "supersecret"
