from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

# Prefer correctly spelled env vars, but support legacy misspelling for backward compatibility.
MASTER_KEY_ENV_PRIMARY = "SEMPERVIGIL_MASTER_KEY"
MASTER_KEY_ENV_LEGACY = "SEMPERIVGIL_MASTER_KEY"

KEY_ID_ENV_PRIMARY = "SEMPERVIGIL_KEY_ID"
KEY_ID_ENV_LEGACY = "SEMPERIVGIL_KEY_ID"

DEFAULT_KEY_ID = "v1"
HKDF_INFO = b"sempervigil:secrets:v1"


@dataclass(frozen=True)
class SecretBox:
    key_id: str
    aesgcm: AESGCM


def load_secret_box() -> SecretBox:
    master_b64 = _get_env(MASTER_KEY_ENV_PRIMARY, MASTER_KEY_ENV_LEGACY)
    if not master_b64:
        raise ValueError(
            f"Master key is not set. Set {MASTER_KEY_ENV_PRIMARY} (preferred) "
            f"or legacy {MASTER_KEY_ENV_LEGACY}."
        )

    try:
        master = base64.urlsafe_b64decode(_pad_b64(master_b64))
    except Exception as exc:  # noqa: BLE001
        raise ValueError("Master key is not valid base64url") from exc

    if len(master) != 32:
        raise ValueError("Master key must be 32 bytes (base64url encoded)")

    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=HKDF_INFO)
    derived = hkdf.derive(master)

    key_id = (
        os.environ.get(KEY_ID_ENV_PRIMARY)
        or os.environ.get(KEY_ID_ENV_LEGACY)
        or DEFAULT_KEY_ID
    )

    return SecretBox(key_id=key_id, aesgcm=AESGCM(derived))


def encrypt_secret(plaintext: str, aad: bytes) -> tuple[str, str]:
    box = load_secret_box()
    nonce = os.urandom(12)
    ciphertext = box.aesgcm.encrypt(nonce, plaintext.encode("utf-8"), aad)
    blob = base64.urlsafe_b64encode(nonce + ciphertext).decode("utf-8")
    return box.key_id, blob


def decrypt_secret(blob_b64: str, aad: bytes) -> str:
    box = load_secret_box()
    data = base64.urlsafe_b64decode(_pad_b64(blob_b64))
    nonce = data[:12]
    ciphertext = data[12:]
    plaintext = box.aesgcm.decrypt(nonce, ciphertext, aad)
    return plaintext.decode("utf-8")


def _get_env(primary: str, legacy: str) -> str:
    """Return primary env var if set, otherwise legacy env var."""
    return os.environ.get(primary) or os.environ.get(legacy, "")


def _pad_b64(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    return value + padding