from __future__ import annotations

import hashlib
import re
import unicodedata


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def normalize_name(value: str) -> str:
    raw = value.strip().lower()
    if not raw:
        return ""
    ascii_folded = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    normalized = _NON_ALNUM.sub("_", ascii_folded)
    normalized = _MULTI_UNDERSCORE.sub("_", normalized).strip("_")
    if normalized:
        return normalized
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"u_{digest}"


def display_name(value: str) -> str:
    value = value.strip()
    if value:
        return value
    return ""


def cpe_to_vendor_product(cpe: str) -> tuple[str | None, str | None]:
    parts = cpe.split(":")
    if len(parts) < 5:
        return None, None
    return parts[3] or None, parts[4] or None
