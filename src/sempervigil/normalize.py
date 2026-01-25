from __future__ import annotations

import re


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def normalize_name(value: str) -> str:
    value = value.strip().lower()
    value = _NON_ALNUM.sub("_", value)
    value = _MULTI_UNDERSCORE.sub("_", value)
    return value.strip("_")


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
