from __future__ import annotations

import hashlib
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode


_TRACKING_PREFIXES = ("utm_",)
_TRACKING_KEYS = {"gclid", "fbclid", "mc_cid", "mc_eid"}


def normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "http").lower()
    netloc = parsed.netloc.lower()
    if ":" in netloc:
        host, port = netloc.rsplit(":", 1)
        if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
            netloc = host
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    query_pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k and not k.startswith(_TRACKING_PREFIXES) and k not in _TRACKING_KEYS
    ]
    query = urlencode(sorted(query_pairs))
    normalized = urlunparse((scheme, netloc, path, "", query, ""))
    return normalized


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()
