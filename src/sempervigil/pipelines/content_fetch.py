from __future__ import annotations

import logging
import re
import urllib.request
from typing import Any

from bs4 import BeautifulSoup

from ..utils import log_event


def fetch_article_content(
    url: str,
    *,
    timeout_seconds: int,
    user_agent: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read()
    except Exception as exc:  # noqa: BLE001
        log_event(logger, logging.WARNING, "content_fetch_failed", url=url, error=str(exc))
        raise
    html = raw.decode("utf-8", errors="replace")
    text = extract_readable_text(html)
    return {"content_text": text, "content_html": html}


def extract_readable_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
    article = soup.find("article")
    if article:
        return _normalize_text(article.get_text(" ", strip=True))
    best = None
    best_len = 0
    for div in soup.find_all("div"):
        text = div.get_text(" ", strip=True)
        if len(text) > best_len:
            best_len = len(text)
            best = text
    if best:
        return _normalize_text(best)
    return _normalize_text(soup.get_text(" ", strip=True))


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
