from __future__ import annotations

import re
from typing import Iterable

from .models import Article

_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)


def extract_cve_ids(texts: Iterable[str]) -> list[str]:
    found: set[str] = set()
    for text in texts:
        if not text:
            continue
        for match in _CVE_RE.findall(text):
            found.add(match.upper())
    return sorted(found)


def build_cve_evidence(article: Article, cve_ids: list[str]) -> dict[str, object]:
    return {
        "extracted_signals": {
            "cve_ids": cve_ids,
            "vendors": [],
            "products": [],
            "incident_keywords": [],
        },
        "candidate_cves": [
            {
                "cve_id": cve_id,
                "component_scores": {"explicit": 1.0},
                "confidence": 1.0,
            }
            for cve_id in cve_ids
        ],
        "final_decision": {
            "decision": "linked",
            "confidence": 1.0,
            "confidence_band": "linked",
            "rule_ids": ["rule.cve.explicit"],
        },
        "citations": {"urls": [article.original_url]},
    }
