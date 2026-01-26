from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from .normalize import normalize_name


@dataclass(frozen=True)
class CveSignals:
    vendors: list[str]
    vendor_norms: list[str]
    products: list[str]
    product_norms: list[str]
    product_versions: list[str]
    cpes: list[str]
    reference_domains: list[str]


def normalize_severity(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().upper()


def extract_signals(cve_item: dict[str, Any]) -> CveSignals:
    vendors: set[str] = set()
    products: set[str] = set()
    product_versions: set[str] = set()
    cpes: set[str] = set()
    reference_domains: set[str] = set()

    configurations = cve_item.get("configurations")
    if isinstance(configurations, dict):
        nodes = configurations.get("nodes") or []
        for node in nodes:
            _collect_cpes(node, cpes, vendors, products, product_versions)
    elif isinstance(configurations, list):
        for entry in configurations:
            if not isinstance(entry, dict):
                continue
            nodes = entry.get("nodes") or []
            for node in nodes:
                _collect_cpes(node, cpes, vendors, products, product_versions)

    references = cve_item.get("references") or []
    for ref in references:
        url = ref.get("url") or ""
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.hostname:
            reference_domains.add(parsed.hostname.lower())

    return CveSignals(
        vendors=sorted(vendors),
        vendor_norms=sorted(_normalize_names(vendors)),
        products=sorted(products),
        product_norms=sorted(_normalize_names(products)),
        product_versions=sorted(product_versions),
        cpes=sorted(cpes),
        reference_domains=sorted(reference_domains),
    )


def matches_filters(
    *,
    preferred_score: float | None,
    preferred_severity: str | None,
    description: str | None,
    signals: CveSignals,
    filters: dict[str, Any],
) -> bool:
    min_cvss = filters.get("min_cvss")
    if min_cvss is not None and preferred_score is not None:
        if float(preferred_score) < float(min_cvss):
            return False
    if min_cvss is not None and preferred_score is None:
        return False

    severities = filters.get("severities") or []
    if severities and preferred_severity:
        if normalize_severity(preferred_severity) not in {normalize_severity(s) for s in severities}:
            return False
    if severities and not preferred_severity:
        return False

    if filters.get("require_known_score") and preferred_score is None:
        return False

    vendor_keywords = _normalize_keywords(filters.get("vendor_keywords") or [])
    product_keywords = _normalize_keywords(filters.get("product_keywords") or [])
    if vendor_keywords or product_keywords:
        haystack = " ".join(
            [description or ""]
            + signals.vendors
            + signals.vendor_norms
            + signals.products
            + signals.product_norms
            + signals.product_versions
            + signals.cpes
            + signals.reference_domains
        ).lower()
        if vendor_keywords and not any(keyword in haystack for keyword in vendor_keywords):
            return False
        if product_keywords and not any(keyword in haystack for keyword in product_keywords):
            return False

    return True


def _normalize_keywords(values: list[str]) -> list[str]:
    return [value.strip().lower() for value in values if value and value.strip()]


def _normalize_names(values: set[str]) -> list[str]:
    return [normalize_name(value) for value in values]


def _collect_cpes(
    node: dict[str, Any],
    cpes: set[str],
    vendors: set[str],
    products: set[str],
    product_versions: set[str],
) -> None:
    for match in node.get("cpeMatch") or []:
        cpe = match.get("criteria") or match.get("cpe23Uri")
        if not cpe:
            continue
        cpes.add(cpe)
        parts = cpe.split(":")
        if len(parts) >= 5:
            vendors.add(parts[3])
            products.add(parts[4])
            if len(parts) >= 6:
                version = parts[5]
                if version not in ("*", "-"):
                    product_versions.add(f"{parts[3]}:{parts[4]}:{version}")
    for child in node.get("children") or []:
        _collect_cpes(child, cpes, vendors, products, product_versions)
