"""Structural acceptance of article enrichment; not factual verification."""


def validated_output(result: dict, kind: str) -> dict:
    """Never promote failed schema output, raw text, or coerced field types."""
    if kind not in {"summary", "context"}:
        raise ValueError("invalid_article_enrichment_kind")
    if type(result) is not dict or result.get("schema_valid") is not True:
        raise ValueError("article_enrichment_schema_failed")
    data = result.get("parsed")
    if type(data) is not dict:
        raise ValueError("article_enrichment_object_required")

    def strings(value):
        return type(value) is list and all(type(item) is str and item.strip() for item in value)

    if kind == "summary":
        if type(data.get("summary")) is not str or not data["summary"].strip():
            raise ValueError("article_summary_text_required")
        # Profiles may omit optional feed metadata, but supplied values must be usable.
        if any(key in data and not strings(data[key]) for key in ("bullets", "entities", "cves", "tags")):
            raise ValueError("article_summary_list_invalid")
    else:
        if any(not strings(data.get(key)) for key in ("facts", "numbers", "iocs", "cves", "timeline", "uncertainties")):
            raise ValueError("article_context_list_invalid")
        entities = data.get("entities")
        if type(entities) is not dict or any(not strings(entities.get(key)) for key in (
                "orgs", "people", "products", "vendors", "threat_actors", "countries")):
            raise ValueError("article_context_entities_invalid")
    return data
