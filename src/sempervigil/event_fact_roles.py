"""Semantic roles assigned to grounded Event facts during Event-scoped curation."""

SECTIONS = (
    "attack_vector",
    "attack_path",
    "timeline",
    "impact",
    "response_recovery",
    "mitigation",
    "attribution",
    "open_question",
    "context",
)


def validate(facts: dict[str, dict], selected: set[str], assignments: list[dict]) -> dict[str, list[str]]:
    """Validate LLM-assigned roles without inferring semantic meaning in code."""
    if not isinstance(assignments, list):
        raise ValueError("event_fact_sections_invalid")
    result: dict[str, list[str]] = {}
    for assignment in assignments:
        if not isinstance(assignment, dict) or set(assignment) != {"fact_id", "sections"}:
            raise ValueError("event_fact_sections_invalid")
        fact_id = assignment["fact_id"]
        sections = assignment["sections"]
        if (fact_id in result or fact_id not in selected or not isinstance(sections, list)
                or not sections or len(sections) > 4 or len(sections) != len(set(sections))
                or any(section not in SECTIONS for section in sections)):
            raise ValueError("event_fact_sections_invalid")
        fact = facts.get(fact_id)
        if not fact:
            raise ValueError("event_fact_sections_invalid")
        roles = set(sections)
        if fact.get("kind") == "recommendation" and not roles <= {"mitigation", "context"}:
            raise ValueError("event_fact_sections_unsafe")
        if "mitigation" in roles and fact.get("kind") != "recommendation":
            raise ValueError("event_fact_sections_unsafe")
        if "timeline" in roles and (not fact.get("date_text")
                or fact.get("date_role") not in {"incident", "disclosure"}):
            raise ValueError("event_fact_sections_unsafe")
        result[fact_id] = [section for section in SECTIONS if section in roles]
    if set(result) != selected:
        raise ValueError("event_fact_sections_incomplete")
    return result
