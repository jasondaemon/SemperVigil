"""Evidence-bound private Event narrative contract and review storage."""
import calendar
import json
import re

import jsonschema

from .event_review import _json
from .investigation import _version
from .utils import utc_now_iso

WORKFLOW = "event-ledger-composition-v9"
LEGACY_WORKFLOW = "event-ledger-composition-v4"
LEGACY_WORKFLOWS = frozenset({
    LEGACY_WORKFLOW, "event-ledger-composition-v5", "event-ledger-composition-v6",
    "event-ledger-composition-v7", "event-ledger-composition-v8",
})
SECTION_POLICY = "curated-sections-v3"
DETERMINISTIC_SECTION_POLICY = "deterministic-sections-v2"
LEGACY_SECTION_POLICY = "stored-union-v1"
MAX_INPUT_BYTES = 48000
MAX_OUTPUT_BYTES = 24000
SECTIONS = (
    "overview", "attack_vector", "attack_path", "timeline", "impact",
    "response_recovery", "mitigations", "attribution", "open_questions",
)
GENERATED_SECTIONS = tuple(section for section in SECTIONS if section != "timeline")
OVERVIEW_DIMENSIONS = (
    ("event_scope", frozenset({"context", "timeline"})),
    ("attack_mechanics", frozenset({"attack_vector", "attack_path"})),
    ("impact", frozenset({"impact"})),
    ("response_and_current_state", frozenset({"response_recovery"})),
)
SYSTEM_PROMPT = """You are a cybersecurity editor. Create a coherent executive
overview and concise analytical sections for the risk Event from the accepted facts.
Explain what happened, how it worked, material impact, and the response or current
state when the facts support them. Consolidate corroborating or equivalent facts into
one reader-facing paragraph with all supporting references. Do not mechanically list
facts, repeat the same claim within or across sections, add unsupported claims or
causal links, strengthen uncertain attribution, or omit material qualifications.

Use each section only for its editorial purpose: attack_vector for initial access or
delivery; attack_path for post-access actions and progression; impact for consequences;
response_recovery for investigation, containment, remediation, and restoration;
mitigations for source-supported defensive guidance; attribution for actor identity
or attribution; and open_questions for material unresolved issues. An empty section
must be an empty array. The timeline is assembled separately and is not requested.

Every material clause must be directly entailed by a cited fact. Do not invent
who identified, concluded, confirmed, or recommended something. Do not turn an
association into a cause, origin, or attribution. Preserve each fact's subject,
object, uncertainty, and technical relationships.

Return exactly one JSON object with the requested section keys. Each section must be
an array of paragraph objects containing only text and fact_refs. Put clean
reader-facing prose in text and every fact that directly supports that paragraph in
fact_refs. Never put F-number labels in the prose or return any other keys."""


def _active_facts(ledger: dict) -> tuple[list[dict], dict[str, dict]]:
    excluded = set(ledger.get("superseded_fact_ids", [])) | set(ledger.get("conflict_fact_ids", []))
    active = [fact for fact in ledger.get("facts", []) if fact.get("fact_id") not in excluded]
    return active, {f"F{index:02d}": fact for index, fact in enumerate(active, 1)}


def _allowed_sections(fact: dict, policy: str = SECTION_POLICY) -> list[str]:
    result = {"overview"}
    mapping = {"timeline": "timeline", "attack_vector": "attack_vector",
               "attack_path": "attack_path",
               "impact": "impact", "mitigation": "mitigations",
               "response_recovery": "response_recovery",
               "attribution": "attribution", "open_question": "open_questions"}
    from .event_ledger import _section_tags
    if policy == SECTION_POLICY:
        sections = set(fact.get("sections", []))
        if not sections:
            raise ValueError("event_composition_fact_sections_missing")
    elif policy == DETERMINISTIC_SECTION_POLICY:
        sections = set(_section_tags(fact))
    elif policy == LEGACY_SECTION_POLICY:
        sections = set(fact.get("sections", [])) | set(_section_tags(fact))
        if any(cue in fact["statement"].lower() for cue in ("downloaded", "stole", "stolen")):
            sections.add("attack_path")
    else:
        raise ValueError("event_composition_section_policy_invalid")
    for section in sections:
        if section in mapping:
            result.add(mapping[section])
        if section == "attack_path" and policy != SECTION_POLICY:
            result.add("attack_vector")
    return [section for section in SECTIONS if section in result]


_MONTHS = {name.lower(): number for number, name in enumerate(calendar.month_name) if name}
_MONTHS.update({name.lower(): number for number, name in enumerate(calendar.month_abbr) if name})


def _date_parts(value: str) -> tuple[int | None, int | None, int | None, str]:
    """Parse common source date labels only far enough to group equivalent labels."""
    text = re.sub(r"\s+", " ", value.strip().lower())
    text = re.sub(r"\b(\d{1,2})(?:st|nd|rd|th)\b", r"\1", text)
    year_match = re.search(r"\b(20\d{2})\b", text)
    year = int(year_match.group(1)) if year_match else None
    month = next((number for name, number in _MONTHS.items()
                  if re.search(rf"\b{re.escape(name)}\b", text)), None)
    day = None
    if month:
        without_year = re.sub(r"\b20\d{2}\b", "", text)
        numbers = [int(item) for item in re.findall(r"\b\d{1,2}\b", without_year)]
        day = next((item for item in numbers if 1 <= item <= 31), None)
    qualifier = next((item for item in ("early", "mid", "late")
                      if re.search(rf"\b{item}\b", text)), "")
    return year, month, day, qualifier


def _timeline_refs(aliases: dict[str, dict]) -> list[str]:
    """Choose one auditable milestone per equivalent non-publication date label."""
    candidates = [(ref, fact, _date_parts(str(fact.get("date_text") or "")))
                  for ref, fact in aliases.items()
                  if fact.get("date_text") and fact.get("date_role") != "publication"
                  and "timeline" in _allowed_sections(fact)]
    explicit_years: dict[tuple[int | None, int | None, str], set[int]] = {}
    for _, _, (year, month, day, qualifier) in candidates:
        if year:
            explicit_years.setdefault((month, day, qualifier), set()).add(year)

    groups: dict[tuple, list[tuple[str, dict]]] = {}
    for ref, fact, (year, month, day, qualifier) in candidates:
        known = explicit_years.get((month, day, qualifier), set())
        if year is None and len(known) == 1:
            year = next(iter(known))
        if month:
            key = (year, month, day, qualifier)
        else:
            key = (re.sub(r"\W+", " ", str(fact["date_text"]).lower()).strip(),)
        groups.setdefault(key, []).append((ref, fact))

    if len(groups) > 8:
        raise ValueError("event_composition_timeline_over_budget")

    milestone_cues = (
        "began", "occurred", "detected", "discovered", "disclosed", "reported",
        "ended", "terminated", "completed", "contained", "blocked", "rotated",
        "lost access", "no longer observed",
    )

    def rank(item: tuple[str, dict]) -> tuple[int, int, int, str]:
        ref, fact = item
        text = fact["statement"].lower()
        role = {"incident": 3, "discovery": 2, "disclosure": 1}.get(
            str(fact.get("date_role") or ""), 0)
        cues = sum(cue in text for cue in milestone_cues)
        # Prefer an atomic milestone over a compound source sentence.
        return role, cues, -len(text.split()), ref

    selected = [max(items, key=rank)[0]
                for _, items in sorted(groups.items(), key=lambda row: str(row[0]))]
    return [ref for ref in aliases if ref in set(selected)]


def _overview_requirements(facts: list[dict]) -> list[dict]:
    requirements = []
    for name, semantic_sections in OVERVIEW_DIMENSIONS:
        fact_ids = [fact["fact_id"] for fact in facts
                    if semantic_sections & set(fact.get("sections", []))]
        if fact_ids:
            requirements.append({"dimension": name, "fact_ids": fact_ids})
    return requirements


def validate_overview_coverage(sections: dict, facts: list[dict]) -> None:
    requirements = _overview_requirements(facts)
    overview = sections.get("overview", []) if isinstance(sections, dict) else []
    if not overview:
        raise ValueError("event_composition_overview_incomplete")
    cited = {fact_id for item in overview for fact_id in item.get("fact_ids", [])}
    if any(cited.isdisjoint(requirement["fact_ids"]) for requirement in requirements):
        raise ValueError("event_composition_overview_incomplete")


def schema(fact_refs: dict[str, list[str]] | None = None) -> dict:
    all_refs = sorted({ref for refs in (fact_refs or {}).values() for ref in refs})
    ref = ({"type": "string", "enum": all_refs} if fact_refs
           else {"type": "string", "pattern": "^F[0-9]{2}$"})
    item = {"type": "object", "additionalProperties": False,
            "required": ["text", "fact_refs"], "properties": {
                "text": {"type": "string", "minLength": 1, "maxLength": 3200},
                "fact_refs": {"type": "array", "minItems": 1, "maxItems": 16,
                              "items": ref}}}
    properties = {}
    for section in GENERATED_SECTIONS:
        refs = (fact_refs or {}).get(section, [])
        properties[section] = {
            "type": "array", "minItems": 1 if section == "overview" else 0,
            "maxItems": 4 if refs else 0, "items": item,
        }
    return {"type": "object", "additionalProperties": False,
            "required": list(GENERATED_SECTIONS), "properties": properties}


def _deterministic_sections(aliases: dict[str, dict]) -> dict[str, list[dict]]:
    """Project only dated milestones; narrative sections are generated and audited."""
    sections = {section: [] for section in SECTIONS}
    timeline_refs = set(_timeline_refs(aliases))
    for ref, fact in aliases.items():
        item = {"text": fact["statement"], "fact_ids": [fact["fact_id"]]}
        if ref in timeline_refs:
            sections["timeline"].append({**item, "date_text": fact["date_text"]})
    return sections


def request(ledger_revision: dict, generation: str) -> dict:
    if (not isinstance(generation, str) or len(generation) != 64
            or any(char not in "0123456789abcdef" for char in generation)):
        raise ValueError("event_composition_generation_required")
    ledger = ledger_revision.get("ledger")
    if (ledger_revision.get("status") != "accepted" or not ledger_revision.get("lineage_current")
            or not isinstance(ledger, dict) or ledger.get("public_eligible") is not False):
        raise ValueError("event_composition_ledger_not_current")
    facts, aliases = _active_facts(ledger)
    if not facts:
        raise ValueError("event_composition_no_active_facts")
    overview_requirements = _overview_requirements(facts)
    aliases_by_id = {fact["fact_id"]: ref for ref, fact in aliases.items()}
    allowed_by_ref = {
        ref: _allowed_sections(fact)
        for ref, fact in aliases.items()
    }
    payload = {"workflow": WORKFLOW, "section_policy": SECTION_POLICY,
               "title": ledger["title"], "kind": ledger["kind"],
               "overview_requirements": [
                   {"dimension": requirement["dimension"],
                    "fact_refs": [aliases_by_id[fact_id]
                                  for fact_id in requirement["fact_ids"]]}
                   for requirement in overview_requirements
               ],
               "facts": [{"ref": ref, "statement": fact["statement"],
                          "kind": fact["kind"], "date_text": fact["date_text"],
                          "date_role": fact["date_role"],
                          "allowed_sections": allowed_by_ref[ref]}
                         for ref, fact in aliases.items()]}
    allowed_refs = {
        section: [ref for ref, allowed in allowed_by_ref.items() if section in allowed]
        for section in GENERATED_SECTIONS
    }
    response_schema = schema(allowed_refs)
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if len((SYSTEM_PROMPT + encoded + json.dumps(response_schema)).encode()) > MAX_INPUT_BYTES:
        raise ValueError("event_composition_input_over_budget")
    identity = {"workflow": WORKFLOW, "ledger_revision_id": ledger_revision["revision_id"],
                "generation": generation, "system": SYSTEM_PROMPT, "input": encoded,
                "schema": response_schema}
    return {**identity, "request_version": _version(identity)}


def validate(raw: bytes, ledger_revision: dict, generation: str) -> dict:
    req = request(ledger_revision, generation)
    value = _json(raw, MAX_OUTPUT_BYTES)
    try:
        jsonschema.validate(value, req["schema"])
    except jsonschema.ValidationError as exc:
        raise ValueError("event_composition_invalid_shape") from exc
    _, aliases = _active_facts(ledger_revision["ledger"])
    sections = _deterministic_sections(aliases)
    normalized_text = set()
    for section in GENERATED_SECTIONS:
        allowed = {ref for ref, fact in aliases.items() if section in _allowed_sections(fact)}
        for item in value[section]:
            text = item["text"].strip()
            if re.search(r"(?:\[F\d{2}\]|\bF\d{2}\b)", text, re.IGNORECASE):
                raise ValueError("event_composition_fact_alias_in_prose")
            if len(item["fact_refs"]) != len(set(item["fact_refs"])):
                raise ValueError("event_composition_duplicate_fact_ref")
            if not set(item["fact_refs"]) <= allowed:
                raise ValueError("event_composition_fact_section_invalid")
            normalized = re.sub(r"\W+", " ", text.lower()).strip()
            if normalized in normalized_text:
                raise ValueError("event_composition_duplicate_narrative")
            normalized_text.add(normalized)
            facts = [aliases[ref] for ref in item["fact_refs"]]
            sections[section].append({"text": text,
                                      "fact_ids": [fact["fact_id"] for fact in facts]})
    validate_overview_coverage(sections, list(aliases.values()))
    return {
        "workflow": WORKFLOW, "ledger_id": ledger_revision["ledger_id"],
        "ledger_revision_id": ledger_revision["revision_id"],
        "generation_version": generation, "request_version": req["request_version"],
        "section_policy": SECTION_POLICY,
        "sections": sections, "change": ledger_revision["change"],
        "status": "unreviewed", "public_eligible": False,
    }


def store_unreviewed(conn, record: dict) -> str:
    if (record.get("workflow") != WORKFLOW or record.get("status") != "unreviewed"
            or record.get("public_eligible") is not False):
        raise ValueError("event_composition_not_private")
    composition_id = "elc_" + _version(record)
    encoded = json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    conn.execute(
        """INSERT INTO event_ledger_compositions
           (composition_id, ledger_id, ledger_revision_id, generation_version,
            request_version, status, composition_json, created_at)
           VALUES (%s,%s,%s,%s,%s,'unreviewed',%s,%s)
           ON CONFLICT (ledger_revision_id, generation_version, request_version) DO NOTHING""",
        (composition_id, record["ledger_id"], record["ledger_revision_id"],
         record["generation_version"], record["request_version"], encoded, utc_now_iso()),
    )
    row = conn.execute(
        """SELECT composition_id, composition_json FROM event_ledger_compositions
           WHERE ledger_revision_id=%s AND generation_version=%s AND request_version=%s""",
        (record["ledger_revision_id"], record["generation_version"], record["request_version"]),
    ).fetchone()
    if not row or row[1] != encoded:
        raise ValueError("event_composition_storage_conflict")
    conn.commit()
    return row[0]


def _current_ledger(conn, ledger_revision_id: str) -> bool:
    row = conn.execute(
        "SELECT status FROM event_ledger_revisions WHERE revision_id=%s", (ledger_revision_id,)
    ).fetchone()
    if not row or row[0] != "accepted":
        return False
    from .event_ledger import _lineage_current
    return _lineage_current(conn, ledger_revision_id)


def review(conn, composition_id: str, decision: str, *, reason: str, reviewer: str) -> dict:
    if decision not in {"accept", "hold", "reject"} or not composition_id.startswith("elc_"):
        raise ValueError("event_composition_review_invalid")
    if not reviewer.strip() or len(reviewer) > 80:
        raise ValueError("event_composition_review_identity_invalid")
    reason = reason.strip()
    if decision != "accept" and not reason:
        raise ValueError("event_composition_review_reason_required")
    if len(reason) > 1000:
        raise ValueError("event_composition_review_reason_too_long")
    row = conn.execute(
        """SELECT ledger_id, ledger_revision_id, status FROM event_ledger_compositions
           WHERE composition_id=%s FOR UPDATE""", (composition_id,)
    ).fetchone()
    if not row or row[2] not in {"unreviewed", "held"}:
        raise ValueError("event_composition_missing_or_decided")
    if decision == "accept" and not _current_ledger(conn, row[1]):
        raise ValueError("event_composition_ledger_stale")
    now = utc_now_iso()
    if decision == "accept":
        conn.execute(
            """UPDATE event_ledger_compositions SET status='superseded', reviewed_at=%s,
               reviewed_by=%s, review_reason='superseded by accepted composition',
               superseded_by_composition_id=%s
               WHERE ledger_id=%s AND status='accepted' AND composition_id<>%s""",
            (now, reviewer, composition_id, row[0], composition_id),
        )
    next_status = {"accept": "accepted", "hold": "held", "reject": "rejected"}[decision]
    conn.execute(
        """UPDATE event_ledger_compositions SET status=%s, reviewed_at=%s,
           reviewed_by=%s, review_reason=%s WHERE composition_id=%s""",
        (next_status, now, reviewer, reason or None, composition_id),
    )
    conn.commit()
    return {"composition_id": composition_id, "ledger_id": row[0],
            "ledger_revision_id": row[1], "status": next_status, "public_eligible": False}


def list_compositions(conn, *, status: str = "unreviewed", limit: int = 50) -> list[dict]:
    allowed = {"unreviewed", "accepted", "held", "rejected", "superseded", "all"}
    if status not in allowed or not 1 <= limit <= 200:
        raise ValueError("event_composition_list_invalid")
    where = "" if status == "all" else "WHERE status=%s"
    params = [] if status == "all" else [status]
    rows = conn.execute(
        f"""SELECT composition_id, ledger_id, ledger_revision_id, status,
                   composition_json, created_at, reviewed_at, reviewed_by, review_reason
            FROM event_ledger_compositions {where}
            ORDER BY created_at DESC, composition_id DESC LIMIT %s""", (*params, limit)
    ).fetchall()
    return [{"composition_id": row[0], "ledger_id": row[1], "ledger_revision_id": row[2],
             "status": row[3], "composition": json.loads(row[4]), "created_at": row[5],
             "reviewed_at": row[6], "reviewed_by": row[7], "review_reason": row[8],
             "ledger_current": _current_ledger(conn, row[2]), "public_eligible": False}
            for row in rows]
