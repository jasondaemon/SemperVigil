"""Reader-facing rendering of qualified source-span reports."""
from html import escape

from . import event_report_contract as contract
from .event_source_report_publication import validate_bundle


def resolve(bundle, *, event_id, expected_revision):
    projection = validate_bundle(bundle,event_id=event_id,expected_revision=expected_revision)
    report = projection["report"]
    dates = sorted(s["published_at"] for s in bundle["sources"] if s.get("published_at"))
    metadata = {"title":report["title"],"event_revision":expected_revision,
        "event_report_format":contract.PUBLIC_WORKFLOW,"event_kind":report["kind"],
        "event_source_count":len(bundle["sources"]),
        "event_section_count":len({x["section"] for x in report["items"]}),
        "event_has_open_questions":any(x["section"]=="open_questions" for x in report["items"]),
        "event_change_kind":"update" if bundle["predecessor"] else "initial",
        "first_seen_at":dates[0] if dates else None,"last_seen_at":dates[-1] if dates else None}
    from .event_report_presentation import source_report_metadata
    metadata.update(source_report_metadata(bundle))
    return metadata,projection


def index_entry(bundle, *, event_id, expected_revision):
    metadata,_ = resolve(bundle,event_id=event_id,expected_revision=expected_revision)
    articles = [{"article_id":s["article_id"],"title":s["title"],"url":s["url"]} for s in bundle["sources"]]
    return {"event_id":event_id,"title":metadata["title"],
        "summary":" ".join(x["text"] for x in bundle["report"]["items"] if x["section"]=="overview"),
        "severity":None,"kind":metadata["event_kind"],"status":"source_backed_event",
        "first_seen_at":metadata["first_seen_at"],"last_seen_at":metadata["last_seen_at"],
        "cves":[],"products":[],"articles":articles,
        "counts":{"cves":0,"products":0,"articles":len(articles)},
        "event_revision":expected_revision,"event_report_format":contract.PUBLIC_WORKFLOW}


def render(bundle, *, event_id, expected_revision):
    metadata,_ = resolve(bundle,event_id=event_id,expected_revision=expected_revision)
    sources = {s["id"]:s for s in bundle["sources"]}
    numbers = {s["id"]:i for i,s in enumerate(bundle["sources"],1)}
    lines = [f'<section id="sv-event-coverage" class="event-report" data-event-id="{escape(event_id)}" data-event-revision="{expected_revision}">',
        '<p class="event-evidence-note">Maintained from attributed reporting. Findings and analyst assessments link to their supporting sources.</p>',
        '<div class="event-report-sections">']
    provenance = bundle.get("revision_provenance") or {}
    if provenance.get("notice"):
        lines.append(f'<p class="event-revision-provenance">{escape(provenance["notice"])}</p>')
    names = {"overview":"Overview","attack_vector":"Attack vector","attack_path":"Attack path",
             "timeline":"Timeline","impact":"Impact","response_recovery":"Response and recovery",
             "mitigations":"Mitigations","attribution":"Attribution","analyst_assessment":"Analyst assessment","open_questions":"Open questions",
             "what_changed":"What changed"}
    for section in contract.SECTIONS:
        items = [x for x in bundle["report"]["items"] if x["section"]==section]
        if not items: continue
        lines.append(f'<section class="event-report-section" id="{section.replace("_","-")}"><h2>{names[section]}</h2>')
        for item in items:
            seen = list(dict.fromkeys(c["source_id"] for c in item["citations"]))
            links = " ".join(f'<a class="event-source" href="{escape(sources[s]["url"],quote=True)}" target="_blank" rel="noopener" aria-label="Source {numbers[s]}">[{numbers[s]}]</a>' for s in seen)
            if item["claim_type"]!="finding":
                lines.append(f'<p class="event-claim-state">{escape(item["claim_type"].replace("_"," ").title())} · {escape(item["confidence"])} confidence</p>')
            if item["date_label"]: lines.append(f'<p><time>{escape(item["date_label"])}</time></p>')
            lines.append(f'<p>{escape(item["text"])} {links}</p>')
            if item["rationale"]: lines.append(f'<p class="event-assessment-rationale">{escape(item["rationale"])}</p>')
        lines.append('</section>')
    lines.append('</div></section>')
    return metadata,"\n".join(lines)
