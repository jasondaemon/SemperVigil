"""Private, extractive Events review workflow. No inference or publication writes.

Operator-supplied aliases retrieve candidate passages, never trusted incident
assignments. The local artifact is review material, not a validated event report.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
from html import escape
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Callable

from .investigation import (
    EVIDENCE_SCOPE, READ_SCOPE, _day, _safe_url, _text, _version,
    _visible_metadata, _unique_object, _bad_constant, postgres_reader,
)

WORKFLOW = "event-extractive-review-v1"
MAX_DOCUMENTS = 12
MAX_TEXT = 32768
MAX_PACKET_BYTES = 3_000_000
MAX_REVIEW_BYTES = 32768
MAX_PASSAGES = 4


def _json(raw: bytes, maximum: int) -> dict:
    if type(raw) is not bytes or not 0 < len(raw) <= maximum:
        raise ValueError("input_size")
    try:
        data = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                          parse_constant=_bad_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid_json") from exc
    if type(data) is not dict:
        raise ValueError("object_required")
    return data


def _aliases(values) -> list[str]:
    if type(values) is not list or not 1 <= len(values) <= 5:
        raise ValueError("one_to_five_aliases_required")
    if any(not _text(v, 80) or len(v.strip()) < 3 for v in values):
        raise ValueError("invalid_alias")
    return sorted(set(v.strip() for v in values))


def snapshot(session: Callable, *, event_id: str, aliases: list[str],
             scopes: frozenset[str]) -> dict:
    """Take one bounded repeatable-read snapshot through a trusted session factory."""
    if not {READ_SCOPE, EVIDENCE_SCOPE} <= scopes:
        raise PermissionError("evidence_scope_required")
    if not _text(event_id, 128):
        raise ValueError("invalid_event_id")
    aliases = _aliases(aliases)
    with session() as conn:
        event = conn.execute("""SELECT id, title, updated_at FROM events
            WHERE id=%s AND visibility='active' AND length(title)<=512
            AND length(updated_at)<=64""", (event_id,)).fetchone()
        if not event:
            raise ValueError("event_unavailable")
        rows = conn.execute("""SELECT a.id,
            CASE WHEN length(a.title)<=512 THEN a.title END AS title,
            CASE WHEN length(a.original_url)<=2048 THEN a.original_url END AS url,
            CASE WHEN length(a.brief_day)<=10 THEN a.brief_day END AS feed_day,
            CASE WHEN length(a.meta_json)<=8192 THEN a.meta_json
                 WHEN a.meta_json IS NULL THEN NULL ELSE 'invalid' END AS policy_meta,
            length(a.content_text) AS text_length,
            CASE WHEN length(a.content_text)<=%s THEN a.content_text END AS text
            FROM articles a WHERE a.id IN
                (SELECT article_id FROM event_articles WHERE event_id=%s)
            ORDER BY a.id ASC LIMIT %s""", (MAX_TEXT, event_id, MAX_DOCUMENTS + 1)).fetchall()
    documents, omissions = [], []
    for row in rows[:MAX_DOCUMENTS]:
        if (not _visible_metadata(row["policy_meta"]) or not _text(row["title"], 512)
                or not _safe_url(row["url"])):
            # Do not leak the identity/title of suppressed or malformed records.
            omissions.append({"reason": "unavailable"})
            continue
        reason = ("content_too_large" if (row["text_length"] or 0) > MAX_TEXT else
                  "content_missing" if not (row["text"] or "").strip() else None)
        if reason:
            omissions.append({"reason": reason})
            continue
        try:
            day = _day(row["feed_day"]).isoformat()
        except (ValueError, TypeError):
            day = None
        documents.append({"article_id": row["id"], "title": row["title"], "url": row["url"],
                          "feed_day": day, "text": row["text"]})
    payload = {"workflow": WORKFLOW, "event": dict(event), "aliases": aliases,
               "documents": documents, "omissions": omissions,
               "links_truncated": len(rows) > MAX_DOCUMENTS,
               "public_eligible": False}
    packet = {**payload, "packet_version": _version(payload)}
    return validate_packet(json.dumps(packet, ensure_ascii=True).encode())


def validate_packet(raw: bytes) -> dict:
    packet = _json(raw, MAX_PACKET_BYTES)
    required = {"workflow", "event", "aliases", "documents", "omissions", "links_truncated",
                "public_eligible", "packet_version"}
    if packet.keys() != required or packet["workflow"] != WORKFLOW or packet["public_eligible"] is not False:
        raise ValueError("invalid_packet_contract")
    event = packet["event"]
    if (type(event) is not dict or event.keys() != {"id", "title", "updated_at"}
            or not _text(event["id"], 128) or not _text(event["title"], 512)
            or not _text(event["updated_at"], 64)):
        raise ValueError("invalid_event")
    if packet["aliases"] != _aliases(packet["aliases"]):
        raise ValueError("noncanonical_aliases")
    if type(packet["links_truncated"]) is not bool:
        raise ValueError("invalid_truncation")
    documents = packet["documents"]
    if type(documents) is not list or len(documents) > MAX_DOCUMENTS:
        raise ValueError("invalid_documents")
    ids = set()
    for doc in documents:
        if type(doc) is not dict or doc.keys() != {"article_id", "title", "url", "feed_day", "text"}:
            raise ValueError("invalid_document")
        if (type(doc["article_id"]) is not int or not 0 < doc["article_id"] < 2**63
                or doc["article_id"] in ids or not _text(doc["title"], 512)
                or not _safe_url(doc["url"]) or type(doc["text"]) is not str
                or not 0 < len(doc["text"]) <= MAX_TEXT or not doc["text"].strip()
                or any(0xD800 <= ord(c) <= 0xDFFF or ord(c) == 0 for c in doc["text"])):
            raise ValueError("invalid_document_values")
        ids.add(doc["article_id"])
        if doc["feed_day"] is not None:
            _day(doc["feed_day"])
    omissions = packet["omissions"]
    if type(omissions) is not list or len(omissions) > MAX_DOCUMENTS or any(
        type(item) is not dict or item.keys() != {"reason"} or item["reason"] not in
        {"unavailable", "content_missing", "content_too_large"} for item in omissions
    ):
        raise ValueError("invalid_omissions")
    payload = {k: v for k, v in packet.items() if k != "packet_version"}
    if packet["packet_version"] != _version(payload):
        raise ValueError("packet_version_mismatch")
    return packet


def draft(packet: dict) -> dict:
    """Produce verbatim suggestions, with no entity matching or semantic approval."""
    packet = validate_packet(json.dumps(packet).encode())
    pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(a) for a in packet["aliases"]) + r")(?!\w)", re.I)
    passages = []
    uncovered = []
    for doc in packet["documents"]:
        found = 0
        for match in re.finditer(r"\S.*?(?:[.!?](?=\s|$)|\n|$)", doc["text"], re.S):
            text = match.group().rstrip()
            if not 35 <= len(text) <= 1200 or not pattern.search(text):
                continue
            start, end = match.start(), match.start() + len(text)
            identity = _version({"packet": packet["packet_version"], "article_id": doc["article_id"],
                                 "start": start, "end": end})
            passages.append({"id": identity, "article_id": doc["article_id"], "start": start,
                             "end": end, "quote": text, "scope_status": "proposed_only"})
            found += 1
            if found == MAX_PASSAGES:
                break
        if not found:
            uncovered.append(doc["article_id"])
    return {"packet_version": packet["packet_version"], "passages": passages,
            "uncovered_article_ids": uncovered, "public_eligible": False}


def validate_review(raw: bytes, packet: dict) -> dict:
    review = _json(raw, MAX_REVIEW_BYTES)
    if (review.keys() != {"workflow", "packet_version", "decisions", "note"}
            or review["workflow"] != WORKFLOW or review["packet_version"] != packet["packet_version"]):
        raise ValueError("stale_or_invalid_review")
    choices = review["decisions"]
    allowed = {p["id"] for p in draft(packet)["passages"]}
    if (type(choices) is not dict or choices.keys() - allowed
            or any(type(v) is not str or v not in {"include", "exclude", "hold"} for v in choices.values())):
        raise ValueError("invalid_decisions")
    if type(review["note"]) is not str or len(review["note"]) > 1000 or any(
            0xD800 <= ord(c) <= 0xDFFF or ord(c) == 0 for c in review["note"]):
        raise ValueError("invalid_note")
    return review


def render(packet: dict, review: dict | None = None, *, assessment: dict | None = None) -> str:
    proposal = draft(packet)
    if assessment is not None:
        from .event_assessment import validate_assessment
        assessment = validate_assessment(assessment, packet)
    review = validate_review(json.dumps(review).encode(), packet) if review is not None else {
        "workflow": WORKFLOW, "packet_version": packet["packet_version"], "decisions": {}, "note": ""}
    cards = []
    by_doc = {}
    for passage in proposal["passages"]:
        by_doc.setdefault(passage["article_id"], []).append(passage)
    for doc in sorted(packet["documents"], key=lambda d: (d["feed_day"] or "9999", d["article_id"])):
        entries = []
        for p in by_doc.get(doc["article_id"], []):
            suggestion = (assessment or {}).get("suggestions", {}).get(p["id"])
            model_note = (f'<p class="muted">Model suggestion: {escape(suggestion["decision"])} '
                          f'({escape(suggestion["reason"])}). Not verified or approved.</p>'
                          if suggestion else '')
            selected = review["decisions"].get(p["id"], "hold")
            options = "".join(f'<option value="{v}"{(" selected" if selected == v else "")}>{label}</option>'
                              for v, label in (("hold", "Needs review"), ("include", "Include in reading view"),
                                               ("exclude", "Exclude from reading view")))
            entries.append(f'<div class="passage" data-passage="{p["id"]}"><blockquote>{escape(p["quote"])}</blockquote>{model_note}'
                           f'<div class="passage-tools"><span>Source characters {p["start"]}-{p["end"]} · proposed scope</span>'
                           f'<label>Decision <select aria-label="Decision for source characters {p["start"]} to {p["end"]}">{options}</select></label></div></div>')
        body = "".join(entries) or '<p class="muted">No bounded alias-matched passage found. Inspect the source; absence is not proof of irrelevance.</p>'
        cards.append(f'<article class="source"><header><div class="eyebrow">Stored feed date: {escape(doc["feed_day"] or "Unknown")} · Article {doc["article_id"]}</div>'
                     f'<h2><a href="{escape(doc["url"], quote=True)}" target="_blank" rel="noopener noreferrer">{escape(doc["title"])}</a></h2></header>{body}'
                     f'<details class="full-source"><summary>Inspect full stored source text</summary><p>{escape(doc["text"])}</p></details></article>')
    template = Path(__file__).with_name("templates").joinpath("event_review.html").read_text()
    script = Path(__file__).with_name("static").joinpath("event_review.js").read_text()
    substitutions = {
        "TITLE": escape(packet["event"]["title"]), "EVENT": escape(packet["event"]["id"]),
        "VERSION": packet["packet_version"], "WORKFLOW": WORKFLOW,
        "ALIASES": escape(", ".join(packet["aliases"])),
        "REVIEWID": _version({"review": review, "assessment": assessment}) if assessment else _version(review),
        "SCRIPTHASH": base64.b64encode(hashlib.sha256(script.encode()).digest()).decode(),
        "COUNT": str(len(packet["documents"])), "PASSAGES": str(len(proposal["passages"])),
        "OMITTED": str(len(packet["omissions"])), "TRUNCATED": "Yes" if packet["links_truncated"] else "No",
        "CARDS": "".join(cards) or '<p>No usable stored evidence. This review must abstain.</p>',
        "NOTE": escape(review["note"]), "SCRIPT": script,
    }
    # One pass: untrusted source text resembling a placeholder is never substituted.
    return re.sub(r"@@([A-Z]+)@@", lambda m: substitutions[m[1]], template)


def _read(path: Path, maximum: int) -> bytes:
    with path.open("rb") as source:
        return source.read(maximum + 1)


def _immutable_write(path: Path, data: bytes) -> None:
    """Atomically install without overwrite; immutable existing bytes must match."""
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".review-")
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or _read(path, len(data)) != data:
                raise ValueError("artifact_conflict")
    finally:
        os.unlink(temporary)


def save(packet: dict, root: Path, review: dict | None = None, *, assessment: dict | None = None) -> Path:
    page = render(packet, review, assessment=assessment)
    folder = root / packet["packet_version"]
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    if folder.is_symlink():
        raise ValueError("symlink_artifact_directory")
    _immutable_write(folder / "packet.json", json.dumps(packet, sort_keys=True, ensure_ascii=True).encode())
    if assessment is not None:
        _immutable_write(folder / ("assessment-" + _version(assessment) + ".json"),
                         json.dumps(assessment, sort_keys=True, ensure_ascii=True).encode())
    if review is not None:
        _immutable_write(folder / ("decisions-" + _version(review) + ".json"),
                         json.dumps(review, sort_keys=True, ensure_ascii=True).encode())
    # HTML versioning also includes renderer changes; old review artifacts survive.
    path = folder / ("review-" + hashlib.sha256(page.encode()).hexdigest()[:16] + ".html")
    _immutable_write(path, page.encode())
    return path.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect = sub.add_parser("snapshot", help="Read one event; emit a bounded private JSON packet")
    collect.add_argument("--event", required=True)
    collect.add_argument("--alias", action="append", required=True)
    view = sub.add_parser("render", help="Create immutable local HTML, optionally with reviewed decisions")
    view.add_argument("packet", type=Path)
    view.add_argument("--review", type=Path)
    view.add_argument("--output", type=Path, default=Path("data/events-review"))
    check = sub.add_parser("check-current", help="Re-read current inputs; reject changed evidence")
    check.add_argument("packet", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "render":
            packet = validate_packet(_read(args.packet, MAX_PACKET_BYTES))
            review = validate_review(_read(args.review, MAX_REVIEW_BYTES), packet) if args.review else None
            print(save(packet, args.output, review))
        else:
            dsn = os.environ.get("SV_INVESTIGATION_DB_URL")
            if not dsn:
                raise ValueError("dedicated_investigation_db_url_required")
            previous = validate_packet(_read(args.packet, MAX_PACKET_BYTES)) if args.command == "check-current" else None
            current = snapshot(lambda: postgres_reader(dsn),
                               event_id=previous["event"]["id"] if previous else args.event,
                               aliases=previous["aliases"] if previous else args.alias,
                               scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
            if previous:
                if current["packet_version"] != previous["packet_version"]:
                    raise ValueError("stale_snapshot")
                print("Inputs unchanged at this read; still not publication approval.")
            else:
                print(json.dumps(current, ensure_ascii=True))
    except (ValueError, PermissionError) as exc:
        # Only known validation errors are raised by this module; no SQL errors/DSNs.
        print("Review stopped: " + str(exc), file=sys.stderr)
        return 2
    except Exception:
        print("Review failed; inspect access and input files. Backend details suppressed.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
