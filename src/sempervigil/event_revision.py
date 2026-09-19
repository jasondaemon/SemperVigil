"""Immutable private assessment receipts, never publication authorization.

Receipts bind a review to its exact inputs and renderer output. Hashes establish
integrity, not factual accuracy, independent review, or current-source freshness.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from .event_assessment import validate_assessment
from .event_review import _immutable_write, validate_packet
from .investigation import _version

WORKFLOW = "event-private-revision-v1"
MAX_BYTES = 65536


def save_revision(packet: dict, assessment: dict, generation: str | None, page: Path) -> dict:
    packet = validate_packet(json.dumps(packet).encode())
    assessment = validate_assessment(assessment, packet)
    if generation is not None and (type(generation) is not str or not re.fullmatch(r"[0-9a-f]{64}", generation)):
        raise ValueError("invalid_revision_generation")
    if page.is_symlink() or page.parent.is_symlink() or page.parent.name != packet["packet_version"]:
        raise ValueError("invalid_revision_artifact")
    html = page.read_bytes()
    digest = hashlib.sha256(html).hexdigest()
    if page.name != "review-" + digest[:16] + ".html":
        raise ValueError("invalid_revision_artifact")
    value = {
        "workflow": WORKFLOW, "event_id": packet["event"]["id"],
        "packet_version": packet["packet_version"], "assessment": assessment,
        "generation_version": generation, "artifact": page.name, "artifact_sha256": digest,
        "status": "proposal_only", "public_eligible": False,
        "publication_gates": ["incident_qualification", "evidence_qualification",
                              "current_input_transaction", "publication_authorization"],
    }
    if generation is None:
        value["publication_gates"].append("generation_provenance")
    version = _version(value)
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError("oversized_private_revision")
    _immutable_write(page.parent / ("revision-" + version + ".json"), encoded)
    return {"workflow": WORKFLOW, "version": version, "status": "proposal_only",
            "public_eligible": False}


def validate_receipt(raw: bytes, descriptor: dict, *, packet_version: str,
                     event_id: str, artifact: str, html: bytes) -> dict:
    from .event_review import _json
    if (type(descriptor) is not dict or descriptor.keys() != {"workflow", "version", "status", "public_eligible"}
            or descriptor["workflow"] != WORKFLOW or descriptor["status"] != "proposal_only"
            or descriptor["public_eligible"] is not False
            or type(descriptor["version"]) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", descriptor["version"])):
        raise ValueError("private_revision_unavailable")
    value = _json(raw, MAX_BYTES)
    if (value.keys() != {"workflow", "event_id", "packet_version", "assessment", "generation_version",
                        "artifact", "artifact_sha256", "status", "public_eligible", "publication_gates"}
            or _version(value) != descriptor["version"]
            or value["workflow"] != WORKFLOW or value["status"] != "proposal_only"
            or value["public_eligible"] is not False or value["event_id"] != event_id
            or value["packet_version"] != packet_version or value["artifact"] != artifact
            or value["artifact_sha256"] != hashlib.sha256(html).hexdigest()):
        raise ValueError("private_revision_unavailable")
    return value
