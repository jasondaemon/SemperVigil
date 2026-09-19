#!/usr/bin/env python3
"""Offline, provisional model regression checks; never publication approval."""
import argparse
import json
import re
from pathlib import Path

from sempervigil.event_assessment import SOURCE_WORKFLOW, request_for, validate_assessment
from sempervigil.event_review import draft, validate_packet


def evaluate(packet: dict, assessment: dict, cases: dict) -> dict:
    assessment = validate_assessment(assessment, packet)
    request = request_for(packet, scope=assessment.get("scope"), article_id=assessment.get("article_id"),
                          paired=assessment.get("paired", False))
    if (cases.get("packet_version") != packet["packet_version"]
            or cases.get("request_version") != request["request_version"]
            or cases.get("event_id") != packet["event"]["id"]):
        raise ValueError("evaluation_snapshot_mismatch")
    checks = cases.get("checks")
    if type(checks) is not list or not checks:
        raise ValueError("evaluation_cases_required")
    results = []
    seen = set()
    for case in checks:
        if (type(case) is not dict or case.keys() != {"passage_id", "label", "allowed"}
                or not isinstance(case["label"], str) or not case["label"]
                or not isinstance(case["passage_id"], str)
                or case["passage_id"] in seen
                or type(case["allowed"]) is not list or not case["allowed"]
                or any(v not in ("include", "exclude", "hold") for v in case["allowed"])):
            raise ValueError("invalid_evaluation_case")
        identity = case["passage_id"]
        seen.add(identity)
        suggestion = assessment["suggestions"].get(identity)
        if suggestion is None:
            raise ValueError("evaluation_passage_not_assessed")
        decision = suggestion["decision"]
        results.append({"label": case["label"], "decision": decision,
                        "passed": decision in case["allowed"],
                        "unsafe_include": decision == "include" and "include" not in case["allowed"]})
    return {"event_id": cases["event_id"], "request_version": request["request_version"],
            "passed": all(row["passed"] for row in results), "checks": results,
            "checked_passages": len(results), "assessed_passages": len(assessment["suggestions"]),
            "omitted_passages": assessment["omitted_passages"],
            "public_eligible": False, "review_status": "assistant-reviewed provisional cases"}


def evaluate_sources(packet: dict, bundle: dict, cases: dict) -> dict:
    """Evaluate a pinned source cohort, not independent cherry-picked successes."""
    if (type(bundle) is not dict or bundle.keys() != {"generation_version", "assessments"}
            or type(bundle["generation_version"]) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", bundle["generation_version"])
            or bundle["generation_version"] != cases.get("generation_version")
            or type(bundle["assessments"]) is not list or not 1 <= len(bundle["assessments"]) <= 12
            or cases.get("event_id") != packet["event"]["id"]
            or cases.get("packet_version") != packet["packet_version"]):
        raise ValueError("evaluation_bundle_mismatch")
    requests = cases.get("source_requests")
    if type(requests) is not dict or not requests or not cases.get("checks"):
        raise ValueError("evaluation_source_cases_required")
    owners = {p["id"]: str(p["article_id"]) for p in draft(packet)["passages"]}
    groups = {key: [] for key in requests}
    seen = set()
    for case in cases["checks"]:
        if type(case) is not dict or type(case.get("passage_id")) is not str:
            raise ValueError("invalid_evaluation_case")
        identity = case["passage_id"]
        if identity in seen or owners.get(identity) not in groups:
            raise ValueError("evaluation_case_coverage")
        seen.add(identity)
        groups[owners[identity]].append(case)
    if any(not group for group in groups.values()):
        raise ValueError("evaluation_case_coverage")
    reports = []
    assessed_sources = set()
    for value in bundle["assessments"]:
        # Each collected result must carry its own guarded configuration identity.
        if (type(value) is not dict or value.keys() != {"generation_version", "assessment"}
                or value["generation_version"] != bundle["generation_version"]):
            raise ValueError("evaluation_generation_mismatch")
        result = validate_assessment(value["assessment"], packet)
        key = str(result.get("article_id"))
        if (result["workflow"] != SOURCE_WORKFLOW or key not in requests or key in assessed_sources
                or result["scope"]["scope_version"] != cases.get("scope_version")):
            raise ValueError("evaluation_source_mismatch")
        assessed_sources.add(key)
        reports.append(evaluate(packet, result, {
            "event_id": cases["event_id"], "packet_version": cases["packet_version"],
            "request_version": requests[key], "checks": groups[key]}))
    if assessed_sources != requests.keys():
        raise ValueError("evaluation_missing_source")
    return {"event_id": cases["event_id"], "passed": all(r["passed"] for r in reports),
            "checked_passages": sum(r["checked_passages"] for r in reports),
            "sources": len(reports), "reports": reports, "public_eligible": False,
            "review_status": "assistant-reviewed provisional cases"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, type=Path)
    parser.add_argument("--assessment", required=True, type=Path)
    parser.add_argument("--cases", required=True, type=Path)
    parser.add_argument("--source-bundle", action="store_true")
    args = parser.parse_args()
    try:
        packet = validate_packet(args.packet.read_bytes())
        evaluator = evaluate_sources if args.source_bundle else evaluate
        report = evaluator(packet, json.loads(args.assessment.read_text()), json.loads(args.cases.read_text()))
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"passed": False, "error_type": type(exc).__name__, "public_eligible": False}))
        return 2
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
