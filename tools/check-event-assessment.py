#!/usr/bin/env python3
"""Offline, provisional model regression checks; never publication approval."""
import argparse
import json
from pathlib import Path

from sempervigil.event_assessment import request_for, validate_assessment
from sempervigil.event_review import validate_packet


def evaluate(packet: dict, assessment: dict, cases: dict) -> dict:
    assessment = validate_assessment(assessment, packet)
    request = request_for(packet)
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, type=Path)
    parser.add_argument("--assessment", required=True, type=Path)
    parser.add_argument("--cases", required=True, type=Path)
    args = parser.parse_args()
    try:
        packet = validate_packet(args.packet.read_bytes())
        report = evaluate(packet, json.loads(args.assessment.read_text()), json.loads(args.cases.read_text()))
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"passed": False, "error_type": type(exc).__name__, "public_eligible": False}))
        return 2
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
