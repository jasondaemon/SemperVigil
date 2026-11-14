"""Offline quality report for private article-applicability comparisons."""

import argparse
import json
from pathlib import Path

from .article_applicability import WORKFLOW

GROUPS = ("kinds", "jurisdictions", "organizations", "products")


def _name(value: str) -> str:
    return " ".join(value.split()).casefold()


def _key(group: str, row: dict) -> tuple:
    if group == "kinds":
        return (row["type"],)
    if group == "jurisdictions":
        return (row["code"], row["role"])
    if group == "organizations":
        return (_name(row["name"]), row["role"])
    return (_name(row["name"]), _name(row["vendor_name"] or ""), row["role"])


def _keys(group: str, rows: list[dict]) -> list[tuple]:
    if not isinstance(rows, list):
        raise ValueError(f"article_applicability_eval_invalid_{group}")
    try:
        return [_key(group, row) for row in rows]
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"article_applicability_eval_invalid_{group}") from exc


def evaluate(casebook: dict, job_results: list[dict]) -> dict:
    """Compare explicitly audited fields only; never silently grade unreviewed data."""
    if casebook.get("version") != 1 or not isinstance(casebook.get("cases"), list):
        raise ValueError("article_applicability_eval_invalid_casebook")
    if not casebook["cases"]:
        raise ValueError("article_applicability_eval_empty_casebook")
    found = {}
    for job in job_results:
        if job.get("workflow") != WORKFLOW or job.get("public_eligible") is not False:
            raise ValueError("article_applicability_eval_not_private_result")
        for row in job.get("articles", []):
            article_id = row["article_id"]
            if article_id in found:
                raise ValueError("article_applicability_eval_duplicate_result")
            found[article_id] = row
    scores = {group: {"tp": 0, "fp": 0, "fn": 0} for group in GROUPS}
    cases = []
    seen = set()
    for case in casebook["cases"]:
        article_id = case.get("article_id")
        source_version = case.get("source_version")
        audited = case.get("audited_groups")
        expected = case.get("expected")
        if (type(article_id) is not int or article_id <= 0 or article_id in seen
                or not isinstance(source_version, str) or len(source_version) != 64
                or not isinstance(audited, list) or not audited
                or len(set(audited)) != len(audited)
                or not set(audited).issubset(GROUPS)
                or not isinstance(expected, dict) or set(expected) != set(audited)):
            raise ValueError("article_applicability_eval_invalid_case")
        seen.add(article_id)
        row = found.get(article_id)
        report = {"article_id": article_id, "status": "missing", "fields": {}}
        if row is None:
            cases.append(report)
            continue
        if row.get("source_version") != source_version:
            report["status"] = "stale_source"
            cases.append(report)
            continue
        if row.get("status") != "private_comparison" or row.get("public_eligible") is not False:
            report["status"] = "held"
            cases.append(report)
            continue
        result = row.get("result")
        if not isinstance(result, dict):
            report["status"] = "invalid_result"
            cases.append(report)
            continue
        report["status"] = "graded"
        for group in audited:
            wanted = _keys(group, expected[group])
            actual = _keys(group, result.get(group))
            if len(wanted) != len(set(wanted)) or len(actual) != len(set(actual)):
                raise ValueError("article_applicability_eval_duplicate_assertion")
            wanted_set, actual_set = set(wanted), set(actual)
            missing = sorted(wanted_set - actual_set)
            unexpected = sorted(actual_set - wanted_set)
            scores[group]["tp"] += len(wanted_set & actual_set)
            scores[group]["fp"] += len(unexpected)
            scores[group]["fn"] += len(missing)
            report["fields"][group] = {"missing": missing, "unexpected": unexpected}
        cases.append(report)
    graded = sum(case["status"] == "graded" for case in cases)
    return {"workflow": WORKFLOW, "graded": graded, "total": len(cases),
            "scores": scores, "cases": cases}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("casebook", type=Path)
    parser.add_argument("results", type=Path, nargs="+")
    args = parser.parse_args()
    casebook = json.loads(args.casebook.read_text(encoding="utf-8"))
    jobs = [json.loads(path.read_text(encoding="utf-8")) for path in args.results]
    print(json.dumps(evaluate(casebook, jobs), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
