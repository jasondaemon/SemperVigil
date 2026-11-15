"""Apply the editorial-day functions without replacing unrelated live storage code."""

import ast
import pathlib
import sys


def functions(lines: list[str]) -> dict[str, ast.FunctionDef]:
    module = ast.parse("".join(lines))
    return {
        node.name: node
        for node in module.body
        if isinstance(node, ast.FunctionDef)
    }


def apply_overlay(base_text: str, source_text: str) -> str:
    base_lines = base_text.splitlines(keepends=True)
    source_lines = source_text.splitlines(keepends=True)
    base_functions = functions(base_lines)
    source_functions = functions(source_lines)
    replacements = {
        "list_cves_for_day": source_lines[
            source_functions["list_cves_for_day"].lineno - 1:
            source_functions["list_cves_for_day"].end_lineno
        ],
        "_brief_day_from": source_lines[
            source_functions["_editorial_timezone_name"].lineno - 1:
            source_functions["_brief_day_from"].end_lineno
        ],
    }
    for name in sorted(replacements, key=lambda key: base_functions[key].lineno, reverse=True):
        old = base_functions[name]
        base_lines[old.lineno - 1:old.end_lineno] = replacements[name]
    result = "".join(base_lines)
    compile(result, "storage.py", "exec")
    return result


if __name__ == "__main__":
    target = pathlib.Path(sys.argv[1])
    source = pathlib.Path(sys.argv[2])
    target.write_text(apply_overlay(target.read_text(), source.read_text()))
