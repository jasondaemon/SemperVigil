from __future__ import annotations

import argparse
import os
import sys

from .searxng import searxng_search


def main() -> int:
    parser = argparse.ArgumentParser(description="SearXNG smoke test")
    parser.add_argument("--query", default="test", help="Search query")
    parser.add_argument("--max-results", type=int, default=10)
    args = parser.parse_args()

    url = os.getenv("SV_SEARXNG_URL", "").strip()
    if not url:
        print("SV_SEARXNG_URL is required", file=sys.stderr)
        return 1
    timeout_s = int(os.getenv("SV_SEARXNG_TIMEOUT_S", "20"))
    engines = os.getenv("SV_SEARXNG_ENGINES")
    categories = os.getenv("SV_SEARXNG_CATEGORIES")
    results = searxng_search(
        args.query,
        url=url,
        timeout_s=timeout_s,
        engines=engines,
        categories=categories,
        max_results=args.max_results,
    )
    print(f"status=ok count={len(results)}")
    for item in results[:3]:
        print(item.get("url"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
