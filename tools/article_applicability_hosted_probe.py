"""Run a bounded, read-only hosted comparison inside an authorized worker pod.

Usage: python - model article_id [article_id ...]
The provider key is decrypted in memory; results contain no credential material.
"""

import json
import base64
import sys
import urllib.error
import urllib.request

from sempervigil import article_applicability as contract
from sempervigil.db import connect_db
from sempervigil.services.ai_service import list_providers, load_provider_secret
from sempervigil.storage import get_article_by_id


def main() -> None:
    if len(sys.argv) < 4 or sys.argv[1] not in {"gpt-5.6-luna", "gpt-5.6-terra"}:
        raise SystemExit("usage: python - MODEL CONTRACT_B64 ARTICLE_ID [ARTICLE_ID ...]")
    model = sys.argv[1]
    config = json.loads(base64.b64decode(sys.argv[2], validate=True))
    if set(config) != {"workflow", "prompt", "kinds"} or not config["workflow"].startswith(
        "article-applicability-v"
    ) or not isinstance(config["prompt"], str) or not isinstance(config["kinds"], list):
        raise SystemExit("invalid contract")
    contract.WORKFLOW = config["workflow"]
    contract.PROMPT = config["prompt"]
    contract.KINDS = tuple(config["kinds"])
    ids = [int(value) for value in sys.argv[3:]]
    if len(ids) > 6 or len(ids) != len(set(ids)) or any(value <= 0 for value in ids):
        raise SystemExit("probe requires 1-6 unique article IDs")
    conn = connect_db()
    try:
        providers = [p for p in list_providers(conn) if p["is_enabled"] and
                     p["type"] == "openai_compatible" and
                     str(p["base_url"]).rstrip("/") == "https://api.openai.com/v1"]
        if len(providers) != 1:
            raise RuntimeError("expected exactly one enabled official OpenAI provider")
        provider = providers[0]
        key = load_provider_secret(conn, provider["id"])
        if not key:
            raise RuntimeError("provider key unavailable")
        generation = "0" * 64
        result = {"workflow": contract.WORKFLOW, "public_eligible": False,
                  "model": model, "articles": [], "usage": {"prompt_tokens": 0,
                  "completion_tokens": 0}}
        for article_id in ids:
            article = get_article_by_id(conn, article_id)
            if not article:
                raise RuntimeError(f"article {article_id} unavailable")
            snapshot = {field: article.get(field) for field in ("id", "title", "content_text")}
            prompt = contract.request(snapshot, generation)
            body = {"model": model,
                    "messages": [{"role": "system", "content": prompt["prompt"]},
                                 {"role": "user", "content": prompt["input"]}],
                    "response_format": {"type": "json_schema", "json_schema": {
                        "name": "article_applicability", "strict": True,
                        "schema": prompt["schema"]}},
                    "reasoning_effort": "none", "max_completion_tokens": 2500}
            request = urllib.request.Request(
                "https://api.openai.com/v1/chat/completions",
                data=json.dumps(body).encode("utf-8"),
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json"}, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=90) as response:
                    output = json.load(response)
                raw = output["choices"][0]["message"]["content"]
                validated = contract.validate(raw, snapshot, generation)
                validated["status"] = "private_comparison"
                row = validated
                for name in result["usage"]:
                    result["usage"][name] += output.get("usage", {}).get(name, 0)
            except (urllib.error.HTTPError, urllib.error.URLError, KeyError, ValueError) as exc:
                row = {"article_id": article_id, "source_version": prompt["source_version"],
                       "status": "held", "error": type(exc).__name__}
            result["articles"].append(row)
        print(json.dumps(result, ensure_ascii=False))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
