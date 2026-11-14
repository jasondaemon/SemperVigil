"""Fail-closed monthly reservation ledger for the official OpenAI transport."""

import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


SCHEMA = """
CREATE TABLE IF NOT EXISTS openai_budget_periods (
    period TEXT PRIMARY KEY,
    cap_usd NUMERIC(12, 6) NOT NULL,
    reserved_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    charged_usd NUMERIC(12, 6) NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS openai_budget_calls (
    id UUID PRIMARY KEY,
    period TEXT NOT NULL REFERENCES openai_budget_periods(period),
    model TEXT NOT NULL,
    stage TEXT NOT NULL,
    reserved_usd NUMERIC(12, 6) NOT NULL,
    charged_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    response_id TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# USD per million text tokens. Unknown models fail closed when the guard is on.
PRICES = {
    "gpt-4o-mini": (Decimal("0.15"), Decimal("0.60")),
    "gpt-4o-mini-2024-07-18": (Decimal("0.15"), Decimal("0.60")),
    "gpt-5.6-luna": (Decimal("0.20"), Decimal("1.20")),
    "gpt-5.6-terra": (Decimal("2.00"), Decimal("12.00")),
    "gpt-5.6-sol": (Decimal("4.00"), Decimal("20.00")),
    "gpt-5.6": (Decimal("4.00"), Decimal("20.00")),
}


class BudgetExceeded(RuntimeError):
    pass


def configured_cap() -> Decimal | None:
    raw = os.environ.get("SV_OPENAI_MONTHLY_BUDGET_USD", "").strip()
    if not raw:
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError("invalid_openai_monthly_budget") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError("invalid_openai_monthly_budget")
    return value


def _connect():
    import psycopg

    from .db import get_db_url

    return psycopg.connect(get_db_url(), application_name="sempervigil-openai-budget")


def _reserved_cost(payload: dict) -> Decimal:
    model = str(payload.get("model") or "")
    if model not in PRICES:
        raise ValueError("openai_budget_unknown_model")
    default_maximum = 16384 if model.startswith("gpt-4o-mini") else 128000
    maximum = payload.get("max_completion_tokens", payload.get("max_tokens", default_maximum))
    if type(maximum) is not int or maximum < 1 or maximum > 128000:
        raise ValueError("openai_budget_invalid_output_cap")
    # UTF-8 request bytes plus framing allowance is a conservative input-token bound.
    input_tokens = len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) + 1024
    input_price, output_price = PRICES[model]
    if input_tokens > 272000 and model.startswith("gpt-5.6"):
        input_price *= 2
        output_price *= Decimal("1.5")
    return ((input_price * input_tokens + output_price * maximum)
            / Decimal(1000000)).quantize(Decimal("0.000001"))


def reserve(payload: dict, stage: str) -> str | None:
    cap = configured_cap()
    if cap is None:
        return None
    cost = _reserved_cost(payload)
    period = datetime.now(timezone.utc).strftime("%Y-%m")
    call_id = str(uuid.uuid4())
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO openai_budget_periods(period,cap_usd)
                VALUES (%s,%s) ON CONFLICT(period) DO NOTHING""", (period, cap))
            cur.execute("""UPDATE openai_budget_periods
                SET cap_usd=%s, reserved_usd=reserved_usd+%s
                WHERE period=%s AND charged_usd+reserved_usd+%s<=%s
                RETURNING period""", (cap, cost, period, cost, cap))
            if cur.fetchone() is None:
                raise BudgetExceeded("openai_monthly_budget_exhausted")
            cur.execute("""INSERT INTO openai_budget_calls
                (id,period,model,stage,reserved_usd,status)
                VALUES (%s,%s,%s,%s,%s,'reserved')""",
                (call_id, period, payload["model"], stage[:100], cost))
    return call_id


def _actual_cost(model: str, usage: dict) -> Decimal | None:
    if model not in PRICES:
        return None
    prompt = usage.get("prompt_tokens", usage.get("input_tokens"))
    completion = usage.get("completion_tokens", usage.get("output_tokens"))
    if type(prompt) is not int or type(completion) is not int or prompt < 0 or completion < 0:
        return None
    input_price, output_price = PRICES[model]
    if prompt > 272000 and model.startswith("gpt-5.6"):
        input_price *= 2
        output_price *= Decimal("1.5")
    return ((input_price * prompt + output_price * completion)
            / Decimal(1000000)).quantize(Decimal("0.000001"))


def finish(call_id: str | None, response: dict | None, *, definitive_failure: bool = False,
           response_id: str | None = None) -> None:
    """Keep an ambiguous call reserved; release only a definitive provider rejection."""
    if not call_id and not response_id:
        return
    with _connect() as conn:
        with conn.cursor() as cur:
            if response_id and not call_id:
                cur.execute("""SELECT id,period,model,reserved_usd,status FROM openai_budget_calls
                    WHERE response_id=%s FOR UPDATE""", (response_id,))
            else:
                cur.execute("""SELECT id,period,model,reserved_usd,status FROM openai_budget_calls
                    WHERE id=%s FOR UPDATE""", (call_id,))
            row = cur.fetchone()
            if not row or row[4] != "reserved":
                return
            identity, period, model, reserved, _ = row
            if definitive_failure:
                actual = Decimal(0)
            elif isinstance(response, dict):
                actual = _actual_cost(model, response.get("usage") or {})
            else:
                actual = None
            if actual is None:
                if isinstance(response, dict) and response.get("status") in {
                    "queued", "in_progress"
                } and response.get("id"):
                    cur.execute("UPDATE openai_budget_calls SET response_id=%s WHERE id=%s",
                                (str(response["id"]), identity))
                    return
                if response is None:
                    return
                actual = reserved
            cur.execute("""UPDATE openai_budget_periods SET
                reserved_usd=reserved_usd-%s, charged_usd=charged_usd+%s WHERE period=%s""",
                (reserved, actual, period))
            cur.execute("""UPDATE openai_budget_calls SET charged_usd=%s,
                status=%s, response_id=COALESCE(response_id,%s) WHERE id=%s""",
                (actual, "rejected" if definitive_failure else "charged",
                 str(response.get("id")) if isinstance(response, dict) and response.get("id") else None,
                 identity))


def status(conn) -> dict:
    period = datetime.now(timezone.utc).strftime("%Y-%m")
    row = conn.execute("""SELECT cap_usd,reserved_usd,charged_usd FROM openai_budget_periods
        WHERE period=%s""", (period,)).fetchone()
    cap = configured_cap()
    return {"period": period, "configured_cap_usd": str(cap) if cap else None,
            "reserved_usd": str(row[1]) if row else "0",
            "charged_usd": str(row[2]) if row else "0"}
