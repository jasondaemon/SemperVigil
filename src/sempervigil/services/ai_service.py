from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from ..security.secrets import decrypt_secret, encrypt_secret
from ..utils import json_dumps, utc_now_iso


def list_providers(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT p.id, p.name, p.type, p.base_url, p.is_enabled, p.timeout_s, p.retries,
               p.last_test_status, p.last_test_at, p.last_test_error,
               s.api_key_last4
        FROM llm_providers p
        LEFT JOIN llm_provider_secrets s ON s.provider_id = p.id
        ORDER BY p.name
        """
    )
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        (
            provider_id,
            name,
            kind,
            base_url,
            is_enabled,
            timeout_s,
            retries,
            last_test_status,
            last_test_at,
            last_test_error,
            api_key_last4,
        ) = row
        rows.append(
            {
                "id": provider_id,
                "name": name,
                "type": kind,
                "base_url": base_url,
                "is_enabled": bool(is_enabled),
                "timeout_s": timeout_s,
                "retries": retries,
                "last_test_status": last_test_status,
                "last_test_at": last_test_at,
                "last_test_error": last_test_error,
                "api_key_last4": api_key_last4 or "",
            }
        )
    return rows


def get_provider(conn: sqlite3.Connection, provider_id: str) -> dict[str, Any] | None:
    for provider in list_providers(conn):
        if provider["id"] == provider_id:
            return provider
    return None


def create_provider(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    kind = str(payload.get("type") or "").strip()
    if not kind:
        raise ValueError("type is required")
    provider_id = str(payload.get("id") or uuid.uuid4())
    base_url = str(payload.get("base_url") or "").strip() or None
    enabled = bool(payload.get("is_enabled", True))
    timeout_s = int(payload.get("timeout_s", 30))
    retries = int(payload.get("retries", 2))
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO llm_providers
            (id, name, type, base_url, is_enabled, timeout_s, retries,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            provider_id,
            name,
            kind,
            base_url,
            1 if enabled else 0,
            timeout_s,
            retries,
            now,
            now,
        ),
    )
    conn.commit()
    return get_provider(conn, provider_id) or {}


def update_provider(conn: sqlite3.Connection, provider_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_provider(conn, provider_id)
    if not current:
        raise ValueError("provider_not_found")
    name = str(payload.get("name") or current["name"]).strip()
    kind = str(payload.get("type") or current["type"]).strip()
    base_url = str(payload.get("base_url") or current.get("base_url") or "").strip() or None
    enabled = bool(payload.get("is_enabled", current["is_enabled"]))
    timeout_s = int(payload.get("timeout_s", current["timeout_s"]))
    retries = int(payload.get("retries", current["retries"]))
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE llm_providers
        SET name = ?, type = ?, base_url = ?, is_enabled = ?, timeout_s = ?,
            retries = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            name,
            kind,
            base_url,
            1 if enabled else 0,
            timeout_s,
            retries,
            now,
            provider_id,
        ),
    )
    conn.commit()
    return get_provider(conn, provider_id) or {}


def delete_provider(conn: sqlite3.Connection, provider_id: str) -> None:
    conn.execute("DELETE FROM llm_provider_secrets WHERE provider_id = ?", (provider_id,))
    conn.execute("DELETE FROM llm_models WHERE provider_id = ?", (provider_id,))
    conn.execute("DELETE FROM llm_profiles WHERE primary_provider_id = ?", (provider_id,))
    conn.execute("DELETE FROM llm_providers WHERE id = ?", (provider_id,))
    conn.commit()


def set_provider_secret(conn: sqlite3.Connection, provider_id: str, api_key: str) -> dict[str, Any]:
    provider = get_provider(conn, provider_id)
    if not provider:
        raise ValueError("provider_not_found")
    key_id, api_key_enc = encrypt_secret(api_key, _provider_aad(provider_id))
    last4 = api_key[-4:] if len(api_key) >= 4 else api_key
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO llm_provider_secrets
            (provider_id, key_id, api_key_enc, api_key_last4, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_id) DO UPDATE SET
            key_id=excluded.key_id,
            api_key_enc=excluded.api_key_enc,
            api_key_last4=excluded.api_key_last4,
            updated_at=excluded.updated_at
        """,
        (provider_id, key_id, api_key_enc, last4, now, now),
    )
    conn.commit()
    return get_provider(conn, provider_id) or {}


def clear_provider_secret(conn: sqlite3.Connection, provider_id: str) -> None:
    conn.execute("DELETE FROM llm_provider_secrets WHERE provider_id = ?", (provider_id,))
    conn.commit()


def load_provider_secret(conn: sqlite3.Connection, provider_id: str) -> str | None:
    row = conn.execute(
        "SELECT api_key_enc FROM llm_provider_secrets WHERE provider_id = ?",
        (provider_id,),
    ).fetchone()
    if not row:
        return None
    return decrypt_secret(row[0], _provider_aad(provider_id))


def list_models(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT m.id, m.provider_id, m.model_name, m.max_context,
               m.default_params_json, m.tags_json, m.is_enabled, p.name
        FROM llm_models m
        JOIN llm_providers p ON p.id = m.provider_id
        ORDER BY p.name, m.model_name
        """
    )
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        (
            model_id,
            provider_id,
            model_name,
            max_context,
            default_params_json,
            tags_json,
            is_enabled,
            provider_name,
        ) = row
        rows.append(
            {
                "id": model_id,
                "provider_id": provider_id,
                "provider_name": provider_name,
                "model_name": model_name,
                "max_context": max_context,
                "default_params": _parse_json(default_params_json, {}),
                "tags": _parse_json(tags_json, []),
                "is_enabled": bool(is_enabled),
            }
        )
    return rows


def create_model(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    provider_id = str(payload.get("provider_id") or "").strip()
    if not provider_id:
        raise ValueError("provider_id is required")
    model_name = str(payload.get("model_name") or "").strip()
    if not model_name:
        raise ValueError("model_name is required")
    model_id = str(payload.get("id") or uuid.uuid4())
    max_context = payload.get("max_context")
    default_params = payload.get("default_params") or {}
    tags = payload.get("tags") or []
    enabled = bool(payload.get("is_enabled", True))
    conn.execute(
        """
        INSERT INTO llm_models
            (id, provider_id, model_name, max_context, default_params_json, tags_json, is_enabled)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            model_id,
            provider_id,
            model_name,
            max_context,
            json_dumps(default_params) if default_params else None,
            json_dumps(tags) if tags else None,
            1 if enabled else 0,
        ),
    )
    conn.commit()
    return get_model(conn, model_id) or {}


def update_model(conn: sqlite3.Connection, model_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_model(conn, model_id)
    if not current:
        raise ValueError("model_not_found")
    model_name = str(payload.get("model_name") or current["model_name"]).strip()
    provider_id = str(payload.get("provider_id") or current["provider_id"]).strip()
    max_context = payload.get("max_context", current.get("max_context"))
    default_params = payload.get("default_params", current.get("default_params", {}))
    tags = payload.get("tags", current.get("tags", []))
    enabled = bool(payload.get("is_enabled", current.get("is_enabled", True)))
    conn.execute(
        """
        UPDATE llm_models
        SET provider_id = ?, model_name = ?, max_context = ?, default_params_json = ?,
            tags_json = ?, is_enabled = ?
        WHERE id = ?
        """,
        (
            provider_id,
            model_name,
            max_context,
            json_dumps(default_params) if default_params else None,
            json_dumps(tags) if tags else None,
            1 if enabled else 0,
            model_id,
        ),
    )
    conn.commit()
    return get_model(conn, model_id) or {}


def delete_model(conn: sqlite3.Connection, model_id: str) -> None:
    conn.execute("DELETE FROM llm_models WHERE id = ?", (model_id,))
    conn.commit()


def get_model(conn: sqlite3.Connection, model_id: str) -> dict[str, Any] | None:
    for model in list_models(conn):
        if model["id"] == model_id:
            return model
    return None


def list_prompts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, name, version, system_template, user_template, notes, created_at
        FROM llm_prompts
        ORDER BY name, version
        """
    )
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "id": row[0],
                "name": row[1],
                "version": row[2],
                "system_template": row[3],
                "user_template": row[4],
                "notes": row[5],
                "created_at": row[6],
            }
        )
    return rows


def create_prompt(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    version = str(payload.get("version") or "v1").strip()
    system_template = str(payload.get("system_template") or "").strip()
    user_template = str(payload.get("user_template") or "").strip()
    if not system_template or not user_template:
        raise ValueError("system_template and user_template are required")
    prompt_id = str(payload.get("id") or uuid.uuid4())
    notes = str(payload.get("notes") or "").strip() or None
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO llm_prompts
            (id, name, version, system_template, user_template, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (prompt_id, name, version, system_template, user_template, notes, now),
    )
    conn.commit()
    return get_prompt(conn, prompt_id) or {}


def update_prompt(conn: sqlite3.Connection, prompt_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_prompt(conn, prompt_id)
    if not current:
        raise ValueError("prompt_not_found")
    name = str(payload.get("name") or current["name"]).strip()
    version = str(payload.get("version") or current["version"]).strip()
    system_template = str(payload.get("system_template") or current["system_template"]).strip()
    user_template = str(payload.get("user_template") or current["user_template"]).strip()
    notes = str(payload.get("notes") or current.get("notes") or "").strip() or None
    conn.execute(
        """
        UPDATE llm_prompts
        SET name = ?, version = ?, system_template = ?, user_template = ?, notes = ?
        WHERE id = ?
        """,
        (name, version, system_template, user_template, notes, prompt_id),
    )
    conn.commit()
    return get_prompt(conn, prompt_id) or {}


def delete_prompt(conn: sqlite3.Connection, prompt_id: str) -> None:
    conn.execute("DELETE FROM llm_prompts WHERE id = ?", (prompt_id,))
    conn.commit()


def get_prompt(conn: sqlite3.Connection, prompt_id: str) -> dict[str, Any] | None:
    for prompt in list_prompts(conn):
        if prompt["id"] == prompt_id:
            return prompt
    return None


def list_schemas(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT id, name, version, json_schema, created_at
        FROM llm_schemas
        ORDER BY name, version
        """
    )
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "id": row[0],
                "name": row[1],
                "version": row[2],
                "json_schema": _parse_json(row[3], {}),
                "created_at": row[4],
            }
        )
    return rows


def create_schema(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    version = str(payload.get("version") or "v1").strip()
    schema = payload.get("json_schema")
    if not schema:
        raise ValueError("json_schema is required")
    schema_id = str(payload.get("id") or uuid.uuid4())
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO llm_schemas
            (id, name, version, json_schema, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (schema_id, name, version, json_dumps(schema), now),
    )
    conn.commit()
    return get_schema(conn, schema_id) or {}


def update_schema(conn: sqlite3.Connection, schema_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_schema(conn, schema_id)
    if not current:
        raise ValueError("schema_not_found")
    name = str(payload.get("name") or current["name"]).strip()
    version = str(payload.get("version") or current["version"]).strip()
    schema = payload.get("json_schema", current.get("json_schema"))
    conn.execute(
        """
        UPDATE llm_schemas
        SET name = ?, version = ?, json_schema = ?
        WHERE id = ?
        """,
        (name, version, json_dumps(schema), schema_id),
    )
    conn.commit()
    return get_schema(conn, schema_id) or {}


def delete_schema(conn: sqlite3.Connection, schema_id: str) -> None:
    conn.execute("DELETE FROM llm_schemas WHERE id = ?", (schema_id,))
    conn.commit()


def get_schema(conn: sqlite3.Connection, schema_id: str) -> dict[str, Any] | None:
    for schema in list_schemas(conn):
        if schema["id"] == schema_id:
            return schema
    return None


def list_profiles(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT p.id, p.name, p.primary_provider_id, p.primary_model_id, p.prompt_id,
               p.schema_id, p.params_json, p.fallback_json, p.is_enabled,
               p.created_at, p.updated_at,
               providers.name, models.model_name, prompts.name
        FROM llm_profiles p
        JOIN llm_providers providers ON providers.id = p.primary_provider_id
        JOIN llm_models models ON models.id = p.primary_model_id
        JOIN llm_prompts prompts ON prompts.id = p.prompt_id
        ORDER BY p.name
        """
    )
    rows = []
    for row in cursor.fetchall():
        (
            profile_id,
            name,
            provider_id,
            model_id,
            prompt_id,
            schema_id,
            params_json,
            fallback_json,
            is_enabled,
            created_at,
            updated_at,
            provider_name,
            model_name,
            prompt_name,
        ) = row
        rows.append(
            {
                "id": profile_id,
                "name": name,
                "primary_provider_id": provider_id,
                "primary_model_id": model_id,
                "prompt_id": prompt_id,
                "schema_id": schema_id,
                "params": _parse_json(params_json, {}),
                "fallback": _parse_json(fallback_json, []),
                "is_enabled": bool(is_enabled),
                "created_at": created_at,
                "updated_at": updated_at,
                "provider_name": provider_name,
                "model_name": model_name,
                "prompt_name": prompt_name,
            }
        )
    return rows


def create_profile(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    provider_id = str(payload.get("primary_provider_id") or "").strip()
    model_id = str(payload.get("primary_model_id") or "").strip()
    prompt_id = str(payload.get("prompt_id") or "").strip()
    if not provider_id or not model_id or not prompt_id:
        raise ValueError("provider_id, model_id, and prompt_id are required")
    profile_id = str(payload.get("id") or uuid.uuid4())
    schema_id = payload.get("schema_id")
    params = payload.get("params") or {}
    fallback = payload.get("fallback") or []
    enabled = bool(payload.get("is_enabled", True))
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO llm_profiles
            (id, name, primary_provider_id, primary_model_id, prompt_id, schema_id,
             params_json, fallback_json, is_enabled, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            profile_id,
            name,
            provider_id,
            model_id,
            prompt_id,
            schema_id,
            json_dumps(params) if params else None,
            json_dumps(fallback) if fallback else None,
            1 if enabled else 0,
            now,
            now,
        ),
    )
    conn.commit()
    return get_profile(conn, profile_id) or {}


def update_profile(conn: sqlite3.Connection, profile_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_profile(conn, profile_id)
    if not current:
        raise ValueError("profile_not_found")
    name = str(payload.get("name") or current["name"]).strip()
    provider_id = str(payload.get("primary_provider_id") or current["primary_provider_id"]).strip()
    model_id = str(payload.get("primary_model_id") or current["primary_model_id"]).strip()
    prompt_id = str(payload.get("prompt_id") or current["prompt_id"]).strip()
    schema_id = payload.get("schema_id", current.get("schema_id"))
    params = payload.get("params", current.get("params", {}))
    fallback = payload.get("fallback", current.get("fallback", []))
    enabled = bool(payload.get("is_enabled", current.get("is_enabled", True)))
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE llm_profiles
        SET name = ?, primary_provider_id = ?, primary_model_id = ?, prompt_id = ?,
            schema_id = ?, params_json = ?, fallback_json = ?, is_enabled = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            name,
            provider_id,
            model_id,
            prompt_id,
            schema_id,
            json_dumps(params) if params else None,
            json_dumps(fallback) if fallback else None,
            1 if enabled else 0,
            now,
            profile_id,
        ),
    )
    conn.commit()
    return get_profile(conn, profile_id) or {}


def delete_profile(conn: sqlite3.Connection, profile_id: str) -> None:
    conn.execute("DELETE FROM llm_profiles WHERE id = ?", (profile_id,))
    conn.commit()


def get_profile(conn: sqlite3.Connection, profile_id: str) -> dict[str, Any] | None:
    for profile in list_profiles(conn):
        if profile["id"] == profile_id:
            return profile
    return None


def list_pipeline_routing(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT stage_name, profile_id, rules_json, updated_at
        FROM pipeline_stage_config
        ORDER BY stage_name
        """
    )
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "stage_name": row[0],
                "profile_id": row[1],
                "rules": _parse_json(row[2], {}),
                "updated_at": row[3],
            }
        )
    return rows


def set_pipeline_routing(conn: sqlite3.Connection, stage_name: str, profile_id: str) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO pipeline_stage_config (stage_name, profile_id, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(stage_name) DO UPDATE SET
            profile_id=excluded.profile_id,
            updated_at=excluded.updated_at
        """,
        (stage_name, profile_id, now),
    )
    conn.commit()


def update_provider_test_status(
    conn: sqlite3.Connection, provider_id: str, status: str, error: str | None
) -> None:
    conn.execute(
        """
        UPDATE llm_providers
        SET last_test_status = ?, last_test_at = ?, last_test_error = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, utc_now_iso(), error, utc_now_iso(), provider_id),
    )
    conn.commit()


def _parse_json(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _provider_aad(provider_id: str) -> bytes:
    return f"provider:{provider_id}".encode("utf-8")
