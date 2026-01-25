from __future__ import annotations

import logging
import os
import sqlite3

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
try:
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
except Exception:  # noqa: BLE001
    ProxyHeadersMiddleware = None
from datetime import datetime, timezone
from pydantic import BaseModel

from .config import (
    ConfigError,
    bootstrap_runtime_config,
    get_runtime_config,
    get_state_db_path,
    load_runtime_config,
    set_runtime_config,
)
from .admin_ui import TEMPLATES, ui_router
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import enqueue_job, get_source_run_streaks, init_db, list_jobs
from .storage import (
    count_articles_since,
    get_last_source_run,
    list_articles_per_day,
    list_source_health_events,
    get_source_stats,
)
from .ingest import process_source
from .services.sources_service import (
    create_source,
    delete_source,
    get_source,
    list_sources,
    record_test_result,
    update_source,
)
from .services.ai_service import (
    clear_provider_secret,
    create_model,
    create_profile,
    create_prompt,
    create_provider,
    create_schema,
    delete_model,
    delete_profile,
    delete_prompt,
    delete_provider,
    delete_schema,
    get_model,
    get_profile,
    get_prompt,
    get_provider,
    get_schema,
    list_models,
    list_pipeline_routing,
    list_profiles,
    list_prompts,
    list_providers,
    list_schemas,
    set_pipeline_routing,
    set_provider_secret,
    update_model,
    update_profile,
    update_prompt,
    update_provider,
    update_provider_test_status,
    update_schema,
)
from .llm import STAGE_NAMES, test_profile, test_provider
from .utils import configure_logging, log_event, utc_now_iso_offset

app = FastAPI(title="SemperVigil Admin API")

ADMIN_COOKIE_NAME = "sv_admin_token"

if ProxyHeadersMiddleware:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

app.mount(
    "/ui/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="ui-static",
)


@app.get("/ui")
def ui_redirect():
    return RedirectResponse("/ui/", status_code=307)


def _require_admin_token(request: Request) -> None:
    token = os.environ.get("SV_ADMIN_TOKEN")
    if not token:
        return
    if not _is_authorized(request, token):
        raise HTTPException(status_code=401, detail="unauthorized")


def _is_authorized(request: Request, token: str) -> bool:
    header = request.headers.get("X-Admin-Token")
    if header and header == token:
        return True
    cookie = request.cookies.get(ADMIN_COOKIE_NAME)
    return cookie == token


def _is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").lower()
    if forwarded_proto:
        return forwarded_proto == "https"
    return request.url.scheme == "https"


@app.middleware("http")
async def _admin_token_middleware(request: Request, call_next):
    if request.url.path.startswith("/ui"):
        if request.url.path.startswith("/ui/login"):
            return await call_next(request)
        if request.url.path.startswith("/ui/static"):
            return await call_next(request)
        token = os.environ.get("SV_ADMIN_TOKEN")
        if token and not _is_authorized(request, token):
            return RedirectResponse("/ui/login", status_code=303)
    return await call_next(request)


class JobRequest(BaseModel):
    job_type: str
    source_id: str | None = None


class RuntimeConfigRequest(BaseModel):
    config: dict


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "SemperVigil Admin API"}


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "ok": True,
        "version": _get_version(),
        "time": datetime.now(tz=timezone.utc).isoformat(),
    }


@app.get("/admin/config/runtime", dependencies=[Depends(_require_admin_token)])
def runtime_config_get() -> dict[str, object]:
    conn = _get_conn()
    try:
        cfg = get_runtime_config(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"config": cfg}


@app.put("/admin/config/runtime", dependencies=[Depends(_require_admin_token)])
def runtime_config_set(payload: RuntimeConfigRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        set_runtime_config(conn, payload.config)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}


@app.get("/ui/login")
def ui_login(request: Request):
    token_enabled = bool(os.environ.get("SV_ADMIN_TOKEN"))
    return TEMPLATES.TemplateResponse(
        "admin/login.html",
        {
            "request": request,
            "token_enabled": token_enabled,
            "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
        },
    )


@app.post("/ui/login")
async def ui_login_post(request: Request):
    token = os.environ.get("SV_ADMIN_TOKEN")
    if not token:
        response = RedirectResponse("/ui", status_code=303)
        return response
    payload = await request.json()
    candidate = str(payload.get("token") or "")
    if candidate != token:
        return JSONResponse({"ok": False, "error": "invalid_token"}, status_code=401)
    response = JSONResponse({"ok": True})
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        token,
        httponly=True,
        secure=_is_secure_request(request),
        samesite="lax",
        max_age=86400,
    )
    return response


@app.post("/ui/logout")
def ui_logout():
    response = RedirectResponse("/ui/login", status_code=303)
    response.delete_cookie(ADMIN_COOKIE_NAME)
    return response


@app.on_event("startup")
def _startup() -> None:
    try:
        conn = init_db(get_state_db_path())
        config = load_runtime_config(conn)
    except ConfigError:
        return
    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))


@app.post("/jobs/enqueue")
def enqueue(job: JobRequest, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    logger = logging.getLogger("sempervigil.admin")
    conn = _get_conn()
    payload = {"source_id": job.source_id} if job.source_id else None
    job_id = enqueue_job(conn, job.job_type, payload, debounce=True)
    log_event(
        logger,
        logging.INFO,
        "job_enqueued",
        job_id=job_id,
        job_type=job.job_type,
    )
    return {"job_id": job_id}


@app.get("/jobs")
def jobs(limit: int = 20) -> list[dict[str, str]]:
    conn = _get_conn()
    rows = []
    for job in list_jobs(conn, limit=limit):
        rows.append(
            {
                "id": job.id,
                "job_type": job.job_type,
                "status": job.status,
                "requested_at": job.requested_at,
                "started_at": job.started_at or "",
                "finished_at": job.finished_at or "",
                "error": job.error or "",
                "result": job.result or {},
            }
        )
    return rows


class SourceRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    kind: str | None = None
    url: str | None = None
    enabled: bool | None = None
    interval_minutes: int | None = None
    tags: list[str] | str | None = None


class ProviderRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    type: str | None = None
    base_url: str | None = None
    is_enabled: bool | None = None
    timeout_s: int | None = None
    retries: int | None = None


class ProviderSecretRequest(BaseModel):
    api_key: str


class ModelRequest(BaseModel):
    id: str | None = None
    provider_id: str | None = None
    model_name: str | None = None
    max_context: int | None = None
    default_params: dict[str, object] | None = None
    tags: list[str] | str | None = None
    is_enabled: bool | None = None


class PromptRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    version: str | None = None
    system_template: str | None = None
    user_template: str | None = None
    notes: str | None = None


class SchemaRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    version: str | None = None
    json_schema: dict[str, object] | None = None


class ProfileRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    primary_provider_id: str | None = None
    primary_model_id: str | None = None
    prompt_id: str | None = None
    schema_id: str | None = None
    params: dict[str, object] | None = None
    fallback: list[dict[str, object]] | None = None
    is_enabled: bool | None = None


class PipelineStageRequest(BaseModel):
    stage_name: str
    profile_id: str


class ProfileTestRequest(BaseModel):
    text: str


class DailyBriefRequest(BaseModel):
    date: str | None = None


class AnalyticsRequest(BaseModel):
    days: int = 30


@app.get("/sources")
def sources_list() -> list[dict[str, object]]:
    conn = _get_conn()
    sources = list_sources(conn)
    since = utc_now_iso_offset(seconds=-24 * 3600)
    for item in sources:
        item["articles_24h"] = count_articles_since(conn, item["id"], since)
        last_run = get_last_source_run(conn, item["id"])
        item["accepted_last_run"] = last_run["items_accepted"] if last_run else 0
    return sources


@app.get("/sources/health")
def sources_health() -> list[dict[str, object]]:
    conn = _get_conn()
    cursor = conn.execute(
        """
        SELECT s.id, s.name, s.enabled, s.pause_until, s.paused_reason,
               r.status, r.started_at, r.items_accepted, r.error
        FROM sources s
        LEFT JOIN (
            SELECT source_id, status, started_at, items_accepted, error
            FROM source_runs
            WHERE (source_id, started_at) IN (
                SELECT source_id, MAX(started_at) FROM source_runs GROUP BY source_id
            )
        ) r ON r.source_id = s.id
        ORDER BY s.id
        """
    )
    rows = []
    for (
        source_id,
        name,
        enabled,
        pause_until,
        paused_reason,
        status,
        started_at,
        items_accepted,
        error,
    ) in cursor.fetchall():
        streaks = get_source_run_streaks(conn, source_id)
        rows.append(
            {
                "id": source_id,
                "name": name,
                "enabled": bool(enabled),
                "pause_until": pause_until,
                "paused_reason": paused_reason,
                "last_status": status,
                "last_run_at": started_at,
                "last_items_accepted": items_accepted,
                "last_error": error,
                "consecutive_errors": streaks["consecutive_errors"],
                "consecutive_zero": streaks["consecutive_zero"],
            }
        )
    return rows


@app.post("/sources")
def sources_create(
    payload: SourceRequest, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_source(conn, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/sources/{source_id}")
def sources_read(source_id: str) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    return source


@app.put("/sources/{source_id}")
@app.patch("/sources/{source_id}")
def sources_update(
    source_id: str,
    payload: SourceRequest,
    _: None = Depends(_require_admin_token),
) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_source(conn, source_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/sources/{source_id}")
def sources_delete(source_id: str, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    conn = _get_conn()
    delete_source(conn, source_id)
    return {"status": "deleted"}


@app.post("/sources/{source_id}/test")
def sources_test(
    source_id: str, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    conn = _get_conn()
    try:
        config = load_runtime_config(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    result = process_source(
        source=source_to_model(source),
        config=config,
        logger=logging.getLogger("sempervigil.admin"),
        conn=conn,
        test_mode=True,
    )
    ok = result.status == "ok"
    record_test_result(conn, source_id, ok=ok, error=result.error)
    preview = []
    for decision in result.decisions[:5]:
        preview.append(
            {
                "title": decision.title,
                "url": decision.normalized_url,
                "published_at": decision.published_at,
                "decision": decision.decision,
                "reasons": decision.reasons,
            }
        )
    return {
        "status": result.status,
        "http_status": result.http_status,
        "error": result.error,
        "found_count": result.found_count,
        "accepted_count": result.accepted_count,
        "items": preview,
    }


@app.get("/sources/{source_id}/health")
def sources_health_history(source_id: str, limit: int = 50) -> list[dict[str, object]]:
    conn = _get_conn()
    return list_source_health_events(conn, source_id, limit=limit)


@app.get("/admin/analytics/articles_per_day")
def analytics_articles_per_day(days: int = 30) -> dict[str, object]:
    conn = _get_conn()
    since_day = (datetime.now(tz=timezone.utc) - timedelta(days=days)).date().isoformat()
    return {"days": days, "data": list_articles_per_day(conn, since_day)}


@app.get("/admin/analytics/source_stats")
def analytics_source_stats(days: int = 7, runs: int = 20) -> dict[str, object]:
    conn = _get_conn()
    return {"days": days, "runs": runs, "data": get_source_stats(conn, days, runs)}




def _setup_logging() -> None:
    configure_logging("sempervigil.admin")


_setup_logging()


def _get_version() -> str:
    try:
        from importlib.metadata import version

        return version("sempervigil")
    except Exception:  # noqa: BLE001
        return "unknown"


def source_to_model(source: dict[str, object]):
    from .models import Source

    return Source(
        id=str(source.get("id")),
        name=str(source.get("name")),
        enabled=bool(source.get("enabled", True)),
        base_url=source.get("url") or source.get("base_url"),
        topic_key=None,
        default_frequency_minutes=int(source.get("interval_minutes", 60)),
        pause_until=source.get("pause_until"),
        paused_reason=source.get("paused_reason"),
        robots_notes=None,
    )


def _get_conn() -> sqlite3.Connection:
    conn = init_db(get_state_db_path())
    bootstrap_runtime_config(conn)
    return conn


ai_router = APIRouter(prefix="/admin/ai", dependencies=[Depends(_require_admin_token)])


@ai_router.get("/providers")
def ai_providers_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_providers(conn)


@ai_router.post("/providers")
def ai_providers_create(payload: ProviderRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_provider(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/providers/{provider_id}")
def ai_providers_get(provider_id: str) -> dict[str, object]:
    conn = _get_conn()
    provider = get_provider(conn, provider_id)
    if not provider:
        raise HTTPException(status_code=404, detail="provider_not_found")
    return provider


@ai_router.put("/providers/{provider_id}")
@ai_router.patch("/providers/{provider_id}")
def ai_providers_update(provider_id: str, payload: ProviderRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_provider(conn, provider_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/providers/{provider_id}")
def ai_providers_delete(provider_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_provider(conn, provider_id)
    return {"status": "deleted"}


@ai_router.post("/providers/{provider_id}/secret")
def ai_providers_set_secret(
    provider_id: str, payload: ProviderSecretRequest
) -> dict[str, object]:
    conn = _get_conn()
    try:
        return set_provider_secret(conn, provider_id, payload.api_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/providers/{provider_id}/secret")
def ai_providers_clear_secret(provider_id: str) -> dict[str, str]:
    conn = _get_conn()
    clear_provider_secret(conn, provider_id)
    return {"status": "cleared"}


@ai_router.post("/providers/{provider_id}/test")
def ai_providers_test(provider_id: str) -> dict[str, object]:
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    try:
        result = test_provider(conn, provider_id, logger)
        update_provider_test_status(conn, provider_id, "ok", None)
        return result
    except Exception as exc:  # noqa: BLE001
        update_provider_test_status(conn, provider_id, "error", str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/models")
def ai_models_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_models(conn)


@ai_router.post("/models")
def ai_models_create(payload: ModelRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_model(conn, _normalize_model_payload(payload))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/models/{model_id}")
def ai_models_get(model_id: str) -> dict[str, object]:
    conn = _get_conn()
    model = get_model(conn, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="model_not_found")
    return model


@ai_router.put("/models/{model_id}")
@ai_router.patch("/models/{model_id}")
def ai_models_update(model_id: str, payload: ModelRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_model(conn, model_id, _normalize_model_payload(payload))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/models/{model_id}")
def ai_models_delete(model_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_model(conn, model_id)
    return {"status": "deleted"}


@ai_router.get("/prompts")
def ai_prompts_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_prompts(conn)


@ai_router.post("/prompts")
def ai_prompts_create(payload: PromptRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_prompt(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/prompts/{prompt_id}")
def ai_prompts_get(prompt_id: str) -> dict[str, object]:
    conn = _get_conn()
    prompt = get_prompt(conn, prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="prompt_not_found")
    return prompt


@ai_router.put("/prompts/{prompt_id}")
@ai_router.patch("/prompts/{prompt_id}")
def ai_prompts_update(prompt_id: str, payload: PromptRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_prompt(conn, prompt_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/prompts/{prompt_id}")
def ai_prompts_delete(prompt_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_prompt(conn, prompt_id)
    return {"status": "deleted"}


@ai_router.get("/schemas")
def ai_schemas_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_schemas(conn)


@ai_router.post("/schemas")
def ai_schemas_create(payload: SchemaRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_schema(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/schemas/{schema_id}")
def ai_schemas_get(schema_id: str) -> dict[str, object]:
    conn = _get_conn()
    schema = get_schema(conn, schema_id)
    if not schema:
        raise HTTPException(status_code=404, detail="schema_not_found")
    return schema


@ai_router.put("/schemas/{schema_id}")
@ai_router.patch("/schemas/{schema_id}")
def ai_schemas_update(schema_id: str, payload: SchemaRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_schema(conn, schema_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/schemas/{schema_id}")
def ai_schemas_delete(schema_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_schema(conn, schema_id)
    return {"status": "deleted"}


@ai_router.get("/profiles")
def ai_profiles_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_profiles(conn)


@ai_router.post("/profiles")
def ai_profiles_create(payload: ProfileRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_profile(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/profiles/{profile_id}")
def ai_profiles_get(profile_id: str) -> dict[str, object]:
    conn = _get_conn()
    profile = get_profile(conn, profile_id)
    if not profile:
        raise HTTPException(status_code=404, detail="profile_not_found")
    return profile


@ai_router.put("/profiles/{profile_id}")
@ai_router.patch("/profiles/{profile_id}")
def ai_profiles_update(profile_id: str, payload: ProfileRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_profile(conn, profile_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/profiles/{profile_id}")
def ai_profiles_delete(profile_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_profile(conn, profile_id)
    return {"status": "deleted"}


@ai_router.post("/profiles/{profile_id}/test")
def ai_profiles_test(profile_id: str, payload: ProfileTestRequest) -> dict[str, object]:
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    try:
        result = test_profile(conn, profile_id, payload.text, logger)
        return {"ok": True, **result}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/pipeline-routing")
def ai_pipeline_list() -> dict[str, object]:
    conn = _get_conn()
    return {"stages": STAGE_NAMES, "routing": list_pipeline_routing(conn)}


@ai_router.post("/pipeline-routing")
def ai_pipeline_set(payload: PipelineStageRequest) -> dict[str, str]:
    conn = _get_conn()
    set_pipeline_routing(conn, payload.stage_name, payload.profile_id)
    return {"status": "ok"}


def _normalize_model_payload(payload: ModelRequest) -> dict[str, object]:
    data = payload.model_dump(exclude_unset=True)
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [item.strip() for item in tags.split(",") if item.strip()]
    return data


app.include_router(ui_router(_require_admin_token), prefix="/ui")
app.include_router(ai_router)


@app.post("/admin/briefs/build")
def build_brief(payload: DailyBriefRequest, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    conn = _get_conn()
    job_id = enqueue_job(
        conn,
        "build_daily_brief",
        {"date": payload.date} if payload.date else {},
    )
    return {"job_id": job_id}
