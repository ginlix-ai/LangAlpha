"""User-saved and repository-shipped workflow API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Response
from pydantic import BaseModel

from ptc_agent.agent.backends import lock_for_namespace
from ptc_agent.agent.backends.workflows import (
    WORKFLOW_NAME_RE,
    MalformedWorkflowError,
    build_workflow_value,
    workflow_key,
    workflow_name_from_key,
    workflow_namespace,
    workflow_script_byte_cap,
    workflow_script_from_value,
)
from ptc_agent.agent.middleware.background_subagent.workflow.engine import (
    WorkflowScriptError,
    acompile_check,
    compile_is_memoized,
)
from ptc_agent.agent.middleware.background_subagent.workflow.prebuilt import (
    get_prebuilt_workflows,
)
from src.config.settings import get_workflow_orchestration_config
from src.server.app import setup
from src.server.app._store_helpers import (
    adelete,
    aget,
    aput,
    paginate_namespace,
    require_store,
)
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/workflows", tags=["Workflows"])

# Describing a row means compiling it, and the agent mount writes scripts this
# router never saw — so a namespace can hold more uncompiled rows than the memo
# holds entries, and every listing would recompile all of them on the shared
# executor. Past this many fresh compiles a listing reports the row without a
# verdict; a namespace of ordinary size never reaches it.
_MAX_LIST_COMPILES = 32


class WorkflowWriteRequest(BaseModel):
    content: str


class WorkflowListEntry(BaseModel):
    name: str
    description: str | None
    # None when this listing did not check — see _MAX_LIST_COMPILES. Absent is
    # not invalid: GET /{name} still answers for the row.
    valid: bool | None
    builtin: bool
    shadows_builtin: bool = False


class WorkflowReadResponse(BaseModel):
    name: str
    description: str | None
    content: str
    builtin: bool


def _script_or_none(value: Any) -> str | None:
    """A corrupt row is reported as invalid here rather than failing the request."""
    try:
        return workflow_script_from_value(value)
    except MalformedWorkflowError:
        return None


async def _describe(content: str, *, expected_name: str) -> tuple[str | None, bool]:
    try:
        meta = await acompile_check(content)
    except WorkflowScriptError:
        return None, False
    if meta.name != expected_name:
        return meta.description, False
    return meta.description, True


def _validate_name(name: str) -> None:
    if not WORKFLOW_NAME_RE.fullmatch(name):
        raise HTTPException(
            status_code=400,
            detail=f"name must match {WORKFLOW_NAME_RE.pattern}",
        )


@router.get("/", response_model=list[WorkflowListEntry])
async def list_workflows(user_id: CurrentUserId) -> list[WorkflowListEntry]:
    store = require_store(setup.store)
    rows, _truncated = await paginate_namespace(
        store,
        workflow_namespace(user_id),
        lambda key, value: (key, value),
    )
    prebuilts = get_prebuilt_workflows()
    builtin_names = set(prebuilts.names())
    entries: dict[str, WorkflowListEntry] = {}
    compiles_left = _MAX_LIST_COMPILES
    for key, value in rows:
        # Skips keys this router could never address again — listing one
        # would only offer a name whose every route 400s.
        name = workflow_name_from_key(key)
        if name is None:
            continue
        content = _script_or_none(value)
        description: str | None = None
        valid: bool | None = False
        if content is not None:
            memoized = compile_is_memoized(content)
            if memoized or compiles_left > 0:
                if not memoized:
                    compiles_left -= 1
                description, valid = await _describe(content, expected_name=name)
            else:
                valid = None
        entries[name] = WorkflowListEntry(
            name=name,
            description=description,
            valid=valid,
            builtin=False,
            shadows_builtin=name in builtin_names,
        )
    for name in prebuilts.names():
        if name in entries:
            continue
        meta = prebuilts.meta(name)
        entries[name] = WorkflowListEntry(
            name=name,
            description=meta.description if meta else None,
            valid=meta is not None,
            builtin=True,
        )
    return [entries[name] for name in sorted(entries)]


@router.get("/{name}", response_model=WorkflowReadResponse)
async def get_workflow(name: str, user_id: CurrentUserId) -> WorkflowReadResponse:
    _validate_name(name)
    store = require_store(setup.store)
    item = await aget(store, workflow_namespace(user_id), workflow_key(name))
    if item is not None:
        content = _script_or_none(item.value)
        if content is None:
            content = ""
        description, _valid = await _describe(content, expected_name=name)
        return WorkflowReadResponse(
            name=name,
            description=description,
            content=content,
            builtin=False,
        )
    prebuilts = get_prebuilt_workflows()
    content = prebuilts.get(name)
    if content is None:
        raise HTTPException(status_code=404, detail="Workflow not found")
    meta = prebuilts.meta(name)
    return WorkflowReadResponse(
        name=name,
        description=meta.description if meta else None,
        content=content,
        builtin=True,
    )


@router.put("/{name}", response_model=WorkflowReadResponse)
async def put_workflow(
    name: str,
    body: WorkflowWriteRequest,
    user_id: CurrentUserId,
) -> WorkflowReadResponse:
    _validate_name(name)
    cap = workflow_script_byte_cap()
    size = len(body.content.encode())
    if size > cap:
        raise HTTPException(
            status_code=400,
            detail=f"workflow script is {size} bytes; max is {cap}",
        )
    try:
        meta = await acompile_check(body.content)
    except WorkflowScriptError as error:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid workflow script: {error}",
        ) from error
    if meta.name != name:
        raise HTTPException(
            status_code=400,
            detail=f"meta.name '{meta.name}' must match workflow name '{name}'",
        )

    store = require_store(setup.store)
    namespace = workflow_namespace(user_id)
    key = workflow_key(name)
    async with lock_for_namespace(namespace):
        existing_item = await aget(store, namespace, key)
        existing = existing_item.value if existing_item else None
        await aput(store, namespace, key, build_workflow_value(body.content, existing))
    return WorkflowReadResponse(
        name=name,
        description=meta.description,
        content=body.content,
        builtin=False,
    )


@router.delete("/{name}", status_code=204)
async def delete_workflow(name: str, user_id: CurrentUserId) -> Response:
    _validate_name(name)
    store = require_store(setup.store)
    namespace = workflow_namespace(user_id)
    key = workflow_key(name)
    async with lock_for_namespace(namespace):
        item = await aget(store, namespace, key)
        if item is None:
            if get_prebuilt_workflows().get(name) is not None:
                raise HTTPException(
                    status_code=404,
                    detail="Pre-built workflows cannot be deleted",
                )
            raise HTTPException(status_code=404, detail="Workflow not found")
        await adelete(store, namespace, key)
    return Response(status_code=204)


def include_workflow_router(app: FastAPI) -> None:
    """Register workflow routes only when orchestration is enabled."""
    if get_workflow_orchestration_config().enabled:
        app.include_router(router)
