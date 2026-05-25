"""Template system REST API.

Endpoints
---------

Public (require ``CurrentUserId``):

  GET  /api/v1/templates
       List all registered template manifests.

  GET  /api/v1/templates/{template_id}
       Get a single template manifest.

  GET  /api/v1/templates/{template_id}/entries
       List user's entries for this template.

  POST /api/v1/templates/{template_id}/entries
       Instantiate: create a new workspace + entry, kick off agent.

  GET  /api/v1/templates/{template_id}/entries/{entry_id}
       Get a single entry (with full payload).

  POST /api/v1/templates/{template_id}/entries/{entry_id}/rerun
       Re-trigger analysis (resets status, reuses workspace).

Internal (sandbox-side scripts; require ``X-Internal-Service-Token`` header
when ``INTERNAL_SERVICE_TOKEN`` env is set):

  POST /api/v1/templates/_internal/entries/{entry_id}/finalize
       Persist analysis result (status, summary, payload).

  POST /api/v1/templates/_internal/entries/{entry_id}/progress
       Update progress + optional status.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from src.server.models.template import (
    TemplateEntryFinalizeRequest,
    TemplateEntryInstantiateRequest,
    TemplateEntryListResponse,
    TemplateEntryProgressUpdate,
    TemplateEntryResponse,
    TemplateEntryStatus,
    TemplateListResponse,
    TemplateManifest,
)
from src.server.services.template_orchestrator import (
    TemplateError,
    TemplateOrchestrator,
)
from src.server.templates import get_template, list_template_manifests
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/templates", tags=["Templates"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry_to_response(entry: dict[str, Any]) -> TemplateEntryResponse:
    params = entry.get("params") or {}
    current_version = params.get("_agent_md_version", "0.0.0")

    # Check if upgradable
    template = get_template(entry["template_id"])
    latest_version = template.manifest.version if template else current_version
    upgradable = current_version < latest_version

    return TemplateEntryResponse(
        entry_id=str(entry["entry_id"]),
        user_id=entry["user_id"],
        template_id=entry["template_id"],
        workspace_id=str(entry["workspace_id"]),
        entry_key=entry["entry_key"],
        display_name=entry.get("display_name"),
        status=TemplateEntryStatus(entry["status"]),
        progress=entry.get("progress") or {},
        summary=entry.get("summary") or {},
        payload=entry.get("payload") or {},
        params=params,
        error_message=entry.get("error_message"),
        upgradable=upgradable,
        current_version=current_version,
        latest_version=latest_version,
        created_at=entry["created_at"],
        updated_at=entry["updated_at"],
        completed_at=entry.get("completed_at"),
    )


def _verify_internal_token(token: str | None) -> None:
    """Verify the internal service token. No-op when env is unset (dev mode)."""
    expected = os.getenv("INTERNAL_SERVICE_TOKEN", "")
    if not expected:
        # Dev mode: accept all calls (sandbox runs locally on the same host).
        return
    if not token or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="Invalid internal service token")


def _collect_release_notes(
    template: Any, from_version: str, to_version: str,
) -> list[dict[str, Any]]:
    """Collect release notes between from_version (exclusive) and to_version (inclusive)."""
    if template is None or not hasattr(template, "release_notes"):
        return []
    notes = []
    for version, note in sorted(template.release_notes.items()):
        if version > from_version and version <= to_version:
            notes.append({"version": version, **note})
    return notes


# ---------------------------------------------------------------------------
# Public — manifests
# ---------------------------------------------------------------------------


@router.get("", response_model=TemplateListResponse)
async def list_templates(_user_id: CurrentUserId) -> TemplateListResponse:
    """List all registered templates."""
    return TemplateListResponse(templates=list_template_manifests())


@router.get("/{template_id}", response_model=TemplateManifest)
async def get_template_manifest(
    template_id: str, _user_id: CurrentUserId
) -> TemplateManifest:
    """Get a single template manifest."""
    template = get_template(template_id)
    if template is None:
        raise HTTPException(status_code=404, detail=f"Template {template_id!r} not found")
    return template.manifest


# ---------------------------------------------------------------------------
# Public — entries
# ---------------------------------------------------------------------------


@router.get("/{template_id}/entries", response_model=TemplateEntryListResponse)
async def list_entries(
    template_id: str,
    user_id: CurrentUserId,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> TemplateEntryListResponse:
    """List user's entries for this template."""
    if get_template(template_id) is None:
        raise HTTPException(status_code=404, detail=f"Template {template_id!r} not found")

    rows, total = await TemplateOrchestrator.get_instance().list_entries(
        template_id=template_id,
        user_id=user_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    return TemplateEntryListResponse(
        entries=[_entry_to_response(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/{template_id}/entries",
    response_model=TemplateEntryResponse,
    status_code=201,
)
async def instantiate(
    template_id: str,
    request: TemplateEntryInstantiateRequest,
    user_id: CurrentUserId,
) -> TemplateEntryResponse:
    """Create a new entry: build workspace, insert row, kick off agent.

    Returns 201 immediately with the entry in 'pending' state. The agent runs
    asynchronously and updates progress / finalizes via the internal endpoints.
    """
    try:
        entry = await TemplateOrchestrator.get_instance().instantiate(
            template_id=template_id,
            user_id=user_id,
            entry_key=request.entry_key,
            display_name=request.display_name,
            params=request.params,
        )
        return _entry_to_response(entry)
    except TemplateError as e:
        # Translate to a sensible HTTP error.
        msg = str(e)
        if "Unknown template" in msg:
            raise HTTPException(status_code=404, detail=msg) from e
        if "duplicate" in msg.lower() or "unique" in msg.lower():
            raise HTTPException(status_code=409, detail=msg) from e
        raise HTTPException(status_code=400, detail=msg) from e
    except Exception as e:
        logger.exception("instantiate failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal error") from e


@router.get(
    "/{template_id}/entries/{entry_id}",
    response_model=TemplateEntryResponse,
)
async def get_entry(
    template_id: str,
    entry_id: str,
    user_id: CurrentUserId,
) -> TemplateEntryResponse:
    """Get a single entry."""
    entry = await TemplateOrchestrator.get_instance().get_entry(entry_id, user_id)
    if entry is None or entry["template_id"] != template_id:
        raise HTTPException(status_code=404, detail="Entry not found")
    return _entry_to_response(entry)


@router.post(
    "/{template_id}/entries/{entry_id}/rerun",
    response_model=TemplateEntryResponse,
)
async def rerun_entry(
    template_id: str,
    entry_id: str,
    user_id: CurrentUserId,
) -> TemplateEntryResponse:
    """Re-trigger analysis for an entry."""
    try:
        entry = await TemplateOrchestrator.get_instance().rerun(entry_id, user_id)
        if entry["template_id"] != template_id:
            raise HTTPException(status_code=400, detail="Template id mismatch")
        return _entry_to_response(entry)
    except TemplateError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post(
    "/{template_id}/entries/{entry_id}/upgrade",
)
async def upgrade_entry_agent_md(
    template_id: str,
    entry_id: str,
    user_id: CurrentUserId,
) -> dict[str, Any]:
    """Upgrade the entry's agent.md to the latest manifest version.

    Returns:
      - entry: updated entry response
      - release_notes: version changelog (summary + changes + suggested_actions)
      - from_version / to_version
    """
    orch = TemplateOrchestrator.get_instance()
    entry_before = await orch.get_entry(entry_id, user_id)
    if entry_before is None:
        raise HTTPException(status_code=404, detail="Entry not found")

    from_version = (entry_before.get("params") or {}).get("_agent_md_version", "0.0.0")

    try:
        entry = await orch.upgrade_agent_md(entry_id, user_id)
    except TemplateError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Collect release notes for all versions between from_version and to_version
    template = get_template(template_id)
    to_version = template.manifest.version if template else from_version
    notes = _collect_release_notes(template, from_version, to_version)

    return {
        "entry": _entry_to_response(entry),
        "from_version": from_version,
        "to_version": to_version,
        "release_notes": notes,
    }


@router.delete(
    "/{template_id}/entries/{entry_id}",
    response_model=dict,
)
async def delete_entry(
    template_id: str,
    entry_id: str,
    user_id: CurrentUserId,
    delete_workspace: bool = Query(
        True,
        description="Also delete the associated workspace (cascade deletes entry via FK).",
    ),
) -> dict:
    """Delete a template entry and optionally its workspace.

    If delete_workspace=True (default), the workspace is hard-deleted and the
    entry is removed via CASCADE FK. If False, only the entry row is removed
    but the workspace persists (orphaned).
    """
    orch = TemplateOrchestrator.get_instance()
    entry = await orch.get_entry(entry_id, user_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Entry not found")
    if entry["template_id"] != template_id:
        raise HTTPException(status_code=400, detail="Template id mismatch")

    if delete_workspace:
        # Full cleanup: session + sandbox files + hard-delete workspace (CASCADE deletes entry)
        from src.server.services.workspace_manager import WorkspaceManager
        from src.server.database.workspace import delete_workspace as db_delete_ws

        workspace_id_str = str(entry["workspace_id"])

        # Step 1: Clean up session + sandbox files (best-effort)
        try:
            manager = WorkspaceManager.get_instance()
            # Remove session from cache + cleanup sandbox runtime (deletes local files)
            session = manager._sessions.pop(workspace_id_str, None)
            if session and session.sandbox:
                await session.sandbox.cleanup()
        except Exception as e:
            logger.warning("Session/sandbox cleanup failed for %s: %s", entry_id, e)

        # Step 2: Hard-delete workspace from DB (CASCADE removes template_entry)
        try:
            await db_delete_ws(workspace_id_str, hard_delete=True)
        except Exception as e:
            logger.warning("Hard-delete workspace failed for %s: %s", entry_id, e)
            # Fallback: at least delete the entry
            from src.server.database import templates as tdb
            await tdb.delete_entry(entry_id)
    else:
        from src.server.database import templates as tdb
        await tdb.delete_entry(entry_id)

    return {"deleted": True, "entry_id": entry_id}


# ---------------------------------------------------------------------------
# Internal — called by sandbox-side persist_entry.py / progress reporter
# ---------------------------------------------------------------------------


@router.post(
    "/_internal/entries/{entry_id}/finalize",
    response_model=TemplateEntryResponse,
)
async def finalize_entry(
    entry_id: str,
    request: TemplateEntryFinalizeRequest,
    x_internal_service_token: str | None = Header(default=None, alias="X-Internal-Service-Token"),
) -> TemplateEntryResponse:
    """Sandbox-side script calls this when analysis is done."""
    _verify_internal_token(x_internal_service_token)

    entry = await TemplateOrchestrator.get_instance().finalize(
        entry_id=entry_id,
        status=request.status.value,
        summary=request.summary,
        payload=request.payload,
        error_message=request.error_message,
    )
    if entry is None:
        raise HTTPException(status_code=404, detail="Entry not found")
    return _entry_to_response(entry)


@router.post(
    "/_internal/entries/{entry_id}/progress",
    response_model=TemplateEntryResponse,
)
async def update_entry_progress(
    entry_id: str,
    request: TemplateEntryProgressUpdate,
    x_internal_service_token: str | None = Header(default=None, alias="X-Internal-Service-Token"),
) -> TemplateEntryResponse:
    """Sandbox-side script calls this to publish per-dimension progress."""
    _verify_internal_token(x_internal_service_token)

    entry = await TemplateOrchestrator.get_instance().update_progress(
        entry_id=entry_id,
        progress=request.progress,
        status=request.status.value if request.status else None,
    )
    if entry is None:
        raise HTTPException(status_code=404, detail="Entry not found")
    return _entry_to_response(entry)
