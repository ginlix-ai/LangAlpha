"""Subagents API: the pane's read and write over the user's subagents.

The agent edits the same subagents as files under ``.agents/user/agents/``;
both writers go through ``services.subagents``, so they refuse the same values
and a save from either moves the version the other checks.
"""

import logging

from fastapi import APIRouter, HTTPException, Query, Response

from src.server.models.subagent import SubagentsResponse, SubagentView, SubagentWrite
from src.server.services.subagents import (
    DESCRIPTION_MAX,
    INSTRUCTIONS_MAX_BYTES,
    MAX_CUSTOM,
    SubagentInvalid,
    SubagentNotFound,
    SubagentsUnavailable,
    VersionConflict,
    delete_subagent,
    list_subagents,
    save_subagent,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Subagents"])


def _unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail="Subagents aren't available while the server starts. Try again in a moment.",
        headers={"Retry-After": "2"},
    )


def _conflict() -> HTTPException:
    return HTTPException(status_code=409, detail={"code": "version_conflict"})


@router.get("/subagents", response_model=SubagentsResponse)
@handle_api_exceptions("list subagents", logger)
async def get_subagents(user_id: CurrentUserId):
    try:
        ctx, views = await list_subagents(user_id)
    except SubagentsUnavailable:
        raise _unavailable()
    return {
        "main": ctx.main_values(),
        "subagents": views,
        "connectors": [
            {"name": c.name, "kind": c.kind, "label": c.label, "enabled": c.enabled}
            for c in ctx.connectors
        ],
        "skills": sorted(ctx.skills),
        "brokerages": {"connected": ctx.brokerages_connected},
        "limits": {
            "max_custom": MAX_CUSTOM,
            "description_max": DESCRIPTION_MAX,
            "instructions_max_bytes": INSTRUCTIONS_MAX_BYTES,
        },
    }


@router.put("/subagents/{name:path}", response_model=SubagentView)
@handle_api_exceptions("save subagent", logger)
async def put_subagent(name: str, body: SubagentWrite, user_id: CurrentUserId):
    try:
        return await save_subagent(
            user_id, name, body.fields, version=body.version, start_from=body.start_from
        )
    except SubagentInvalid as exc:
        raise HTTPException(
            status_code=422, detail={"errors": [e.as_dict() for e in exc.errors]}
        )
    except VersionConflict:
        raise _conflict()
    except SubagentsUnavailable:
        raise _unavailable()


@router.delete("/subagents/{name:path}", status_code=204)
@handle_api_exceptions("delete subagent", logger)
async def remove_subagent(
    name: str, user_id: CurrentUserId, version: str = Query(...)
):
    """A custom subagent is deleted; a base loses the user's changes (the reset)."""
    try:
        await delete_subagent(user_id, name, version=version)
    except SubagentNotFound:
        raise HTTPException(status_code=404, detail=f"No subagent named {name}.")
    except VersionConflict:
        raise _conflict()
    except SubagentsUnavailable:
        raise _unavailable()
    return Response(status_code=204)
