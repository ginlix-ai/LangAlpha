"""Bot management API.

A bot is an identity over a thread inside one existing workspace; the
workspace's sandbox, files and workspace memory are untouched. The Home bot
(the user's Chief of Staff) is derived and created lazily, so it is readable
here but never created or deleted through this router.

Endpoints:
- GET    /api/v1/bots?workspace_id=  - The caller's bots
- POST   /api/v1/bots                - Create a bot in a workspace
- GET    /api/v1/bots/{bot_id}       - One bot
- PATCH  /api/v1/bots/{bot_id}       - Edit identity/behaviour (versioned)
- DELETE /api/v1/bots/{bot_id}       - Soft-delete (the row stays auditable)
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.server.database import bots as bot_db
from src.server.database.home_workspace import get_home_workspace_id, is_home_id
from src.server.database.workspace import get_workspace
from src.server.dependencies.principal import (
    ReadPrincipal,
    WorkspaceWritePrincipal,
    assert_reaches,
)
from src.server.models.bot import (
    MAX_BOTS_PER_WORKSPACE,
    BotCreate,
    BotDeleteResponse,
    BotInfo,
    BotListResponse,
    BotUpdate,
)
from src.server.utils.api import require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/bots", tags=["Bots"])


def _info(row: dict) -> BotInfo:
    return BotInfo(**row)


async def _owned_bot(user_id: str, bot_id: str) -> dict:
    bot = await bot_db.get_bot_for_user(user_id, bot_id)
    if bot is None:
        raise HTTPException(status_code=404, detail="Bot not found")
    return bot


async def _own_workspace(user_id: str, workspace_id: str) -> dict:
    workspace = await get_workspace(str(workspace_id))
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


async def _reachable_bot(principal, bot_id: str) -> dict:
    """An owned bot the caller's computer also holds; body ids are checked here."""
    bot = await _owned_bot(principal.user_id, bot_id)
    await assert_reaches(principal, workspace_id=bot["workspace_id"])
    return bot


@router.get("", response_model=BotListResponse)
async def list_bots(
    principal: ReadPrincipal,
    workspace_id: Optional[str] = Query(default=None),
) -> BotListResponse:
    """The caller's live bots, narrowed to one workspace when named.

    Unfiltered, the derived Chief of Staff is made first when it does not
    exist yet: it belongs to every user's roster, and Home has no bot until a
    Home turn or this read creates one. A filtered read makes nothing: the
    caller asked for one workspace's roster, not their account's.
    """
    user_id = principal.user_id
    if workspace_id:
        workspace = await _own_workspace(user_id, workspace_id)
        await assert_reaches(
            principal, workspace_id=str(workspace["workspace_id"])
        )
    elif principal.computer_id is None:
        # A computer token's unfiltered read never creates (or reaches) Home:
        # the derived Chief of Staff is the account's, not the machine's.
        await bot_db.ensure_home_bot(user_id)
    rows = await bot_db.list_bots(
        user_id,
        workspace_id=workspace_id,
        computer_id=principal.computer_id,
    )
    return BotListResponse(bots=[_info(r) for r in rows])


@router.post("", response_model=BotInfo, status_code=201)
async def create_bot(principal: WorkspaceWritePrincipal, body: BotCreate) -> BotInfo:
    """Create an analyst bot in one of the caller's workspaces.

    The Home workspace is where the derived Chief of Staff runs, so a second
    hand-made bot there would compete with it; it is refused rather than
    silently allowed.
    """
    user_id = principal.user_id
    workspace = await _own_workspace(user_id, str(body.workspace_id))
    await assert_reaches(principal, workspace_id=str(workspace["workspace_id"]))
    if is_home_id(user_id, workspace["workspace_id"]):
        raise HTTPException(
            status_code=400,
            detail="Home holds the Chief of Staff; create bots in a workspace.",
        )
    if body.model:
        from src.server.services.llm.thread_model import require_selectable_model

        # A model the account cannot run fails every turn of this bot; refuse
        # it at the pick instead, where the user can change it.
        await require_selectable_model(user_id, body.model)
    try:
        row = await bot_db.create_bot(
            user_id,
            str(body.workspace_id),
            body.name.strip(),
            role=body.role,
            persona=body.persona,
            avatar=body.avatar,
            provider=body.provider,
            model=body.model,
            skills=body.skills,
            tools_allow=body.tools_allow,
            tools_deny=body.tools_deny,
            approval=body.approval,
            memory_scopes=body.memory_scopes,
        )
    except bot_db.BotNameTaken:
        raise HTTPException(
            status_code=409,
            detail=f"A bot named {body.name!r} already exists in this workspace.",
        ) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return _info(row)


@router.get("/{bot_id}", response_model=BotInfo)
async def get_bot(principal: ReadPrincipal, bot_id: str) -> BotInfo:
    return _info(await _reachable_bot(principal, bot_id))


@router.patch("/{bot_id}", response_model=BotInfo)
async def update_bot(
    principal: WorkspaceWritePrincipal, bot_id: str, body: BotUpdate
) -> BotInfo:
    """Edit the bot. Versioned fields (persona, model, tool surface…) keep a
    row in ``bot_versions`` so a change is auditable and revertible."""
    user_id = principal.user_id
    current = await _reachable_bot(principal, bot_id)
    changes = body.model_dump(exclude_none=True)
    if current["role"] == "chief_of_staff" and changes.get("status") == "paused":
        raise HTTPException(
            status_code=409,
            detail="The Chief of Staff is derived from Home and cannot be paused.",
        )
    if not changes:
        return _info(current)
    if "name" in changes:
        changes["name"] = changes["name"].strip()
    if changes.get("model"):
        from src.server.services.llm.thread_model import require_selectable_model

        await require_selectable_model(user_id, changes["model"])
    try:
        updated = await bot_db.update_bot(user_id, bot_id, changes)
    except bot_db.BotNameTaken:
        raise HTTPException(
            status_code=409,
            detail=f"A bot named {changes.get('name')!r} already exists "
            "in this workspace.",
        ) from None
    if updated is None:
        raise HTTPException(status_code=404, detail="Bot not found")
    return _info(updated)


@router.delete("/{bot_id}", response_model=BotDeleteResponse)
async def delete_bot(
    principal: WorkspaceWritePrincipal, bot_id: str
) -> BotDeleteResponse:
    """Soft-delete a bot. Its threads stay readable; the row keeps the name
    and is restored by clearing ``deleted_at`` out of band."""
    user_id = principal.user_id
    current = await _reachable_bot(principal, bot_id)
    if current["role"] == "chief_of_staff":
        raise HTTPException(
            status_code=409,
            detail="The Chief of Staff is derived from Home and cannot be deleted.",
        )
    deleted = await bot_db.soft_delete_bot(user_id, bot_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Bot not found")
    return BotDeleteResponse(bot_id=str(current["bot_id"]))


__all__ = [
    "router",
    "MAX_BOTS_PER_WORKSPACE",
    "get_home_workspace_id",
]
