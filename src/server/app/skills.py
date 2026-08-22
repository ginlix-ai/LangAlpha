"""Skills API — the merged platform + user + workspace tiers.

Anonymous callers keep the original platform-only listing (auth is optional,
not required, so no existing caller breaks); an identified caller gets the
merged view plus CRUD over their own tier. Builtin skills can be disabled per
user (stored in preferences) but never deleted; user and workspace skills are
full CRUD backed by ``user_skills`` rows + archive storage.

The workspace tier mirrors workspace MCP servers: a second router under
``/api/v1/workspaces/{id}/skills`` manages rows scoped to one workspace,
which shadow same-named user skills there; inherited skills (platform + user
tier) can be disabled per workspace but not deleted. The main ``GET`` accepts
``workspace_id`` to return the workspace-effective merged view — that is what
the slash-command menu inside a workspace reads.

The default ``GET`` returns enabled rows only — it feeds the slash-command
menu. The management surface passes ``include_disabled=true`` to render
re-enable toggles.
"""

import asyncio
import io
import logging
import re
import zipfile
from typing import Literal, Optional

from fastapi import APIRouter, File, HTTPException, Query, Response, UploadFile
from pydantic import BaseModel, Field

from ptc_agent.agent.middleware.skills import (
    SKILL_REGISTRY,
    SkillMode,
    build_effective_skill_registry,
    list_skills,
    load_skill_content,
)
from src.server.database.user_skills import (
    delete_user_skill,
    get_user_skill,
    list_all_user_skills,
    list_enabled_user_skills,
    list_skill_disables_for_user,
    list_user_skills,
    list_workspace_skill_disables,
    move_user_skill,
    set_user_skill_command,
    set_user_skill_enabled,
    set_workspace_skill_disable,
    upsert_user_skill,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.services import skill_archive_storage
from src.server.services.features import (
    get_disabled_builtin_skills,
    get_skill_command_overrides,
    set_builtin_skill_disabled,
)
from src.server.services.user_skills import (
    SkillValidationError,
    drop_archive_if_unused,
    fetch_skill_archive,
    reserved_skill_names,
    valid_command,
    validate_skill_archive,
)
from src.server.services.user_skills.commands import (
    effective_trigger,
    ensure_free_of_platform,
    set_platform_alias,
    upload_seed,
)
from src.server.services.user_skills.limits import (
    MAX_SKILL_ARCHIVE_BYTES,
    MAX_SKILL_INLINE_BLOB_BYTES,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import (
    CurrentUserId,
    OptionalUserId,
    handle_api_exceptions,
    require_workspace_owner,
)
from src.server.utils.uploads import read_capped

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/skills", tags=["Skills"])
workspace_router = APIRouter(prefix="/api/v1/workspaces", tags=["Skills"])

# The Agent Skills name charset; also keeps {name} path params away from any
# DB or filesystem use before validation.
_NAME_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


def _validate_name_param(name: str) -> None:
    if len(name) > 64 or not _NAME_RE.match(name):
        raise HTTPException(status_code=404, detail="Skill not found")


class SkillInfo(BaseModel):
    name: str
    description: str
    tool_count: int
    tools: list[str] = Field(default_factory=list)
    command: str | None = None
    origin: Literal["platform", "user", "workspace"] = "platform"
    enabled: bool = True
    editable: bool = False
    deletable: bool = False
    confirmed: bool = True
    plugin_id: str | None = None
    # Display-only provenance: the owning plugin's name and its enable state
    # (None on hand-made or detached rows). plugin_enabled=False explains a
    # row the delivery predicate is suppressing.
    plugin_name: str | None = None
    plugin_enabled: bool | None = None
    size_bytes: int = 0
    updated_at: str | None = None
    # Which tier switched an inherited skill off (workspace views only).
    disabled_scope: Literal["user", "workspace"] | None = None
    # Workspace row reusing (and thereby hiding) a user-tier name here.
    shadows_inherited: bool = False
    # The scope a workspace-tier row belongs to (None = user/platform tier).
    workspace_id: str | None = None
    # Workspaces where an all-workspaces skill is switched off (deny-list) —
    # populated in the all-scopes view only, for the "active in" checklist.
    disabled_workspace_ids: list[str] = Field(default_factory=list)


class SkillsResponse(BaseModel):
    skills: list[SkillInfo]


class SkillEnabledInput(BaseModel):
    """PATCH body; both fields optional so one call can toggle, re-alias, or
    both. ``command: null`` (sent explicitly) clears the alias back to the
    name — ``model_fields_set`` is what tells that apart from absent."""

    enabled: bool | None = None
    command: str | None = None


class SkillContentResponse(BaseModel):
    name: str
    content: str


def _validated_patch_fields(body: SkillEnabledInput) -> set[str]:
    """400 on an empty PATCH; 422 on an explicit ``enabled: null`` (only
    ``command`` is nullable — null means "back to the name")."""
    fields = body.model_fields_set & {"enabled", "command"}
    if not fields:
        raise HTTPException(status_code=400, detail="Nothing to update")
    if "enabled" in fields and body.enabled is None:
        raise HTTPException(status_code=422, detail="enabled must be true or false")
    return fields


def _user_row_to_info(
    row: dict, *, editable: bool = True, deletable: bool = True
) -> SkillInfo:
    """``editable``/``deletable`` mean "through the surface returning this row"
    — a user-tier skill listed in a workspace view is managed elsewhere."""
    return SkillInfo(
        name=row["name"],
        description=row["description"],
        tool_count=0,
        tools=[],
        command=effective_trigger(row),
        origin="workspace" if row.get("workspace_id") else "user",
        enabled=bool(row["enabled"]),
        editable=editable,
        deletable=deletable,
        confirmed=bool(row["confirmed"]),
        # Indexed, not .get(): every read joins these and every writer
        # subselects them, so an absent key means a new writer skipped the
        # projection. A .get() default would answer "no plugin" to that and
        # quietly drop the badge and the suppressed state instead of failing.
        plugin_id=row["plugin_id"],
        plugin_name=row["plugin_name"],
        plugin_enabled=(
            bool(row["plugin_enabled"])
            if row["plugin_enabled"] is not None
            else None
        ),
        size_bytes=int(row.get("archive_bytes") or 0),
        updated_at=row.get("updated_at"),
        workspace_id=row.get("workspace_id"),
    )


def _platform_info(
    entry: dict, *, enabled: bool = True, command_override: str | None = None
) -> SkillInfo:
    info = SkillInfo(**entry, enabled=enabled)
    if command_override:
        info.command = command_override
    return info


def _builtin_info(
    skill,
    *,
    enabled: bool,
    overrides: dict[str, str],
    disabled_scope: Literal["user", "workspace"] | None = None,
) -> SkillInfo:
    """Response for a registry-backed (platform) skill, alias applied."""
    return SkillInfo(
        name=skill.name,
        description=skill.description,
        tool_count=len(skill.get_tool_names()),
        tools=skill.get_tool_names(),
        command=overrides.get(skill.name) or skill.command,
        enabled=enabled,
        disabled_scope=disabled_scope,
    )


async def _require_owned_workspace(workspace_id: str, user_id: str) -> None:
    require_workspace_owner(await db_get_workspace(workspace_id), user_id=user_id)


async def _assemble_skills(
    user_id: str,
    mode: SkillMode | None,
    include_disabled: bool,
    workspace_id: str | None,
) -> dict:
    """The merged listing for one scope: platform + user tier, plus — inside a
    workspace — that workspace's rows shadowing same-named user skills and its
    disables of inherited ones."""
    platform = list_skills(mode=mode)
    disabled_builtins = await get_disabled_builtin_skills(user_id)
    overrides = await get_skill_command_overrides(user_id)
    ws_disabled: set[str] = (
        await list_workspace_skill_disables(workspace_id) if workspace_id else set()
    )

    skills: list[SkillInfo] = []
    for entry in platform:
        user_dis = entry["name"] in disabled_builtins
        ws_dis = entry["name"] in ws_disabled
        override = overrides.get(entry["name"])
        if not (user_dis or ws_dis):
            skills.append(_platform_info(entry, command_override=override))
        elif include_disabled:
            info = _platform_info(entry, enabled=False, command_override=override)
            # disabled_scope is a workspace-view annotation only: it tells
            # that surface which disables it cannot undo. The user view can
            # undo its own disables, so it stays unset there.
            if workspace_id is not None:
                info.disabled_scope = "user" if user_dis else "workspace"
            skills.append(info)

    user_rows = (
        await list_user_skills(user_id)
        if include_disabled
        else await list_enabled_user_skills(user_id)
    )
    ws_rows: list[dict] = []
    # Shadowing is by name regardless of enabled state (same rule as the
    # delivery bundle), so ws_names comes from the unfiltered rows and the
    # enabled filter only decides what this listing emits.
    ws_names: set[str] = set()
    if workspace_id:
        ws_rows = await list_user_skills(user_id, workspace_id=workspace_id)
        ws_names = {r["name"] for r in ws_rows}
        if not include_disabled:
            ws_rows = [r for r in ws_rows if r["enabled"]]
    user_names = {r["name"] for r in user_rows}

    for r in user_rows:
        if workspace_id is None:
            skills.append(_user_row_to_info(r))
            continue
        if r["name"] in ws_names:
            # Shadowed — the workspace row below represents this name.
            continue
        row_disabled = not r["enabled"]
        ws_dis = r["name"] in ws_disabled
        if (row_disabled or ws_dis) and not include_disabled:
            continue
        info = _user_row_to_info(r, editable=False, deletable=False)
        if row_disabled:
            # A user-level disable is not workspace-reversible (mirrors the
            # MCP builtin-disable asymmetry) — surfaced so the UI can say why.
            info.disabled_scope = "user"
        elif ws_dis:
            info.enabled = False
            info.disabled_scope = "workspace"
        skills.append(info)

    for r in ws_rows:
        info = _user_row_to_info(r)
        info.shadows_inherited = r["name"] in user_names
        skills.append(info)

    return {"skills": skills}


async def _assemble_all_scopes(
    user_id: str, mode: SkillMode | None, include_disabled: bool
) -> dict:
    """Every scope at once — the Plugins page's scope-management inventory.

    No shadowing or workspace-disable filtering here: each row appears exactly
    once, tagged with its scope (``workspace_id``) and, for all-workspaces
    entries, the deny-list of workspaces that switched it off.
    """
    disabled_builtins = await get_disabled_builtin_skills(user_id)
    overrides = await get_skill_command_overrides(user_id)
    disables_by_name: dict[str, list[str]] = {}
    for d in await list_skill_disables_for_user(user_id):
        disables_by_name.setdefault(d["name"], []).append(d["workspace_id"])

    skills: list[SkillInfo] = []
    for entry in list_skills(mode=mode):
        user_dis = entry["name"] in disabled_builtins
        if user_dis and not include_disabled:
            continue
        info = _platform_info(
            entry,
            enabled=not user_dis,
            command_override=overrides.get(entry["name"]),
        )
        info.disabled_workspace_ids = sorted(
            disables_by_name.get(entry["name"], [])
        )
        skills.append(info)

    rows = await list_all_user_skills(user_id)
    user_names = {r["name"] for r in rows if not r.get("workspace_id")}
    for r in rows:
        if not r["enabled"] and not include_disabled:
            continue
        info = _user_row_to_info(r)
        if r.get("workspace_id"):
            info.shadows_inherited = r["name"] in user_names
        else:
            info.disabled_workspace_ids = sorted(
                disables_by_name.get(r["name"], [])
            )
        skills.append(info)
    return {"skills": skills}


@router.get("", response_model=SkillsResponse)
@handle_api_exceptions("list skills", logger)
async def get_skills(
    user_id: OptionalUserId,
    mode: Optional[SkillMode] = Query(
        None, description="Filter by agent mode: ptc or flash"
    ),
    include_disabled: bool = Query(
        False,
        description="Include disabled entries (management view); the default "
        "enabled-only response feeds the slash-command menu.",
    ),
    workspace_id: Optional[str] = Query(
        None,
        description="Return the workspace-effective view (workspace rows "
        "shadow user rows, workspace disables apply). Requires auth and "
        "workspace ownership.",
    ),
    all_scopes: bool = Query(
        False,
        description="Return every scope at once (user tier plus every "
        "workspace's rows, unfiltered) for scope management. Requires auth; "
        "mutually exclusive with workspace_id.",
    ),
):
    """List skills: platform tier always, plus the caller's own tiers."""
    if all_scopes:
        if user_id is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        if workspace_id is not None:
            raise HTTPException(
                status_code=400,
                detail="all_scopes and workspace_id are mutually exclusive",
            )
        return await _assemble_all_scopes(user_id, mode, include_disabled)
    if workspace_id is not None:
        if user_id is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        await _require_owned_workspace(workspace_id, user_id)
    if user_id is None:
        return {"skills": [_platform_info(s) for s in list_skills(mode=mode)]}
    return await _assemble_skills(user_id, mode, include_disabled, workspace_id)


async def _upload_skill_archive(
    user_id: str, file: UploadFile, *, workspace_id: str | None = None
) -> SkillInfo:
    """Shared upload pipeline for both scopes.

    Re-uploading an existing name replaces it in place (within its scope).
    Mirrors the memo upload's phase ordering: the slow object PUT happens
    before the DB write; on DB failure the object is deleted; a replaced
    row's superseded object is deleted after commit (unless another
    same-content row still references it).
    """
    raw = await read_capped(file, MAX_SKILL_ARCHIVE_BYTES)
    try:
        # Unzip + re-zip + hash over as much as 8 MB: off the event loop.
        validated = await asyncio.to_thread(validate_skill_archive, raw)
    except SkillValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # The name is itself a live trigger (effective_trigger falls back to it),
    # so it has to clear the platform tier the same way an alias does — a
    # builtin the user renamed to this name would otherwise be shadowed by
    # the upload. Renaming onto an existing row is already blocked in
    # set_platform_alias; this is the same rule in the other order.
    try:
        await ensure_free_of_platform(user_id, validated.name)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    command_seed = await upload_seed(user_id, validated, workspace_id)

    archive_key: str | None = None
    archive_blob: bytes | None = None
    if skill_archive_storage.is_configured():
        try:
            archive_key = await skill_archive_storage.store_archive(
                user_id=user_id,
                content=validated.canonical_zip,
                content_hash=validated.content_hash,
            )
        # Base class, not just the upload subclass: a key the adapter refuses
        # to build is still a storage failure the caller should see as one,
        # and the reconciler's copy of this call already catches the base.
        except skill_archive_storage.SkillArchiveStorageError as exc:
            raise HTTPException(
                status_code=502,
                detail="Could not store the skill archive — please retry.",
            ) from exc
    else:
        if len(validated.canonical_zip) > MAX_SKILL_INLINE_BLOB_BYTES:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Object storage is not configured on this deployment; "
                    f"skill archives are limited to {MAX_SKILL_INLINE_BLOB_BYTES} "
                    "bytes"
                ),
            )
        archive_blob = validated.canonical_zip

    try:
        row, superseded_key = await upsert_user_skill(
            user_id,
            validated.name,
            description=validated.description,
            license=validated.license,
            frontmatter=validated.frontmatter,
            allowed_tools=validated.allowed_tools,
            confirmed=True,
            content_hash=validated.content_hash,
            archive_key=archive_key,
            archive_blob=archive_blob,
            archive_bytes=len(validated.canonical_zip),
            file_count=validated.file_count,
            workspace_id=workspace_id,
            command=command_seed,
        )
    except BaseException:
        await drop_archive_if_unused(user_id, archive_key)
        raise

    await drop_archive_if_unused(user_id, superseded_key)
    return _user_row_to_info(row)


@router.post("", response_model=SkillInfo, status_code=201)
@handle_api_exceptions("upload skill", logger, conflict_on_value_error=True)
async def upload_skill(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
):
    """Upload a user-tier skill zip (SKILL.md at the root or in a single
    top-level dir); visible in every workspace."""
    return await _upload_skill_archive(user_id, file)


def _normalize_command_input(raw: str | None) -> str | None:
    """A user-typed trigger: leading slash tolerated, empty clears the alias
    (falls back to the skill name), anything else must pass the charset."""
    if raw is None:
        return None
    command = raw.strip().lstrip("/").strip()
    if not command:
        return None
    if not valid_command(command):
        raise HTTPException(
            status_code=422,
            detail=(
                "Commands are lowercase letters, digits and hyphens, "
                "64 characters max"
            ),
        )
    return command


async def _apply_platform_command_edit(
    user_id: str, skill, command: str | None
) -> SkillInfo:
    """Rename a builtin's trigger; collision policy lives in
    :func:`set_platform_alias`."""
    try:
        overrides = await set_platform_alias(
            user_id, skill.name, skill.command, command
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    disabled = await get_disabled_builtin_skills(user_id)
    return _builtin_info(
        skill, enabled=skill.name not in disabled, overrides=overrides
    )


async def _apply_user_command_edit(
    user_id: str, name: str, raw: str | None
) -> SkillInfo:
    command = _normalize_command_input(raw)
    if command == name:
        command = None

    # A registry name is definitively the platform tier: user skill names can
    # never equal builtin names (reserved at upload).
    skill = SKILL_REGISTRY.get(name)
    if skill is not None:
        return await _apply_platform_command_edit(user_id, skill, command)

    try:
        if command is not None:
            await ensure_free_of_platform(user_id, command)
        row = await set_user_skill_command(user_id, name, command)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if row is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    return _user_row_to_info(row)


async def _apply_workspace_command_edit(
    user_id: str, workspace_id: str, name: str, raw: str | None
) -> SkillInfo:
    command = _normalize_command_input(raw)
    if command == name:
        command = None
    try:
        if command is not None:
            await ensure_free_of_platform(user_id, command)
        row = await set_user_skill_command(
            user_id, name, command, workspace_id=workspace_id
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if row is not None:
        return _user_row_to_info(row)
    if name in SKILL_REGISTRY or await get_user_skill(user_id, name) is not None:
        raise HTTPException(
            status_code=409,
            detail="This skill is inherited; rename its command in Plugins",
        )
    raise HTTPException(status_code=404, detail="Skill not found")


@router.patch("/{name}", response_model=SkillInfo)
@handle_api_exceptions("update skill", logger)
async def patch_skill(
    name: str,
    body: SkillEnabledInput,
    user_id: CurrentUserId,
):
    """Enable/disable and/or re-alias a skill; dispatches on tier by name.

    A user-skill name updates its row; a builtin name writes the per-user
    disable or command override in preferences. Builtin changes take effect
    on the next agent build and the next sandbox sync.
    """
    _validate_name_param(name)
    fields = _validated_patch_fields(body)

    info: SkillInfo | None = None
    if "command" in fields:
        info = await _apply_user_command_edit(user_id, name, body.command)
    if "enabled" not in fields:
        return info

    row = await set_user_skill_enabled(user_id, name, body.enabled)
    if row is not None:
        return _user_row_to_info(row)

    skill = SKILL_REGISTRY.get(name)
    if skill is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    disabled = await set_builtin_skill_disabled(user_id, name, disabled=not body.enabled)
    overrides = await get_skill_command_overrides(user_id)
    return _builtin_info(skill, enabled=name not in disabled, overrides=overrides)


class SkillMoveInput(BaseModel):
    """Both scopes are explicit: names are only unique within one scope, so
    the source disambiguates which row moves."""

    from_workspace_id: str | None = None
    to_workspace_id: str | None = None


@router.post("/{name}/move", response_model=SkillInfo)
@handle_api_exceptions("move skill", logger)
async def move_skill(name: str, body: SkillMoveInput, user_id: CurrentUserId):
    """Re-scope a skill: user tier (every workspace) ↔ one workspace.

    The row moves in place, carrying its archive and enabled flag. Plugin
    provenance does not travel: a plugin-owned row cannot move into a
    workspace at all (409 — the plugin manages it at the account level), and
    one moving back up is detached. 409 too when the destination scope
    already has the name, since shadowing is created by uploading a workspace
    copy, never implicitly by a move. Platform skills have no row and cannot
    move.
    """
    _validate_name_param(name)
    if body.from_workspace_id == body.to_workspace_id:
        raise HTTPException(
            status_code=400, detail="The skill is already in that scope"
        )
    for ws in (body.from_workspace_id, body.to_workspace_id):
        if ws is not None:
            await _require_owned_workspace(ws, user_id)
    try:
        row = await move_user_skill(
            user_id,
            name,
            from_workspace_id=body.from_workspace_id,
            to_workspace_id=body.to_workspace_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if row is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    for ws in (body.from_workspace_id, body.to_workspace_id):
        if ws is not None:
            WorkspaceManager.schedule_skill_reconcile(ws, user_id, source="ws_move")
    return _user_row_to_info(row)


@router.delete("/{name}", status_code=204)
@handle_api_exceptions("delete skill", logger)
async def delete_skill(name: str, user_id: CurrentUserId):
    """Delete a user skill. Builtins can be disabled, not deleted."""
    _validate_name_param(name)
    if name in reserved_skill_names():
        raise HTTPException(
            status_code=409,
            detail="Built-in skills can be disabled, not deleted",
        )
    row = await delete_user_skill(user_id, name)
    if row is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    await drop_archive_if_unused(user_id, row.get("archive_key"))
    return Response(status_code=204)


@router.get("/{name}/content", response_model=SkillContentResponse)
@handle_api_exceptions("read skill content", logger)
async def get_skill_content(
    name: str,
    user_id: CurrentUserId,
    workspace_id: Optional[str] = Query(
        None, description="Prefer this workspace's row over the user tier."
    ),
):
    """The SKILL.md text, most specific tier first (workspace → user →
    platform; reserved names keep the platform tier collision-free, so the
    tier walk is belt-and-braces)."""
    _validate_name_param(name)
    row = None
    if workspace_id is not None:
        await _require_owned_workspace(workspace_id, user_id)
        row = await get_user_skill(user_id, name, workspace_id=workspace_id)
    if row is None:
        row = await get_user_skill(user_id, name)
    if row is not None:
        data = await fetch_skill_archive(user_id, row)
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                content = zf.read(f"{name}/SKILL.md").decode("utf-8")
        except (KeyError, zipfile.BadZipFile) as exc:
            logger.exception(
                "stored skill archive unreadable (user=%s name=%s)",
                user_id,
                name,
            )
            raise HTTPException(
                status_code=502, detail="Stored skill archive is unreadable"
            ) from exc
        return {"name": name, "content": content}

    # Platform tier: resolve through the effective registry so a hidden skill,
    # or one the account has disabled, is not readable here. Deliberately the
    # account tier only, matching the row branch above: this reads a skill's
    # source for management, and a workspace deny-list scopes where a skill
    # runs, not whether its owner may look at it.
    registry = build_effective_skill_registry(
        None, disabled_skills=await get_disabled_builtin_skills(user_id)
    )
    skill = registry.get(name)
    if skill is None or skill.exposure == "hidden":
        raise HTTPException(status_code=404, detail="Skill not found")
    content = load_skill_content(name, registry=registry)
    if content is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    return {"name": name, "content": content}


# ---------------------------------------------------------------------------
# Workspace tier
# ---------------------------------------------------------------------------

_USER_LEVEL_DISABLED = (
    "This skill is disabled at the user level; enable it in Plugins first"
)
_INHERITED_DELETE = (
    "This skill is inherited from your Plugins. Delete it there, or "
    "disable it for this workspace."
)


# The workspace-effective *listing* is `GET /api/v1/skills?workspace_id=` —
# one merged view across all three tiers, so it belongs on the merged endpoint.
# The routes below own what is genuinely workspace-scoped: this workspace's own
# rows, and its disables of inherited ones.


@workspace_router.post(
    "/{workspace_id}/skills", response_model=SkillInfo, status_code=201
)
@handle_api_exceptions("upload workspace skill", logger, conflict_on_value_error=True)
async def upload_workspace_skill(
    workspace_id: str,
    user_id: CurrentUserId,
    file: UploadFile = File(...),
):
    """Upload a skill zip scoped to this workspace; it shadows a same-named
    user-tier skill here. Platform names stay reserved in both scopes."""
    await _require_owned_workspace(workspace_id, user_id)
    info = await _upload_skill_archive(user_id, file, workspace_id=workspace_id)
    WorkspaceManager.schedule_skill_reconcile(workspace_id, user_id, source="ws_upload")
    return info


@workspace_router.patch("/{workspace_id}/skills/{name}", response_model=SkillInfo)
@handle_api_exceptions("update workspace skill", logger)
async def patch_workspace_skill(
    workspace_id: str,
    name: str,
    body: SkillEnabledInput,
    user_id: CurrentUserId,
):
    """Enable/disable or re-alias a skill within one workspace.

    A workspace row updates its own flags; an inherited name (platform or
    user tier) can only be workspace-disabled here, never renamed. A
    user-level disable is not workspace-reversible — mirrors the MCP
    builtin-disable asymmetry.
    """
    _validate_name_param(name)
    await _require_owned_workspace(workspace_id, user_id)
    fields = _validated_patch_fields(body)

    info: SkillInfo | None = None
    if "command" in fields:
        info = await _apply_workspace_command_edit(
            user_id, workspace_id, name, body.command
        )
    if "enabled" not in fields:
        return info

    row = await set_user_skill_enabled(
        user_id, name, body.enabled, workspace_id=workspace_id
    )
    if row is not None:
        # A workspace row's dir is the reconciler's alone to write or remove —
        # the prune path preserves linked names on purpose — so without this a
        # skill the user just disabled keeps its SKILL.md on disk and stays
        # readable for the whole of the next turn. Same call the upload and
        # delete paths make, and best-effort in the same way.
        WorkspaceManager.schedule_skill_reconcile(
            workspace_id, user_id, source="ws_toggle"
        )
        return _user_row_to_info(row)

    user_row = await get_user_skill(user_id, name)
    if user_row is not None:
        if not user_row["enabled"] and body.enabled:
            raise HTTPException(status_code=409, detail=_USER_LEVEL_DISABLED)
        await set_workspace_skill_disable(workspace_id, name, not body.enabled)
        info = _user_row_to_info(user_row, editable=False, deletable=False)
        if not user_row["enabled"]:
            info.disabled_scope = "user"
        elif not body.enabled:
            info.enabled = False
            info.disabled_scope = "workspace"
        return info

    skill = SKILL_REGISTRY.get(name)
    if skill is None:
        raise HTTPException(status_code=404, detail="Skill not found")
    user_disabled = name in await get_disabled_builtin_skills(user_id)
    if user_disabled and body.enabled:
        raise HTTPException(status_code=409, detail=_USER_LEVEL_DISABLED)
    await set_workspace_skill_disable(workspace_id, name, not body.enabled)
    overrides = await get_skill_command_overrides(user_id)
    return _builtin_info(
        skill,
        enabled=body.enabled,
        overrides=overrides,
        disabled_scope=None if body.enabled else "workspace",
    )


@workspace_router.delete("/{workspace_id}/skills/{name}", status_code=204)
@handle_api_exceptions("delete workspace skill", logger)
async def delete_workspace_skill(
    workspace_id: str, name: str, user_id: CurrentUserId
):
    """Delete a workspace-scoped skill. Inherited skills (platform or user
    tier) can only be disabled for the workspace, not deleted through it."""
    _validate_name_param(name)
    await _require_owned_workspace(workspace_id, user_id)
    row = await delete_user_skill(user_id, name, workspace_id=workspace_id)
    if row is None:
        if name in reserved_skill_names():
            raise HTTPException(
                status_code=409,
                detail="Built-in skills can be disabled, not deleted",
            )
        if await get_user_skill(user_id, name) is not None:
            raise HTTPException(status_code=409, detail=_INHERITED_DELETE)
        raise HTTPException(status_code=404, detail="Skill not found")
    await drop_archive_if_unused(user_id, row.get("archive_key"))
    WorkspaceManager.schedule_skill_reconcile(workspace_id, user_id, source="ws_delete")
    return Response(status_code=204)
