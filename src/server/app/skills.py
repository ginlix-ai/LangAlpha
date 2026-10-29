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
import logging
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Literal, Optional

from fastapi import APIRouter, File, HTTPException, Query, Response, UploadFile

from ptc_agent.agent.middleware.skills import (
    SKILL_REGISTRY,
    build_effective_skill_registry,
    list_skills,
    resolve_skill_dir,
)
from src.server.database.account_disables import list_account_disables
from src.server.database.user_skills import (
    MAX_SKILL_TOTAL_BYTES_PER_USER,
    MAX_SKILLS_PER_USER,
    SkillCapReached,
    SkillTriggerTaken,
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
from src.server.models.skill import (
    SkillContentResponse,
    SkillEnabledInput,
    SkillInfo,
    SkillLimitsResponse,
    SkillMoveInput,
    SkillsResponse,
)
from src.server.services import skill_archive_storage
from src.server.services.features import (
    get_disabled_builtin_skills,
    get_skill_command_overrides,
    set_builtin_skill_disabled,
)
from src.server.services.user_skills import (
    SkillNamesUnavailable,
    SkillValidationError,
    configured_skill_dirs,
    drop_archive_if_unused,
    fetch_skill_archive,
    reserved_skill_names,
    valid_command,
    validate_skill_archive,
)
from src.server.services.user_skills.browse import (
    DiskFolder,
    SkillFolder,
    ZipFolder,
    skill_content_response,
)
from src.server.services.user_skills.commands import (
    effective_trigger,
    ensure_free_of_platform,
    set_platform_alias,
    upload_seed,
)
from src.server.services.user_skills.limits import (
    MAX_SKILL_ARCHIVE_BYTES,
    MAX_SKILL_FILES,
    MAX_SKILL_INLINE_BLOB_BYTES,
    MAX_SKILL_MD_BYTES,
    MAX_SKILL_SINGLE_FILE_BYTES,
    MAX_SKILL_UNCOMPRESSED_BYTES,
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


def _refused(status: int, code: str, message: str, **params: object) -> HTTPException:
    """A refusal a client explains in its own words: ``code`` says which one,
    ``params`` fill its sentence, and ``message`` serves any client that does
    not know the code."""
    return HTTPException(
        status_code=status, detail={"code": code, "message": message, **params}
    )


def _conflict(e: SkillTriggerTaken | SkillCapReached) -> HTTPException:
    if isinstance(e, SkillTriggerTaken):
        return _refused(409, e.code, str(e), **e.params)
    if e.limit == "count":
        return _refused(409, "skill_count_cap", str(e), max=MAX_SKILLS_PER_USER)
    return _refused(
        409, "skill_storage_cap", str(e), max=MAX_SKILL_TOTAL_BYTES_PER_USER
    )


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
    — a user-tier skill listed in a workspace view is managed elsewhere, and a
    plugin's skill goes with its plugin."""
    return SkillInfo(
        name=row["name"],
        description=row["description"],
        tool_count=0,
        tools=[],
        command=effective_trigger(row),
        origin="workspace" if row.get("workspace_id") else "user",
        enabled=bool(row["enabled"]),
        editable=editable,
        deletable=deletable and not row["plugin_id"],
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


@dataclass(frozen=True)
class _BundleState:
    """Which bundle ships each platform skill, and whether it is switched on.

    Read once per listing rather than per row: both halves are one directory
    read and one query, and every platform skill asks the same question. A
    skill no bundle claims answers ``(None, None)`` — the same "no provenance"
    a hand-uploaded row carries.
    """

    owners: Mapping[str, str]
    disabled: Collection[str]

    def of(self, name: str) -> tuple[str | None, bool | None]:
        owner = self.owners.get(name)
        if owner is None:
            return None, None
        return owner, owner not in self.disabled


async def _bundle_state(user_id: str) -> _BundleState:
    from src.server.services.plugins.bundled import component_owners

    return _BundleState(
        owners=component_owners().skills,
        disabled=(await list_account_disables(user_id)).bundles,
    )


def _platform_info(
    entry: dict,
    *,
    enabled: bool = True,
    command_override: str | None = None,
    bundles: _BundleState | None = None,
) -> SkillInfo:
    info = SkillInfo(**entry, enabled=enabled)
    if command_override:
        info.command = command_override
    if bundles is not None:
        info.plugin_name, info.plugin_enabled = bundles.of(info.name)
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
        tool_count=len(skill.listed_tool_names()),
        tools=skill.listed_tool_names(),
        command=overrides.get(skill.name) or skill.command,
        enabled=enabled,
        disabled_scope=disabled_scope,
    )


async def _require_owned_workspace(workspace_id: str, user_id: str) -> None:
    require_workspace_owner(await db_get_workspace(workspace_id), user_id=user_id)


async def _assemble_skills(
    user_id: str,
    include_disabled: bool,
    workspace_id: str | None,
) -> dict:
    """The merged listing for one scope: platform + user tier, plus — inside a
    workspace — that workspace's rows shadowing same-named user skills and its
    disables of inherited ones."""
    platform = list_skills()
    disabled_builtins = await get_disabled_builtin_skills(user_id)
    overrides = await get_skill_command_overrides(user_id)
    bundles = await _bundle_state(user_id)
    ws_disabled: set[str] = (
        await list_workspace_skill_disables(workspace_id) if workspace_id else set()
    )

    skills: list[SkillInfo] = []
    for entry in platform:
        user_dis = entry["name"] in disabled_builtins
        ws_dis = entry["name"] in ws_disabled
        override = overrides.get(entry["name"])
        if not (user_dis or ws_dis):
            info = _platform_info(entry, command_override=override, bundles=bundles)
            # The enabled-only response is the slash-command menu, and
            # materialize already subtracts a switched-off bundle's skills from
            # what the agent loads, so advertising one here offers a command
            # that cannot run. The management view keeps the row: the skill's
            # own switch really is on, and ``plugin_enabled`` is what says why
            # it is not in effect.
            if include_disabled or info.plugin_enabled is not False:
                skills.append(info)
        elif include_disabled:
            info = _platform_info(
                entry, enabled=False, command_override=override, bundles=bundles
            )
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


async def _assemble_all_scopes(user_id: str, include_disabled: bool) -> dict:
    """Every scope at once — the Plugins page's scope-management inventory.

    No shadowing or workspace-disable filtering here: each row appears exactly
    once, tagged with its scope (``workspace_id``) and, for all-workspaces
    entries, the deny-list of workspaces that switched it off.
    """
    disabled_builtins = await get_disabled_builtin_skills(user_id)
    overrides = await get_skill_command_overrides(user_id)
    bundles = await _bundle_state(user_id)
    disables_by_name: dict[str, list[str]] = {}
    for d in await list_skill_disables_for_user(user_id):
        disables_by_name.setdefault(d["name"], []).append(d["workspace_id"])

    skills: list[SkillInfo] = []
    for entry in list_skills():
        user_dis = entry["name"] in disabled_builtins
        if user_dis and not include_disabled:
            continue
        info = _platform_info(
            entry,
            enabled=not user_dis,
            command_override=overrides.get(entry["name"]),
            bundles=bundles,
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
        return await _assemble_all_scopes(user_id, include_disabled)
    if workspace_id is not None:
        if user_id is None:
            raise HTTPException(status_code=401, detail="Authentication required")
        await _require_owned_workspace(workspace_id, user_id)
    if user_id is None:
        # No attribution here: the bundle a skill came from is only meaningful
        # next to the switch that turns it off, and this view has no account.
        return {"skills": [_platform_info(s) for s in list_skills()]}
    return await _assemble_skills(user_id, include_disabled, workspace_id)


@router.get("/limits", response_model=SkillLimitsResponse)
async def get_skill_limits() -> SkillLimitsResponse:
    """What one skill upload may hold on this server.

    Two size ceilings, because they weigh different bytes: the wire cap the
    zip as sent, the stored cap the skill as kept, re-zipped once hidden
    files and dependencies are set aside. Folding them into one refuses a zip
    whose set-aside files are all that put it over. The stored cap is
    deployment configuration: without object storage an archive is kept in
    its database row, where the smaller inline cap applies.
    """
    return SkillLimitsResponse(
        max_upload_bytes=MAX_SKILL_ARCHIVE_BYTES,
        max_stored_bytes=(
            None
            if skill_archive_storage.is_configured()
            else MAX_SKILL_INLINE_BLOB_BYTES
        ),
        max_files=MAX_SKILL_FILES,
        max_file_bytes=MAX_SKILL_SINGLE_FILE_BYTES,
        max_unpacked_bytes=MAX_SKILL_UNCOMPRESSED_BYTES,
        max_skill_md_bytes=MAX_SKILL_MD_BYTES,
    )


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
    try:
        raw = await read_capped(file, MAX_SKILL_ARCHIVE_BYTES)
    except HTTPException as e:
        if e.status_code != 413:
            raise
        raise _refused(
            413,
            "upload_too_large",
            str(e.detail),
            size=file.size,
            max=MAX_SKILL_ARCHIVE_BYTES,
        ) from e
    try:
        # Unzip + re-zip + hash over as much as 8 MB: off the event loop.
        validated = await asyncio.to_thread(validate_skill_archive, raw, upload=True)
    except SkillValidationError as e:
        raise _refused(400, e.code, str(e), **e.params) from e

    # The name is itself a live trigger (effective_trigger falls back to it),
    # so it has to clear the platform tier the same way an alias does — a
    # builtin the user renamed to this name would otherwise be shadowed by
    # the upload. Renaming onto an existing row is already blocked in
    # set_platform_alias; this is the same rule in the other order.
    try:
        await ensure_free_of_platform(user_id, validated.name)
    except SkillTriggerTaken as e:
        raise _conflict(e) from e
    except SkillNamesUnavailable as e:
        # Not the upload's fault and not a name conflict: a directory skills
        # ship from would not list, so the reservation is short rather than
        # clear. Refusing here is also what keeps the row out of the database,
        # since everything that writes runs below this line.
        raise HTTPException(status_code=503, detail=str(e)) from e
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
            raise _refused(
                400,
                "upload_too_large",
                "Object storage is not configured on this deployment; "
                f"skill archives are limited to {MAX_SKILL_INLINE_BLOB_BYTES} "
                "bytes",
                size=len(validated.canonical_zip),
                max=MAX_SKILL_INLINE_BLOB_BYTES,
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
    except (SkillTriggerTaken, SkillCapReached) as e:
        await drop_archive_if_unused(user_id, archive_key)
        raise _conflict(e) from e
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


async def _refuse_plugin_owned(user_id: str, name: str) -> None:
    owned = await get_user_skill(user_id, name)
    if owned is not None and owned["plugin_id"]:
        raise _refused(
            409,
            "skill_plugin_owned",
            "This skill came with a plugin and goes with it: uninstall the "
            "plugin, or switch the skill off",
            plugin=owned["plugin_name"],
        )


@router.delete("/{name}", status_code=204)
@handle_api_exceptions("delete skill", logger)
async def delete_skill(name: str, user_id: CurrentUserId):
    """Delete a user skill. Builtins can be disabled, not deleted, and a
    plugin's skill goes with its plugin."""
    _validate_name_param(name)
    if name in reserved_skill_names():
        raise HTTPException(
            status_code=409,
            detail="Built-in skills can be disabled, not deleted",
        )
    await _refuse_plugin_owned(user_id, name)
    row = await delete_user_skill(user_id, name, hand_made=True)
    if row is None:
        # A plugin may have taken the name since it was read.
        await _refuse_plugin_owned(user_id, name)
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
    path: str = Query(
        "SKILL.md",
        max_length=512,
        description="A file in the skill's folder, as listed in ``files``.",
    ),
):
    """One file of a skill, SKILL.md by default, with the list of every file
    in its folder."""
    _validate_name_param(name)
    folder = await _skill_folder(user_id, name, workspace_id)
    response = await asyncio.to_thread(skill_content_response, folder, name, path)
    if response is None:
        raise HTTPException(status_code=404, detail="File not found")
    return response


async def _skill_folder(
    user_id: str, name: str, workspace_id: str | None
) -> SkillFolder:
    """The folder a skill's page reads. Most specific tier first (workspace →
    user → platform; reserved names keep the platform tier collision-free, so
    the tier walk is belt-and-braces)."""
    row = None
    if workspace_id is not None:
        await _require_owned_workspace(workspace_id, user_id)
        row = await get_user_skill(user_id, name, workspace_id=workspace_id)
    if row is None:
        row = await get_user_skill(user_id, name)
    if row is not None:
        folder = ZipFolder(await fetch_skill_archive(user_id, row), name)
        if "SKILL.md" not in folder.files:
            logger.error(
                "stored skill archive unreadable (user=%s name=%s)", user_id, name
            )
            raise HTTPException(
                status_code=502, detail="Stored skill archive is unreadable"
            )
        return folder

    # Platform tier: resolve through the effective registry so a hidden skill,
    # or one the account has disabled, is not readable here. Deliberately the
    # account tier only, matching the row branch above: this reads a skill's
    # source for management, and a workspace deny-list scopes where a skill
    # runs, not whether its owner may look at it.
    registry = build_effective_skill_registry(
        disabled_skills=await get_disabled_builtin_skills(user_id)
    )
    skill = registry.get(name)
    # The configured drop-in root, not the field default: an operator who
    # overrode a shipped skill has to see their own copy here, or the page
    # shows one set of instructions while the agent runs another.
    root = (
        resolve_skill_dir(
            name, [str(d) for d in configured_skill_dirs()], registry=registry
        )
        if skill is not None and not skill.hidden
        else None
    )
    folder = await asyncio.to_thread(DiskFolder, root) if root is not None else None
    if folder is None or "SKILL.md" not in folder.files:
        raise HTTPException(status_code=404, detail="Skill not found")
    return folder


# ---------------------------------------------------------------------------
# Workspace tier
# ---------------------------------------------------------------------------

_USER_LEVEL_DISABLED = (
    "This skill is disabled at the user level; enable it in Plugins first"
)
_BUNDLE_DISABLED = (
    "This skill's plugin is switched off; enable the plugin in Plugins first"
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


def _schedule_disable_reconcile(workspace_id: str, user_id: str) -> None:
    """Converge the workspace's link view after a disable of an inherited skill.

    The shared skill directory keeps the skill for the siblings, so the only
    place this workspace's disable takes effect on disk is the link pass. The
    delivery view no longer moves for it either, so nothing else on the next
    turn would prune the link before the post-turn pass.
    """
    WorkspaceManager.schedule_skill_reconcile(
        workspace_id, user_id, source="ws_disable"
    )


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
        _schedule_disable_reconcile(workspace_id, user_id)
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
    # A switched-off bundle subtracts its skills by name at materialize time,
    # into the same set the per-skill disable writes to, but it never lands in
    # that set here. Without this the toggle answers 200 and the skill stays
    # gone, which is the one outcome worse than refusing: the same asymmetry a
    # user-level disable already has, so it answers the same way.
    if body.enabled:
        _, bundle_on = (await _bundle_state(user_id)).of(name)
        if bundle_on is False:
            raise HTTPException(status_code=409, detail=_BUNDLE_DISABLED)
    await set_workspace_skill_disable(workspace_id, name, not body.enabled)
    _schedule_disable_reconcile(workspace_id, user_id)
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
