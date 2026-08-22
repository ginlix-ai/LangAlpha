"""Agent Plugins API (agent-plugins.org packages).

Endpoints (user-scoped):
- GET    /api/v1/plugins
- POST   /api/v1/plugins                      {source_url, subdir?} — public git/archive
- POST   /api/v1/plugins/upload               multipart zip/tar (+ subdir field)
- GET    /api/v1/plugins/{name}
- POST   /api/v1/plugins/{name}/update        re-fetch a git source
- POST   /api/v1/plugins/{name}/update/upload multipart replacement
- POST   /api/v1/plugins/{name}/sse-upgrades  {keys} — consent an sse→http upgrade
- POST   /api/v1/plugins/{name}/bindings      {secrets} — fill declared credentials
- PATCH  /api/v1/plugins/{name}/enabled
- DELETE /api/v1/plugins/{name}
- GET    /api/v1/plugins/{name}/export

Fatal validation problems (unreadable archive, invalid plugin.json past the
tolerated warns, any ``ai.langalpha`` extension error) are a 422 carrying
the collected diagnostics; everything survivable lands as a 201 whose
InstallReport names each component's outcome. An archive holding several
plugins (a marketplace repo) is a 422 with ``code: "multiple_plugins"`` and
the discovered candidates — the wizard renders a chooser and re-requests
with ``subdir``.
"""

import asyncio
import io
import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.server.database.mcp_servers import list_catalog_servers
from src.server.database.plugins import (
    MAX_PLUGINS_PER_USER,
    get_plugin,
    list_plugin_server_names,
    list_plugins,
    set_plugin_enabled,
)
from src.server.database.user_skills import list_all_user_skills
from src.server.models.mcp_server import EnabledInput
from src.server.models.plugin import (
    BindingsResponse,
    InstallReport,
    InstallResponse,
    PluginComponentRef,
    PluginEnabledResponse,
    PluginInfo,
    PluginListResponse,
    UninstallResponse,
    plugin_row_to_info,
)
from src.server.services.plugins import (
    MAX_PACKAGE_BYTES,
    PluginAmbiguous,
    PluginFatal,
    PluginRejected,
    ValidatedPackage,
    apply_bindings,
    apply_sse_upgrades,
    compose_subdir_url,
    export_plugin_zip,
    fetch_plugin_source,
    install_plugin_package,
    uninstall_plugin,
    update_plugin_package,
    validate_package,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions
from src.server.utils.uploads import read_capped

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/plugins", tags=["Plugins"])


class PluginSourceInput(BaseModel):
    """POST body for a remote install: a public repo or archive URL, plus
    the chosen subdirectory when the archive holds several plugins."""

    source_url: str
    subdir: str | None = None

    model_config = {"extra": "forbid"}


class SseUpgradeInput(BaseModel):
    """POST body consenting specific held-back sse entries to install as
    streamable HTTP."""

    keys: list[str]

    model_config = {"extra": "forbid"}


class BindingsInput(BaseModel):
    """POST body filling a plugin's declared secrets: name → value."""

    secrets: dict[str, str]

    model_config = {"extra": "forbid"}


async def _require_plugin(name: str, user_id: CurrentUserId) -> dict[str, Any]:
    """The installed plugin, or 404 — every endpoint that addresses one."""
    row = await get_plugin(user_id, name)
    if row is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    return row


CurrentPlugin = Annotated[dict[str, Any], Depends(_require_plugin)]


async def _components_by_plugin(
    user_id: str,
) -> dict[str, list[PluginComponentRef]]:
    """plugin_id → still-owned component refs, in two queries total."""
    out: dict[str, list[PluginComponentRef]] = {}
    for row in await list_catalog_servers(user_id):
        plugin_id = row.get("plugin_id")
        if plugin_id:
            out.setdefault(plugin_id, []).append(
                PluginComponentRef(
                    kind="mcp",
                    name=row["name"],
                    key=row.get("plugin_server_key") or row["name"],
                )
            )
    for row in await list_all_user_skills(user_id):
        plugin_id = row.get("plugin_id")
        if plugin_id:
            out.setdefault(plugin_id, []).append(
                PluginComponentRef(
                    kind="skill",
                    name=row["name"],
                    key=row.get("plugin_skill_dir") or row["name"],
                )
            )
    return out


def _header_safe(value: Any) -> str:
    """A manifest string reduced to what a Content-Disposition filename can
    carry. Empty when nothing survives, which the caller reads as absent."""
    return "".join(
        c for c in str(value or "") if c.isascii() and (c.isalnum() or c in "._-")
    )[:64]


def _fatal_to_http(e: PluginFatal) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={
            "message": str(e),
            "diagnostics": [d.model_dump() for d in e.diagnostics],
        },
    )


def _ambiguous_to_http(e: PluginAmbiguous) -> HTTPException:
    """The chooser payload: which plugins the archive holds, and where."""
    return HTTPException(
        status_code=422,
        detail={
            "message": str(e),
            "code": "multiple_plugins",
            "discovered": [
                {
                    "path": c.path,
                    "dialect": c.dialect,
                    "name": c.name,
                    "description": c.description,
                    "version": c.version,
                    # Set when the plugin lives in another repo (a
                    # marketplace external entry): install via this URL, not
                    # via subdir.
                    "source_url": c.source_url,
                }
                for c in e.candidates
            ],
        },
    )


@router.get("", response_model=PluginListResponse)
@handle_api_exceptions("list plugins", logger)
async def list_plugins_endpoint(user_id: CurrentUserId) -> PluginListResponse:
    rows = await list_plugins(user_id)
    components = await _components_by_plugin(user_id)
    return PluginListResponse(
        plugins=[
            plugin_row_to_info(
                r, components=components.get(r["user_plugin_id"], [])
            )
            for r in rows
        ],
        max_plugins=MAX_PLUGINS_PER_USER,
        remaining_slots=max(0, MAX_PLUGINS_PER_USER - len(rows)),
    )


async def _validated(
    raw: bytes,
    *,
    subdir: str | None = None,
    expected_name: str | None = None,
) -> ValidatedPackage:
    try:
        # Unzip + validate up to 50 MiB of tree: off the event loop.
        return await asyncio.to_thread(validate_package, raw, subdir=subdir)
    except PluginFatal as e:
        raise _fatal_to_http(e)
    except PluginAmbiguous as e:
        # Update knows which plugin it wants: a unique name match settles a
        # multi-plugin archive without re-asking the user.
        if expected_name is not None:
            matches = [c for c in e.candidates if c.name == expected_name]
            if len(matches) == 1:
                return await _validated(raw, subdir=matches[0].path)
        raise _ambiguous_to_http(e)


async def _plugin_response(
    user_id: str, row: dict[str, Any], report: InstallReport
) -> InstallResponse:
    """The one shape every component-installing endpoint answers in.

    Install, update and sse-upgrades are the same operation at different
    moments, so each returns the plugin as it now stands plus what it just
    did; a caller renders all three through one code path.
    """
    components = await _components_by_plugin(user_id)
    return InstallResponse(
        plugin=plugin_row_to_info(
            row, components=components.get(row["user_plugin_id"], [])
        ),
        report=report,
    )


async def _install_response(
    user_id: str,
    package: ValidatedPackage,
    *,
    source_type: str,
    source_ref: str | None,
) -> InstallResponse:
    row, report = await install_plugin_package(
        user_id, package, source_type=source_type, source_ref=source_ref
    )
    return await _plugin_response(user_id, row, report)


@router.post("", response_model=InstallResponse, status_code=201)
@handle_api_exceptions("install plugin", logger, conflict_on_value_error=True)
async def install_from_url(
    user_id: CurrentUserId, body: PluginSourceInput
) -> InstallResponse:
    """Install from a public repository or archive URL (https only).

    The URL itself may deep-link a subdirectory (a forge tree URL or a
    ``#subdir=`` fragment); an explicit ``subdir`` in the body — the wizard's
    chooser answer — wins over it, and is folded into the stored source_ref
    so update lands on the same plugin.
    """
    try:
        raw, url_subdir = await fetch_plugin_source(body.source_url)
    except PluginFatal as e:
        raise _fatal_to_http(e)
    subdir = body.subdir if body.subdir is not None else url_subdir
    package = await _validated(raw, subdir=subdir)
    source_ref = (
        compose_subdir_url(body.source_url, body.subdir)
        if body.subdir is not None
        else body.source_url
    )
    return await _install_response(
        user_id, package, source_type="git", source_ref=source_ref
    )


@router.post("/upload", response_model=InstallResponse, status_code=201)
@handle_api_exceptions("install plugin", logger, conflict_on_value_error=True)
async def upload_plugin(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
    subdir: str | None = Form(None),
) -> InstallResponse:
    """Install a plugin from an uploaded package (zip or tar)."""
    raw = await read_capped(file, MAX_PACKAGE_BYTES)
    package = await _validated(raw, subdir=subdir)
    return await _install_response(
        user_id, package, source_type="zip", source_ref=file.filename
    )


@router.get("/{name}", response_model=PluginInfo)
@handle_api_exceptions("get plugin", logger)
async def get_plugin_endpoint(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> PluginInfo:
    components = await _components_by_plugin(user_id)
    return plugin_row_to_info(
        plugin, components=components.get(plugin["user_plugin_id"], [])
    )


async def _update_response(
    user_id: str,
    plugin: dict[str, Any],
    package: ValidatedPackage,
    *,
    source_ref: str | None,
) -> InstallResponse:
    if package.name != plugin["name"]:
        raise HTTPException(
            status_code=409,
            detail=(
                f"the package declares name {package.name!r} but this plugin "
                f"is installed as {plugin['name']!r}"
            ),
        )
    row, report = await update_plugin_package(
        user_id, plugin, package, source_ref=source_ref
    )
    return await _plugin_response(user_id, row, report)


@router.post("/{name}/update", response_model=InstallResponse)
@handle_api_exceptions("update plugin", logger, conflict_on_value_error=True)
async def update_plugin_endpoint(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> InstallResponse:
    """Re-fetch a git-sourced plugin and reconcile its components."""
    if plugin["source_type"] != "git" or not plugin.get("source_ref"):
        raise HTTPException(
            status_code=409,
            detail=(
                "this plugin was installed from an uploaded package; "
                "upload a new package to update it"
            ),
        )
    try:
        raw, url_subdir = await fetch_plugin_source(plugin["source_ref"])
    except PluginFatal as e:
        raise _fatal_to_http(e)
    package = await _validated(
        raw, subdir=url_subdir, expected_name=plugin["name"]
    )
    return await _update_response(
        user_id, plugin, package, source_ref=plugin["source_ref"]
    )


@router.post("/{name}/update/upload", response_model=InstallResponse)
@handle_api_exceptions("update plugin", logger, conflict_on_value_error=True)
async def update_plugin_upload(
    plugin: CurrentPlugin,
    user_id: CurrentUserId,
    file: UploadFile = File(...),
) -> InstallResponse:
    """Update from an uploaded replacement package."""
    raw = await read_capped(file, MAX_PACKAGE_BYTES)
    package = await _validated(raw, expected_name=plugin["name"])
    # A git plugin keeps its remote source_ref: source_type is not editable, so
    # overwriting the URL with a filename would leave the row claiming 'git'
    # while holding something no fetch can resolve, and every later Update
    # would fail URL validation. None preserves the stored value.
    return await _update_response(
        user_id,
        plugin,
        package,
        source_ref=None if plugin["source_type"] == "git" else file.filename,
    )


@router.post("/{name}/sse-upgrades", response_model=InstallResponse)
@handle_api_exceptions("upgrade plugin sse entries", logger)
async def sse_upgrades(
    plugin: CurrentPlugin, body: SseUpgradeInput, user_id: CurrentUserId
) -> InstallResponse:
    """Install consented held-back sse entries as streamable HTTP.

    An upgrade installs components, so it answers like install and update:
    the consent changed the plugin's component set, and the caller reads the
    new one off the response instead of refetching to find out.
    """
    if not plugin.get("mcp_document"):
        raise HTTPException(
            status_code=409, detail="this plugin has no MCP component"
        )
    try:
        report = await apply_sse_upgrades(user_id, plugin, body.keys)
    except PluginFatal as e:
        raise _fatal_to_http(e)
    return await _plugin_response(
        user_id, await get_plugin(user_id, plugin["name"]) or plugin, report
    )


@router.post("/{name}/bindings", response_model=BindingsResponse)
@handle_api_exceptions("bind plugin secrets", logger)
async def bind_plugin_secrets(
    plugin: CurrentPlugin, body: BindingsInput, user_id: CurrentUserId
) -> BindingsResponse:
    """Fill declared plugin secrets into the user vault (create or update)."""
    try:
        written = await apply_bindings(user_id, plugin, body.secrets)
    except PluginFatal as e:
        raise _fatal_to_http(e)
    except PluginRejected as e:
        raise HTTPException(status_code=422, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return BindingsResponse(set=written)


@router.get(
    "/{name}/export",
    # The route streams a zip; without this the generated schema advertises
    # application/json, and a client generated from it breaks on a contract
    # the running server never had.
    response_class=StreamingResponse,
    responses={200: {"content": {"application/zip": {}}}},
)
@handle_api_exceptions("export plugin", logger)
async def export_plugin(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> StreamingResponse:
    """Download the plugin as a spec-compliant package zip (no secret values
    can appear: the stored document is scrubbed of credential literals at
    validation, and skills are the shipped bytes)."""
    data = await export_plugin_zip(user_id, plugin)
    # version is whatever the manifest said — the spec constrains it to a
    # string and nothing more, so it reaches here able to carry an emoji or a
    # newline. A header value is latin-1 and single-line, so build the
    # filename from a conservative charset rather than trusting the manifest.
    name = plugin["name"]
    version = _header_safe(plugin.get("version"))
    filename = f"{name}-{version}.zip" if version else f"{name}.zip"
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.patch("/{name}/enabled", response_model=PluginEnabledResponse)
@handle_api_exceptions("toggle plugin", logger)
async def set_plugin_enabled_endpoint(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> PluginEnabledResponse:
    """Plugin-level switch: suppresses every still-owned component through
    the delivery join predicate, without touching the component rows."""
    from src.server.services.mcp_oauth.lifecycle import revoke_live_grants

    row = await set_plugin_enabled(user_id, name, body.enabled)
    if row is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    if not body.enabled:
        # The join predicate only decides what the NEXT acquire delivers. A
        # sandbox that already holds a relay JWT for one of these servers keeps
        # reaching it for hours, so disabling a plugin owes its components the
        # same egress cut a per-server disable performs.
        await revoke_live_grants(
            user_id,
            [
                s["name"]
                for s in await list_plugin_server_names(
                    user_id, row["user_plugin_id"]
                )
            ],
        )
    return PluginEnabledResponse(name=name, enabled=body.enabled)


@router.delete("/{name}", response_model=UninstallResponse)
@handle_api_exceptions("uninstall plugin", logger)
async def delete_plugin_endpoint(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> UninstallResponse:
    return UninstallResponse(deleted=await uninstall_plugin(user_id, plugin))
