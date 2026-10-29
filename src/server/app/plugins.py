"""Agent Plugins API (agent-plugins.org packages).

Endpoints (user-scoped):
- GET    /api/v1/plugins
- POST   /api/v1/plugins                      {source_url, subdir?, secrets?} — public git/archive
- POST   /api/v1/plugins/upload               multipart zip/tar (+ subdir, secrets fields)
- GET    /api/v1/plugins/{name}
- POST   /api/v1/plugins/{name}/update        {source_url?, secrets?} — re-fetch a git source, or another revision of it
- POST   /api/v1/plugins/{name}/update/upload multipart replacement (+ secrets field)
- POST   /api/v1/plugins/{name}/sse-upgrades  {keys} — consent an sse→http upgrade
- POST   /api/v1/plugins/{name}/bindings      {secrets} — set a new value for a key it asks for
- PATCH  /api/v1/plugins/{name}/enabled
- DELETE /api/v1/plugins/{name}
- GET    /api/v1/plugins/{name}/export

Fatal validation problems (unreadable archive, invalid plugin.json past the
tolerated warns) are a 422 carrying the collected diagnostics; everything
survivable lands as a 201 whose InstallReport names each component's
outcome. That includes an ``ai.langalpha`` key or entry this build cannot
read: it is dropped with a warning and the rest of the block applies, so a
newer contract field costs only itself. An archive holding several
plugins (a marketplace repo) is a 422 with ``code: "multiple_plugins"`` and
the discovered candidates — the wizard renders a chooser and re-requests
with ``subdir``.

Every server a request would write is checked first, and nothing is written
unless all of them pass (``services/plugins/checks``). ``secrets`` carries the
values for the keys the plugin declares or its entries reference, which the
checks use and the vault receives only after they pass. The refusals, each
with nothing written:

- 422 ``plugin_secrets_required`` with ``secrets: [{name, label, description?,
  regex?, docs_url?}]``: declared keys the vault does not hold and the request
  did not send.
- 422 ``mcp_check_failed`` with ``failures: [{server, verdict, reason,
  missing_secrets}]``: every server that did not pass.
- 409 ``mcp_sign_in_required`` with ``servers: [{server, url}]``: one server
  asks for a sign-in, which the client starts as a pending sign-in.
- 422 ``plugin_sign_in_unsupported`` with ``servers``: more than one sign-in
  on install, or any new one on update.
"""

import asyncio
import io
import logging
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.server.database.account_disables import (
    list_account_disables,
    set_account_disable,
)
from src.server.database.mcp_servers import list_catalog_servers
from src.server.database.plugin_subagents import list_user_plugin_subagent_names
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
from src.server.services.mcp_catalog import sign_in_missing, sign_in_to_enable
from src.server.services.plugins import (
    MAX_PACKAGE_BYTES,
    PackageSource,
    ValidatedPackage,
    apply_bindings,
    apply_sse_upgrades,
    export_plugin_zip,
    install_plugin_package,
    refusals_as_http,
    resolve_update_source,
    resolve_upload_source,
    resolve_url_source,
    uninstall_plugin,
    update_plugin_package,
    validate_source,
)
from src.server.services.plugins.source import SECRETS_FORM, SECRETS_REFUSAL
from src.server.utils.api import CurrentUserId, handle_api_exceptions
from src.server.utils.uploads import json_form_field, read_capped
from src.server.services.plugins.bundled import (
    bundled_names,
    BundleOwnershipUnavailable,
    enforcement_owners,
    list_bundled,
)
from src.server.services.plugins.icons import plugin_mark

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/plugins", tags=["Plugins"])


class PluginSourceInput(BaseModel):
    """POST body for a remote install: a public repo or archive URL, plus
    the chosen subdirectory when the archive holds several plugins, and
    values for the keys the plugin declares."""

    source_url: str
    subdir: str | None = None
    secrets: dict[str, str] | None = None

    model_config = {"extra": "forbid"}


class PluginUpdateInput(BaseModel):
    """Optional POST body for an update: another revision of the plugin's
    own source to move it to (a catalog entry's ``source_url``), and values
    for keys a new or moved server of the new version needs."""

    source_url: str | None = None
    secrets: dict[str, str] | None = None

    model_config = {"extra": "forbid"}


class SseUpgradeInput(BaseModel):
    """POST body consenting specific held-back sse entries to install as
    streamable HTTP."""

    keys: list[str]

    model_config = {"extra": "forbid"}


class BindingsInput(BaseModel):
    """POST body setting new values for the keys a plugin asks for."""

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
    """plugin_id → still-owned component refs, one query per kind."""
    servers, skills, subagents = await asyncio.gather(
        list_catalog_servers(user_id),
        list_all_user_skills(user_id),
        list_user_plugin_subagent_names(user_id),
    )
    out: dict[str, list[PluginComponentRef]] = {}
    for row in servers:
        plugin_id = row.get("plugin_id")
        if plugin_id:
            out.setdefault(plugin_id, []).append(
                PluginComponentRef(
                    kind="mcp",
                    name=row["name"],
                    key=row.get("plugin_server_key") or row["name"],
                )
            )
    for row in skills:
        plugin_id = row.get("plugin_id")
        if plugin_id:
            out.setdefault(plugin_id, []).append(
                PluginComponentRef(
                    kind="skill",
                    name=row["name"],
                    key=row.get("plugin_skill_dir") or row["name"],
                )
            )
    for row in subagents:
        out.setdefault(row["plugin_id"], []).append(
            PluginComponentRef(
                kind="subagent", name=row["name"], key=row["name"], edited=row["edited"]
            )
        )
    return out


def _header_safe(value: Any) -> str:
    """A manifest string reduced to what a Content-Disposition filename can
    carry. Empty when nothing survives, which the caller reads as absent."""
    return "".join(
        c for c in str(value or "") if c.isascii() and (c.isalnum() or c in "._-")
    )[:64]


@router.get("", response_model=PluginListResponse)
@handle_api_exceptions("list plugins", logger)
async def list_plugins_endpoint(user_id: CurrentUserId) -> PluginListResponse:
    rows = await list_plugins(user_id)
    components = await _components_by_plugin(user_id)
    # Bundles ship with the app rather than being installed, so they lead the
    # list and are not counted against the user's slots.
    return PluginListResponse(
        plugins=[
            *await list_bundled((await list_account_disables(user_id)).bundles),
            *[
                plugin_row_to_info(
                    r,
                    components=components.get(r["user_plugin_id"], []),
                    icon_url=await plugin_mark(r),
                )
                for r in rows
            ],
        ],
        max_plugins=MAX_PLUGINS_PER_USER,
        remaining_slots=max(0, MAX_PLUGINS_PER_USER - len(rows)),
    )


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
            row,
            components=components.get(row["user_plugin_id"], []),
            icon_url=await plugin_mark(row),
        ),
        report=report,
    )


async def _install_response(
    user_id: str, source: PackageSource, secrets: dict[str, str] | None
) -> InstallResponse:
    with refusals_as_http():
        package = await validate_source(source)
        row, report = await install_plugin_package(
            user_id,
            package,
            source_type=source.source_type,
            source_ref=source.source_ref,
            secrets=secrets,
        )
    return await _plugin_response(user_id, row, report)


@router.post("", response_model=InstallResponse, status_code=201)
@handle_api_exceptions("install plugin", logger, conflict_on_value_error=True)
async def install_from_url(
    user_id: CurrentUserId, body: PluginSourceInput
) -> InstallResponse:
    """Install from a public repository or archive URL (https only)."""
    with refusals_as_http():
        source = await resolve_url_source(body.source_url, body.subdir)
    return await _install_response(user_id, source, body.secrets)


@router.post("/upload", response_model=InstallResponse, status_code=201)
@handle_api_exceptions("install plugin", logger, conflict_on_value_error=True)
async def upload_plugin(
    user_id: CurrentUserId,
    file: UploadFile = File(...),
    subdir: str | None = Form(None),
    secrets: str | None = Form(None),
) -> InstallResponse:
    """Install a plugin from an uploaded package (zip or tar).

    ``secrets`` is the JSON body's field as a form field holding JSON.
    """
    parsed = json_form_field(secrets, SECRETS_FORM, SECRETS_REFUSAL)
    raw = await read_capped(file, MAX_PACKAGE_BYTES)
    return await _install_response(
        user_id, resolve_upload_source(raw, file.filename, subdir), parsed
    )


@router.get("/{name}", response_model=PluginInfo)
@handle_api_exceptions("get plugin", logger)
async def get_plugin_endpoint(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> PluginInfo:
    components = await _components_by_plugin(user_id)
    return plugin_row_to_info(
        plugin,
        components=components.get(plugin["user_plugin_id"], []),
        icon_url=await plugin_mark(plugin),
    )


async def _update_response(
    user_id: str,
    plugin: dict[str, Any],
    package: ValidatedPackage,
    *,
    source_ref: str | None,
    secrets: dict[str, str] | None = None,
) -> InstallResponse:
    if package.name != plugin["name"]:
        raise HTTPException(
            status_code=409,
            detail=(
                f"the package declares name {package.name!r} but this plugin "
                f"is installed as {plugin['name']!r}"
            ),
        )
    with refusals_as_http():
        row, report = await update_plugin_package(
            user_id, plugin, package, source_ref=source_ref, secrets=secrets
        )
    return await _plugin_response(user_id, row, report)


@router.post("/{name}/update", response_model=InstallResponse)
@handle_api_exceptions("update plugin", logger, conflict_on_value_error=True)
async def update_plugin_endpoint(
    plugin: CurrentPlugin,
    user_id: CurrentUserId,
    body: PluginUpdateInput | None = None,
) -> InstallResponse:
    """Re-fetch a git-sourced plugin and reconcile its components.

    A ``source_url`` moves the plugin to that revision of its own source, and
    the row then remembers it; a 422 ``plugin_source_mismatch`` refuses any
    other repository or folder before anything is fetched.
    """
    if plugin["source_type"] != "git" or not plugin.get("source_ref"):
        raise HTTPException(
            status_code=409,
            detail=(
                "this plugin was installed from an uploaded package; "
                "upload a new package to update it"
            ),
        )
    with refusals_as_http():
        source = await resolve_update_source(
            plugin["source_ref"], body.source_url if body is not None else None
        )
        package = await validate_source(source, expected_name=plugin["name"])
    return await _update_response(
        user_id,
        plugin,
        package,
        source_ref=source.source_ref,
        secrets=body.secrets if body is not None else None,
    )


@router.post("/{name}/update/upload", response_model=InstallResponse)
@handle_api_exceptions("update plugin", logger, conflict_on_value_error=True)
async def update_plugin_upload(
    plugin: CurrentPlugin,
    user_id: CurrentUserId,
    file: UploadFile = File(...),
    secrets: str | None = Form(None),
) -> InstallResponse:
    """Update from an uploaded replacement package."""
    parsed = json_form_field(secrets, SECRETS_FORM, SECRETS_REFUSAL)
    raw = await read_capped(file, MAX_PACKAGE_BYTES)
    with refusals_as_http():
        package = await validate_source(
            resolve_upload_source(raw, file.filename, None),
            expected_name=plugin["name"],
        )
    # A git plugin keeps its remote source_ref: source_type is not editable, so
    # overwriting the URL with a filename would leave the row claiming 'git'
    # while holding something no fetch can resolve, and every later Update
    # would fail URL validation. None preserves the stored value.
    return await _update_response(
        user_id,
        plugin,
        package,
        source_ref=None if plugin["source_type"] == "git" else file.filename,
        secrets=parsed,
    )


@router.post("/{name}/sse-upgrades", response_model=InstallResponse)
@handle_api_exceptions(
    "upgrade plugin sse entries", logger, conflict_on_value_error=True
)
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
    with refusals_as_http():
        report = await apply_sse_upgrades(user_id, plugin, body.keys)
    return await _plugin_response(
        user_id, await get_plugin(user_id, plugin["name"]) or plugin, report
    )


@router.post("/{name}/bindings", response_model=BindingsResponse)
@handle_api_exceptions("bind plugin secrets", logger)
async def bind_plugin_secrets(
    plugin: CurrentPlugin, body: BindingsInput, user_id: CurrentUserId
) -> BindingsResponse:
    """Set new values for keys the plugin declares, such as a rotated key.

    Every server that reads one, whoever owns it, is checked with the new
    value first; a 422 ``mcp_check_failed`` leaves the vault as it was, and so
    does a 409 for a server that changed while the check ran.
    """
    try:
        with refusals_as_http():
            written = await apply_bindings(user_id, plugin, body.secrets)
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


async def _refuse_unconnected_servers(user_id: str, name: str) -> None:
    """Refuse to switch on a plugin whose servers would go live unsigned-in.

    Switching a plugin on puts every server of it whose own switch is on in
    front of the agent, so each of those that needs a sign-in has to have one
    first, the same rule the server's own switch keeps. A server switched off
    on its own stays off and is not asked about.
    """
    plugin = await get_plugin(user_id, name)
    if plugin is None or plugin.get("enabled"):
        return
    needed = []
    for row in await list_catalog_servers(user_id):
        if str(row.get("plugin_id")) != str(plugin["user_plugin_id"]) or not row.get("enabled"):
            continue
        if (missing := await sign_in_missing(user_id, row)) is not None:
            needed.append(missing)
    if needed:
        raise sign_in_to_enable(*needed)


@router.patch("/{name}/enabled", response_model=PluginEnabledResponse)
@handle_api_exceptions("toggle plugin", logger)
async def set_plugin_enabled_endpoint(
    name: str, body: EnabledInput, user_id: CurrentUserId
) -> PluginEnabledResponse:
    """Plugin-level switch: suppresses every still-owned component through
    the delivery join predicate, without touching the component rows.

    A bundle has no row to flag, so its answer is stored the other way up: a
    disable is written, an enable is the absence of one. Both resolvers expand
    the bundle into its components' names, so what the user sees switch off is
    the same set either kind of package suppresses.
    """
    from src.server.services.mcp_oauth.lifecycle import revoke_live_grants

    if body.enabled:
        await _refuse_unconnected_servers(user_id, name)
    row = await set_plugin_enabled(user_id, name, body.enabled)
    if row is None:
        if name not in bundled_names():
            # A disable outlives the name it was written against: rename a
            # bundle between releases and the live list no longer answers to
            # what the user switched off. Refusing the way back out would
            # strand that row forever, unreachable from the page that wrote
            # it, so an enable is allowed to clear one that is already there.
            # Only the way out -- a disable still needs a package that exists.
            if body.enabled and name in (
                await list_account_disables(user_id)
            ).bundles:
                await set_account_disable(user_id, "bundle", name, disabled=False)
                return PluginEnabledResponse(name=name, enabled=True)
            raise HTTPException(status_code=404, detail="Plugin not found")
        # Installed row first, bundle second, and the order is the rule rather
        # than an optimization. Install refuses a name a bundle already ships,
        # so the only way both can exist is a deploy introducing a name someone
        # installed earlier, and the package the user chose themselves is the
        # one whose switch has to keep working.
        # Resolve BEFORE the write. owned_by refuses when the boot snapshot
        # cannot say what this bundle owns, and a disable committed ahead of
        # that refusal is the worst of both: the request 500s, the row stays,
        # and every later turn re-raises on the same unanswerable name. Read
        # from the snapshot, like every other withdrawal: the grants being cut
        # belong to the servers this process is running, not to whatever
        # plugins/ says a moment later.
        #
        # Only on the way off. Re-enabling is how a user gets out of a disable
        # that stopped being enforceable, so it must never need the answer that
        # is missing.
        servers: frozenset[str] = frozenset()
        if not body.enabled:
            try:
                servers, _ = enforcement_owners().owned_by({name})
            except BundleOwnershipUnavailable as e:
                raise HTTPException(status_code=503, detail=str(e)) from e
        await set_account_disable(user_id, "bundle", name, disabled=not body.enabled)
        if not body.enabled:
            await revoke_live_grants(user_id, sorted(servers))
        return PluginEnabledResponse(name=name, enabled=body.enabled)

    if not body.enabled:
        # The join predicate only decides what the NEXT acquire delivers. A
        # sandbox that already holds a relay JWT for one of these servers keeps
        # reaching it for hours, so disabling a plugin owes its components the
        # same egress cut a per-server disable performs.
        #
        # Read separately, and log rather than 500: the flag is already
        # committed, so a read that raises inside the revoke call would report
        # failure for an operation that half succeeded, and cut nothing while
        # looking indistinguishable from an empty server list.
        try:
            servers = await list_plugin_server_names(
                user_id, row["user_plugin_id"]
            )
        except Exception:
            logger.exception(
                "plugin %s disabled, but its component list could not be read "
                "— live egress grants were left in place",
                name,
            )
        else:
            await revoke_live_grants(user_id, [s["name"] for s in servers])
        return PluginEnabledResponse(name=name, enabled=False)

    # As a server's own switch does: a component switched off with its plugin
    # dropped its queued description, and switching it back on moves no tools.
    from src.server.services.mcp_descriptions.worker import requeue_server

    try:
        servers = await list_plugin_server_names(user_id, row["user_plugin_id"])
    except Exception:
        logger.warning(
            "plugin %s enabled, but its components could not be requeued for "
            "descriptions",
            name,
            exc_info=True,
        )
        servers = []
    for server in servers:
        await requeue_server(user_id, server["name"])
    return PluginEnabledResponse(name=name, enabled=True)


@router.delete("/{name}", response_model=UninstallResponse)
@handle_api_exceptions("uninstall plugin", logger, conflict_on_value_error=True)
async def delete_plugin_endpoint(
    plugin: CurrentPlugin, user_id: CurrentUserId
) -> UninstallResponse:
    return UninstallResponse(deleted=await uninstall_plugin(user_id, plugin))
