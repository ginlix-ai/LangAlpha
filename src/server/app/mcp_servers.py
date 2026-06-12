"""Per-workspace MCP server API.

The effective-list endpoint calls the SAME ``resolve_mcp_config`` chokepoint the
sandbox-sync path uses and only decorates each server with live status drawn
from the discovery schema cache + the workspace vault. Mutations are DB-write
+ version-bump ONLY (plan §8): no sandbox push, no per-workspace lock, no live
mutation. The running session picks the change up on its next post-cooldown
acquire (≤30s).

Endpoints (all require_workspace_owner):
- GET    /api/v1/workspaces/{id}/mcp/servers
- POST   /api/v1/workspaces/{id}/mcp/servers
- PUT    /api/v1/workspaces/{id}/mcp/servers/{name}
- PATCH  /api/v1/workspaces/{id}/mcp/servers/{name}/enabled
- DELETE /api/v1/workspaces/{id}/mcp/servers/{name}
- POST   /api/v1/workspaces/{id}/mcp/servers/{name}/discover
- POST   /api/v1/workspaces/{id}/mcp/servers/{name}/promote
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, HTTPException
from pydantic import ValidationError

from src.ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.mcp_servers import (
    MAX_MCP_SERVERS_PER_WORKSPACE,
    create_catalog_server,
    delete_workspace_server,
    get_catalog_server,
    get_tool_schemas,
    get_workspace_servers_and_version,
    insert_workspace_server,
    list_workspace_servers,
    set_workspace_server_enabled,
    update_catalog_server,
    upsert_workspace_server,
)
from src.server.database.vault_secrets import (
    create_secret as create_secret_db,
    delete_secret as delete_secret_db,
    get_workspace_secret_names,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.handlers.chat.mcp_config import resolve_mcp_config
from src.server.services.mcp_discovery import mcp_discovery_fingerprint
from src.server.models.mcp_server import (
    CatalogServer,
    EffectiveServer,
    EffectiveServerList,
    EnabledInput,
    McpServerInput,
    PromoteInput,
    ToolSummary,
    _format_validation_error,
    catalog_row_to_response,
    collect_vault_refs,
    parse_mcp_servers_payload,
)
from src.server.services.workspace_manager import WorkspaceManager
from src.server.utils.api import CurrentUserId, handle_api_exceptions, require_workspace_owner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["MCP Servers"])

# Re-running discovery for a freshly-discovered server is wasteful; skip it if
# the cached row at the current version is < this many seconds old and not
# pending (kept simple — no Redis).
_DISCOVER_DEBOUNCE_SECONDS = 15

# On bulk import, an env/header value is auto-extracted into a vault secret when
# it looks like a credential — either the key name reads like one, or the value
# is a long opaque token. Benign config (``MODE=prod``, ``LOG_LEVEL=ERROR``)
# stays an inline literal so we don't clutter the vault.
_SECRET_KEY_RE = re.compile(
    r"(?i)(secret|token|password|passwd|pwd|apikey|api[_-]?key|access[_-]?key|"
    r"authorization|auth|bearer|credential|cred|private[_-]?key|\bpat\b|\bkey\b)"
)
_OPAQUE_TOKEN_MIN_LEN = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _builtin_names() -> set[str]:
    """Names of the process-global built-in MCP servers (from agent_config)."""
    from src.server.app import setup

    if setup.agent_config is None:
        return set()
    return {s.name for s in setup.agent_config.mcp.servers}


async def _require_owned_workspace(workspace_id: str, user_id: str) -> dict:
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


def _missing_secrets(
    env_refs: list[str], header_refs: list[str], secret_names: set[str]
) -> list[str]:
    """Sorted, de-duplicated vault refs not present in the workspace vault."""
    return sorted({n for n in (*env_refs, *header_refs) if n not in secret_names})


def _derive_status(
    *,
    origin: str,
    env_refs: list[str],
    header_refs: list[str],
    secret_names: set[str],
    schema_row: dict[str, Any] | None,
) -> tuple[str, str, list[str]]:
    """Derive the (status, error, missing_secrets) triple for one effective server.

    - builtin disabled-marker rows never reach here (excluded from effective).
    - builtins are process-global ⇒ ``connected``.
    - a workspace server with a ``${vault:NAME}`` ref naming a secret missing
      from the workspace vault ⇒ ``needs_secret``.
    - else from the schema cache at the current version: ``ok`` ⇒ connected,
      ``error`` ⇒ error (with text), missing row ⇒ pending.
    """
    missing = _missing_secrets(env_refs, header_refs, secret_names)
    if origin == "builtin":
        return "connected", "", missing
    if missing:
        return "needs_secret", "", missing
    if schema_row is None:
        return "pending", "", missing
    status = schema_row.get("status")
    if status == "ok":
        return "connected", "", missing
    if status == "error":
        return "error", str(schema_row.get("error") or "discovery failed"), missing
    return "pending", "", missing


def _tools_from_schema(schema_row: dict[str, Any] | None) -> list[ToolSummary]:
    if not schema_row:
        return []
    return [
        ToolSummary(
            name=str(t.get("name") or ""),
            description=str(t.get("description") or ""),
            input_schema=t.get("input_schema") or {},
        )
        for t in (schema_row.get("tools") or [])
    ]


def _sandbox_running(workspace: dict) -> bool:
    return workspace.get("status") == "running"


# Statuses where the sandbox is on its way *up* toward running — a warm is in
# flight (our proactive MCP apply, or workspace entry, kicked one). The UI uses
# this to keep polling and show "Starting workspace…" through the
# stopped→starting→running gap, rather than freezing on a stale "stopped".
_WARMING_STATUSES = frozenset({"starting", "creating"})


def _sandbox_warming(workspace: dict) -> bool:
    return workspace.get("status") in _WARMING_STATUSES


# ---------------------------------------------------------------------------
# GET — effective list
# ---------------------------------------------------------------------------


def _effective_server(
    srv: Any,
    *,
    origin: str,
    enabled: bool,
    status: str,
    config_version: int,
    error: str = "",
    tools: list[ToolSummary] | None = None,
    missing_secrets: list[str] | None = None,
    env_refs: list[str] | None = None,
    header_refs: list[str] | None = None,
) -> EffectiveServer:
    """Build one effective-list row; editable/deletable derive from origin."""
    tools = tools or []
    return EffectiveServer(
        name=srv.name,
        origin=origin,
        transport=srv.transport,
        enabled=enabled,
        editable=(origin == "workspace"),
        deletable=(origin == "workspace"),
        status=status,
        error=error,
        tool_count=len(tools),
        tools=tools,
        missing_secrets=missing_secrets or [],
        env_refs=env_refs or [],
        header_refs=header_refs or [],
        # Echo the stored reference maps (refs/literals, never resolved
        # secrets) so the edit form round-trips them; built-ins stay empty.
        env=dict(srv.env or {}) if origin == "workspace" else {},
        headers=dict(srv.headers or {}) if origin == "workspace" else {},
        description=srv.description or "",
        instruction=srv.instruction or "",
        tool_exposure_mode=srv.tool_exposure_mode or "summary",
        discovery_uses_secrets=bool(getattr(srv, "discovery_uses_secrets", False)),
        command=srv.command,
        args=list(srv.args or []),
        url=srv.url,
        config_version=config_version,
    )


@router.get("/{workspace_id}/mcp/servers")
@handle_api_exceptions("list workspace MCP servers", logger)
async def list_servers(workspace_id: str, user_id: CurrentUserId) -> EffectiveServerList:
    workspace = await _require_owned_workspace(workspace_id, user_id)

    from src.server.app import setup

    base_config = setup.agent_config
    if base_config is None:
        # Startup race: report an empty effective set rather than 500.
        return EffectiveServerList(
            servers=[], sandbox_running=False,
            max_servers=MAX_MCP_SERVERS_PER_WORKSPACE, config_version=0,
        )

    resolved, secret_names, schema_rows = await asyncio.gather(
        resolve_mcp_config(base_config, user_id, workspace_id),
        get_workspace_secret_names(workspace_id),
        get_tool_schemas(workspace_id),
    )
    schema_by_name = {r["server_name"]: r for r in schema_rows}

    servers: list[EffectiveServer] = []
    for srv in resolved.servers:
        origin = "builtin" if srv.name in resolved.builtin_names else "workspace"
        env_refs = collect_vault_refs(dict(srv.env or {}))
        header_refs = collect_vault_refs(dict(srv.headers or {}))
        schema_row = schema_by_name.get(srv.name) if origin == "workspace" else None
        # Accept a cached snapshot only if it's for THIS server's current config.
        # A stale-hash row (the server's own config changed but it hasn't been
        # re-discovered yet) reads as pending → re-verify; an unrelated mutation
        # leaves the hash untouched → the row stays a valid hit.
        if schema_row is not None and schema_row.get("config_hash") != mcp_discovery_fingerprint(srv):
            schema_row = None
        status, error, missing = _derive_status(
            origin=origin,
            env_refs=env_refs,
            header_refs=header_refs,
            secret_names=secret_names,
            schema_row=schema_row,
        )
        tools = _tools_from_schema(schema_row)
        servers.append(
            _effective_server(
                srv,
                origin=origin,
                enabled=srv.enabled,
                status=status,
                error=error,
                tools=tools,
                missing_secrets=missing,
                env_refs=env_refs,
                header_refs=header_refs,
                config_version=resolved.version,
            )
        )

    # Disabled built-ins are filtered out of the resolver's effective set, but
    # the UI still needs a row (with its toggle) to re-enable them.
    for srv in base_config.mcp.servers:
        if srv.name not in resolved.disabled_builtin_names:
            continue
        servers.append(
            _effective_server(
                srv,
                origin="builtin",
                enabled=False,
                status="disabled",
                config_version=resolved.version,
            )
        )

    # Disabled workspace servers are likewise dropped from the resolver's
    # effective set; surface them (greyed, with their toggle) so disabling a
    # workspace server isn't a one-way trip — mirrors the disabled-builtin
    # re-add above.
    for srv in resolved.disabled_workspace_servers:
        env_refs = collect_vault_refs(dict(srv.env or {}))
        header_refs = collect_vault_refs(dict(srv.headers or {}))
        servers.append(
            _effective_server(
                srv,
                origin="workspace",
                enabled=False,
                status="disabled",
                env_refs=env_refs,
                header_refs=header_refs,
                config_version=resolved.version,
            )
        )

    # Version the running session has actually applied (no I/O) — drives the
    # frontend's version-accurate "synced" state. None when no warm session.
    applied_version: int | None = None
    try:
        applied_version = WorkspaceManager.get_instance().get_applied_mcp_config_version(
            workspace_id
        )
    except Exception:
        logger.debug("[mcp] applied version lookup failed for %s", workspace_id)

    return EffectiveServerList(
        servers=servers,
        sandbox_running=_sandbox_running(workspace),
        sandbox_warming=_sandbox_warming(workspace),
        max_servers=MAX_MCP_SERVERS_PER_WORKSPACE,
        config_version=resolved.version,
        applied_config_version=applied_version,
    )


# ---------------------------------------------------------------------------
# POST — add (full def OR from_template)
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers", status_code=201)
@handle_api_exceptions("add workspace MCP server", logger)
async def add_server(
    workspace_id: str,
    user_id: CurrentUserId,
    body: dict = Body(...),
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if "from_template" in body:
        server = await _server_from_template(user_id, body)
    else:
        try:
            server = McpServerInput(**body)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=_format_validation_error(e))

    if server.name in _builtin_names():
        raise HTTPException(
            status_code=409,
            detail=f"{server.name!r} collides with a built-in server name",
        )

    try:
        # Conflict-safe insert (ON CONFLICT DO NOTHING): a concurrent create of
        # the same new name can't silently turn into an UPDATE. None ⇒ the name
        # already exists. The pre-check above only short-circuits built-ins.
        row = await insert_workspace_server(
            workspace_id,
            server.name,
            config=server.to_config_blob(),
        )
    except ValueError as e:
        # DB layer signals over-cap by raising ValueError under the advisory lock.
        raise HTTPException(status_code=409, detail=str(e))
    if row is None:
        raise HTTPException(
            status_code=409, detail=f"{server.name!r} already exists in this workspace"
        )
    _schedule_proactive_apply(workspace_id, user_id)
    return {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}


async def _server_from_template(user_id: str, body: dict) -> McpServerInput:
    """Load a catalog template and re-validate it as a workspace server def."""
    if set(body) != {"from_template"}:
        raise HTTPException(
            status_code=422,
            detail="from_template must be the only field in the body",
        )
    template = await get_catalog_server(user_id, body["from_template"])
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    # Re-validate the stored template through the same input model. A template
    # that no longer passes the (possibly tightened) policy yields a 422.
    try:
        return McpServerInput(
            name=template["name"],
            transport=template["transport"],
            command=template.get("command"),
            args=template.get("args") or [],
            url=template.get("url"),
            env=template.get("env") or {},
            headers=template.get("headers") or {},
            description=template.get("description") or "",
            instruction=template.get("instruction") or "",
            tool_exposure_mode=template.get("tool_exposure_mode") or "summary",
            discovery_uses_secrets=bool(
                template.get("discovery_uses_secrets", False)
            ),
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))


# ---------------------------------------------------------------------------
# POST — promote a workspace server UP into the user's template catalog
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/{name}/promote", status_code=201)
@handle_api_exceptions("promote workspace MCP server to template", logger)
async def promote_server(
    workspace_id: str,
    name: str,
    user_id: CurrentUserId,
    body: PromoteInput | None = None,
) -> CatalogServer:
    """Save a workspace server's definition as a reusable user-level template.

    The inverse of ``from_template``: copies the workspace row's config into the
    user catalog (re-validated through the same input model). Only
    ``${vault:NAME}`` reference names travel — secret values are workspace-scoped
    and never copied, so the template surfaces ``missing_secrets`` when later
    added to another workspace. ``overwrite`` replaces an existing template of
    the same name; without it a name clash is a 409.
    """
    await _require_owned_workspace(workspace_id, user_id)
    overwrite = bool(body and body.overwrite)

    if name in _builtin_names():
        raise HTTPException(
            status_code=409,
            detail="Built-in servers are global; only workspace servers can be "
            "saved as templates",
        )

    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    existing = rows.get(name)
    if existing is None or existing["source"] != "workspace":
        raise HTTPException(status_code=404, detail="MCP server not found")

    # Re-validate the stored config so a template is never minted from a row that
    # no longer passes the (possibly tightened) policy.
    try:
        server = McpServerInput(**(existing.get("config") or {}))
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=_format_validation_error(e))

    fields = {
        "transport": server.transport,
        "command": server.command,
        "args": server.args,
        "url": server.url,
        "env": server.env,
        "headers": server.headers,
        "description": server.description,
        "instruction": server.instruction,
        "tool_exposure_mode": server.tool_exposure_mode,
        "discovery_uses_secrets": server.discovery_uses_secrets,
    }

    if overwrite:
        row = await update_catalog_server(user_id, server.name, updates=fields)
        if row is not None:
            return catalog_row_to_response(row)
        # Nothing to overwrite (raced delete / never existed) ⇒ fall through.

    if await get_catalog_server(user_id, server.name) is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A template named {server.name!r} already exists. "
            "Pass overwrite to replace it.",
        )
    try:
        row = await create_catalog_server(user_id, server.name, **fields)
    except ValueError as e:
        # DB layer signals over-cap (or a raced duplicate) by raising ValueError.
        raise HTTPException(status_code=409, detail=str(e))
    return catalog_row_to_response(row)


# ---------------------------------------------------------------------------
# POST — bulk import a standard `mcpServers` JSON blob
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/import")
@handle_api_exceptions("import workspace MCP servers", logger)
async def import_servers(
    workspace_id: str,
    user_id: CurrentUserId,
    body: dict = Body(...),
) -> dict:
    """Parse a standard ``{"mcpServers": {...}}`` blob and create each server.

    Names are coerced to our identifier shape, transports are mapped, and inline
    literal secrets are auto-extracted into the workspace vault (rewritten to
    ``${vault:NAME}`` refs, deduped by value across the import). Per-server
    outcomes are reported so a partial import is legible. Like every mutation,
    this only writes DB rows + bumps the config version — the change applies on
    the next agent run (≤30s).
    """
    await _require_owned_workspace(workspace_id, user_id)

    parsed = parse_mcp_servers_payload(body)
    if not parsed:
        raise HTTPException(
            status_code=422,
            detail='No MCP servers found. Expected a JSON object like '
            '{"mcpServers": { "<name>": { ... } }}.',
        )

    builtins = _builtin_names()
    existing_rows, _ = await get_workspace_servers_and_version(workspace_id)
    existing_names = {r["name"] for r in existing_rows}
    current_ws_count = sum(1 for r in existing_rows if r["source"] == "workspace")
    used_secret_names = set(await get_workspace_secret_names(workspace_id))

    # value → ${vault:NAME}, so an identical token reused across servers (common
    # for a single provider) is stored once.
    allocated: dict[str, str] = {}
    secrets_created: list[str] = []
    seen_names: set[str] = set()
    results: list[dict[str, Any]] = []
    created_count = 0

    for entry in parsed:
        base = {
            "original_name": entry.original_name,
            "name": entry.name,
            "renamed": entry.renamed,
        }
        if entry.error:
            results.append({**base, "status": "invalid", "error": entry.error})
            continue
        if entry.name in builtins:
            results.append(
                {**base, "status": "skipped", "reason": "collides with a built-in server"}
            )
            continue
        if entry.name in seen_names or entry.name in existing_names:
            reason = (
                "duplicate name after normalization"
                if entry.name in seen_names
                else "already exists in this workspace"
            )
            status = "skipped" if entry.name in seen_names else "exists"
            results.append({**base, "status": status, "reason": reason})
            continue
        if current_ws_count + created_count >= MAX_MCP_SERVERS_PER_WORKSPACE:
            results.append(
                {
                    **base,
                    "status": "error",
                    "error": f"workspace MCP server cap "
                    f"({MAX_MCP_SERVERS_PER_WORKSPACE}) reached",
                }
            )
            continue

        seen_names.add(entry.name)
        config = dict(entry.config)
        try:
            made = await _extract_literals_to_vault(
                workspace_id,
                entry.name,
                config,
                allocated=allocated,
                used_secret_names=used_secret_names,
            )
        except ValueError as e:
            results.append({**base, "status": "error", "error": str(e)})
            continue

        # An authenticated remote server needs its header even to list tools, so
        # discovery must resolve secrets — set it explicitly so the stored value
        # (and the UI toggle) is honest (matches discovery_should_use_secrets).
        if config.get("transport") in ("http", "sse"):
            headers = config.get("headers") or {}
            if any(VAULT_REF_RE.search(str(v)) for v in headers.values()):
                config["discovery_uses_secrets"] = True

        try:
            server = McpServerInput(**config)
        except ValidationError as e:
            await _rollback_import_secrets(
                workspace_id, made, allocated=allocated, used_secret_names=used_secret_names
            )
            results.append(
                {**base, "status": "invalid", "error": _format_validation_error(e)}
            )
            continue

        try:
            row = await insert_workspace_server(
                workspace_id, server.name, config=server.to_config_blob()
            )
        except ValueError as e:
            await _rollback_import_secrets(
                workspace_id, made, allocated=allocated, used_secret_names=used_secret_names
            )
            results.append({**base, "status": "error", "error": str(e)})
            continue
        if row is None:
            await _rollback_import_secrets(
                workspace_id, made, allocated=allocated, used_secret_names=used_secret_names
            )
            results.append({**base, "status": "exists"})
            continue

        secrets_created.extend(made)
        created_count += 1
        results.append({**base, "status": "created"})

    # Imported secrets are usable immediately on a live sandbox (best-effort);
    # the server set itself applies on the next agent run.
    if secrets_created:
        await _push_vault_to_sandbox(workspace_id)

    _, version = await get_workspace_servers_and_version(workspace_id)
    if created_count > 0:
        _schedule_proactive_apply(workspace_id, user_id)
    return {
        "results": results,
        "created": created_count,
        "secrets_created": secrets_created,
        "config_version": version,
    }


def _looks_like_secret(key: str, value: str) -> bool:
    """Heuristic: should this env/header literal be vaulted rather than inlined?"""
    if _SECRET_KEY_RE.search(key or ""):
        return True
    v = value or ""
    return len(v) >= _OPAQUE_TOKEN_MIN_LEN and " " not in v and not v.isdigit()


def _vault_secret_name(server_name: str, key: str, used: set[str]) -> str:
    """Allocate a unique, NAME_RE-legal vault secret name for ``server.key``."""
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{server_name}_{key}".upper())
    if base and base[0].isdigit():
        base = f"_{base}"
    base = base[:64] or "IMPORTED_SECRET"
    name = base
    i = 2
    while name in used:
        suffix = f"_{i}"
        name = f"{base[: 64 - len(suffix)]}{suffix}"
        i += 1
    return name


async def _rollback_import_secrets(
    workspace_id: str,
    names: list[str],
    *,
    allocated: dict[str, str],
    used_secret_names: set[str],
) -> None:
    """Best-effort removal of vault secrets created for a server whose import failed.

    Also unwinds the cross-server dedupe bookkeeping so a later server in the
    same import can't reuse a ref that points at a deleted secret.
    """
    if not names:
        return
    refs = {f"${{vault:{n}}}" for n in names}
    for literal in [k for k, v in allocated.items() if v in refs]:
        del allocated[literal]
    for n in names:
        used_secret_names.discard(n)
        try:
            await delete_secret_db(workspace_id, n)
        except Exception:
            logger.warning(
                "[mcp] failed to roll back imported secret %s for workspace %s",
                n,
                workspace_id,
                exc_info=True,
            )


async def _extract_literals_to_vault(
    workspace_id: str,
    server_name: str,
    config: dict[str, Any],
    *,
    allocated: dict[str, str],
    used_secret_names: set[str],
) -> list[str]:
    """Move credential-looking env/header literals into the vault, in place.

    Existing ``${vault:NAME}`` refs and benign config literals are left alone.
    Returns the names of any vault secrets created. May raise ``ValueError`` if
    the vault secret cap is reached — secrets already created for THIS server
    are rolled back first, so a cap hit never strands orphans.
    """
    created: list[str] = []
    try:
        return await _extract_literals_inner(
            workspace_id,
            server_name,
            config,
            allocated=allocated,
            used_secret_names=used_secret_names,
            created=created,
        )
    except Exception:
        await _rollback_import_secrets(
            workspace_id, created, allocated=allocated, used_secret_names=used_secret_names
        )
        raise


async def _extract_literals_inner(
    workspace_id: str,
    server_name: str,
    config: dict[str, Any],
    *,
    allocated: dict[str, str],
    used_secret_names: set[str],
    created: list[str],
) -> list[str]:
    for field in ("env", "headers"):
        mapping = config.get(field)
        if not isinstance(mapping, dict):
            continue
        out: dict[str, Any] = {}
        for k, v in mapping.items():
            if (
                not isinstance(v, str)
                or not v.strip()
                or VAULT_REF_RE.fullmatch(v)
                or not _looks_like_secret(str(k), v)
            ):
                out[k] = v
                continue
            ref = allocated.get(v)
            if ref is None:
                secret_name = _vault_secret_name(server_name, str(k), used_secret_names)
                await create_secret_db(
                    workspace_id,
                    secret_name,
                    v,
                    f"Imported with MCP server {server_name}",
                )
                used_secret_names.add(secret_name)
                created.append(secret_name)
                ref = f"${{vault:{secret_name}}}"
                allocated[v] = ref
            out[k] = ref
        config[field] = out

    # stdio ``args`` is a list; the common credential shape is a single
    # ``--flag=VALUE`` token (or ``KEY=VALUE``). Split on the first ``=`` and
    # vault the value half when the flag or value looks secret, rewriting the arg
    # to ``--flag=${vault:NAME}`` (the generated client resolves refs in args).
    # Bare / space-separated arg secrets (``--token VALUE``) are left as-is —
    # too ambiguous to auto-extract without over-vaulting benign positionals.
    args = config.get("args")
    if isinstance(args, list):
        new_args: list[Any] = []
        for arg in args:
            if not isinstance(arg, str) or "=" not in arg:
                new_args.append(arg)
                continue
            flag, _, val = arg.partition("=")
            if (
                not val.strip()
                or VAULT_REF_RE.search(val)
                or not _looks_like_secret(flag, val)
            ):
                new_args.append(arg)
                continue
            ref = allocated.get(val)
            if ref is None:
                key_hint = flag.lstrip("-") or "arg"
                secret_name = _vault_secret_name(
                    server_name, key_hint, used_secret_names
                )
                await create_secret_db(
                    workspace_id,
                    secret_name,
                    val,
                    f"Imported with MCP server {server_name}",
                )
                used_secret_names.add(secret_name)
                created.append(secret_name)
                ref = f"${{vault:{secret_name}}}"
                allocated[val] = ref
            new_args.append(f"{flag}={ref}")
        config["args"] = new_args
    return created


async def _push_vault_to_sandbox(workspace_id: str) -> None:
    """Best-effort push of vault secrets to a running sandbox (mirrors vault.py)."""
    try:
        wm = WorkspaceManager.get_instance()
        await wm.push_vault_secrets(workspace_id)
    except Exception:
        logger.warning(
            "[mcp] failed to push imported vault secrets for %s",
            workspace_id,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# PUT — edit a workspace-source row
# ---------------------------------------------------------------------------


@router.put("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("edit workspace MCP server", logger)
async def edit_server(
    workspace_id: str, name: str, body: McpServerInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        raise HTTPException(status_code=409, detail="Cannot edit a built-in server")
    if body.name != name:
        raise HTTPException(
            status_code=409, detail="name in body must match the path name"
        )

    rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
    existing = rows.get(name)
    if existing is None:
        raise HTTPException(status_code=404, detail="MCP server not found")
    if existing["source"] != "workspace":
        raise HTTPException(status_code=409, detail="Cannot edit a built-in server")

    row = await upsert_workspace_server(
        workspace_id,
        name,
        source="workspace",
        enabled=bool(existing["enabled"]),
        config=body.to_config_blob(),
    )
    _schedule_proactive_apply(workspace_id, user_id)
    return {"name": row["name"], "source": row["source"], "enabled": row["enabled"]}


# ---------------------------------------------------------------------------
# PATCH — enabled toggle (handles builtin disable-marker semantics)
# ---------------------------------------------------------------------------


@router.patch("/{workspace_id}/mcp/servers/{name}/enabled")
@handle_api_exceptions("toggle workspace MCP server", logger)
async def set_enabled(
    workspace_id: str, name: str, body: EnabledInput, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        # Built-ins are toggled by an explicit (source='builtin', enabled=false)
        # disable-marker row; enabling = delete the marker.
        if body.enabled:
            await delete_workspace_server(workspace_id, name)
        else:
            await upsert_workspace_server(
                workspace_id, name, source="builtin", enabled=False, config=None
            )
        _schedule_proactive_apply(workspace_id, user_id)
        return {"name": name, "enabled": body.enabled}

    found = await set_workspace_server_enabled(workspace_id, name, body.enabled)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    _schedule_proactive_apply(workspace_id, user_id)
    return {"name": name, "enabled": body.enabled}


# ---------------------------------------------------------------------------
# DELETE — remove a workspace row (409 on builtin)
# ---------------------------------------------------------------------------


@router.delete("/{workspace_id}/mcp/servers/{name}")
@handle_api_exceptions("delete workspace MCP server", logger)
async def delete_server(
    workspace_id: str, name: str, user_id: CurrentUserId
) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    if name in _builtin_names():
        raise HTTPException(status_code=409, detail="Cannot delete a built-in server")

    found = await delete_workspace_server(workspace_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="MCP server not found")
    _schedule_proactive_apply(workspace_id, user_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# POST — on-demand discovery probe (debounced; no lock, no sandbox mutation)
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/mcp/servers/{name}/discover")
@handle_api_exceptions("discover workspace MCP server", logger)
async def discover_server(
    workspace_id: str, name: str, user_id: CurrentUserId
) -> dict:
    workspace = await _require_owned_workspace(workspace_id, user_id)

    from src.server.app import setup
    from src.server.services.mcp_discovery import discover_and_cache

    base_config = setup.agent_config
    if base_config is None:
        raise HTTPException(status_code=503, detail="Agent config not ready")

    if name in _builtin_names():
        raise HTTPException(
            status_code=409, detail="Discovery is for user servers only"
        )

    resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
    server = next((s for s in resolved.servers if s.name == name), None)
    if server is None or name not in resolved.user_names:
        raise HTTPException(status_code=404, detail="MCP server not found")

    # Debounce: if the cached snapshot is for this server's CURRENT config and is
    # fresh + not pending, return it without re-running discovery. A stale-hash
    # row (config changed) always falls through to a real probe.
    existing = {r["server_name"]: r for r in await get_tool_schemas(workspace_id)}
    cached = existing.get(name)
    if (
        cached is not None
        and cached.get("config_hash") == mcp_discovery_fingerprint(server)
        and cached.get("status") != "pending"
        and _is_fresh(cached.get("discovered_at"))
    ):
        return {"server": _discovery_row_to_dict(cached)}

    sandbox = _get_live_sandbox(workspace_id, workspace)
    rows = await discover_and_cache(workspace_id, sandbox, [server])
    row = rows[0] if rows else None
    if row is not None and row.get("status") == "ok":
        # The probe wrote a fresh snapshot WITHOUT a version bump, so a live
        # session's composite/summary would short-circuit past it on the next
        # acquire. Refresh explicitly (background, best-effort) so the agent
        # sees the same tools the UI now shows.
        _schedule_session_mcp_refresh(workspace_id, user_id)
    return {"server": _discovery_row_to_dict(row)}


# Strong refs to in-flight proactive-apply tasks so they aren't GC'd mid-run.
_proactive_apply_tasks: set[asyncio.Task] = set()
_proactive_apply_pending: dict[str, asyncio.Task] = {}
_PROACTIVE_APPLY_SETTLE_S = 1.5


def _schedule_proactive_apply(workspace_id: str, user_id: str) -> None:
    """Front-load verifying + applying a just-saved MCP config.

    Fire-and-forget so it never blocks (or fails) the mutation response. It
    drives a background session acquire that brings the applied config up to the
    new version — warming (cold-starting) the sandbox if it isn't running yet —
    so the change is discovered and live before the user's next turn (no
    surprise). Best-effort: any failure falls back to the next-message apply.

    Mutations within the settle window coalesce into one apply: a newer
    mutation cancels a still-waiting sleeper, never an in-flight apply.
    """
    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return

    pending = _proactive_apply_pending.get(workspace_id)
    if pending is not None and not pending.done():
        pending.cancel()

    async def _settle_then_apply() -> None:
        await asyncio.sleep(_PROACTIVE_APPLY_SETTLE_S)
        # Past the settle window: deregister so newer mutations schedule a
        # fresh apply instead of cancelling this one mid-flight.
        if _proactive_apply_pending.get(workspace_id) is asyncio.current_task():
            _proactive_apply_pending.pop(workspace_id, None)
        await wm.proactively_apply_mcp_config(workspace_id, user_id)

    task = asyncio.create_task(_settle_then_apply())
    _proactive_apply_pending[workspace_id] = task
    _proactive_apply_tasks.add(task)

    def _cleanup(t: asyncio.Task) -> None:
        _proactive_apply_tasks.discard(t)
        if _proactive_apply_pending.get(workspace_id) is t:
            _proactive_apply_pending.pop(workspace_id, None)

    task.add_done_callback(_cleanup)


def _schedule_session_mcp_refresh(workspace_id: str, user_id: str) -> None:
    """Background composite rebuild after an out-of-band schema-cache update.

    Unlike ``_schedule_proactive_apply`` there is no version bump to apply, so
    this goes through ``refresh_session_mcp`` (which busts the session's cached
    version first). Undebounced: probes are explicit single user actions.
    """
    try:
        wm = WorkspaceManager.get_instance()
    except Exception:
        return
    task = asyncio.create_task(wm.refresh_session_mcp(workspace_id, user_id))
    _proactive_apply_tasks.add(task)
    task.add_done_callback(_proactive_apply_tasks.discard)


def _get_live_sandbox(workspace_id: str, workspace: dict) -> Any | None:
    """Return the in-memory live sandbox if one is ready, else None.

    Reads the cached session directly (no lock, no acquire) so discovery never
    races the warm/Phase-2 machinery. A stopped/cold workspace ⇒ None, which
    ``discover_and_cache`` turns into ``pending`` rows.
    """
    if not _sandbox_running(workspace):
        return None
    try:
        wm = WorkspaceManager.get_instance()
        if not wm.has_ready_session(workspace_id):
            return None
        session = wm._sessions.get(workspace_id)
        return session.sandbox if session else None
    except Exception:
        logger.warning(
            "[mcp] could not resolve live sandbox for %s", workspace_id, exc_info=True
        )
        return None


def _is_fresh(discovered_at: Any) -> bool:
    """True if ``discovered_at`` (ISO string or datetime) is within the debounce."""
    if not discovered_at:
        return False
    if isinstance(discovered_at, str):
        try:
            dt = datetime.fromisoformat(discovered_at)
        except ValueError:
            return False
    elif isinstance(discovered_at, datetime):
        dt = discovered_at
    else:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - dt).total_seconds()
    return age < _DISCOVER_DEBOUNCE_SECONDS


def _discovery_status(raw: Any) -> str:
    """Map a schema-cache status to the McpStatus enum the effective list emits.

    The cache stores ``ok``; the API surfaces ``connected`` so the discovery
    probe and the effective list agree. ``error`` / ``pending`` pass through.
    """
    return "connected" if raw == "ok" else (str(raw) if raw else "pending")


def _discovery_row_to_dict(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {"status": "pending", "tools": [], "error": ""}
    return {
        "server_name": row.get("server_name"),
        "status": _discovery_status(row.get("status")),
        "tools": row.get("tools") or [],
        "error": row.get("error") or "",
        "config_hash": row.get("config_hash"),
        "discovered_at": row.get("discovered_at"),
    }
