"""Convergence after a vault-secret mutation, shared by both vault tiers.

A mutation changes secret VALUES, and every config fingerprint in the MCP
machinery hashes ``${vault:NAME}`` reference strings rather than values — so
nothing downstream can see the change on its own. This module is the explicit
compensation, and it is one module rather than a block per router because the
workspace and user tiers differ only in which rows they scan, which caches they
purge, and which workspaces they converge.

Every step is best-effort in that a failure here must never fail the mutation
that triggered it — but "best-effort" stops at the config-version bump, the one
DURABLE convergence trigger: when the work that feeds it fails, the bump still
fires blindly rather than being skipped.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_sanitize import discovery_should_use_secrets, vault_refs
from src.server.database.mcp_servers import (
    bump_user_workspaces_mcp_version,
    bump_workspace_mcp_version,
    list_catalog_servers,
    list_local_servers_for_user,
    list_workspace_servers,
)
from src.server.database.mcp_tool_schemas import (
    delete_tool_schemas_and_bump,
    delete_user_and_workspace_tool_schemas_and_bump,
)
from src.server.database.workspace import (
    get_running_workspace_ids_for_user,
    get_workspace,
)
from src.server.services.mcp_config import (
    user_row_to_server_config,
    workspace_row_to_server_config,
)
from src.server.services.workspace_manager import WorkspaceManager

logger = logging.getLogger(__name__)


def refs_for_server(server: MCPServerConfig) -> set[str]:
    """Vault names a server actually substitutes at resolve time.

    Only env/headers/args/url are substituted, so those are the only fields
    scanned — matching on the whole stored config would let a ``${vault:X}``
    string sitting in free-text description/instruction force a config bump.
    """
    refs: set[str] = set()
    for mapping in (server.env or {}, server.headers or {}):
        for value in mapping.values():
            refs.update(vault_refs(str(value)))
    for arg in server.args or []:
        refs.update(vault_refs(str(arg)))
    refs.update(vault_refs(str(server.url or "")))
    return refs


async def _workspace_servers(workspace_id: str, user_id: str) -> list[MCPServerConfig]:
    """Every server a WORKSPACE secret can satisfy: the workspace's own rows
    plus the user plugins this workspace inherits.

    The inherited half is not optional. Both vault tiers resolve out of one
    merged namespace in the sandbox (workspace wins), so a workspace secret
    really does satisfy an inherited connector's ``${vault:NAME}`` — and an
    inherited connector's discovery snapshot is cached per-workspace, so it is
    this tier's purge that clears it. Scanning workspace rows alone would find
    none of them: an inherited connector appears in ``workspace_mcp_servers``
    only as a marker row, its live config living in ``user_mcp_servers`` and
    merged at resolve time.
    """
    rows = await list_workspace_servers(workspace_id)
    # Only source='workspace' rows carry a config; 'user'/'builtin' rows are
    # markers (config NULL) whose refs live in the tier they point at.
    local_rows = [row for row in rows if row.get("source") == "workspace"]
    local = _to_configs(local_rows, workspace_row_to_server_config)
    # A local row shadows the inherited server of the same name (enabled or
    # not — a disabled fork must not fall back to the config it forked), and it
    # is already scanned above as itself.
    shadowed = {row["name"] for row in local_rows}
    # Tombstone: the inherited server is removed from THIS workspace.
    tombstoned = {
        row["name"]
        for row in rows
        if row.get("source") == "user" and not row.get("enabled")
    }
    inherited = _to_configs(
        [
            row
            for row in await list_catalog_servers(user_id)
            if row["name"] not in shadowed and row["name"] not in tombstoned
        ],
        user_row_to_server_config,
    )
    return local + inherited


async def _user_servers(owner_id: str, user_id: str) -> list[MCPServerConfig]:
    """Every server a USER secret can satisfy: the user's catalog plus the
    workspace-LOCAL servers of every workspace they own.

    The local half is not optional, and it mirrors the workspace tier's
    inherited half: the sandbox resolves both tiers out of one merged namespace
    (workspace wins), so a user secret really does satisfy a local server's
    ``${vault:NAME}`` — and since the fingerprint hashes the ref string rather
    than the value, a local snapshot left unpurged is re-accepted forever. ALL
    the user's workspaces, not just the running ones, because a cached snapshot
    outlives the sandbox that wrote it.
    """
    # owner_id IS user_id here; the argument exists so both tiers share one
    # signature.
    # Disabled rows included: a snapshot outlives the row being switched off,
    # and re-enabling bumps versions without purging — an enabled-only scan
    # would leave that snapshot fingerprint-valid forever.
    catalog = _to_configs(
        await list_catalog_servers(user_id), user_row_to_server_config
    )
    local = _to_configs(
        await list_local_servers_for_user(user_id), workspace_row_to_server_config
    )
    return catalog + local


def _to_configs(
    rows: list[dict], convert: Callable[[dict], MCPServerConfig]
) -> list[MCPServerConfig]:
    out: list[MCPServerConfig] = []
    for row in rows:
        try:
            out.append(convert(row))
        except Exception:
            continue  # unparseable stored row: it can't be resolved either
    return out


@dataclass(frozen=True)
class VaultTier:
    """What distinguishes one vault tier's convergence from the other's."""

    label: str
    log_prefix: str
    # Called with (owner_id, user_id): the workspace tier needs the owning user
    # to reach the plugins its workspace inherits, and one signature for both
    # tiers keeps the caller free of tier branching.
    servers: Callable[[str, str], Awaitable[list[MCPServerConfig]]]
    # Purges every snapshot tier the owner's discovery can land in — for the
    # user tier that spans ALL its workspaces' rows, deliberately wider than
    # ``workspaces`` below. A cached snapshot outlives the sandbox that wrote it.
    purge_and_bump: Callable[[str, list[str]], Awaitable[int]]
    bump: Callable[[str], Awaitable[object]]
    # Workspaces to push to and re-apply — RUNNING ones only, in both tiers.
    # A proactive apply cold-starts an idle sandbox, and that is pure waste
    # here: every start path pushes the vault unconditionally, so a stopped
    # workspace already receives the new value the moment it next starts.
    workspaces: Callable[[str], Awaitable[list[str]]]


async def _own_workspace(workspace_id: str) -> list[str]:
    """This workspace, and only while its sandbox is running.

    One primary-key read per mutation, paid to avoid waking an idle sandbox
    for a secret it will pick up on its own at next start. Checking the row is
    cheaper than a new bespoke query and keeps "running" defined in one place.
    """
    workspace = await get_workspace(workspace_id)
    if not workspace or workspace.get("status") != "running":
        return []
    return [workspace_id]


WORKSPACE_TIER = VaultTier(
    label="workspace",
    log_prefix="[vault]",
    servers=_workspace_servers,
    purge_and_bump=delete_tool_schemas_and_bump,
    bump=bump_workspace_mcp_version,
    workspaces=_own_workspace,
)

USER_TIER = VaultTier(
    label="user",
    log_prefix="[user_vault]",
    servers=_user_servers,
    purge_and_bump=delete_user_and_workspace_tool_schemas_and_bump,
    bump=bump_user_workspaces_mcp_version,
    workspaces=get_running_workspace_ids_for_user,
)


async def after_secret_change(
    tier: VaultTier,
    owner_id: str,
    secret_name: str,
    *,
    user_id: str,
    value_changed: bool = True,
) -> None:
    """Push the new secret set to live sandboxes and invalidate MCP caches.

    Durable half FIRST: the push does seconds of sandbox I/O in request
    context, and a client disconnect cancels it with CancelledError — which
    clears its ``except Exception`` — so push-then-bump could strand a
    committed rotation with no convergence trigger at all. The push is only
    the same-process fast path; the bump is what makes every other worker's
    next sync deliver the value.

    ``value_changed`` is False for a description-only edit: nothing a server
    resolves has moved, so the cache half is skipped.
    """
    if value_changed:
        await _invalidate_mcp(tier, owner_id, secret_name, user_id)
    await _push_secrets(tier, owner_id, user_id)


async def after_secrets_changed(
    tier: VaultTier,
    owner_id: str,
    secret_names: Iterable[str],
    *,
    user_id: str,
) -> None:
    """``after_secret_change`` for a set of names written in one operation.

    The purge stays per name — it is scoped to the servers that reference that
    one credential, and collapsing it would leave the others' discovery
    snapshots stale. The two expensive halves do not: scheduling applies and
    pushing to live sandboxes both act on the owner's whole secret set, so
    running them once per name multiplies seconds of sandbox I/O by however
    many credentials a plugin happens to declare, for no additional effect.
    """
    names = list(dict.fromkeys(secret_names))
    if not names:
        return
    for name in names:
        try:
            await _purge_and_bump(tier, owner_id, name, user_id)
        except Exception:
            logger.warning(
                f"{tier.log_prefix} MCP invalidation failed for {tier.label} "
                f"{owner_id}; falling back to a bare config bump",
                exc_info=True,
            )
            try:
                await tier.bump(owner_id)
            except Exception:
                logger.error(
                    f"{tier.log_prefix} {tier.label} {owner_id} is UNCONVERGED "
                    f"after secret {name!r} changed: the fallback config bump "
                    f"failed too, so live sandboxes keep serving the retired "
                    f"value until the next config write for this {tier.label}",
                    exc_info=True,
                )
    await _schedule_applies(tier, owner_id, user_id)
    await _push_secrets(tier, owner_id, user_id)


async def _push_secrets(tier: VaultTier, owner_id: str, user_id: str) -> None:
    """Push the merged secret set to whichever sandboxes are live in THIS
    process — a fast path only, and one that misses under multiple workers.

    Convergence itself is owned by the version bump in ``_invalidate_mcp``: it
    is what makes the owning worker's next sync re-push, whichever process that
    turns out to be.
    """
    try:
        wm = WorkspaceManager.get_instance()
        for workspace_id in await tier.workspaces(owner_id):
            await wm.push_vault_secrets(workspace_id, user_id=user_id)
    except Exception:
        logger.warning(
            f"{tier.log_prefix} failed to push secrets for {tier.label} {owner_id}",
            exc_info=True,
        )


async def _invalidate_mcp(
    tier: VaultTier, owner_id: str, secret_name: str, user_id: str
) -> None:
    """Bump the config version, purge the discovery snapshots that could depend
    on the changed value, and schedule a proactive apply so a
    ``needs_secret``/``pending`` server comes alive without waiting for the
    user's next message.

    The bump fires on every value change and survives its own inputs failing:
    it is the only DURABLE convergence trigger a secret has — a warm session
    re-syncs its sandbox assets (the vault push rides along) solely on a
    config-version delta — so a failure in the scan or the purge falls back to
    bumping blindly. That costs one needless re-resolve; skipping it would leave
    the retired value readable from an always-on sandbox indefinitely while the
    CRUD endpoint reports success. Only the purge stays scoped to referencing
    servers, because only their cached discovery can depend on the credential.
    """
    try:
        await _purge_and_bump(tier, owner_id, secret_name, user_id)
    except Exception:
        logger.warning(
            f"{tier.log_prefix} MCP invalidation failed for {tier.label} "
            f"{owner_id}; falling back to a bare config bump",
            exc_info=True,
        )
        try:
            await tier.bump(owner_id)
        except Exception:
            logger.error(
                f"{tier.log_prefix} {tier.label} {owner_id} is UNCONVERGED after "
                f"secret {secret_name!r} changed: the fallback config bump failed "
                f"too, so live sandboxes keep serving the retired value until the "
                f"next config write for this {tier.label}",
                exc_info=True,
            )

    await _schedule_applies(tier, owner_id, user_id)


async def _purge_and_bump(
    tier: VaultTier, owner_id: str, secret_name: str, user_id: str
) -> None:
    """The durable half — scan, purge, bump — as ONE failure domain, because a
    partial result here is exactly what the caller's fallback bump covers."""
    referencing = [
        server
        for server in await tier.servers(owner_id, user_id)
        if secret_name in refs_for_server(server)
    ]

    # Only servers whose discovery runs WITH secrets can have a cached
    # tools/list that depends on the credential. Distinct names: one name can
    # reach the scan from both halves of a tier (a workspace-local fork of a
    # catalog server), and the purge takes a name list.
    purge = list(
        dict.fromkeys(s.name for s in referencing if discovery_should_use_secrets(s))
    )

    # Purge + bump in ONE transaction: a partial purge with an un-bumped
    # version would let live sessions skip re-resolution against the
    # half-purged cache.
    if purge:
        await tier.purge_and_bump(owner_id, purge)
    else:
        await tier.bump(owner_id)

    logger.info(
        f"{tier.log_prefix} secret {secret_name!r} change bumped config for "
        f"{tier.label} {owner_id} ({len(referencing)} referencing server(s), "
        f"{len(purge)} snapshot(s) purged)"
    )


async def _schedule_applies(tier: VaultTier, owner_id: str, user_id: str) -> None:
    """Same-process nicety: bring a ``needs_secret``/``pending`` server alive now
    instead of at the user's next message. Its own failure domain — it must
    never take the version bump down with it."""
    try:
        # Lazy: the scheduler lives in a router, and a service must not import
        # an app module at import time.
        from src.server.app.mcp_servers import _schedule_proactive_apply

        for workspace_id in await tier.workspaces(owner_id):
            _schedule_proactive_apply(workspace_id, user_id)
    except Exception:
        logger.warning(
            f"{tier.log_prefix} proactive apply failed for {tier.label} {owner_id}",
            exc_info=True,
        )
