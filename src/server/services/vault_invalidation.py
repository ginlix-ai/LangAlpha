"""Convergence after a vault-secret mutation.

A mutation changes secret VALUES, and every config fingerprint in the MCP
machinery hashes ``${vault:NAME}`` reference strings rather than values — so
nothing downstream can see the change on its own. This module is the explicit
compensation, in one place so every door that writes a secret (the vault page,
an import, a plugin install) converges the same way.

Every step is best-effort in that a failure here must never fail the mutation
that triggered it — but "best-effort" stops at the config-version bump, the one
DURABLE convergence trigger: when the work that feeds it fails, the bump still
fires blindly rather than being skipped.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_sanitize import discovery_should_use_secrets, vault_refs
from src.server.database.mcp_config_version import bump_user_workspaces_mcp_version
from src.server.database.mcp_servers import list_catalog_servers
from src.server.database.mcp_tool_schemas import (
    delete_user_and_workspace_tool_schemas_and_bump,
)
from src.server.database.workspace import get_running_workspace_ids_for_user
from src.server.services.mcp_config import user_row_to_server_config
from src.server.services.workspace_manager import WorkspaceManager

logger = logging.getLogger(__name__)

_LOG = "[vault]"


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


async def _user_servers(user_id: str) -> list[MCPServerConfig]:
    """Every server a secret can satisfy: the user's whole catalog.

    Disabled rows included: a snapshot outlives the row being switched off,
    and re-enabling bumps versions without purging, so an enabled-only scan
    would leave that snapshot fingerprint-valid forever.
    """
    out: list[MCPServerConfig] = []
    for row in await list_catalog_servers(user_id):
        try:
            out.append(user_row_to_server_config(row))
        except Exception:
            continue  # unparseable stored row: it can't be resolved either
    return out


def _rediscover_catalog_rows(user_id: str, names: list[str]) -> None:
    """Kick the host-side probe for each purged name.

    The purge emptied their snapshot and no sandbox refills a remote row's. A
    row the user has switched off is refused for the reason it is refused
    everywhere else: its snapshot stays purged until the switch, which is what
    kicks the next probe.
    """
    # Lazy: discovery imports the DB layer this module's callers sit above.
    from src.server.services.mcp_oauth.discovery import schedule_catalog_discovery

    for name in names:
        schedule_catalog_discovery(user_id, name, reason="secret-change")


async def after_secret_change(
    user_id: str, secret_name: str, *, value_changed: bool = True
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
        await _invalidate_mcp(user_id, [secret_name])
    await _push_secrets(user_id)


async def after_secrets_changed(user_id: str, secret_names: Iterable[str]) -> None:
    """``after_secret_change`` for a set of names written in one operation.

    The purge stays per name — it is scoped to the servers that reference that
    one credential, and collapsing it would leave the others' discovery
    snapshots stale. Everything around it collapses: the row set the purge
    scans is the same for every name, and scheduling applies and pushing to
    live sandboxes both act on the user's whole secret set, so running any of
    them once per name multiplies the work by however many credentials a
    plugin happens to declare, for no additional effect.
    """
    names = list(dict.fromkeys(secret_names))
    if not names:
        return
    await _invalidate_mcp(user_id, names)
    await _push_secrets(user_id)


async def _push_secrets(user_id: str) -> None:
    """Push the secret set to whichever sandboxes are live in THIS process: a
    fast path only, and one that misses under multiple workers.

    Convergence itself is owned by the version bump in ``_invalidate_mcp``: it
    is what makes the owning worker's next sync re-push, whichever process that
    turns out to be.
    """
    try:
        wm = WorkspaceManager.get_instance()
        await wm.push_user_vault(
            user_id, await get_running_workspace_ids_for_user(user_id)
        )
    except Exception:
        logger.warning(
            f"{_LOG} failed to push secrets for user {user_id}", exc_info=True
        )


async def _invalidate_mcp(user_id: str, secret_names: list[str]) -> None:
    """Bump the config version, purge the discovery snapshots that could depend
    on the changed values, and schedule a proactive apply so a
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
        servers = await _user_servers(user_id)
    except Exception:
        # A fast path, not a new failure domain: hand the loop nothing and it
        # reads per name, fallback bump included.
        servers = None
    purged: list[str] = []
    for name in secret_names:
        try:
            purged += await _purge_and_bump(user_id, name, servers=servers)
        except Exception:
            logger.warning(
                f"{_LOG} MCP invalidation failed for user {user_id}; falling "
                "back to a bare config bump",
                exc_info=True,
            )
            try:
                await bump_user_workspaces_mcp_version(user_id)
            except Exception:
                logger.error(
                    f"{_LOG} user {user_id} is UNCONVERGED after secret "
                    f"{name!r} changed: the fallback config bump failed too, so "
                    "live sandboxes keep serving the retired value until the "
                    "next config write for this user",
                    exc_info=True,
                )
    await _schedule_applies(user_id)
    _schedule_rediscovery(user_id, purged)


def _schedule_rediscovery(user_id: str, names: list[str]) -> None:
    """Refill what the purge emptied. Its own failure domain, like the applies."""
    if not names:
        return
    try:
        _rediscover_catalog_rows(user_id, list(dict.fromkeys(names)))
    except Exception:
        logger.warning(
            f"{_LOG} host-side rediscovery failed for user {user_id}",
            exc_info=True,
        )


async def _purge_and_bump(
    user_id: str,
    secret_name: str,
    *,
    servers: list[MCPServerConfig] | None = None,
) -> list[str]:
    """The durable half — scan, purge, bump — as ONE failure domain, because a
    partial result here is exactly what the caller's fallback bump covers.
    Returns the names whose snapshots were purged.

    ``servers`` lets a batch caller hand the row set in once. The scan below is
    per name; the rows it scans are not, and re-reading them per name is real
    Postgres work in the install request path.
    """
    if servers is None:
        servers = await _user_servers(user_id)
    referencing = [
        server for server in servers if secret_name in refs_for_server(server)
    ]

    # Only servers whose discovery runs WITH secrets can have a cached
    # tools/list that depends on the credential.
    purge = [s.name for s in referencing if discovery_should_use_secrets(s)]

    # Purge + bump in ONE transaction: a partial purge with an un-bumped
    # version would let live sessions skip re-resolution against the
    # half-purged cache. The purge spans ALL the user's workspaces, not just
    # the running ones, because a cached snapshot outlives the sandbox that
    # wrote it.
    if purge:
        await delete_user_and_workspace_tool_schemas_and_bump(user_id, purge)
    else:
        await bump_user_workspaces_mcp_version(user_id)

    logger.info(
        f"{_LOG} secret {secret_name!r} change bumped config for user "
        f"{user_id} ({len(referencing)} referencing server(s), "
        f"{len(purge)} snapshot(s) purged)"
    )
    return purge


async def _schedule_applies(user_id: str) -> None:
    """Same-process nicety: bring a ``needs_secret``/``pending`` server alive now
    instead of at the user's next message. Only running workspaces: a
    proactive apply cold-starts an idle sandbox, and every start path pushes
    the vault anyway. Its own failure domain: it must never take the version
    bump down with it."""
    try:
        # Lazy: the scheduler lives in a router, and a service must not import
        # an app module at import time.
        from src.server.app.mcp_servers import _schedule_proactive_apply

        for workspace_id in await get_running_workspace_ids_for_user(user_id):
            _schedule_proactive_apply(workspace_id, user_id)
    except Exception:
        logger.warning(
            f"{_LOG} proactive apply failed for user {user_id}", exc_info=True
        )
