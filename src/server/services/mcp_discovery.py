"""Shared MCP discovery service: run in-sandbox discovery, sanitize, cache.

Single implementation used by both the on-demand API probe and the session
Phase-2 sync path, so sanitization and the schema cache never diverge.
Discovery executes untrusted code merely to list tools — it runs without
vault access (the generated client substitutes inert placeholders).
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Iterable
from typing import Any

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_sanitize import (
    discovery_affecting_payload,
    sanitize_tool_name,
    sanitize_tool_text,
    unsalvageable_required_params,
)

from src.server.database import mcp_servers as mcp_db
from src.server.database.mcp_tool_schemas import upsert_tool_schemas
from src.server.services.mcp_identity import bounded_identity

logger = logging.getLogger(__name__)

# Discovery-boundary caps for hostile/buggy servers (plan §6). The prompt-side
# detailed-mode caps live in the formatter; these bound what we cache at all.
#
# So neither bounds prompt cost, which is what makes them cheap to raise: a
# server over the formatter's caps renders as a summary either way, and what
# these actually size is the cached JSON and the wrapper module a sandbox gets.
#
# Sized against what brokers and data vendors really ship, not a round number.
# Going over is not graceful degradation -- the cut is by list position, so a
# server one tool past the cap loses whichever capability it happened to
# enumerate last. moomoo publishes 88 tools, and the old cap of 64 silently
# took its entire paper-trading suite along with news, insider and short
# interest data, none of which anything on screen could explain.
MAX_TOOLS_PER_SERVER = 128
MAX_SCHEMA_CHARS_PER_SERVER = 400_000


def mcp_discovery_fingerprint(server: MCPServerConfig) -> str:
    """Stable per-server hash of discovery-affecting config — never secret values.

    Captures everything that can change a server's ``tools/list`` result:
    transport, command, args, url, the full env/header maps (literal values AND
    ``${vault:NAME}`` ref strings — the stored values are never resolved
    secrets), and the secret-less-discovery decision. It deliberately EXCLUDES
    ``enabled`` (toggling a server off/on reuses its cached schema) and the
    prompt-only fields (description / instruction / tool_exposure_mode).

    This is the discovery-cache key, keyed off the server's OWN identity, so
    mutating or toggling an UNRELATED server never orphans this one's snapshot.
    Shares :func:`discovery_affecting_payload` with the sandbox asset-upload hash
    so a config change can never invalidate one without the other.

    Because only the ``${vault:NAME}`` ref STRING is hashed, changing a secret's
    VALUE never churns this hash — vault mutations instead invalidate explicitly
    (version bump + snapshot purge for secret-dependent servers; see
    ``src/server/services/vault_invalidation.py``).
    """
    payload = discovery_affecting_payload(server, include_identity=False)
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def ok_snapshot(row: dict[str, Any]) -> bool:
    """Acceptance predicate for consumers that may only serve real tools."""
    return row.get("status") == "ok"


class ToolSnapshotIndex:
    """Hash-gated view over the two discovery-snapshot tiers.

    A cached snapshot may be served only if it was discovered under the
    server's CURRENT ``mcp_discovery_fingerprint`` — an unrelated mutation
    leaves that hash untouched (cache hit), the server's own edit does not
    (miss ⇒ re-verify). Which tier answers is also fixed here: an inherited
    (``source='user'``) server reads its USER-tier snapshot first, because the
    host-side OAuth discovery that writes it is purged on disconnect and
    refreshed on connect, whereas the per-workspace snapshot's fingerprint is
    OAuth-blind and can outlive a disconnect/reconnect. The tier is chosen by
    which one HAS a matching snapshot, before any status filter, so a rejected
    user-tier row never falls through to a stale workspace one.

    Rows are supplied by the caller (each lane already reads what it needs);
    the index owns only the acceptance rule.
    """

    def __init__(
        self,
        *,
        workspace_rows: Iterable[dict[str, Any]] = (),
        user_rows: Iterable[dict[str, Any]] = (),
    ) -> None:
        self._workspace = {(r["server_name"], r.get("config_hash")): r for r in workspace_rows}
        self._user = {(r["server_name"], r.get("config_hash")): r for r in user_rows}

    def snapshot(
        self,
        server: MCPServerConfig,
        *,
        accept: Callable[[dict[str, Any]], bool] | None = None,
    ) -> dict[str, Any] | None:
        """The current-config snapshot for ``server``, or None.

        ``accept`` further filters the row the tier precedence selected (e.g.
        ok-only, or a freshness window); a rejected row reads as no snapshot.
        """
        key = (server.name, mcp_discovery_fingerprint(server))
        tiers = (
            (self._user, self._workspace)
            if getattr(server, "source", None) == "user"
            else (self._workspace,)
        )
        for tier in tiers:
            row = tier.get(key)
            if row is None:
                continue
            return row if (accept is None or accept(row)) else None
        return None

    def ok(self, server: MCPServerConfig) -> dict[str, Any] | None:
        """The current-config snapshot for ``server`` iff discovery succeeded."""
        return self.snapshot(server, accept=ok_snapshot)


def sanitize_discovered_tools(
    tools: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """Sanitize one server's raw ``tools/list`` snapshot for caching.

    Keeps the ORIGINAL tool name (wrappers must call the server by its real
    name; identifier sanitization happens again at codegen), but drops tools
    whose names cannot become a legal identifier or that collide after
    sanitization, sanitizes description text, and enforces count/size caps.
    Returns ``(kept, skipped)`` where skipped entries are ``(name, reason)``.

    The schema container is validated here, not downstream: a malformed
    ``input_schema`` (or ``properties``) reaches wrapper generation as an
    AttributeError that has no per-server isolation, so one hostile server
    would wedge a whole workspace's asset sync. A tool whose REQUIRED param
    name is unsalvageable is dropped for the opposite reason — codegen would
    emit it, minus that param, as a permanently uncallable wrapper.
    """
    kept: list[dict[str, Any]] = []
    skipped: list[tuple[str, str]] = []
    seen: set[str] = set()
    total_chars = 0
    for tool in tools:
        name = str(tool.get("name") or "")
        sanitized = sanitize_tool_name(name)
        if sanitized is None:
            skipped.append((name, "name is not a valid Python identifier"))
            continue
        if sanitized in seen:
            skipped.append((name, f"sanitized name {sanitized!r} collides with another tool"))
            continue
        raw_schema = tool.get("input_schema")
        if raw_schema is None:
            raw_schema = {}
        if not isinstance(raw_schema, dict):
            skipped.append((name, "input_schema is not a JSON object"))
            continue
        properties = raw_schema.get("properties")
        if properties is not None and not isinstance(properties, dict):
            skipped.append((name, "input_schema properties is not a JSON object"))
            continue
        if bad_required := unsalvageable_required_params(raw_schema):
            joined = ", ".join(repr(p) for p in bad_required)
            skipped.append(
                (name, f"required parameter {joined} is not a valid Python identifier")
            )
            continue
        if len(kept) >= MAX_TOOLS_PER_SERVER:
            skipped.append((name, f"server exceeds {MAX_TOOLS_PER_SERVER}-tool cap"))
            continue
        entry = {
            "name": name,
            "description": sanitize_tool_text(tool.get("description")),
            "input_schema": raw_schema,
        }
        entry_chars = len(json.dumps(entry, ensure_ascii=False))
        if total_chars + entry_chars > MAX_SCHEMA_CHARS_PER_SERVER:
            skipped.append((name, "server exceeds total schema size cap"))
            continue
        seen.add(sanitized)
        total_chars += entry_chars
        kept.append(entry)
    return kept, skipped


async def _stale_server_names(
    workspace_id: str, servers: list[MCPServerConfig]
) -> set[str]:
    """Servers whose CURRENT DB config no longer matches their kick-time state.

    A name is stale when its row is gone (deleted/disabled mid-discovery) or
    its recomputed fingerprint differs (edited mid-discovery). Malformed rows
    count as stale — dropping a result is always safe; clobbering is not.
    Workspace-origin servers check ``workspace_mcp_servers``; inherited
    (``source='user'``) servers check the owner's Plugins catalog — their
    results cache under this workspace like any other, so the guard must know
    both tiers or every inherited discovery would be dropped as "deleted".
    """
    from src.server.database.workspace import get_workspace
    from src.server.services.mcp_config import (
        user_row_to_server_config,
        workspace_row_to_server_config,
    )

    rows = {
        r["name"]: r
        for r in await mcp_db.list_workspace_servers(workspace_id)
        if r.get("source") == "workspace"
    }
    user_rows: dict[str, dict[str, Any]] = {}
    if any(getattr(s, "source", None) == "user" for s in servers):
        workspace = await get_workspace(workspace_id)
        user_id = (workspace or {}).get("user_id")
        if user_id:
            user_rows = {
                r["name"]: r
                for r in await mcp_db.list_enabled_user_servers(str(user_id))
            }
    stale: set[str] = set()
    for server in servers:
        inherited = getattr(server, "source", None) == "user"
        row = (user_rows if inherited else rows).get(server.name)
        if row is None:
            stale.add(server.name)
            continue
        try:
            current = (
                # oauth_connection_id is fingerprint-exempt, so None is fine.
                user_row_to_server_config(row)
                if inherited
                else workspace_row_to_server_config(row)
            )
            current_fp = mcp_discovery_fingerprint(current)
        except Exception:  # noqa: BLE001
            stale.add(server.name)
            continue
        if current_fp != mcp_discovery_fingerprint(server):
            stale.add(server.name)
    return stale


async def discover_and_cache(
    workspace_id: str,
    sandbox: Any,
    servers: list[MCPServerConfig],
) -> list[dict[str, Any]]:
    """Discover ``servers`` inside ``sandbox``, sanitize, and cache snapshots.

    Each snapshot is cached under the server's own config fingerprint
    (``mcp_discovery_fingerprint``), not the workspace config version, so it
    survives unrelated mutations. Per-server error isolation: one broken server
    yields an ``error`` row and never blocks the others. A missing/stopped
    sandbox (or one predating the discovery driver) marks every server
    ``pending``. Returns the upserted ``workspace_mcp_tool_schemas`` rows.

    Stale-result guard: discovery can take up to ~30s (stdio cold-start), so
    before caching, each server's fingerprint is recomputed from its CURRENT
    DB config; results for servers edited or deleted mid-discovery are dropped
    (a late write would otherwise purge the newer config's snapshot).
    """
    rows: list[dict[str, Any]] = []
    discover = getattr(sandbox, "discover_user_mcp_schemas", None) if sandbox else None
    if discover is None:
        stale = await _stale_server_names(workspace_id, servers)
        for server in servers:
            if server.name in stale:
                continue
            rows.append(
                await upsert_tool_schemas(
                    workspace_id, server.name, mcp_discovery_fingerprint(server),
                    status="pending",
                )
            )
        return rows

    try:
        results: dict[str, dict[str, Any]] = await discover(servers)
    except Exception as exc:
        logger.warning("[MCP_DISCOVERY] sandbox discovery failed for %s: %s", workspace_id, exc)
        results = {s.name: {"status": "error", "error": str(exc), "tools": []} for s in servers}

    stale = await _stale_server_names(workspace_id, servers)
    for server in servers:
        fingerprint = mcp_discovery_fingerprint(server)
        if server.name in stale:
            logger.info(
                "[MCP_DISCOVERY] dropping stale result for %s/%s "
                "(config changed or server removed mid-discovery)",
                workspace_id,
                server.name,
            )
            continue
        result = results.get(server.name) or {
            "status": "error",
            "error": "no discovery result returned",
            "tools": [],
        }
        if result.get("status") != "ok":
            rows.append(
                await upsert_tool_schemas(
                    workspace_id,
                    server.name,
                    fingerprint,
                    status="error",
                    error=str(result.get("error") or "discovery failed")[:2000],
                )
            )
            continue
        kept, skipped = sanitize_discovered_tools(result.get("tools") or [])
        rows.append(
            await upsert_tool_schemas(
                workspace_id,
                server.name,
                fingerprint,
                tools=kept,
                status="ok",
                observed_meta={
                    "tool_count": len(kept),
                    "skipped": [list(item) for item in skipped],
                    "server_info": bounded_identity(result.get("server_info")),
                },
            )
        )
    return rows
