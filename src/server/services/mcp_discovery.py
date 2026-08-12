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
from typing import Any

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_sanitize import (
    discovery_affecting_payload,
    sanitize_tool_name,
    sanitize_tool_text,
)

from src.server.database import mcp_servers as mcp_db

logger = logging.getLogger(__name__)

# Discovery-boundary caps for hostile/buggy servers (plan §6). The prompt-side
# detailed-mode caps live in the formatter; these bound what we cache at all.
MAX_TOOLS_PER_SERVER = 64
MAX_SCHEMA_CHARS_PER_SERVER = 200_000


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
    ``src/server/app/vault.py``).
    """
    payload = discovery_affecting_payload(server, include_identity=False)
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def sanitize_discovered_tools(
    tools: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """Sanitize one server's raw ``tools/list`` snapshot for caching.

    Keeps the ORIGINAL tool name (wrappers must call the server by its real
    name; identifier sanitization happens again at codegen), but drops tools
    whose names cannot become a legal identifier or that collide after
    sanitization, sanitizes description text, and enforces count/size caps.
    Returns ``(kept, skipped)`` where skipped entries are ``(name, reason)``.
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
        if len(kept) >= MAX_TOOLS_PER_SERVER:
            skipped.append((name, f"server exceeds {MAX_TOOLS_PER_SERVER}-tool cap"))
            continue
        entry = {
            "name": name,
            "description": sanitize_tool_text(tool.get("description")),
            "input_schema": tool.get("input_schema") or {},
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

    A name is stale when its row is gone (deleted mid-discovery) or its
    recomputed fingerprint differs (edited mid-discovery). Malformed rows
    count as stale — dropping a result is always safe; clobbering is not.
    """
    from src.server.services.mcp_config import workspace_row_to_server_config

    rows = {
        r["name"]: r
        for r in await mcp_db.list_workspace_servers(workspace_id)
        if r.get("source") == "workspace"
    }
    stale: set[str] = set()
    for server in servers:
        row = rows.get(server.name)
        if row is None:
            stale.add(server.name)
            continue
        try:
            current_fp = mcp_discovery_fingerprint(workspace_row_to_server_config(row))
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
                await mcp_db.upsert_tool_schemas(
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
                await mcp_db.upsert_tool_schemas(
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
            await mcp_db.upsert_tool_schemas(
                workspace_id,
                server.name,
                fingerprint,
                tools=kept,
                status="ok",
                observed_meta={
                    "tool_count": len(kept),
                    "skipped": [list(item) for item in skipped],
                },
            )
        )
    return rows
