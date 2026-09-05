"""Vault-mutation → MCP cache invalidation, both tiers.

The discovery fingerprint hashes ``${vault:NAME}`` ref strings, never secret
values, so a value change alone can't churn any config hash. These tests pin
the explicit compensation: EVERY value change bumps the config version and
schedules a proactive apply — that pair is what carries the new value to the
sandbox, including for a secret only agent code reads — while the discovery
snapshot purge stays scoped to the servers that actually reference it. The bump
is pinned as the DURABLE half: it fires even when the DB reads that decide what
to purge fail underneath it.

The tiers are values, so a test swaps the two DB writes and the workspace
fan-out with ``dataclasses.replace``; a separate test pins that the shipped
tiers point at the real functions.
"""

from __future__ import annotations

import asyncio
import dataclasses
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.server.app.mcp_servers as mcp_servers_mod
import src.server.services.vault_invalidation as vi
from ptc_agent.config.core import MCPServerConfig
from src.server.services.vault_invalidation import USER_TIER, WORKSPACE_TIER, refs_for_server


def _ws_row(name: str, config: dict, enabled: bool = True) -> dict:
    return {"name": name, "source": "workspace", "enabled": enabled, "config": config}


def _tombstone_row(name: str) -> dict:
    """How an inherited user connector removed from a workspace is stored: a
    marker row with no config."""
    return {"name": name, "source": "user", "enabled": False, "config": None}


def _http_cfg(secret: str) -> dict:
    return {
        "transport": "http",
        "url": "https://api.example.com/mcp",
        "headers": {"Authorization": f"${{vault:{secret}}}"},
    }


def _user_row(name: str = "svc", **overrides) -> dict:
    row = {
        "name": name,
        "transport": "stdio",
        "command": "npx",
        "args": [],
        "url": None,
        "env": {},
        "headers": {},
        "description": "",
        "instruction": "",
    }
    row.update(overrides)
    return row


@pytest.fixture
def pushes(monkeypatch):
    """Intercept the sandbox push at the WorkspaceManager boundary, not at
    ``_push_secrets`` — stubbing the function itself would leave the half that
    decides WHICH workspaces get the new secret set untested."""
    push = AsyncMock()
    wm = MagicMock()
    wm.push_vault_secrets = push
    monkeypatch.setattr(
        vi, "WorkspaceManager", MagicMock(get_instance=MagicMock(return_value=wm))
    )
    return push


@pytest.fixture
def probes(monkeypatch, pushes):
    """Swap both DB writes, the workspace fan-out and the apply scheduler;
    return (purge_and_bump, bump, schedule)."""
    purge_bump = AsyncMock(return_value=1)
    bump = AsyncMock()
    sched = MagicMock()
    monkeypatch.setattr(mcp_servers_mod, "_schedule_proactive_apply", sched)
    # The workspace tier reads the Connectors catalog too (inherited servers);
    # tests that care about it override this.
    monkeypatch.setattr(vi, "list_catalog_servers", AsyncMock(return_value=[]))
    # …and the user tier reads every workspace's local rows.
    monkeypatch.setattr(vi, "list_local_servers_for_user", AsyncMock(return_value=[]))
    return purge_bump, bump, sched


def _tier(base, probes, workspaces=("ws-1",)):
    purge_bump, bump, _ = probes
    return dataclasses.replace(
        base,
        purge_and_bump=purge_bump,
        bump=bump,
        workspaces=AsyncMock(return_value=list(workspaces)),
    )


# ---------------------------------------------------------------------------
# refs_for_server
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"env": {"TOKEN": "${vault:API_KEY}"}},
        {"headers": {"Authorization": "Bearer ${vault:API_KEY}"}},
        {"args": ["--key", "${vault:API_KEY}"]},
        {"url": "https://example.com/mcp?k=${vault:API_KEY}"},
    ],
    ids=["env", "headers", "args", "url"],
)
def test_substituted_fields_are_scanned(kwargs):
    server = MCPServerConfig(name="svc", source="user", **kwargs)
    assert refs_for_server(server) == {"API_KEY"}


@pytest.mark.parametrize("field", ["description", "instruction"])
def test_free_text_fields_are_not_scanned(field):
    """These are never substituted, so a ref written there is just prose."""
    server = MCPServerConfig(
        name="svc", source="user", **{field: "use ${vault:API_KEY} here"}
    )
    assert refs_for_server(server) == set()


def test_collects_every_referenced_name():
    server = MCPServerConfig(
        name="svc",
        source="user",
        env={"A": "${vault:ONE}"},
        headers={"H": "${vault:TWO}"},
        args=["${vault:THREE}"],
        url="https://x/${vault:FOUR}",
    )
    assert refs_for_server(server) == {"ONE", "TWO", "THREE", "FOUR"}


# ---------------------------------------------------------------------------
# Workspace tier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_secret_change_purges_and_bumps_for_secret_using_server(
    monkeypatch, probes
):
    purge_bump, bump, sched = probes
    rows = [
        # Remote server authenticating via the changed secret: its discovery
        # runs WITH secrets, so its cached tools/list may depend on the value.
        _ws_row("authy", {
            "transport": "http",
            "url": "https://api.example.com/mcp",
            "headers": {"Authorization": "${vault:API_KEY}"},
        }),
        # References a DIFFERENT secret — untouched.
        _ws_row("other", {
            "transport": "stdio",
            "command": "npx",
            "env": {"TOKEN": "${vault:OTHER_KEY}"},
        }),
    ]
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    # Purge and version bump ride ONE atomic call; no separate bump.
    purge_bump.assert_awaited_once_with("ws-1", ["authy"])
    bump.assert_not_awaited()
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_stdio_env_ref_bumps_without_purge(monkeypatch, probes):
    """A stdio server's discovery runs secret-less, so its snapshot can't
    depend on the value — no purge, but the bump still re-resolves the live
    session (covers the needs_secret → ready transition)."""
    purge_bump, bump, sched = probes
    rows = [
        _ws_row("plain", {
            "transport": "stdio",
            "command": "npx",
            "env": {"TOKEN": "${vault:API_KEY}"},
        }),
    ]
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_unreferenced_secret_still_bumps_and_applies(monkeypatch, probes):
    """A secret no server references is consumed by agent code in the sandbox
    (``vault.get()`` / ``load_env()``), and the bump is the only thing that can
    deliver it: a warm session re-syncs its assets — vault push included — only
    on a config-version delta, so skipping the bump leaves the retired value
    readable in the sandbox indefinitely. The purge stays out of it: no cached
    discovery can depend on a secret nothing resolves.
    """
    purge_bump, bump, sched = probes
    rows = [
        _ws_row("authy", {
            "transport": "http",
            "url": "https://api.example.com/mcp",
            "headers": {"Authorization": "${vault:API_KEY}"},
        }),
    ]
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "UNRELATED", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_unreferenced_secret_is_pushed_to_live_sandboxes(monkeypatch, probes, pushes):
    """The same-process fast path fires too — the bump above is what covers the
    other workers, this is what makes the mutating one immediate."""
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=[]))

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "UNRELATED", user_id="user-1"
    )

    pushes.assert_awaited_once_with("ws-1", user_id="user-1")


@pytest.mark.asyncio
async def test_cancelled_push_cannot_strand_the_bump(monkeypatch, probes, pushes):
    """REGRESSION: the durable bump runs BEFORE the sandbox push. The push does
    seconds of I/O in request context and a client disconnect cancels it with
    CancelledError, which clears its ``except Exception`` — push-first left a
    committed rotation with no convergence trigger, invisible to every later
    read because fingerprints hash refs, not values."""
    purge_bump, bump, _ = probes
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=[]))
    pushes.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await vi.after_secret_change(
            _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
        )

    bump.assert_awaited_once_with("ws-1")


@pytest.mark.asyncio
async def test_description_only_edit_skips_the_cache_half(monkeypatch, probes):
    purge_bump, bump, sched = probes
    servers = AsyncMock()
    monkeypatch.setattr(vi, "list_workspace_servers", servers)

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY",
        user_id="user-1", value_changed=False,
    )

    servers.assert_not_awaited()
    purge_bump.assert_not_awaited()
    bump.assert_not_awaited()
    sched.assert_not_called()


@pytest.mark.asyncio
async def test_invalidation_failure_never_raises(monkeypatch, probes):
    """Best-effort: a DB failure during invalidation must not fail the vault
    mutation that triggered it."""
    monkeypatch.setattr(
        vi, "list_workspace_servers", AsyncMock(side_effect=RuntimeError("db down"))
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )  # no raise


# ---------------------------------------------------------------------------
# Failure domains
#
# The version bump is the ONLY durable convergence trigger — the warm path
# re-pushes the vault solely on a version delta — so it must not share a
# failure domain with the DB reads that merely decide what to purge.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_failure_still_bumps_the_config_version(monkeypatch, probes):
    """A transient read failure would otherwise skip the bump while the CRUD
    endpoint reports success, leaving the rotated credential usable from an
    always-on sandbox indefinitely. The bare bump needs none of the scan's
    inputs; over-invalidating costs one re-resolve."""
    purge_bump, bump, sched = probes
    monkeypatch.setattr(
        vi, "list_workspace_servers", AsyncMock(side_effect=RuntimeError("db down"))
    )

    # Still no raise: the endpoint-visible outcome is unchanged.
    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_purge_failure_falls_back_to_a_bare_bump(monkeypatch, probes):
    """Same domain as the scan: the atomic purge+bump failing must not take the
    bump with it."""
    purge_bump, bump, _ = probes
    purge_bump.side_effect = RuntimeError("db down")
    monkeypatch.setattr(
        vi,
        "list_workspace_servers",
        AsyncMock(return_value=[_ws_row("authy", _http_cfg("API_KEY"))]),
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("ws-1", ["authy"])
    bump.assert_awaited_once_with("ws-1")


@pytest.mark.asyncio
async def test_fallback_bump_failure_still_does_not_raise(monkeypatch, probes):
    """Nothing is left to try — the mutation still succeeds, and the log is what
    says the tier is unconverged."""
    _, bump, sched = probes
    bump.side_effect = RuntimeError("db down")
    monkeypatch.setattr(
        vi, "list_workspace_servers", AsyncMock(side_effect=RuntimeError("db down"))
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )  # no raise

    sched.assert_called_once_with("ws-1", "user-1")


# ---------------------------------------------------------------------------
# Workspace tier: inherited connectors
#
# An inherited connector exists in workspace_mcp_servers only as a marker row,
# so scanning workspace rows alone finds none of them — yet a workspace secret
# resolves its refs (one merged namespace) and its discovery snapshot is cached
# in THIS workspace, so the workspace tier is what must purge and bump it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_inherited_connector_is_scanned_by_a_workspace_secret(
    monkeypatch, probes
):
    purge_bump, bump, sched = probes
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "authy", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
            ),
        ]),
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("ws-1", ["authy"])
    bump.assert_not_awaited()
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_tombstoned_inherited_connector_is_skipped(monkeypatch, probes):
    """The connector was removed from this workspace, so no config it holds can
    go stale here — it must not count as referencing. The value still moved, so
    the bump and apply fire regardless (as they do for any value change)."""
    purge_bump, bump, sched = probes
    monkeypatch.setattr(
        vi, "list_workspace_servers", AsyncMock(return_value=[_tombstone_row("authy")])
    )
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "authy", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
            ),
        ]),
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_locally_shadowed_name_is_scanned_once(monkeypatch, probes):
    """A duplicated name would purge the same snapshot row twice."""
    purge_bump, _, _ = probes
    monkeypatch.setattr(
        vi,
        "list_workspace_servers",
        AsyncMock(return_value=[_ws_row("authy", _http_cfg("API_KEY"))]),
    )
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "authy", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
            ),
        ]),
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("ws-1", ["authy"])


@pytest.mark.asyncio
async def test_shadowed_inherited_config_is_not_scanned(monkeypatch, probes):
    """The local fork is what runs — even disabled, it never falls back to the
    inherited config — so only the fork's own refs count toward the purge."""
    purge_bump, bump, sched = probes
    monkeypatch.setattr(
        vi,
        "list_workspace_servers",
        AsyncMock(return_value=[_ws_row("authy", _http_cfg("OTHER_KEY"), enabled=False)]),
    )
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "authy", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
            ),
        ]),
    )

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")
    sched.assert_called_once_with("ws-1", "user-1")


# ---------------------------------------------------------------------------
# User tier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_user_secret_purges_user_snapshots_and_fans_out(monkeypatch, probes):
    """The user tier gets the same purge the workspace tier always had, and the
    proactive apply reaches every RUNNING workspace of the user."""
    purge_bump, bump, sched = probes
    rows = [
        _user_row(
            "authy",
            transport="http",
            command=None,
            url="https://api.example.com/mcp",
            headers={"Authorization": "${vault:API_KEY}"},
        ),
    ]
    monkeypatch.setattr(vi, "list_catalog_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(USER_TIER, probes, workspaces=("ws-1", "ws-2")),
        "user-1", "API_KEY", user_id="user-1",
    )

    purge_bump.assert_awaited_once_with("user-1", ["authy"])
    bump.assert_not_awaited()
    assert [c.args for c in sched.call_args_list] == [
        ("ws-1", "user-1"), ("ws-2", "user-1"),
    ]


@pytest.mark.asyncio
async def test_user_free_text_reference_does_not_purge(monkeypatch, probes):
    """Prose mentioning a ref never resolves it, so no snapshot can depend on
    the value — the purge stays out even though the bump fires."""
    purge_bump, bump, sched = probes
    rows = [_user_row(description="set ${vault:API_KEY} first")]
    monkeypatch.setattr(vi, "list_catalog_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(USER_TIER, probes), "user-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_user_secret_purges_a_workspace_local_server(monkeypatch, probes):
    """A workspace-LOCAL server resolves user secrets too (one merged namespace,
    workspace-wins), and its fingerprint hashes the ref string, not the value —
    so a name that never reaches the purge leaves the stale ``ok`` snapshot to
    be re-accepted after every bump, forever."""
    purge_bump, bump, _ = probes
    monkeypatch.setattr(
        vi,
        "list_local_servers_for_user",
        AsyncMock(return_value=[_ws_row("local-authy", _http_cfg("API_KEY"))]),
    )

    await vi.after_secret_change(
        _tier(USER_TIER, probes), "user-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("user-1", ["local-authy"])
    bump.assert_not_awaited()


@pytest.mark.asyncio
async def test_locally_forked_catalog_name_purges_once(monkeypatch, probes):
    """The purge deletes by name across both tiers and every workspace, so the
    fork and the server it shadows collapse to one entry."""
    purge_bump, _, _ = probes
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "authy", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
            ),
        ]),
    )
    monkeypatch.setattr(
        vi,
        "list_local_servers_for_user",
        AsyncMock(return_value=[_ws_row("authy", _http_cfg("API_KEY"))]),
    )

    await vi.after_secret_change(
        _tier(USER_TIER, probes), "user-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("user-1", ["authy"])


@pytest.mark.asyncio
async def test_user_stdio_ref_bumps_every_workspace(monkeypatch, probes):
    purge_bump, bump, _ = probes
    rows = [_user_row(env={"TOKEN": "${vault:API_KEY}"})]
    monkeypatch.setattr(vi, "list_catalog_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(USER_TIER, probes), "user-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------


def test_shipped_tiers_write_their_own_tables():
    """The tests above swap the callables, so pin the real wiring here — a tier
    crossed over would invalidate the wrong owner's cache."""
    from src.server.database.mcp_servers import (
        bump_user_workspaces_mcp_version,
        bump_workspace_mcp_version,
    )
    from src.server.database.mcp_tool_schemas import (
        delete_tool_schemas_and_bump,
        delete_user_and_workspace_tool_schemas_and_bump,
    )
    from src.server.database.workspace import get_running_workspace_ids_for_user

    assert WORKSPACE_TIER.purge_and_bump is delete_tool_schemas_and_bump
    assert WORKSPACE_TIER.bump is bump_workspace_mcp_version
    # The user tier's purge MUST be the both-tiers one: a user server caches
    # in-sandbox discovery per workspace, and the user-tier-only purge left
    # those same-hash rows to be served forever.
    assert (
        USER_TIER.purge_and_bump is delete_user_and_workspace_tool_schemas_and_bump
    )
    assert USER_TIER.bump is bump_user_workspaces_mcp_version
    assert USER_TIER.workspaces is get_running_workspace_ids_for_user
    assert WORKSPACE_TIER.workspaces is vi._own_workspace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "workspace, expected",
    [
        ({"status": "running"}, ["ws-9"]),
        ({"status": "stopped"}, []),
        ({"status": "starting"}, []),
        (None, []),
    ],
    ids=["running", "stopped", "starting", "missing"],
)
async def test_workspace_tier_converges_only_itself_and_only_while_running(
    monkeypatch, workspace, expected
):
    """Both tiers converge RUNNING workspaces only. A proactive apply from any
    worker cold-starts an idle sandbox, and that start pushes the vault
    unconditionally anyway — so waking one buys nothing and costs a restart.
    """
    monkeypatch.setattr(vi, "get_workspace", AsyncMock(return_value=workspace))
    assert await WORKSPACE_TIER.workspaces("ws-9") == expected


@pytest.mark.asyncio
async def test_disable_markers_are_not_scanned(monkeypatch, probes):
    """Rows with no config (built-in disable markers) carry no refs, so nothing
    to purge."""
    purge_bump, bump, _ = probes
    rows = [{"name": "builtin", "source": "builtin", "enabled": False, "config": None}]
    monkeypatch.setattr(vi, "list_workspace_servers", AsyncMock(return_value=rows))

    await vi.after_secret_change(
        _tier(WORKSPACE_TIER, probes), "ws-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("ws-1")


@pytest.mark.asyncio
async def test_disabled_catalog_connector_is_still_scanned(monkeypatch, probes):
    """A snapshot outlives the row being switched off, and re-enabling bumps
    versions without purging — so the scan must cover disabled rows or their
    snapshots stay fingerprint-valid under a rotated secret forever."""
    purge_bump, bump, sched = probes
    monkeypatch.setattr(vi, "list_local_servers_for_user", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        vi,
        "list_catalog_servers",
        AsyncMock(return_value=[
            _user_row(
                "dormant", transport="http", command=None,
                url="https://api.example.com/mcp",
                headers={"Authorization": "${vault:API_KEY}"},
                enabled=False,
            ),
        ]),
    )

    await vi.after_secret_change(
        _tier(USER_TIER, probes), "user-1", "API_KEY", user_id="user-1"
    )

    purge_bump.assert_awaited_once_with("user-1", ["dormant"])
