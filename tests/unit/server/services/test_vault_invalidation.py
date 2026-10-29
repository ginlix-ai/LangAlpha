"""Vault-mutation → MCP cache invalidation.

The discovery fingerprint hashes ``${vault:NAME}`` ref strings, never secret
values, so a value change alone can't churn any config hash. These tests pin
the explicit compensation: EVERY value change bumps the config version and
schedules a proactive apply — that pair is what carries the new value to the
sandbox, including for a secret only agent code reads — while the discovery
snapshot purge stays scoped to the servers that actually reference it. The bump
is pinned as the DURABLE half: it fires even when the DB reads that decide what
to purge fail underneath it.

There is one vault per user, so the scan is the user's whole Plugins catalog
and every write fans out across the user's workspaces.
"""

from __future__ import annotations

import asyncio
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

import src.server.services.vault_invalidation as vi
from ptc_agent.config.core import MCPServerConfig
from src.server.services.vault_invalidation import refs_for_server


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


def _http_row(name: str, secret: str, **overrides) -> dict:
    """A remote catalog row authenticating with ``secret``: its discovery runs
    WITH secrets, so its cached tools/list may depend on the value."""
    return _user_row(
        name,
        transport="http",
        command=None,
        url="https://api.example.com/mcp",
        headers={"Authorization": f"${{vault:{secret}}}"},
        **overrides,
    )


@pytest.fixture
def pushes(monkeypatch):
    """Intercept the sandbox push at the WorkspaceManager boundary, not at
    ``_push_secrets`` — stubbing the function itself would leave the half that
    decides WHICH workspaces get the new secret set untested."""
    push = AsyncMock()
    wm = MagicMock()
    wm.push_user_vault = push
    monkeypatch.setattr(
        vi, "WorkspaceManager", MagicMock(get_instance=MagicMock(return_value=wm))
    )
    return push


@pytest.fixture
def running(monkeypatch):
    """The user's running workspaces; tests that fan out override the value."""
    reader = AsyncMock(return_value=["ws-1"])
    monkeypatch.setattr(vi, "get_running_workspace_ids_for_user", reader)
    return reader


@pytest.fixture
def rediscover(monkeypatch):
    """The host-side refill spawns a task that reaches the database; recorded
    so a unit run never fires one."""
    calls = MagicMock()
    monkeypatch.setattr(vi, "_rediscover_catalog_rows", calls)
    return calls


@pytest.fixture
def probes(monkeypatch, pushes, running, rediscover):
    """Swap both DB writes, the catalog read and the apply scheduler;
    return (purge_and_bump, bump, schedule)."""
    purge_bump = AsyncMock(return_value=1)
    bump = AsyncMock()
    sched = MagicMock()
    monkeypatch.setattr(vi, "delete_user_and_workspace_tool_schemas_and_bump", purge_bump)
    monkeypatch.setattr(vi, "bump_user_workspaces_mcp_version", bump)
    monkeypatch.setattr(vi, "schedule_proactive_apply", sched)
    monkeypatch.setattr(vi, "list_catalog_servers", AsyncMock(return_value=[]))
    return purge_bump, bump, sched


def _catalog(monkeypatch, rows=None, **kw) -> AsyncMock:
    reader = AsyncMock(return_value=list(rows or []), **kw)
    monkeypatch.setattr(vi, "list_catalog_servers", reader)
    return reader


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
# after_secret_change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_secret_change_purges_and_fans_out(monkeypatch, probes, running, pushes):
    """Only the referencing server is purged, the purge and the version bump
    ride ONE atomic call, and the apply and push reach every RUNNING workspace
    of the user: a proactive apply cold-starts an idle sandbox, and every start
    path pushes the vault anyway."""
    purge_bump, bump, sched = probes
    running.return_value = ["ws-1", "ws-2"]
    _catalog(monkeypatch, [
        _http_row("authy", "API_KEY"),
        _user_row("other", env={"TOKEN": "${vault:OTHER_KEY}"}),
    ])

    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_awaited_once_with("user-1", ["authy"], description_scopes=ANY)
    assert len(purge_bump.await_args.kwargs["description_scopes"]) == 1
    bump.assert_not_awaited()
    running.assert_awaited_with("user-1")
    assert [c.args for c in sched.call_args_list] == [
        ("ws-1", "user-1"), ("ws-2", "user-1"),
    ]
    pushes.assert_awaited_once_with("user-1", ["ws-1", "ws-2"])


@pytest.mark.asyncio
async def test_purged_rows_are_rediscovered_host_side(monkeypatch, probes, rediscover):
    """The purge emptied their snapshot and no sandbox refills a remote row's."""
    _catalog(monkeypatch, [_http_row("authy", "API_KEY")])

    await vi.after_secret_change("user-1", "API_KEY")

    rediscover.assert_called_once_with("user-1", ["authy"])


@pytest.mark.asyncio
async def test_stdio_env_ref_bumps_without_purge(monkeypatch, probes, rediscover):
    """A stdio server's discovery runs secret-less, so its snapshot can't
    depend on the value — no purge, but the bump still re-resolves the live
    session (covers the needs_secret → ready transition)."""
    purge_bump, bump, sched = probes
    _catalog(monkeypatch, [_user_row("plain", env={"TOKEN": "${vault:API_KEY}"})])

    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")
    sched.assert_called_once_with("ws-1", "user-1")
    rediscover.assert_not_called()


@pytest.mark.asyncio
async def test_unreferenced_secret_still_bumps_and_applies(monkeypatch, probes, pushes):
    """A secret no server references is consumed by agent code in the sandbox
    (``vault.get()`` / ``load_env()``), and the bump is the only thing that can
    deliver it: a warm session re-syncs its assets — vault push included — only
    on a config-version delta, so skipping the bump leaves the retired value
    readable in the sandbox indefinitely. The purge stays out of it: no cached
    discovery can depend on a secret nothing resolves. The same-process push
    fires too; the bump is what covers the other workers.
    """
    purge_bump, bump, sched = probes
    _catalog(monkeypatch, [_http_row("authy", "API_KEY")])

    await vi.after_secret_change("user-1", "UNRELATED")

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")
    sched.assert_called_once_with("ws-1", "user-1")
    pushes.assert_awaited_once_with("user-1", ["ws-1"])


@pytest.mark.asyncio
async def test_free_text_reference_does_not_purge(monkeypatch, probes):
    """Prose mentioning a ref never resolves it, so no snapshot can depend on
    the value, so the purge stays out even though the bump fires."""
    purge_bump, bump, _ = probes
    _catalog(monkeypatch, [_user_row(description="set ${vault:API_KEY} first")])

    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")


@pytest.mark.asyncio
async def test_disabled_catalog_connector_is_still_scanned(monkeypatch, probes):
    """A snapshot outlives the row being switched off, and re-enabling bumps
    versions without purging, so the scan must cover disabled rows or their
    snapshots stay fingerprint-valid under a rotated secret forever."""
    purge_bump, _, _ = probes
    _catalog(monkeypatch, [_http_row("dormant", "API_KEY", enabled=False)])

    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_awaited_once_with("user-1", ["dormant"], description_scopes=ANY)


@pytest.mark.asyncio
async def test_cancelled_push_cannot_strand_the_bump(monkeypatch, probes, pushes):
    """REGRESSION: the durable bump runs BEFORE the sandbox push. The push does
    seconds of I/O in request context and a client disconnect cancels it with
    CancelledError, which clears its ``except Exception`` — push-first left a
    committed rotation with no convergence trigger, invisible to every later
    read because fingerprints hash refs, not values."""
    _, bump, _ = probes
    pushes.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await vi.after_secret_change("user-1", "API_KEY")

    bump.assert_awaited_once_with("user-1")


@pytest.mark.asyncio
async def test_description_only_edit_skips_the_cache_half(monkeypatch, probes):
    purge_bump, bump, sched = probes
    reader = _catalog(monkeypatch)

    await vi.after_secret_change("user-1", "API_KEY", value_changed=False)

    reader.assert_not_awaited()
    purge_bump.assert_not_awaited()
    bump.assert_not_awaited()
    sched.assert_not_called()


@pytest.mark.asyncio
async def test_invalidation_failure_never_raises(monkeypatch, probes):
    """Best-effort: a DB failure during invalidation must not fail the vault
    mutation that triggered it."""
    _catalog(monkeypatch, side_effect=RuntimeError("db down"))

    await vi.after_secret_change("user-1", "API_KEY")  # no raise


# ---------------------------------------------------------------------------
# after_secrets_changed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_batch_purges_per_name_and_converges_once(
    monkeypatch, probes, running, pushes, rediscover
):
    """The purge stays per name, scoped to that credential's servers; the
    catalog read, the applies and the push act on the whole vault and run once
    for the batch, however many names a plugin declares. A repeated name is
    one name."""
    purge_bump, bump, sched = probes
    running.return_value = ["ws-1", "ws-2"]
    reader = _catalog(monkeypatch, [
        _http_row("alpha", "ALPHA_KEY"),
        _http_row("beta", "BETA_KEY"),
        _user_row("gamma", env={"TOKEN": "${vault:GAMMA_KEY}"}),
    ])

    await vi.after_secrets_changed(
        "user-1", ["ALPHA_KEY", "BETA_KEY", "ALPHA_KEY", "GAMMA_KEY"]
    )

    reader.assert_awaited_once_with("user-1")
    assert [c.args for c in purge_bump.await_args_list] == [
        ("user-1", ["alpha"]), ("user-1", ["beta"]),
    ]
    bump.assert_awaited_once_with("user-1")  # GAMMA_KEY: stdio, no purge
    assert [c.args for c in sched.call_args_list] == [
        ("ws-1", "user-1"), ("ws-2", "user-1"),
    ]
    pushes.assert_awaited_once_with("user-1", ["ws-1", "ws-2"])
    rediscover.assert_called_once_with("user-1", ["alpha", "beta"])


@pytest.mark.asyncio
async def test_an_empty_batch_touches_nothing(monkeypatch, probes, pushes):
    purge_bump, bump, sched = probes
    reader = _catalog(monkeypatch)

    await vi.after_secrets_changed("user-1", [])

    reader.assert_not_awaited()
    purge_bump.assert_not_awaited()
    bump.assert_not_awaited()
    sched.assert_not_called()
    pushes.assert_not_awaited()


# ---------------------------------------------------------------------------
# Failure domains
#
# The version bump is the ONLY durable convergence trigger (the warm path
# re-pushes the vault solely on a version delta), so it must not share a
# failure domain with the DB reads that merely decide what to purge.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_failure_still_bumps_the_config_version(monkeypatch, probes):
    """A transient read failure would otherwise skip the bump while the CRUD
    endpoint reports success, leaving the rotated credential usable from an
    always-on sandbox indefinitely. The bare bump needs none of the scan's
    inputs; over-invalidating costs one re-resolve."""
    purge_bump, bump, sched = probes
    _catalog(monkeypatch, side_effect=RuntimeError("db down"))

    # Still no raise: the endpoint-visible outcome is unchanged.
    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_not_awaited()
    bump.assert_awaited_once_with("user-1")
    sched.assert_called_once_with("ws-1", "user-1")


@pytest.mark.asyncio
async def test_a_failed_batch_scan_bumps_for_every_name(monkeypatch, probes):
    """The batch hands the loop no row set when its one read fails, so each
    name re-reads and falls back to its own bump."""
    _, bump, _ = probes
    _catalog(monkeypatch, side_effect=RuntimeError("db down"))

    await vi.after_secrets_changed("user-1", ["ONE", "TWO"])

    assert bump.await_count == 2


@pytest.mark.asyncio
async def test_purge_failure_falls_back_to_a_bare_bump(monkeypatch, probes):
    """Same domain as the scan: the atomic purge+bump failing must not take the
    bump with it."""
    purge_bump, bump, _ = probes
    purge_bump.side_effect = RuntimeError("db down")
    _catalog(monkeypatch, [_http_row("authy", "API_KEY")])

    await vi.after_secret_change("user-1", "API_KEY")

    purge_bump.assert_awaited_once_with("user-1", ["authy"], description_scopes=ANY)
    bump.assert_awaited_once_with("user-1")


@pytest.mark.asyncio
async def test_fallback_bump_failure_still_does_not_raise(monkeypatch, probes):
    """Nothing is left to try: the mutation still succeeds, and the log is what
    says the user is unconverged."""
    _, bump, sched = probes
    bump.side_effect = RuntimeError("db down")
    _catalog(monkeypatch, side_effect=RuntimeError("db down"))

    await vi.after_secret_change("user-1", "API_KEY")  # no raise

    sched.assert_called_once_with("ws-1", "user-1")
