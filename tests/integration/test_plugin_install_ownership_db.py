"""An install racing another plugin write for one name, against real PostgreSQL.

Install refuses a server or skill another plugin owns by reading the account's
rows, but a plugin's rows only land in its fan-out, well after that read. The
refusal holds only if no other plugin write can take the name between one
install's read and its own write, which is a property of the lock and so needs
the real one.

A plugin write that dies partway must also have moved every workspace's MCP
config version past the server rows it already committed: a warm session keeps
its config until that number changes, and nothing retries the write.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
from unittest.mock import AsyncMock, patch

import pytest

from tests.unit.server.mcp_builders import all_passing, zip_package

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

@pytest.fixture(autouse=True)
def _every_server_passes_its_check():
    """The checks dial vendors and start the user's computer; what these tests
    pin is the lock and the version bump around the writes that follow."""
    with patch(
        "src.server.services.plugins.gate.check_servers",
        new=AsyncMock(side_effect=all_passing),
    ):
        yield


PLUGIN_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
MCP_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
SKILL, SERVER = "quarterly-digest", "feed"
FIRST, RIVAL = "first-plugin", "second-plugin"
HTTP = {"type": "streamable-http", "url": "https://example.com/mcp"}
SSE = {"type": "sse", "url": "https://example.com/sse"}


def _package(
    name: str,
    *,
    skill: str | None = None,
    server: dict | None = None,
    version: str = "1.0.0",
):
    from src.server.services.plugins import validate_package

    members = {
        "plugin.json": json.dumps(
            {"$schema": PLUGIN_SCHEMA, "name": name, "version": version}
        ).encode(),
    }
    if skill is not None:
        members[f"skills/{skill}/SKILL.md"] = (
            f"---\nname: {skill}\ndescription: A test skill that does a thing.\n"
            "---\n\nBody.\n"
        ).encode()
    if server is not None:
        members["mcp.json"] = json.dumps(
            {"$schema": MCP_SCHEMA, "mcpServers": {SERVER: server}}
        ).encode()
    return validate_package(zip_package(members))


async def _install(user_id: str, package):
    from src.server.services.plugins import lifecycle

    return await lifecycle.install_plugin_package(
        user_id, package, source_type="zip", source_ref=None
    )


async def _by_install(user_id: str):
    return lambda: _install(user_id, _package(FIRST, skill=SKILL))


async def _by_update(user_id: str):
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins.update import update_plugin_package

    await _install(user_id, _package(FIRST))
    plugin = await get_plugin(user_id, FIRST)
    return lambda: update_plugin_package(
        user_id, plugin, _package(FIRST, skill=SKILL), source_ref=None
    )


async def _by_sse_upgrade(user_id: str):
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins.post_install import apply_sse_upgrades

    await _install(user_id, _package(FIRST, server=SSE))
    plugin = await get_plugin(user_id, FIRST)
    return lambda: apply_sse_upgrades(user_id, plugin, [SERVER])


_SKILL_REFUSAL = f"Skill {SKILL!r} is owned by plugin {FIRST!r}; uninstall it first"
_SERVER_REFUSAL = (
    f"MCP server {SERVER!r} is owned by plugin {FIRST!r}; uninstall it first"
)

# The write that claims the name, where it claims it, and the rival install
# that wants the same name with the refusal it owes once the claim has landed.
_CLAIMS = {
    "install": (
        _by_install,
        "src.server.services.plugins.lifecycle.fan_out_skills",
        lambda: _package(RIVAL, skill=SKILL),
        _SKILL_REFUSAL,
    ),
    "update": (
        _by_update,
        "src.server.services.plugins.update.fan_out_skills",
        lambda: _package(RIVAL, skill=SKILL),
        _SKILL_REFUSAL,
    ),
    "sse-upgrade": (
        _by_sse_upgrade,
        "src.server.services.plugins.post_install.fan_out_servers",
        lambda: _package(RIVAL, server=HTTP),
        _SERVER_REFUSAL,
    ),
}


@pytest.mark.parametrize("claim", list(_CLAIMS))
async def test_an_install_racing_a_claim_on_its_name_is_refused_whole(
    claim, seed_user, patched_get_db_connection, test_user_id
):
    from src.server.database.plugins import list_plugins
    from src.server.services.plugins import lifecycle

    setup, writes_at, rival_package, refusal = _CLAIMS[claim]
    module, attr = writes_at.rsplit(".", 1)
    write = getattr(importlib.import_module(module), attr)
    create_plugin = lifecycle.create_plugin
    claiming, checked = asyncio.Event(), asyncio.Event()

    async def mark_checked(user_id, name, **kwargs):
        # Only an install its ownership check let through gets here.
        if name == RIVAL:
            checked.set()
        return await create_plugin(user_id, name, **kwargs)

    async def stalled_write(*args, **kwargs):
        # Hold the claim's first write until the rival has checked too: the
        # interleaving that loses the refusal. Bounded, because writes that
        # serialize never let the rival check while the claim is running.
        if not claiming.is_set():
            claiming.set()
            try:
                await asyncio.wait_for(checked.wait(), timeout=1.0)
            except TimeoutError:
                pass
        return await write(*args, **kwargs)

    with (
        patch(
            "src.server.services.skill_archive_storage.is_configured",
            return_value=False,
        ),
        # Reads its count positionally, which the test pool's dict rows break.
        patch(
            "src.server.services.plugins.skill_fanout.count_user_skills",
            new=AsyncMock(return_value=0),
        ),
        # Network: the held-back sse probe and the discovery after a create.
        patch(
            "src.server.services.plugins.server_fanout._probe_sse_entries",
            new=AsyncMock(return_value={}),
        ),
        patch("src.server.services.plugins.server_fanout.schedule_catalog_discovery"),
    ):
        run_claim = await setup(test_user_id)
        with (
            patch.object(lifecycle, "create_plugin", mark_checked),
            patch(writes_at, stalled_write),
        ):
            first = asyncio.create_task(run_claim())
            await claiming.wait()
            rival = asyncio.create_task(_install(test_user_id, rival_package()))
            refused = (await asyncio.gather(rival, return_exceptions=True))[0]
            await first

    assert isinstance(refused, ValueError), refused
    assert str(refused) == refusal
    assert [p["name"] for p in await list_plugins(test_user_id)] == [FIRST]


ALPHA, BETA = "alpha-digest", "beta-digest"


async def _first_by_install(user_id: str):
    return (
        "src.server.services.plugins.lifecycle.after_secrets_changed",
        lambda: _install(user_id, _package(FIRST, skill=ALPHA)),
    )


async def _first_by_update(user_id: str):
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins.update import update_plugin_package

    await _install(user_id, _package(FIRST))
    plugin = await get_plugin(user_id, FIRST)
    return (
        "src.server.services.plugins.update.after_secrets_changed",
        lambda: update_plugin_package(
            user_id, plugin, _package(FIRST, skill=ALPHA), source_ref=None
        ),
    )


@pytest.mark.parametrize("first", ["install", "update"])
async def test_the_row_names_the_package_whose_components_landed(
    first, seed_user, patched_get_db_connection, test_user_id
):
    """A write that let the lock go before its row would record its own
    package over an update that landed its components in between."""
    from src.server.database.plugins import get_plugin, list_plugin_skill_names
    from src.server.services.plugins.update import update_plugin_package

    setup = {"install": _first_by_install, "update": _first_by_update}[first]
    after_lock, run_first = await setup(test_user_id)
    module, attr = after_lock.rsplit(".", 1)
    side_effects = getattr(importlib.import_module(module), attr)
    second = _package(FIRST, skill=BETA)
    stalled, second_done = asyncio.Event(), asyncio.Event()

    async def stall_once(*args, **kwargs):
        # The first write's post-lock step waits out a whole second update.
        if not stalled.is_set():
            stalled.set()
            try:
                await asyncio.wait_for(second_done.wait(), timeout=5.0)
            except TimeoutError:
                pass
        return await side_effects(*args, **kwargs)

    async def run_second():
        await stalled.wait()
        plugin = await get_plugin(test_user_id, FIRST)
        try:
            return await update_plugin_package(
                test_user_id, plugin, second, source_ref=None
            )
        finally:
            second_done.set()

    with (
        patch(
            "src.server.services.skill_archive_storage.is_configured",
            return_value=False,
        ),
        patch(
            "src.server.services.plugins.skill_fanout.count_user_skills",
            new=AsyncMock(return_value=0),
        ),
        patch(after_lock, stall_once),
    ):
        await asyncio.gather(run_first(), run_second())

    row = await get_plugin(test_user_id, FIRST)
    skills = await list_plugin_skill_names(test_user_id, row["user_plugin_id"])
    assert [s["name"] for s in skills] == [BETA]
    assert row["content_hash"] == second.content_hash


@pytest.mark.parametrize("stale_skill", [BETA, ALPHA])
async def test_an_update_that_outlived_its_plugin_is_a_conflict(
    stale_skill, seed_user, patched_get_db_connection, test_user_id
):
    """The update read its plugin before the fetch; a reinstall under the same
    name since then is another plugin, whose row and skills it must not touch,
    whether or not the fetched package shares a skill with the replacement."""
    from src.server.database.plugins import get_plugin, list_plugin_skill_names
    from src.server.services.plugins import lifecycle
    from src.server.services.plugins.update import update_plugin_package

    with (
        patch(
            "src.server.services.skill_archive_storage.is_configured",
            return_value=False,
        ),
        patch(
            "src.server.services.plugins.skill_fanout.count_user_skills",
            new=AsyncMock(return_value=0),
        ),
    ):
        await _install(test_user_id, _package(FIRST, skill=ALPHA))
        stale = await get_plugin(test_user_id, FIRST)
        await lifecycle.uninstall_plugin(test_user_id, stale)
        replacement = _package(FIRST, skill=BETA)
        await _install(test_user_id, replacement)
        with pytest.raises(ValueError, match="uninstalled while this update waited"):
            await update_plugin_package(
                test_user_id,
                stale,
                _package(FIRST, skill=stale_skill, version="2.0.0"),
                source_ref=None,
            )

    row = await get_plugin(test_user_id, FIRST)
    skills = await list_plugin_skill_names(test_user_id, row["user_plugin_id"])
    assert row["user_plugin_id"] != stale["user_plugin_id"]
    assert (row["version"], row["content_hash"]) == ("1.0.0", replacement.content_hash)
    assert [s["name"] for s in skills] == [BETA]


async def test_an_uninstall_waits_out_an_update(
    seed_user, patched_get_db_connection, test_user_id
):
    """The update found its plugin under the fan-out lock and writes against it
    until it lets go, so an uninstall landing in between waits, then deletes
    what the update left."""
    from src.server.database.plugins import get_plugin, list_plugin_skill_names
    from src.server.services.plugins import lifecycle, update

    in_lock, release = asyncio.Event(), asyncio.Event()
    update_skills = update._update_skills

    async def stalled(*args, **kwargs):
        in_lock.set()
        await asyncio.wait_for(release.wait(), timeout=10.0)
        return await update_skills(*args, **kwargs)

    with (
        patch(
            "src.server.services.skill_archive_storage.is_configured",
            return_value=False,
        ),
        patch(
            "src.server.services.plugins.skill_fanout.count_user_skills",
            new=AsyncMock(return_value=0),
        ),
    ):
        await _install(test_user_id, _package(FIRST, skill=ALPHA))
        before = await get_plugin(test_user_id, FIRST)
        with patch.object(update, "_update_skills", stalled):
            updating = asyncio.create_task(
                update.update_plugin_package(
                    test_user_id,
                    before,
                    _package(FIRST, skill=BETA, version="2.0.0"),
                    source_ref=None,
                )
            )
            await in_lock.wait()
            uninstalling = asyncio.create_task(
                lifecycle.uninstall_plugin(test_user_id, before)
            )
            await _until_a_session_waits_on_an_advisory_lock()
            release.set()
            row, _ = await updating
            deleted = await uninstalling

    assert (row["user_plugin_id"], row["version"]) == (
        before["user_plugin_id"],
        "2.0.0",
    )
    assert deleted.skills == [BETA]
    assert await get_plugin(test_user_id, FIRST) is None
    assert await list_plugin_skill_names(test_user_id, before["user_plugin_id"]) == []


async def _until_a_session_waits_on_an_advisory_lock() -> None:
    from src.server.database.pool import get_db_connection

    for _ in range(100):
        async with get_db_connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) AS n FROM pg_locks "
                "WHERE locktype = 'advisory' AND NOT granted"
            )
            if (await cur.fetchone())["n"]:
                return
        await asyncio.sleep(0.05)
    raise AssertionError("nothing waited on the fan-out lock")


async def test_an_sse_upgrade_that_waited_out_an_update_reads_the_new_package(
    seed_user, patched_get_db_connection, test_user_id
):
    """The update dropped the held-back entry while the upgrade waited for the
    lock, so the upgrade finds nothing to install instead of the old entry."""
    from src.server.database.mcp_servers import get_catalog_server
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins import update
    from src.server.services.plugins.post_install import apply_sse_upgrades

    in_lock, release = asyncio.Event(), asyncio.Event()
    update_skills = update._update_skills

    async def stalled(*args, **kwargs):
        in_lock.set()
        await asyncio.wait_for(release.wait(), timeout=10.0)
        return await update_skills(*args, **kwargs)

    with (
        patch(
            "src.server.services.skill_archive_storage.is_configured",
            return_value=False,
        ),
        patch(
            "src.server.services.plugins.skill_fanout.count_user_skills",
            new=AsyncMock(return_value=0),
        ),
        patch(
            "src.server.services.plugins.server_fanout._probe_sse_entries",
            new=AsyncMock(return_value={}),
        ),
        patch("src.server.services.plugins.server_fanout.schedule_catalog_discovery"),
    ):
        await _install(test_user_id, _package(FIRST, server=SSE))
        before = await get_plugin(test_user_id, FIRST)
        with patch.object(update, "_update_skills", stalled):
            updating = asyncio.create_task(
                update.update_plugin_package(
                    test_user_id, before, _package(FIRST), source_ref=None
                )
            )
            await in_lock.wait()
            upgrading = asyncio.create_task(
                apply_sse_upgrades(test_user_id, before, [SERVER])
            )
            await _until_a_session_waits_on_an_advisory_lock()
            release.set()
            await updating
            report = await upgrading

    assert await get_catalog_server(test_user_id, SERVER) is None
    assert [(c.key, c.status) for c in report.components] == [(SERVER, "error")]


STDIO = {"type": "stdio", "command": "uvx", "args": ["mcp-server-time==0.6.2"]}
STDIO_NEXT = {**STDIO, "args": ["mcp-server-time==0.6.3"]}


class _Boom(RuntimeError):
    """A failure, or the worker dying, after a plugin write's rows commit."""


async def _version(workspace_id) -> int:
    from src.server.database.workspace import get_workspace

    return int((await get_workspace(workspace_id))["mcp_config_version"])


@contextlib.contextmanager
def _offline():
    # Network: the held-back sse probe and the discovery after a create.
    with (
        patch(
            "src.server.services.plugins.server_fanout._probe_sse_entries",
            new=AsyncMock(return_value={}),
        ),
        patch("src.server.services.plugins.server_fanout.schedule_catalog_discovery"),
    ):
        yield


@pytest.mark.parametrize(
    "dies_in",
    [
        "fan_out_skills",
        "stamp_plugin_content_hash",
        "after_secrets_changed",
        "pending_secret_declarations",
    ],
)
async def test_an_install_that_fails_after_its_rows_commit_has_bumped(
    dies_in, seed_workspace, patched_get_db_connection, test_user_id
):
    """The row is live the moment it commits, and nothing repairs a missed
    bump: a retried install is refused as a duplicate, and update finds the
    row it would write already there and reports it unchanged."""
    from src.server.database.mcp_servers import get_catalog_server
    from src.server.services.plugins import lifecycle

    workspace_id = str(seed_workspace["workspace_id"])
    before = await _version(workspace_id)
    with (
        _offline(),
        patch.object(lifecycle, dies_in, new=AsyncMock(side_effect=_Boom)),
        pytest.raises(_Boom),
    ):
        await _install(test_user_id, _package(FIRST, server=STDIO))

    assert (await get_catalog_server(test_user_id, SERVER))["enabled"] is True
    assert await _version(workspace_id) > before


async def _update_dying_at_the_plugin_row(user_id: str, incoming) -> None:
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins import update

    plugin = await get_plugin(user_id, FIRST)
    with (
        patch.object(update, "update_plugin_row", new=AsyncMock(side_effect=_Boom)),
        pytest.raises(_Boom),
    ):
        await update.update_plugin_package(user_id, plugin, incoming, source_ref=None)


async def test_an_update_that_adds_a_server_bumps_with_the_row(
    seed_workspace, patched_get_db_connection, test_user_id
):
    from src.server.database.mcp_servers import get_catalog_server

    workspace_id = str(seed_workspace["workspace_id"])
    with _offline():
        await _install(test_user_id, _package(FIRST))
        before = await _version(workspace_id)
        await _update_dying_at_the_plugin_row(
            test_user_id, _package(FIRST, server=STDIO)
        )

    assert (await get_catalog_server(test_user_id, SERVER))["enabled"] is True
    assert await _version(workspace_id) > before


@pytest.mark.parametrize("arm", ["in-place", "delete"])
async def test_an_update_that_rewrites_or_drops_a_server_bumps_with_the_row(
    arm, seed_workspace, patched_get_db_connection, test_user_id, monkeypatch
):
    """The update's other two server arms, which write through the catalog
    helpers that bump inside their own transaction."""
    from src.server.database.mcp_servers import get_catalog_server

    # Both arms read the server's OAuth connection, which is encrypted at rest.
    monkeypatch.setenv("BYOK_ENCRYPTION_KEY", "test-ownership-key")
    workspace_id = str(seed_workspace["workspace_id"])
    incoming = (
        _package(FIRST, server=STDIO_NEXT) if arm == "in-place" else _package(FIRST)
    )
    with _offline():
        await _install(test_user_id, _package(FIRST, server=STDIO))
        before = await _version(workspace_id)
        await _update_dying_at_the_plugin_row(test_user_id, incoming)

    row = await get_catalog_server(test_user_id, SERVER)
    if arm == "in-place":
        assert row["args"] == STDIO_NEXT["args"]
    else:
        assert row is None
    assert await _version(workspace_id) > before


async def test_an_sse_upgrade_bumps_with_the_row(
    seed_workspace, patched_get_db_connection, test_user_id
):
    from src.server.database.mcp_servers import get_catalog_server
    from src.server.database.plugins import get_plugin
    from src.server.services.plugins import post_install

    workspace_id = str(seed_workspace["workspace_id"])
    with _offline():
        await _install(test_user_id, _package(FIRST, server=SSE))
        plugin = await get_plugin(test_user_id, FIRST)
        before = await _version(workspace_id)
        with (
            patch.object(
                post_install, "after_secrets_changed",
                new=AsyncMock(side_effect=_Boom),
            ),
            pytest.raises(_Boom),
        ):
            await post_install.apply_sse_upgrades(test_user_id, plugin, [SERVER])

    assert (await get_catalog_server(test_user_id, SERVER))["enabled"] is True
    assert await _version(workspace_id) > before
