"""Where a user server starts in a workspace created after it, on a real Postgres.

A server added from inside one workspace is tombstoned in every other one that
exists, and carries ``enabled_in_new_workspaces = FALSE`` so a workspace made
later starts with it off too. These pin that rule on each insert path, and the
lock that keeps a create racing a workspace insert from slipping past both.
"""

import asyncio

import pytest

from src.server.database import mcp_servers as servers
from src.server.database import workspace as workspaces
from src.server.database.computer import create_computer
from src.server.database.user_lock import lock_user_writes

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_STDIO = {"transport": "stdio", "command": "uvx"}


async def _computer(user_id) -> str:
    computer = await create_computer(user_id, kind="docker", name="Test computer")
    return str(computer["computer_id"])


async def _workspace(user_id, computer_id, name) -> str:
    row = await workspaces.create_workspace_on_computer(user_id, name, computer_id)
    return str(row["workspace_id"])


async def _off_in(pool, workspace_id, source="user") -> set[str]:
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT name FROM workspace_mcp_servers "
            "WHERE workspace_id = %s AND source = %s AND NOT enabled",
            (workspace_id, source),
        )
        return {row["name"] for row in await result.fetchall()}


async def _versions(pool, user_id) -> dict[str, int]:
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT workspace_id, mcp_config_version FROM workspaces WHERE user_id = %s",
            (user_id,),
        )
        return {str(r["workspace_id"]): r["mcp_config_version"] for r in await result.fetchall()}


async def test_a_server_added_from_a_workspace_starts_off_in_a_later_one(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    await servers.create_workspace_catalog_server(user_id, research, "notes", **_STDIO)

    later = await _workspace(user_id, computer_id, "Later")

    assert "notes" not in await _off_in(test_db_pool, research)
    assert "notes" in await _off_in(test_db_pool, later)
    row = await servers.get_catalog_server(user_id, "notes")
    assert (row["enabled"], row["enabled_in_new_workspaces"]) == (True, False)


async def test_an_inert_flagged_server_is_still_off_once_switched_live(seed_user, test_db_pool):
    """The tombstone lands while the row is inert, so going live later does
    not start it in a workspace the user never added it to."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    await servers.create_workspace_catalog_server(user_id, research, "notes", **_STDIO)
    await servers.set_catalog_server_enabled(user_id, "notes", False)

    later = await _workspace(user_id, computer_id, "Later")
    await servers.set_catalog_server_enabled(user_id, "notes", True)

    assert "notes" in await _off_in(test_db_pool, later)


async def test_a_plugins_page_server_starts_on_in_a_later_workspace(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    await servers.create_catalog_server(user_id, "wiki", enabled=True, **_STDIO)

    later = await _workspace(user_id, computer_id, "Later")

    assert await _off_in(test_db_pool, later) == set()
    assert (await servers.get_catalog_server(user_id, "wiki"))["enabled_in_new_workspaces"]


async def test_switching_the_default_back_on_reaches_later_workspaces_only(seed_user, test_db_pool):
    """No existing workspace's set changes, so none of them re-resolves."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    await servers.create_workspace_catalog_server(user_id, research, "notes", **_STDIO)
    before = await _versions(test_db_pool, user_id)

    row = await servers.set_catalog_server_new_workspace_default(user_id, "notes", True)

    assert row["enabled_in_new_workspaces"] is True
    assert await _versions(test_db_pool, user_id) == before
    assert "notes" in await _off_in(test_db_pool, other)
    later = await _workspace(user_id, computer_id, "Later")
    assert "notes" not in await _off_in(test_db_pool, later)
    assert await servers.set_catalog_server_new_workspace_default(user_id, "ghost", True) is None


async def test_a_duplicate_keeps_its_sources_state_for_a_flagged_server(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    await servers.create_workspace_catalog_server(user_id, research, "notes", **_STDIO)

    copy_of_research = await workspaces.duplicate_workspace_on_computer(
        research, user_id, "Research copy", computer_id
    )
    copy_of_other = await workspaces.duplicate_workspace_on_computer(
        other, user_id, "Other copy", computer_id
    )

    assert "notes" not in await _off_in(test_db_pool, str(copy_of_research["workspace_id"]))
    assert "notes" in await _off_in(test_db_pool, str(copy_of_other["workspace_id"]))


async def test_a_duplicate_keeps_what_its_source_switched_off(seed_user, test_db_pool):
    """A server on for new workspaces, and a built-in, switched off by hand in
    the source stay off in its copy, and stay on in a copy of a workspace that
    left them on."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    wiki = await servers.create_catalog_server(user_id, "wiki", enabled=True, **_STDIO)
    assert await servers.tombstone_user_server(
        user_id, research, "wiki", wiki["user_mcp_server_id"]
    )
    await servers.upsert_workspace_server(
        research, "price_data", source="builtin", enabled=False, config=None
    )

    copy_of_research = await workspaces.duplicate_workspace_on_computer(
        research, user_id, "Research copy", computer_id
    )
    copy_of_other = await workspaces.duplicate_workspace_on_computer(
        other, user_id, "Other copy", computer_id
    )

    copy_of_research = str(copy_of_research["workspace_id"])
    assert await _off_in(test_db_pool, copy_of_research) == {"wiki"}
    assert await _off_in(test_db_pool, copy_of_research, "builtin") == {"price_data"}
    copy_of_other = str(copy_of_other["workspace_id"])
    assert await _off_in(test_db_pool, copy_of_other) == set()
    assert await _off_in(test_db_pool, copy_of_other, "builtin") == set()


async def test_the_home_upsert_starts_the_selection_only_on_its_first_insert(seed_user, test_db_pool):
    """Every Home turn runs the upsert. A server the user switched on in Home
    must stay on, so only the call that inserted the row may tombstone."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    await servers.create_workspace_catalog_server(user_id, research, "notes", **_STDIO)

    home = await workspaces.get_or_create_home_workspace(user_id)
    home_id = str(home["workspace_id"])
    assert "inserted" not in home
    assert "notes" in await _off_in(test_db_pool, home_id)

    await servers.delete_workspace_server(home_id, "notes")
    again = await workspaces.get_or_create_home_workspace(user_id)

    assert str(again["workspace_id"]) == home_id
    assert "inserted" not in again
    assert await _off_in(test_db_pool, home_id) == set()


async def test_a_server_created_while_a_workspace_waits_is_off_in_it(seed_user, test_db_pool):
    """The workspace insert waits on the lock a server create holds to its
    commit, then reads the flag afresh: it cannot miss a server whose fan-out
    missed it."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)

    async with test_db_pool.connection() as holder:
        async with holder.transaction():
            await lock_user_writes(holder, user_id)
            await holder.execute(
                "INSERT INTO user_mcp_servers (user_id, name, transport, command, "
                "enabled, enabled_in_new_workspaces) "
                "VALUES (%s, 'notes', 'stdio', 'uvx', TRUE, FALSE)",
                (user_id,),
            )
            create = asyncio.create_task(_workspace(user_id, computer_id, "Racing"))
            await _until_waiting_on_user_lock(test_db_pool, user_id)
            assert not create.done()
        racing = await create

    assert "notes" in await _off_in(test_db_pool, racing)


async def test_a_switch_racing_a_delete_finds_the_server_gone(seed_user, test_db_pool):
    """A tombstone landing after the delete's purge would hold the name's slot
    in that workspace with nothing left to clear it. The switch waits on the
    lock the delete holds, then writes nothing."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    notes = await servers.create_catalog_server(user_id, "notes", enabled=True, **_STDIO)

    async with test_db_pool.connection() as holder:
        async with holder.transaction():
            await lock_user_writes(holder, user_id)
            await holder.execute(
                "DELETE FROM user_mcp_servers WHERE user_id = %s AND name = 'notes'",
                (user_id,),
            )
            switch = asyncio.create_task(
                servers.tombstone_user_server(
                    user_id, research, "notes", notes["user_mcp_server_id"]
                )
            )
            await _until_waiting_on_user_lock(test_db_pool, user_id)
            assert not switch.done()
        assert await switch is False

    assert await _off_in(test_db_pool, research) == set()


async def test_a_disable_leaves_a_replacement_on(seed_user, test_db_pool):
    """A disable that read one server, then lost the lock to a delete and a
    recreate that runs everywhere, finds another server under the name:
    switching that off would undo a placement its creator just made."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    read = await servers.create_catalog_server(user_id, "notes", enabled=True, **_STDIO)

    await servers.delete_catalog_server(user_id, "notes")
    replacement = await servers.create_catalog_server(
        user_id, "notes", enabled=True, **_STDIO
    )

    assert not await servers.tombstone_user_server(
        user_id, research, "notes", read["user_mcp_server_id"]
    )
    assert "notes" not in await _off_in(test_db_pool, research)
    # The switch the user makes on the server they now see still lands.
    assert await servers.tombstone_user_server(
        user_id, research, "notes", replacement["user_mcp_server_id"]
    )
    assert "notes" in await _off_in(test_db_pool, research)


async def test_a_delete_racing_a_switch_purges_its_tombstone(seed_user, test_db_pool):
    """The other order: a switch mid-write holds the lock, so the delete's
    purge runs after its tombstone commits and takes it too."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    await servers.create_catalog_server(user_id, "notes", enabled=True, **_STDIO)

    async with test_db_pool.connection() as holder:
        async with holder.transaction():
            await lock_user_writes(holder, user_id)
            await holder.execute(
                "INSERT INTO workspace_mcp_servers (workspace_id, name, source, enabled) "
                "VALUES (%s, 'notes', 'user', FALSE)",
                (research,),
            )
            delete = asyncio.create_task(
                servers.delete_catalog_server(user_id, "notes")
            )
            await _until_waiting_on_user_lock(test_db_pool, user_id)
            assert not delete.done()
        assert await delete is True

    assert await _off_in(test_db_pool, research) == set()


async def _tombstone_id(pool, workspace_id, name) -> str:
    async with pool.connection() as conn:
        result = await conn.execute(
            "SELECT workspace_mcp_server_id FROM workspace_mcp_servers "
            "WHERE workspace_id = %s AND name = %s AND source = 'user' AND NOT enabled",
            (workspace_id, name),
        )
        return str((await result.fetchone())["workspace_mcp_server_id"])


async def test_an_enable_leaves_a_replacements_tombstone_alone(seed_user, test_db_pool):
    """An enable that read one server's tombstone, then lost the lock to a
    delete and a recreate scoped elsewhere, finds another server's tombstone
    under the name: dropping it would start the replacement where its
    creator switched it off."""
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    await servers.create_workspace_catalog_server(user_id, other, "notes", **_STDIO)
    read = await _tombstone_id(test_db_pool, research, "notes")

    await servers.delete_catalog_server(user_id, "notes")
    await servers.create_workspace_catalog_server(user_id, other, "notes", **_STDIO)

    assert await servers.untombstone_user_server(user_id, research, "notes", read) == "gone"
    assert "notes" in await _off_in(test_db_pool, research)


async def test_an_enable_drops_the_tombstone_it_read_once(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    await servers.create_workspace_catalog_server(user_id, other, "notes", **_STDIO)
    read = await _tombstone_id(test_db_pool, research, "notes")
    before = (await _versions(test_db_pool, user_id))[research]

    assert await servers.untombstone_user_server(user_id, research, "notes", read) == "enabled"
    # A second click that classified the same tombstone finds the server on.
    assert await servers.untombstone_user_server(user_id, research, "notes", read) == "enabled"
    assert "notes" not in await _off_in(test_db_pool, research)
    assert (await _versions(test_db_pool, user_id))[research] == before + 1


async def test_an_enable_keeps_the_tombstone_while_the_account_is_off(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    computer_id = await _computer(user_id)
    research = await _workspace(user_id, computer_id, "Research")
    other = await _workspace(user_id, computer_id, "Other")
    await servers.create_workspace_catalog_server(user_id, other, "notes", **_STDIO)
    read = await _tombstone_id(test_db_pool, research, "notes")
    await servers.set_catalog_server_enabled(user_id, "notes", False)

    assert await servers.untombstone_user_server(user_id, research, "notes", read) == "account_off"
    assert "notes" in await _off_in(test_db_pool, research)


async def _until_waiting_on_user_lock(pool, user_id, timeout=5.0) -> None:
    """Until a session in this database waits on ``user_id``'s write lock.

    Scoped to this database and this key: any other advisory waiter in the
    cluster, another test's or another worktree's, would otherwise pass for
    the one this test started. pg_locks shows a bigint key as classid (high
    32 bits) and objid (low 32 bits), with objsubid 1.
    """

    async def waiting() -> bool:
        async with pool.connection() as conn:
            result = await conn.execute(
                """
                WITH k AS (SELECT hashtext(%s::text)::bigint AS key)
                SELECT 1 FROM pg_locks l, k
                WHERE l.locktype = 'advisory' AND NOT l.granted
                  AND l.database = (
                      SELECT oid FROM pg_database WHERE datname = current_database()
                  )
                  AND l.classid::bigint = (k.key >> 32) & 4294967295
                  AND l.objid::bigint = k.key & 4294967295
                  AND l.objsubid = 1
                """,
                (user_id,),
            )
            return await result.fetchone() is not None

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await waiting():
        assert loop.time() < deadline, "nothing ever waited on the user's lock"
        await asyncio.sleep(0.02)
