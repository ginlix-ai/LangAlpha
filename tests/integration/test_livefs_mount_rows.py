"""What every server worker reads of a computer's file mount at a turn's
start: the last start's answer on the token row, and the folders linked.

A workspace leaving a computer takes its link rows in the same transaction,
and a link answer recorded after that leaves it out, so no worker serves a
folder as linked that is gone or made anew without its links.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SANDBOX = "sb-rows-1"
CODE = "0123456789abcdef"
URL = "http://relay.test"
LAYOUT = "layout-1"


@pytest_asyncio.fixture
async def machine(seed_user, patched_get_db_connection):
    from src.server.database.computer import create_computer
    from src.server.database.workspace import create_workspace_on_computer

    owner = seed_user["user_id"]
    computer = await create_computer(
        owner, kind="daytona", name="Cloud computer", is_primary=True, status="running"
    )
    cid = str(computer["computer_id"])
    first = await create_workspace_on_computer(owner, "first", cid)
    second = await create_workspace_on_computer(owner, "second", cid)
    return SimpleNamespace(
        owner=owner,
        cid=cid,
        first=str(first["workspace_id"]),
        second=str(second["workspace_id"]),
    )


async def _linked(cid: str, sandbox_id: str = SANDBOX, layout: str = LAYOUT) -> frozenset:
    from src.server.database.livefs_links import mount_view

    return (await mount_view(cid, sandbox_id, layout))[1]


async def test_a_link_records_the_computers_whole_set_once_per_sandbox(
    machine, test_db_pool
):
    from src.server.database.livefs_links import save_links

    everything = frozenset({None, machine.first, machine.second})
    assert await save_links(machine.cid, SANDBOX, everything, LAYOUT) == everything
    # The computer's own row is one row, however often it is laid.
    assert await save_links(machine.cid, SANDBOX, everything, LAYOUT) == everything
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM livefs_links WHERE computer_id = %s",
            (machine.cid,),
        )
        assert (await cur.fetchone())["n"] == 3

    # A folder whose links did not all go in leaves the set.
    await save_links(machine.cid, SANDBOX, frozenset({None, machine.first}), LAYOUT)
    assert await _linked(machine.cid) == {None, machine.first}
    await save_links(machine.cid, SANDBOX, frozenset({machine.first}), LAYOUT)
    assert await _linked(machine.cid) == {machine.first}

    # A rebuilt sandbox starts with nothing linked, and the old one's rows go.
    assert await _linked(machine.cid, "sb-rows-2") == frozenset()
    await save_links(machine.cid, "sb-rows-2", frozenset({None}), LAYOUT)
    assert await _linked(machine.cid) == frozenset()
    assert await _linked(machine.cid, "sb-rows-2") == {None}


async def test_a_deleted_workspace_takes_its_links_and_is_never_recorded_again(
    machine,
):
    from src.server.database.livefs_links import save_links
    from src.server.database.workspace import delete_workspace

    everything = frozenset({None, machine.first, machine.second})
    await save_links(machine.cid, SANDBOX, everything, LAYOUT)

    assert await delete_workspace(machine.first)

    assert await _linked(machine.cid) == {None, machine.second}
    # A link that read the folders before the delete answers after it.
    assert await save_links(machine.cid, SANDBOX, everything, LAYOUT) == {
        None,
        machine.second,
    }
    assert await _linked(machine.cid) == {None, machine.second}


async def test_a_move_takes_the_links_on_the_computer_it_left(machine):
    from src.server.database.computer import create_computer
    from src.server.database.livefs_links import save_links
    from src.server.database.workspace import bind_workspace_to_computer

    other = str(
        (
            await create_computer(
                machine.owner, kind="daytona", name="Second", status="running"
            )
        )["computer_id"]
    )
    await save_links(machine.cid, SANDBOX, frozenset({None, machine.first}), LAYOUT)
    await save_links(other, "sb-other", frozenset({None}), LAYOUT)

    assert await bind_workspace_to_computer(
        machine.first, other, expected_computer_id=machine.cid
    )

    assert await _linked(machine.cid) == {None}
    # Laid on the computer it left, the answer records nothing for it there.
    assert await save_links(
        machine.cid, SANDBOX, frozenset({None, machine.first}), LAYOUT
    ) == {None}
    assert await save_links(
        other, "sb-other", frozenset({None, machine.first}), LAYOUT
    ) == {None, machine.first}


async def test_a_link_record_waiting_on_a_delete_leaves_the_workspace_out(
    machine, test_db_pool
):
    """The record takes a share lock on each workspace row before touching
    a link row, so it waits out a tombstone in flight and then reads it."""
    from src.server.database.livefs_links import drop_workspace_links, save_links

    await save_links(machine.cid, SANDBOX, frozenset({None, machine.first}), LAYOUT)
    async with test_db_pool.connection() as held:
        async with held.transaction():
            async with held.cursor() as cur:
                await cur.execute(
                    "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
                    (machine.first,),
                )
                await drop_workspace_links(cur, machine.first)
            recording = asyncio.ensure_future(
                save_links(
                    machine.cid,
                    SANDBOX,
                    frozenset({None, machine.first, machine.second}),
                    LAYOUT,
                )
            )
            await asyncio.sleep(0.5)
            assert not recording.done()
        assert await asyncio.wait_for(recording, timeout=5) == {None, machine.second}

    assert await _linked(machine.cid) == {None, machine.second}


async def test_the_token_row_says_who_serves_through_a_renewal_until_one_answers_down(
    machine,
):
    from src.server.database import livefs_tokens as db
    from src.server.database.livefs_links import mount_view

    def _at(minutes: int) -> datetime:
        return (datetime.now(UTC) + timedelta(minutes=minutes)).replace(microsecond=0)

    served = db.Served(CODE, URL)
    first, second, third, fourth = _at(60), _at(61), _at(62), _at(63)

    assert await db.save_token(machine.cid, machine.owner, b"d1", first)
    assert await db.current_token(machine.cid) == db.TokenRow(first)

    await db.mark_held(machine.cid, SANDBOX, first, served)
    up = db.TokenRow(first, SANDBOX, SANDBOX, served, first)
    assert await db.current_token(machine.cid) == up
    assert (await mount_view(machine.cid, SANDBOX, LAYOUT))[0] == up

    # A renewal's mint: the daemon serves on with the token it holds.
    assert await db.save_token(machine.cid, machine.owner, b"d2", second)
    assert await db.current_token(machine.cid) == db.TokenRow(
        second, None, SANDBOX, served, first
    )
    await db.mark_held(machine.cid, SANDBOX, second, served)
    assert (await db.current_token(machine.cid)).served_until == second

    # A mint whose publish never landed, then another: the daemon's token
    # is out of both slots.
    assert await db.save_token(machine.cid, machine.owner, b"d3", third)
    assert (await db.current_token(machine.cid)).served_by == SANDBOX
    assert await db.save_token(machine.cid, machine.owner, b"d4", fourth)
    assert await db.current_token(machine.cid) == db.TokenRow(fourth)

    # A start that took the token but cannot serve with it.
    await db.mark_held(machine.cid, SANDBOX, fourth, None)
    assert await db.current_token(machine.cid) == db.TokenRow(fourth, SANDBOX)

    # A start keeping the token: only for the sandbox that holds it, and
    # only while it is the current one.
    await db.mark_served(machine.cid, "sb-rows-2", fourth, served)
    await db.mark_served(machine.cid, SANDBOX, third, served)
    assert await db.current_token(machine.cid) == db.TokenRow(fourth, SANDBOX)
    await db.mark_served(machine.cid, SANDBOX, fourth, served)
    assert await db.current_token(machine.cid) == db.TokenRow(
        fourth, SANDBOX, SANDBOX, served, fourth
    )

    # A stop revokes: every worker reads no token.
    await db.delete_token(machine.cid)
    assert await mount_view(machine.cid, SANDBOX, LAYOUT) == (db.TokenRow(), frozenset())


async def test_a_restart_recorded_down_clears_only_the_sandbox_that_served(machine):
    from src.server.database import livefs_tokens as db

    expires_at = (datetime.now(UTC) + timedelta(minutes=60)).replace(microsecond=0)
    served = db.Served(CODE, URL)
    assert await db.save_token(machine.cid, machine.owner, b"d1", expires_at)
    await db.mark_held(machine.cid, SANDBOX, expires_at, served)
    up = db.TokenRow(expires_at, SANDBOX, SANDBOX, served, expires_at)

    # A sandbox that serves nothing restarting says nothing of the one that does.
    await db.mark_down(machine.cid, "sb-rows-2")
    assert await db.current_token(machine.cid) == up

    # The token stays the sandbox's: it is on its disk, and a start reads it.
    await db.mark_down(machine.cid, SANDBOX)
    assert await db.current_token(machine.cid) == db.TokenRow(expires_at, SANDBOX)
    await db.mark_served(machine.cid, SANDBOX, expires_at, served)
    assert await db.current_token(machine.cid) == up


async def test_links_laid_under_another_layout_are_not_read_and_go_with_the_next_record(
    machine, test_db_pool
):
    from src.server.database.livefs_links import save_links

    everything = frozenset({None, machine.first, machine.second})
    await save_links(machine.cid, SANDBOX, everything, "layout-old")

    assert await _linked(machine.cid, layout="layout-old") == everything
    assert await _linked(machine.cid) == frozenset()

    await save_links(machine.cid, SANDBOX, frozenset({None}), LAYOUT)
    assert await _linked(machine.cid) == {None}
    async with test_db_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT layout FROM livefs_links WHERE computer_id = %s", (machine.cid,)
        )
        assert [row["layout"] for row in await cur.fetchall()] == [LAYOUT]


async def test_a_link_record_waits_out_a_computer_write_before_any_workspace_row(
    machine, test_db_pool
):
    """A write to a computer and its workspaces locks the computer first and
    then each workspace in scan order. A record that share-locked one of
    those workspaces first and waited on another would hold what the write
    waits on, and each would wait on the other."""
    from src.server.database.livefs_links import save_links

    low, high = sorted([machine.first, machine.second])
    async with test_db_pool.connection() as held:
        async with held.transaction():
            await held.execute(
                "UPDATE computers SET updated_at = NOW() WHERE computer_id = %s",
                (machine.cid,),
            )
            await held.execute(
                "UPDATE workspaces SET updated_at = NOW() WHERE workspace_id = %s",
                (high,),
            )
            recording = asyncio.ensure_future(
                save_links(machine.cid, SANDBOX, frozenset({None, low, high}), LAYOUT)
            )
            await asyncio.sleep(0.5)
            assert not recording.done()
            # Under the old order the record holds ``low`` here, and this waits.
            await asyncio.wait_for(
                held.execute(
                    "UPDATE workspaces SET updated_at = NOW() WHERE workspace_id = %s",
                    (low,),
                ),
                timeout=5,
            )
        assert await asyncio.wait_for(recording, timeout=5) == {None, low, high}

    assert await _linked(machine.cid) == {None, low, high}
