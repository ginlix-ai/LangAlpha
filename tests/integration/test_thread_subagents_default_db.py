"""A thread's effective subagent switch against real PostgreSQL.

A thread with no value of its own stores NULL, and ``read_thread_subagents_allowed``
resolves it from the owner's ``other_preference.subagents_default`` in its
query, so only a real JSONB compare proves which stored values turn it off and
that a later change to the default reaches it. The writes compare against the
same default to store the side it is on as NULL.
"""

from __future__ import annotations

import uuid

import pytest
from psycopg.types.json import Json

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _create(workspace_id: str, **kwargs) -> str:
    from src.server.database.conversation import create_thread

    row = await create_thread(
        conversation_thread_id=str(uuid.uuid4()),
        workspace_id=workspace_id,
        current_status="completed",
        msg_type="ptc",
        **kwargs,
    )
    return str(row["conversation_thread_id"])


async def _read(thread_id: str) -> bool | None:
    from src.server.database.conversation.threads_read import (
        read_thread_subagents_allowed,
    )

    return await read_thread_subagents_allowed(thread_id)


async def _set_other_preference(get_conn, user_id: str, other: dict | None) -> None:
    """Rewrite the preferences row ``create_user`` made; None deletes it."""
    async with get_conn() as conn:
        if other is None:
            await conn.execute(
                "DELETE FROM user_preferences WHERE user_id = %s", (user_id,)
            )
        else:
            await conn.execute(
                "UPDATE user_preferences SET other_preference = %s WHERE user_id = %s",
                (Json(other), user_id),
            )


async def test_an_unnamed_switch_is_stored_as_null(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.conversation import create_thread

    row = await create_thread(
        conversation_thread_id=str(uuid.uuid4()),
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
    )

    assert row["subagents_allowed"] is None


@pytest.mark.parametrize(
    "other, expected",
    [
        (None, True),  # no preferences row
        ({"theme": "dark"}, True),  # no key
        ({"subagents_default": False}, False),
        ({"subagents_default": "false"}, True),  # a string is not a JSON false
    ],
)
async def test_a_null_switch_reads_as_the_owner_default(
    seed_workspace, patched_get_db_connection, other, expected
):
    await _set_other_preference(
        patched_get_db_connection, seed_workspace["user_id"], other
    )
    thread_id = await _create(str(seed_workspace["workspace_id"]))

    assert await _read(thread_id) is expected


@pytest.mark.parametrize("default, named", [(False, True), (True, False)])
async def test_a_named_switch_wins_over_the_owner_default(
    seed_workspace, patched_get_db_connection, default, named
):
    await _set_other_preference(
        patched_get_db_connection,
        seed_workspace["user_id"],
        {"subagents_default": default},
    )
    thread_id = await _create(
        str(seed_workspace["workspace_id"]), subagents_allowed=named
    )

    assert await _read(thread_id) is named


async def test_changing_the_default_moves_only_threads_without_a_value(
    seed_workspace, patched_get_db_connection
):
    workspace_id = str(seed_workspace["workspace_id"])
    following = await _create(workspace_id)
    switched = await _create(workspace_id, subagents_allowed=False)

    for default in (False, True):
        await _set_other_preference(
            patched_get_db_connection,
            seed_workspace["user_id"],
            {"subagents_default": default},
        )

    assert await _read(following) is True
    assert await _read(switched) is False


@pytest.mark.parametrize("default", [True, False])
async def test_the_side_the_default_is_on_is_stored_as_following_it(
    seed_workspace, patched_get_db_connection, default
):
    """Create and PATCH alike, so the decision rests on the default as
    committed, never on a client's copy of it."""
    from src.server.database.conversation import create_thread, update_thread_fields

    await _set_other_preference(
        patched_get_db_connection,
        seed_workspace["user_id"],
        {"subagents_default": default},
    )
    created = await create_thread(
        conversation_thread_id=str(uuid.uuid4()),
        workspace_id=str(seed_workspace["workspace_id"]),
        current_status="completed",
        msg_type="ptc",
        subagents_allowed=default,
    )
    thread_id = str(created["conversation_thread_id"])
    own = await update_thread_fields(thread_id, subagents_allowed=not default)
    back = await update_thread_fields(thread_id, subagents_allowed=default)

    assert created["subagents_allowed"] is None
    assert own["subagents_allowed"] is (not default)
    assert back["subagents_allowed"] is None
    assert await _read(thread_id) is default


async def test_clearing_the_switch_returns_the_thread_to_the_default(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.conversation import update_thread_fields

    await _set_other_preference(
        patched_get_db_connection,
        seed_workspace["user_id"],
        {"subagents_default": False},
    )
    thread_id = await _create(
        str(seed_workspace["workspace_id"]), subagents_allowed=True
    )

    row = await update_thread_fields(thread_id, subagents_allowed=None)

    assert row["subagents_allowed"] is None
    assert await _read(thread_id) is False
