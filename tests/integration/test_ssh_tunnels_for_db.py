"""A tunnel lookup on a caller's transaction, against real Postgres: a read
that fails is shown as not known and leaves that transaction usable, which
only a real database can show.
"""

from __future__ import annotations

import pytest

from src.server.services.ssh_computers import tunnels_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.mark.usefixtures("patched_get_db_connection")
async def test_a_tunnel_read_that_fails_on_a_borrowed_transaction_leaves_it_usable(db_conn):
    computer = {"computer_id": "c-1", "kind": "ssh", "provider_config": {"tunnel": "not-a-uuid"}}
    async with db_conn.transaction():
        assert await tunnels_for("user-1", [computer], conn=db_conn) is None
        async with db_conn.cursor() as cur:
            await cur.execute("SELECT 1 AS one")
            assert (await cur.fetchone())["one"] == 1
