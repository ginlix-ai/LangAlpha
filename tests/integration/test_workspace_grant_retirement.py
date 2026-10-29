"""A deleted project must lose relay access before its shared computer stops."""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def claimed_grants(seed_user, patched_get_db_connection, test_db_pool):
    from src.server.database.computer import create_computer
    from src.server.database.workspace import create_workspace_on_computer

    owner = seed_user['user_id']
    computer = await create_computer(
        owner, kind='daytona', name='Cloud computer', is_primary=True
    )
    cid = str(computer['computer_id'])
    first = await create_workspace_on_computer(owner, 'first', cid)
    second = await create_workspace_on_computer(owner, 'second', cid)
    wid, sibling = str(first['workspace_id']), str(second['workspace_id'])
    grants = {name: str(uuid.uuid4()) for name in ('private', 'shared')}
    async with test_db_pool.connection() as conn:
        for name, gid in grants.items():
            await conn.execute(
                "INSERT INTO sandbox_egress_grants (grant_id,user_id,workspace_id,computer_id,kind,server_name,destination_url) "
                "VALUES (%s,%s,%s,%s,'header_mcp',%s,'https://example.com/test')",
                (gid, owner, wid, cid, name),
            )
            await conn.execute('INSERT INTO sandbox_egress_grant_claims (grant_id,workspace_id) VALUES (%s,%s)', (gid,wid))
        await conn.execute('INSERT INTO sandbox_egress_grant_claims (grant_id,workspace_id) VALUES (%s,%s)', (grants['shared'],sibling))
    return owner, wid, cid, grants


async def test_delete_revokes_only_the_last_live_claim(claimed_grants, test_db_pool):
    from src.server.database.workspace import delete_workspace
    from src.server.database.egress_grants import active_relay_grants_for_computer, fetch_grant_for_relay

    owner, wid, cid, grants = claimed_grants
    assert await delete_workspace(wid)
    assert (await fetch_grant_for_relay(grants['private']))['grant_status'] == 'revoked'
    assert (await fetch_grant_for_relay(grants['shared']))['grant_status'] == 'active'
    assert await active_relay_grants_for_computer(cid, user_id=owner) == {'shared': grants['shared']}
    async with test_db_pool.connection() as conn:
        cur = await conn.execute('SELECT count(*) AS n FROM sandbox_egress_grant_claims WHERE workspace_id=%s', (wid,))
        assert (await cur.fetchone())['n'] == 0


async def test_delete_and_revocation_roll_back_together(claimed_grants, test_db_pool):
    from src.server.database.workspace import delete_workspace, get_workspace

    _, wid, _, _ = claimed_grants
    with patch('src.server.database.egress_grants.retire_workspace_grants', AsyncMock(side_effect=RuntimeError('failed revocation'))):
        with pytest.raises(RuntimeError, match='failed revocation'):
            await delete_workspace(wid)
    assert (await get_workspace(wid))['status'] != 'deleted'
    async with test_db_pool.connection() as conn:
        cur = await conn.execute('SELECT count(*) AS n FROM sandbox_egress_grant_claims WHERE workspace_id=%s', (wid,))
        assert (await cur.fetchone())['n'] == 2


async def test_stale_sync_cannot_reactivate_deleted_project(claimed_grants):
    from src.server.database.workspace import delete_workspace, get_workspace
    from src.server.database.egress_grants import GrantRef, fetch_grant_for_relay, sync_egress_grants

    owner, wid, _, grants = claimed_grants
    version = (await get_workspace(wid))['mcp_config_version']
    assert await delete_workspace(wid)
    assert await sync_egress_grants(
        user_id=owner, workspace_id=wid, config_version=version,
        refs=[GrantRef(kind='header_mcp', server_name='private')],
    ) is None
    assert (await fetch_grant_for_relay(grants['private']))['grant_status'] == 'revoked'
