"""A restore behind a skill reconcile pass, against real PostgreSQL.

The restore has to wait the pass out holding nothing else: not a pool slot,
which the pass needs to finish, and not the workspace's sync lock, which its
backups queue on.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.paths import WorkspaceLayout

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_LAYOUT = WorkspaceLayout("/home/workspace", "probe")


def _pooled_in_use(pool) -> int:
    stats = pool.get_stats()
    return stats["pool_size"] - stats["pool_available"]


async def test_a_restore_waits_out_a_pass_holding_nothing_backups_need(
    seed_workspace, patched_get_db_connection, test_db_pool
):
    from src.server.database.user_skills import workspace_skill_sync_lock
    from src.server.database.workspace_file import workspace_sync_lock
    from src.server.services.persistence.restore import restore_to_sandbox

    ws_id = str(seed_workspace["workspace_id"])

    async with workspace_skill_sync_lock(ws_id):
        # A complete restore marks the sandbox populated, an empty one too.
        sandbox = SimpleNamespace(aupload_file_bytes=AsyncMock(return_value=True))
        restore = asyncio.create_task(
            restore_to_sandbox(ws_id, sandbox, layout=_LAYOUT)
        )
        await asyncio.sleep(0.5)
        assert not restore.done()
        assert _pooled_in_use(test_db_pool) == 0
        async with asyncio.timeout(2):
            async with workspace_sync_lock(ws_id):
                pass

    assert await asyncio.wait_for(restore, 5) == {"restored": 0, "errors": 0}
    # Closing the restore's session released the reconcile lock along with it.
    async with workspace_skill_sync_lock(ws_id):
        pass


async def test_a_pass_that_outlasts_the_wait_fails_the_restore_as_busy(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.user_skills import (
        SkillSyncLockBusy,
        workspace_skill_sync_lock,
    )
    from src.server.services.persistence.restore import restore_to_sandbox

    ws_id = str(seed_workspace["workspace_id"])

    with patch("src.server.services.persistence.restore._SKILL_SYNC_WAIT_S", 0.2):
        async with workspace_skill_sync_lock(ws_id):
            with pytest.raises(SkillSyncLockBusy):
                await restore_to_sandbox(ws_id, SimpleNamespace(), layout=_LAYOUT)
