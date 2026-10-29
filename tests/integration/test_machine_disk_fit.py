"""The downgrade disk guard's per-project sizes, read in one query against a real
database: every live project counts, an empty one as zero, a deleted one not at all."""

import pytest

from src.server.database.computer import create_computer
from src.server.database.workspace import create_workspace_on_computer
from src.server.database.workspace_file import get_live_project_sizes_for_computer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _files(test_db_pool, workspace_id, sizes):
    async with test_db_pool.connection() as conn:
        for i, size in enumerate(sizes):
            await conn.execute(
                "INSERT INTO workspace_files (workspace_id, file_path, file_name, file_size)"
                " VALUES (%s, %s, %s, %s)",
                (workspace_id, f"f{i}.bin", f"f{i}.bin", size),
            )


async def test_sizes_cover_every_live_project_and_only_those(seed_user, test_db_pool):
    computer = await create_computer(seed_user["user_id"], kind="docker", name="Fit")
    computer_id = str(computer["computer_id"])
    ids = []
    for name in ("big", "small", "empty", "gone"):
        ws = await create_workspace_on_computer(seed_user["user_id"], name, computer_id)
        ids.append(str(ws["workspace_id"]))
    big, small, _empty, gone = ids
    await _files(test_db_pool, big, [3 * 1024**3, 1024**3])
    await _files(test_db_pool, small, [10, 20])
    await _files(test_db_pool, gone, [5 * 1024**3])
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status='deleted' WHERE workspace_id=%s", (gone,)
        )

    sizes = await get_live_project_sizes_for_computer(computer_id)

    assert sorted(sizes) == [0, 30, 4 * 1024**3]
    assert all(isinstance(s, int) for s in sizes)
