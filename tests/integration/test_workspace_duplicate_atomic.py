"""A workspace row commits with what must be visible beside it.

For a duplicate that is its file manifest; for a create or a bind, the release
of its folder from any sibling's former folders.
"""

import asyncio

import pytest

from src.server.database import workspace as workspaces
from src.server.database import workspace_file as files
from src.server.database.computer import create_computer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _source(user_id, pool):
    computer = await create_computer(user_id, kind="docker", name="Test computer")
    computer_id = str(computer["computer_id"])
    source = await workspaces.create_workspace_on_computer(user_id, "Source", computer_id)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO workspace_files (workspace_id, file_path, file_name, file_size, content_text) "
            "VALUES (%s, 'proof.txt', 'proof.txt', 5, 'proof')",
            (source["workspace_id"],),
        )
    return source, computer_id


async def test_duplicate_is_invisible_until_files_commit(seed_user, test_db_pool, monkeypatch):
    user_id = seed_user["user_id"]
    source, computer_id = await _source(user_id, test_db_pool)
    entered, release = asyncio.Event(), asyncio.Event()
    original = files.copy_workspace_files

    async def delayed_copy(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(files, "copy_workspace_files", delayed_copy)
    task = asyncio.create_task(workspaces.duplicate_workspace_on_computer(
        str(source["workspace_id"]), user_id, "Copy", computer_id,
    ))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with test_db_pool.connection() as conn:
            result = await conn.execute("SELECT workspace_id FROM workspaces WHERE name='Copy'")
            assert await result.fetchone() is None
    finally:
        release.set()
        duplicate = await task
    async with test_db_pool.connection() as conn:
        result = await conn.execute(
            "SELECT content_text FROM workspace_files WHERE workspace_id=%s",
            (duplicate["workspace_id"],),
        )
        assert (await result.fetchone())["content_text"] == "proof"


async def test_failed_copy_rolls_back_project(seed_user, test_db_pool, monkeypatch):
    source, computer_id = await _source(seed_user["user_id"], test_db_pool)

    async def fail(*args, **kwargs):
        raise RuntimeError("copy failed")

    monkeypatch.setattr(files, "copy_workspace_files", fail)
    with pytest.raises(RuntimeError, match="copy failed"):
        await workspaces.duplicate_workspace_on_computer(
            str(source["workspace_id"]), seed_user["user_id"], "Copy", computer_id,
        )
    async with test_db_pool.connection() as conn:
        result = await conn.execute("SELECT workspace_id FROM workspaces WHERE name='Copy'")
        assert await result.fetchone() is None


async def test_a_held_folder_retry_keeps_outer_copy_transaction_usable(
    seed_user, test_db_pool, monkeypatch
):
    """A deleted workspace awaiting cleanup still holds its folder, so the copy
    is placed under a placeholder, inside the copy's own transaction. The
    holder is hidden from the create's read, as one that lands after it is,
    so the insert's own violation is what places the copy."""
    user_id = seed_user["user_id"]
    source, computer_id = await _source(user_id, test_db_pool)
    gone = await workspaces.create_workspace_on_computer(user_id, "Copy", computer_id)
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE workspaces SET status = 'deleted' WHERE workspace_id = %s",
            (gone["workspace_id"],),
        )

    async def nothing_held(*args, **kwargs):
        return ()

    monkeypatch.setattr(workspaces, "get_workspace_dir_names_for_computer", nothing_held)
    duplicate = await workspaces.duplicate_workspace_on_computer(
        str(source["workspace_id"]), user_id, "Copy", computer_id,
    )
    assert gone["dir_name"] == "Copy"
    assert duplicate["dir_name"].startswith("Copy-")
    async with test_db_pool.connection() as conn:
        result = await conn.execute(
            "SELECT content_text FROM workspace_files WHERE workspace_id=%s",
            (duplicate["workspace_id"],),
        )
        assert (await result.fetchone())["content_text"] == "proof"


async def test_a_taken_name_rolls_the_copy_back_and_names_the_holder(seed_user, test_db_pool):
    user_id = seed_user["user_id"]
    source, computer_id = await _source(user_id, test_db_pool)
    with pytest.raises(workspaces.WorkspaceNameTaken) as caught:
        await workspaces.duplicate_workspace_on_computer(
            str(source["workspace_id"]), user_id, "SOURCE", computer_id,
        )
    assert caught.value.workspace_id == str(source["workspace_id"])
    assert caught.value.name == "Source"
    async with test_db_pool.connection() as conn:
        result = await conn.execute(
            "SELECT count(*) AS n FROM workspaces WHERE user_id = %s AND status <> 'deleted'",
            (user_id,),
        )
        assert (await result.fetchone())["n"] == 1


async def _release_fails(*args, **kwargs):
    raise RuntimeError("release failed")


async def test_a_failed_folder_release_leaves_no_created_workspace(
    seed_user, test_db_pool, monkeypatch
):
    """The row and its folder's release commit together, so a create that fails
    after the insert leaves nothing holding the name and a retry succeeds."""
    user_id = seed_user["user_id"]
    computer = await create_computer(user_id, kind="docker", name="Test computer")
    computer_id = str(computer["computer_id"])

    monkeypatch.setattr(workspaces, "release_former_folder", _release_fails)
    with pytest.raises(RuntimeError, match="release failed"):
        await workspaces.create_workspace_on_computer(user_id, "Research", computer_id)
    async with test_db_pool.connection() as conn:
        result = await conn.execute("SELECT workspace_id FROM workspaces WHERE name='Research'")
        assert await result.fetchone() is None

    monkeypatch.undo()
    created = await workspaces.create_workspace_on_computer(user_id, "Research", computer_id)
    assert created["dir_name"] == "Research"


async def test_a_failed_folder_release_leaves_the_workspace_unbound(
    seed_user, test_db_pool, monkeypatch
):
    user_id = seed_user["user_id"]
    unbound = await workspaces.create_workspace(user_id, "Research", status="running")
    computer = await create_computer(user_id, kind="docker", name="Test computer")

    monkeypatch.setattr(workspaces, "release_former_folder", _release_fails)
    with pytest.raises(RuntimeError, match="release failed"):
        await workspaces.bind_workspace_to_computer(
            str(unbound["workspace_id"]), str(computer["computer_id"]),
            expected_computer_id=None, dir_name="Research",
        )
    async with test_db_pool.connection() as conn:
        result = await conn.execute(
            "SELECT computer_id, dir_name FROM workspaces WHERE workspace_id = %s",
            (unbound["workspace_id"],),
        )
        assert await result.fetchone() == {"computer_id": None, "dir_name": None}
