"""A shared workspace's per-thread folders are missing to whoever does not
reach the thread, through every spelling of their path.

Maya's thread is private; Lee is a member of the workspace. A folder no thread
claims is missing to both. A personal workspace lists and reads exactly as
before and asks nothing about threads.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from src.server.app.workspace_files import _shared, crud
from src.server.database.conversation import thread_reach
from src.server.services import workspace_files
from src.server.services.shared_workspaces.access import Access
from src.server.services.workspace_file_containment import ContainedFile

pytestmark = pytest.mark.asyncio

MAYA, LEE = "maya", "lee"
HOLDER = "shared-0000"
WORKSPACE_ID = "00000000-0000-4000-8000-00000000bbbb"
ROOT = "/home/workspace"
MAYAS, LEES, STRAY = "7157f2a5", "a1b2c3d4", "deadbeef"
#: Each thread's folder prefix, its starter and whether it is shared.
THREADS = {MAYAS: (MAYA, False), LEES: (LEE, False)}
STORED = [
    "notes.md",
    f".agents/threads/{MAYAS}/request.md",
    f".agents/scratchpad/{MAYAS}/note/task.md",
    f".agents/large_tool_results/{MAYAS}/r1.json",
    f".agents/threads/{LEES}/request.md",
    f".agents/threads/{STRAY}/request.md",
    ".agents/threads/README",
]


def _unreached(names_asked: list):
    async def unreached_folders(workspace_id, user_id, names):
        names_asked.append(set(names))
        return {
            n for n in names
            if n not in THREADS or not (THREADS[n][1] or THREADS[n][0] == user_id)
        }

    return unreached_folders


@pytest.fixture
def desk(monkeypatch):
    """A workspace on a running computer whose sandbox lists and reads
    ``STORED``; ``owner`` picks a shared one or a personal one."""
    asked: list = []
    monkeypatch.setattr(thread_reach, "unreached_folders", _unreached(asked))
    sandbox = MagicMock()
    sandbox.is_ready.return_value = True
    sandbox.validate_path.return_value = True
    sandbox.aglob_files = AsyncMock(return_value=[f"{ROOT}/{p}" for p in STORED])

    def _set(owner: str, status: str = "running"):
        workspace = {
            "workspace_id": WORKSPACE_ID, "user_id": owner, "status": status,
            "config": None, "sandbox_id": "sb",
        }
        read = AsyncMock(return_value=workspace)
        monkeypatch.setattr(crud, "db_get_workspace", read)
        monkeypatch.setattr(workspace_files, "db_get_workspace", read)
        access = AsyncMock(return_value=Access(owner))
        monkeypatch.setattr(crud, "workspace_access", access)
        monkeypatch.setattr(workspace_files, "workspace_access", access)

        @asynccontextmanager
        async def _acquire(*_):
            yield sandbox, workspace

        monkeypatch.setattr(crud, "acquire_sandbox_to_change", _acquire)
        return workspace

    monkeypatch.setattr(crud, "owner_work_dir", lambda _ws: ROOT)
    monkeypatch.setattr(_shared, "owner_work_dir", lambda _ws: ROOT)
    monkeypatch.setattr(crud, "_virtual_files_in_scope", AsyncMock(return_value=[]))
    monkeypatch.setattr(crud, "get_vault_secrets_for_redaction", AsyncMock(return_value=[]))
    monkeypatch.setattr(crud, "contained_sandbox_path", AsyncMock(side_effect=lambda _s, p, work_dir: p))
    monkeypatch.setattr(
        crud.FilePersistenceService, "get_file_tree",
        AsyncMock(return_value=[{"path": p} for p in STORED]),
    )
    return SimpleNamespace(set=_set, sandbox=sandbox, asked=asked)


async def _list(caller: str, path: str = ".") -> list[str]:
    result = await crud.list_workspace_files(
        workspace_id=WORKSPACE_ID, x_user_id=caller, path=path, include_system=True,
        pattern="**/*", wait_for_sandbox=False, auto_start=False,
    )
    return result["files"]


@pytest.mark.parametrize("status", ["running", "stopped"])
async def test_a_listing_leaves_out_the_folders_of_threads_the_caller_does_not_reach(desk, status):
    desk.set(HOLDER, status)

    assert await _list(LEE) == ["notes.md", f".agents/threads/{LEES}/request.md"]
    assert await _list(MAYA) == [p for p in STORED if MAYAS in p or p == "notes.md"]
    # One read per listing, for the prefixes that could be threads.
    assert desk.asked[0] == {MAYAS, LEES, STRAY}


async def test_a_hidden_folder_lists_as_a_missing_one(desk):
    desk.set(HOLDER)
    desk.sandbox.aglob_files.reset_mock()

    with pytest.raises(HTTPException) as e:
        await _list(LEE, f".agents/threads/{MAYAS}")
    assert (e.value.status_code, e.value.detail) == (404, "Not found")
    desk.sandbox.aglob_files.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        f".agents/threads/{MAYAS}/request.md",
        f"work/../.agents//threads/{MAYAS}/request.md",
        f".AGENTS/Threads/{MAYAS.upper()}/request.md",
        f"{ROOT}/.agents/scratchpad/{MAYAS}/note/task.md",
        f".agents/threads/{STRAY}/request.md",
    ],
)
@pytest.mark.parametrize("status", ["running", "stopped"])
async def test_a_read_of_a_hidden_folder_is_a_missing_file(desk, monkeypatch, path, status):
    desk.set(HOLDER, status)
    reached = AsyncMock(side_effect=AssertionError("the file was read"))
    monkeypatch.setattr(crud, "_read_contained_target", reached)
    monkeypatch.setattr(crud.FilePersistenceService, "get_file_content", reached)

    with pytest.raises(HTTPException) as e:
        await crud.read_workspace_file(
            workspace_id=WORKSPACE_ID, x_user_id=LEE, path=path, offset=0, limit=10, unlimited=True
        )
    assert (e.value.status_code, e.value.detail) == (404, "File not found")


async def test_a_link_into_a_hidden_folder_is_judged_where_it_lands(desk, monkeypatch):
    desk.set(HOLDER)
    landed = f"{ROOT}/.agents/threads/{MAYAS}/request.md"
    monkeypatch.setattr(
        crud,
        "_read_contained_target",
        AsyncMock(return_value=ContainedFile(canonical=landed, size=2, content=b"hi")),
    )
    desk.sandbox.virtualize_path = lambda p: p

    with pytest.raises(HTTPException) as e:
        await crud.read_workspace_file(
            workspace_id=WORKSPACE_ID, x_user_id=LEE, path="work/link.md", offset=0, limit=10,
            unlimited=True,
        )
    assert e.value.status_code == 404

    read = await crud.read_workspace_file(
        workspace_id=WORKSPACE_ID, x_user_id=MAYA, path="work/link.md", offset=0, limit=10,
        unlimited=True,
    )
    assert read["content"] == "hi"


async def test_a_write_into_a_hidden_folder_is_refused_before_the_sandbox(desk, monkeypatch):
    desk.set(HOLDER)
    monkeypatch.setattr(crud, "contained_target", AsyncMock(side_effect=AssertionError("written")))

    with pytest.raises(HTTPException) as e:
        await crud.write_workspace_file(
            workspace_id=WORKSPACE_ID, x_user_id=LEE, path=f".agents/threads/{MAYAS}/x.md",
            body=crud.WriteFileRequest(content="planted"),
        )
    assert (e.value.status_code, e.value.detail) == (404, "File not found")


async def test_a_delete_in_a_hidden_folder_reads_as_a_missing_file(desk, monkeypatch):
    desk.set(HOLDER)
    desk.sandbox.execute_bash_command = AsyncMock(side_effect=AssertionError("removed"))
    monkeypatch.setattr(
        crud, "contained_sandbox_paths", AsyncMock(side_effect=lambda _s, paths, work_dir: paths)
    )

    result = await crud.delete_workspace_files(
        workspace_id=WORKSPACE_ID, x_user_id=LEE,
        body=crud.DeleteFilesRequest(paths=[f".agents/threads/{MAYAS}/request.md"]),
    )
    assert result == {
        "deleted": [],
        "errors": [{"path": f".agents/threads/{MAYAS}/request.md", "detail": "File not found"}],
    }


@pytest.mark.parametrize("status", ["running", "stopped"])
async def test_a_personal_workspace_lists_every_folder_and_asks_nothing(desk, status):
    desk.set(MAYA, status)

    assert await _list(MAYA) == STORED
    assert desk.asked == []


async def test_the_prefix_read_is_scoped_to_the_workspace_and_needs_every_namesake():
    sql = thread_reach.unreached_folders_sql("%(user)s")
    assert "t.workspace_id = %(workspace)s::uuid" in sql
    assert "left(t.conversation_thread_id::text, 8) = ANY(%(names)s::text[])" in sql
    assert f"HAVING bool_and({thread_reach.reaches_sql('t', '%(user)s')})" in sql
