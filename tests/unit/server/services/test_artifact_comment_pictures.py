"""A deleted comment's pictures leave a running computer and never wake a
stopped one; no failure there undoes the delete."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.artifacts import comment_pictures

_W = "00000000-0000-0000-0000-00000000000f"
_DIR = "/home/workspace/Reports"
_PICTURES = (".agents/comments/b-2.png", ".agents/comments/a 1.png")


def _sandbox(rm: dict | Exception) -> MagicMock:
    sandbox = MagicMock()
    locked: list[str] = []

    @asynccontextmanager
    async def _lock(path):
        locked.append(path)
        yield

    sandbox.path_write_lock = _lock
    sandbox.locked = locked
    sandbox.execute_bash_command = AsyncMock(
        side_effect=rm if isinstance(rm, Exception) else None,
        return_value=None if isinstance(rm, Exception) else rm,
    )
    return sandbox


@asynccontextmanager
async def _row(workspace_id):
    yield {"workspace_id": workspace_id}


def _patched(sandbox, canonical=lambda paths: list(paths), named=()):
    async def _contained(_sandbox, paths, *, work_dir):
        assert work_dir == _DIR
        return canonical(paths)

    async def _unnamed(workspace_id, pictures):
        assert workspace_id == _W
        return [p for p in pictures if p not in named]

    return (
        patch.object(comment_pictures, "sandbox_if_running", AsyncMock(return_value=sandbox)),
        patch.object(comment_pictures, "held_workspace_row", _row),
        patch.object(comment_pictures, "owner_work_dir", lambda _ws: _DIR),
        patch.object(comment_pictures, "contained_sandbox_paths", _contained),
        patch.object(comment_pictures.artifacts_db, "unnamed_pictures", _unnamed),
    )


async def _remove(sandbox, **kw):
    a, b, c, d, e = _patched(sandbox, **kw)
    with a, b, c, d, e:
        return await comment_pictures.remove_pictures(_W, "owner", _PICTURES)


@pytest.mark.asyncio
async def test_the_pictures_leave_the_folder_under_their_path_locks():
    sandbox = _sandbox({"success": True})
    removed = await _remove(sandbox)
    targets = [f"{_DIR}/.agents/comments/a 1.png", f"{_DIR}/.agents/comments/b-2.png"]
    assert removed == targets
    assert sandbox.locked == targets
    # ``rm -f``: a picture already gone is no failure.
    sandbox.execute_bash_command.assert_awaited_once_with(
        "rm -f -- '/home/workspace/Reports/.agents/comments/a 1.png' "
        "/home/workspace/Reports/.agents/comments/b-2.png"
    )


@pytest.mark.asyncio
async def test_a_picture_that_resolves_outside_the_folder_is_left_alone():
    sandbox = _sandbox({"success": True})
    removed = await _remove(
        sandbox, canonical=lambda paths: [None if "a 1" in p else p for p in paths]
    )
    assert removed == [f"{_DIR}/.agents/comments/b-2.png"]


@pytest.mark.asyncio
async def test_a_picture_a_comment_named_since_the_delete_is_kept():
    sandbox = _sandbox({"success": True})
    removed = await _remove(sandbox, named={".agents/comments/a 1.png"})
    assert removed == [f"{_DIR}/.agents/comments/b-2.png"]


@pytest.mark.asyncio
async def test_the_sandbox_is_found_under_the_folder_hold():
    """A move to another computer cannot land between finding the sandbox and
    reading the folder it names."""
    order: list[str] = []

    @asynccontextmanager
    async def _held(workspace_id):
        order.append("hold")
        yield {"workspace_id": workspace_id}

    async def _running(workspace_id, user_id):
        order.append("sandbox")
        return None

    a, b, c, d, e = _patched(None)
    with (
        a, b, c, d, e,
        patch.object(comment_pictures, "held_workspace_row", _held),
        patch.object(comment_pictures, "sandbox_if_running", _running),
    ):
        assert await comment_pictures.remove_pictures(_W, "owner", _PICTURES) == []
    assert order == ["hold", "sandbox"]


@pytest.mark.asyncio
async def test_a_stopped_computer_is_not_woken():
    a, b, c, d, e = _patched(None)
    with a as running, b, c, d, e:
        assert await comment_pictures.remove_pictures(_W, "owner", _PICTURES) == []
    running.assert_awaited_once_with(_W, "owner")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rm", [{"success": False, "stderr": "Read-only file system"}, RuntimeError("gone")]
)
async def test_a_failure_on_the_computer_never_raises(rm):
    assert await _remove(_sandbox(rm)) == []


@pytest.mark.asyncio
async def test_no_pictures_ask_nothing_of_the_computer():
    with patch.object(comment_pictures, "sandbox_if_running", AsyncMock()) as running:
        assert await comment_pictures.remove_pictures(_W, "owner", ()) == []
        comment_pictures.remove_pictures_soon(_W, "owner", ())
    running.assert_not_awaited()
