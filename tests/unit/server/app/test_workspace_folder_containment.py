"""Serve roots stay inside one workspace folder when a computer holds several.

The tree is two workspace folders on one computer root, which is the shape
layout v4 produces and the shape every check here is about. Workspace A is the
one the credential names; B is the sibling. The sandbox double reads a real temp
tree through the real ``/bin/sh`` probe, so a missed check does not merely return
the wrong status -- it hands the test the bytes of B's file.

Three spellings reach for B, and each has a different mechanism:

* ``?path=/tmp`` names an allowed directory that is *not* workspace data. It is
  shared by every workspace on the computer, so no file route may reach it. A
  client's leading slash is its virtual spelling of a workspace path, so the
  contained form of ``/tmp`` is ``<A>/tmp``: the assertion is that nothing under
  the machine's real ``/tmp`` is ever named, read or listed.
* ``?path=../B/...`` is lexical traversal. The sandbox validator collapses ``.``
  but not ``..``, and ``<root>/A/../B`` passes its prefix test against the
  computer root, so only a check anchored on A's folder denies it.
* a symlink inside A pointing at ``../B`` is the one the lexical checks cannot
  see at all, which is why the canonical path is what every gate runs on.

Written against WP7 and red until it lands.
"""

from __future__ import annotations

import os
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.share_files import (
    download_shared_file,
    list_shared_files,
    read_shared_file,
    serve_shared_file,
)
from src.server.services.workspace_file_containment import contained_sandbox_path
from src.server.app.workspace_files.crud import (
    delete_workspace_files,
    download_workspace_file,
    get_backup_status,
    list_workspace_files,
    read_workspace_file,
    resolve_workspace_file,
    workspace_file_download_url,
    write_workspace_file,
)
from src.server.app.workspace_files.crud import DeleteFilesRequest, WriteFileRequest
from src.server.app.workspace_files.file_refs import ResolveFileRefRequest
from src.server.app.workspace_files.serve import serve_workspace_file
from src.server.database.workspace_folders import moving_path

WS_A = "ws-folder-a"
WS_B = "ws-folder-b"
OWNER = "user-folder-1"
DIR_A = "a-11111111"
DIR_B = "b-22222222"

B_SECRET = b"sibling-workspace-bytes"
A_REPORT = b"<html><body>a report</body></html>"

_SERVE_DBWS = "src.server.app.workspace_files.serve.db_get_workspace"
_SERVE_FP = "src.server.app.workspace_files.serve.FilePersistenceService"
_SERVE_WD = "src.server.app.workspace_files.serve.work_dir_for"
_SERVE_WSMGR = "src.server.app.workspace_files.serve.WorkspaceManager"
_SERVE_VAULT = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"

_SHARE_WD = "src.server.app.share_access.work_dir_for"
_SHARE_THREAD = "src.server.app.share_access.get_thread_by_share_token"
_SHARE_DBWS = "src.server.app.share_access.db_get_workspace"
_SHARE_VAULT = "src.server.app.share_files.get_vault_secrets_for_redaction"

_CRUD_DBWS = "src.server.app.workspace_files.crud.db_get_workspace"
_CRUD_OWNER = "src.server.app.workspace_files.crud.require_workspace_owner"
_CRUD_ACQUIRE_TO_CHANGE = "src.server.app.workspace_files.crud.acquire_sandbox_to_change"
_CRUD_WD = "src.server.app.workspace_files.crud.owner_work_dir"
_CRUD_VAULT = "src.server.app.workspace_files.crud.get_vault_secrets_for_redaction"
_CRUD_LINK = "src.server.app.workspace_files.crud.live_download_link"
_CRUD_SCAN = "src.server.app.workspace_files.crud.FilePersistenceService.list_sandbox_files"
_SYNC_META = "src.server.database.workspace_file.get_file_metadata_for_sync"
_TOTAL_SIZE = "src.server.database.workspace_file.get_workspace_total_size"

# What the held acquisition and an owner route's guard read through.
_FILES_DBWS = "src.server.services.workspace_files.db_get_workspace"
_FILES_WSMGR = "src.server.services.workspace_files.WorkspaceManager"
_FILES_HOLD = "src.server.services.workspace_files.workspace_folder_in_use"
# The row a URL-only route re-reads after a live read.
_RECHECK_DBWS = "src.server.app.workspace_files._shared.db_get_workspace"
_SHARE_FP_TREE = "src.server.app.share_files.FilePersistenceService.get_file_tree"


class _ShellRuntime:
    """Runs the shipped probe for real, so the test exercises the command we send."""

    def __init__(self) -> None:
        self.commands: list[str] = []

    async def exec(self, command: str, timeout: int = 60):
        self.commands.append(command)
        done = subprocess.run(
            ["/bin/sh", "-c", command], capture_output=True, text=True
        )
        return SimpleNamespace(
            stdout=done.stdout, stderr=done.stderr, exit_code=done.returncode
        )


class _ComputerSandbox:
    """One sandbox serving a computer root that holds two workspace folders.

    ``validate_and_normalize_path`` keeps the production shape: a lexical prefix
    test against the *computer* root that resolves neither ``..`` nor a symlink,
    and that leaves a path under another allowed directory (``/tmp``) alone.
    That is exactly the hole the workspace-anchored checks exist to close.
    """

    def __init__(self, root: str) -> None:
        self.root = root
        self.sandbox_id = "sb-folder"
        self.config = SimpleNamespace(
            filesystem=SimpleNamespace(
                allowed_directories=[root, "/tmp"],
                denied_directories=[f"{root}/_internal"],
                enable_path_validation=True,
            )
        )
        self.runtime = _ShellRuntime()
        self.reads: list[str] = []
        self.writes: list[str] = []
        self.globs: list[tuple[str, str]] = []
        self.bash: list[str] = []

    # -- path layer ---------------------------------------------------------
    def validate_path(self, filepath: str, project=None) -> bool:
        filesystem = self.config.filesystem
        for denied in filesystem.denied_directories:
            if filepath == denied or filepath.startswith(f"{denied}/"):
                return False
        return any(
            filepath == allowed or filepath.startswith(f"{allowed}/")
            for allowed in filesystem.allowed_directories
        )

    def normalize_path(self, path: str, project=None) -> str:
        for allowed in self.config.filesystem.allowed_directories:
            if path.startswith(allowed):
                return path
        if path.startswith("/"):
            return f"{self.root}{path}"
        return f"{self.root}/{path}"

    def validate_and_normalize_path(self, path: str, project=None):
        absolute = self.normalize_path(path, project)
        if not self.validate_path(absolute, project):
            return absolute, f"Access denied: {path} is not in allowed directories"
        return absolute, None

    def virtualize_path(self, path: str, project=None) -> str:
        if path.startswith(f"{self.root}/"):
            return path[len(self.root):]
        if path == self.root:
            return "/"
        return path

    def is_ready(self) -> bool:
        return True

    # -- io -----------------------------------------------------------------
    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        self.reads.append(filepath)
        try:
            return Path(filepath).read_bytes()
        except OSError:
            return None

    async def awrite_file_text(self, filepath: str, content: str) -> bool:
        self.writes.append(filepath)
        try:
            Path(filepath).write_text(content)
        except OSError:
            return False
        return True

    async def aupload_file_bytes(self, filepath: str, content: bytes, **_: object) -> bool:
        self.writes.append(filepath)
        try:
            Path(filepath).write_bytes(content)
        except OSError:
            return False
        return True

    async def aglob_files(self, pattern: str, path: str = ".", **_: object) -> list[str]:
        self.globs.append((pattern, path))
        base = path if path.startswith("/") else f"{self.root}/{path}"
        return [str(p) for p in Path(base).rglob("*") if p.is_file()]

    async def execute_bash_command(self, command: str) -> dict:
        self.bash.append(command)
        done = subprocess.run(
            ["/bin/sh", "-c", command], capture_output=True, text=True
        )
        return {
            "success": done.returncode == 0,
            "stdout": done.stdout,
            "stderr": done.stderr,
        }

    @asynccontextmanager
    async def path_write_lock(self, _normalized_path: str):
        yield


@pytest.fixture
def computer(tmp_path: Path) -> SimpleNamespace:
    """A computer root holding workspace folders A and B, plus a symlink out of A."""
    base = Path(os.path.realpath(tmp_path))
    root = base / "home" / "workspace"
    a = root / DIR_A
    b = root / DIR_B
    (a / "work").mkdir(parents=True)
    b.mkdir(parents=True)
    (root / "_internal").mkdir()
    (b / "secret.txt").write_bytes(B_SECRET)
    (a / "work" / "report.html").write_bytes(A_REPORT)
    # A's own ``tmp``: a client spelling a path with a leading slash names this,
    # never the machine's scratch directory. It exists so the containment probe
    # has something to resolve on a host whose ``realpath`` cannot name a path
    # that is not there yet.
    (a / "tmp").mkdir()
    (a / "tmp" / "note.txt").write_bytes(b"a's own note")
    # The escape the lexical checks cannot see: relative, inside A, pointing at B.
    (a / "work" / "peek").symlink_to(Path("..") / ".." / DIR_B)
    return SimpleNamespace(
        root=str(root),
        work_dir=str(a),
        sibling=str(b),
        sandbox=_ComputerSandbox(str(root)),
    )


ESCAPES = [
    "/tmp",
    f"../{DIR_B}/secret.txt",
    "work/peek/secret.txt",
]

SINGLE_FILE_ESCAPES = [
    "/tmp/anything.txt",
    f"../{DIR_B}/secret.txt",
    "work/peek/secret.txt",
]


def _workspace(workspace_id: str = WS_A, dir_name: str = DIR_A) -> dict:
    return {
        "workspace_id": workspace_id,
        "user_id": OWNER,
        "status": "running",
        "config": None,
        "sandbox_id": "sb-folder",
        "dir_name": dir_name,
    }


def _shared_thread(perms: dict | None = None) -> dict:
    return {
        "conversation_thread_id": "thread-folder-1",
        "workspace_id": WS_A,
        "share_permissions": perms or {"allow_files": True, "allow_download": True},
    }


def _json_request() -> MagicMock:
    request = MagicMock()
    request.headers = {"accept": "application/json"}
    return request


def _warm(mock_mgr: MagicMock, sandbox: object) -> None:
    mock_mgr.get_instance.return_value.get_session_if_ready.return_value = MagicMock(
        sandbox=sandbox
    )


def _nothing_from_the_sibling(computer: SimpleNamespace) -> None:
    """No path the sandbox was asked to touch lies outside workspace A.

    A bash command is checked by substring rather than parsed: the deletion
    route builds one ``rm -f`` line, and the question asked of it is whether
    any name outside A appears in it at all.
    """
    inside = computer.work_dir.rstrip("/") + "/"
    for path in computer.sandbox.reads + computer.sandbox.writes:
        assert path == computer.work_dir or path.startswith(inside), (
            f"left workspace A through {path!r}"
        )
    for _, path in computer.sandbox.globs:
        assert path == computer.work_dir or path.startswith(inside), (
            f"listed outside workspace A through {path!r}"
        )
    for command in computer.sandbox.bash:
        assert computer.sibling not in command, (
            f"named the sibling in {command!r}"
        )
        assert f"{computer.root}/_internal" not in command, (
            f"named the machine's reserved directory in {command!r}"
        )
        assert " /tmp" not in command, (
            f"named the machine's scratch directory in {command!r}"
        )


# --- the mechanism --------------------------------------------------------


@pytest.mark.asyncio
async def test_containment_root_is_the_workspace_folder_not_the_computer(
    computer,
) -> None:
    """The computer root contains both folders, so it is the wrong root to judge on."""
    sibling = f"{computer.root}/{DIR_B}/secret.txt"
    assert (
        await contained_sandbox_path(
            computer.sandbox, sibling, work_dir=computer.work_dir
        )
        is None
    )
    mine = f"{computer.work_dir}/work/report.html"
    assert (
        await contained_sandbox_path(
            computer.sandbox, mine, work_dir=computer.work_dir
        )
        == mine
    )


@pytest.mark.asyncio
async def test_containment_denies_the_shared_scratch_directory(computer) -> None:
    """``/tmp`` is an allowed directory of the computer and is shared by every
    workspace on it, so it is never a serve root."""
    assert (
        await contained_sandbox_path(
            computer.sandbox, "/tmp/anything.txt", work_dir=computer.work_dir
        )
        is None
    )


# --- public share routes --------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", SINGLE_FILE_ESCAPES)
async def test_shared_read_denies_every_reach_for_the_sibling(
    computer, requested
) -> None:
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_WD, return_value=computer.work_dir),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(
            "src.server.services.persistence.file.FilePersistenceService."
            "get_file_content",
            AsyncMock(return_value=None),
        ),
        patch(_SHARE_VAULT, AsyncMock(return_value=[])),
    ):
        _warm(mgr, computer.sandbox)
        with pytest.raises(HTTPException) as exc:
            await read_shared_file("tok", path=requested)
    assert exc.value.status_code == 404
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", SINGLE_FILE_ESCAPES)
async def test_shared_download_denies_every_reach_for_the_sibling(
    computer, requested
) -> None:
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_WD, return_value=computer.work_dir),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(
            "src.server.services.persistence.file.FilePersistenceService."
            "get_file_content",
            AsyncMock(return_value=None),
        ),
        patch(_SHARE_VAULT, AsyncMock(return_value=[])),
    ):
        _warm(mgr, computer.sandbox)
        with pytest.raises(HTTPException) as exc:
            await download_shared_file("tok", path=requested)
    assert exc.value.status_code == 404
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", SINGLE_FILE_ESCAPES)
async def test_shared_serve_denies_every_reach_for_the_sibling(
    computer, requested
) -> None:
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_WD, return_value=computer.work_dir),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=computer.work_dir),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, computer.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        # The route may answer either way; what it may never do is hand back
        # the sibling's bytes, so both shapes are read for the same thing.
        try:
            response = await serve_shared_file(_json_request(), "tok", path=requested)
        except HTTPException as e:
            assert e.status_code == 404
            body = b""
        else:
            status = getattr(response, "status_code", None)
            assert status == 404, f"served {requested!r} with {status}"
            body = bytes(getattr(response, "body", b"") or b"")
    assert B_SECRET not in body
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", ESCAPES)
async def test_shared_listing_never_names_the_sibling(computer, requested) -> None:
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_WD, return_value=computer.work_dir),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(
            "src.server.services.persistence.file.FilePersistenceService."
            "get_file_tree",
            AsyncMock(return_value=[]),
        ),
    ):
        _warm(mgr, computer.sandbox)
        try:
            result = await list_shared_files("tok", path=requested)
        except HTTPException as exc:
            assert exc.value.status_code == 404 if hasattr(exc, "value") else True
            assert exc.status_code == 404
        else:
            assert all("secret.txt" not in f for f in result["files"]), result
    _nothing_from_the_sibling(computer)


def _acquired(sandbox, workspace):
    """Stands in for the folder-holding acquisition a change route opens."""

    @asynccontextmanager
    async def acquire(*_args):
        yield sandbox, workspace

    return acquire


# --- authenticated owner routes -------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", SINGLE_FILE_ESCAPES)
async def test_read_denies_every_reach_for_the_sibling(computer, requested) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
        patch(_CRUD_VAULT, AsyncMock(return_value=[])),
    ):
        with pytest.raises(HTTPException) as exc:
            await read_workspace_file(WS_A, OWNER, path=requested)
    assert exc.value.status_code == 404
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", SINGLE_FILE_ESCAPES)
async def test_download_denies_every_reach_for_the_sibling(computer, requested) -> None:
    request = MagicMock()
    request.headers = {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
        patch(_CRUD_VAULT, AsyncMock(return_value=[])),
    ):
        with pytest.raises(HTTPException) as exc:
            await download_workspace_file(WS_A, OWNER, request, path=requested)
    assert exc.value.status_code == 404
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", ESCAPES)
async def test_listing_never_leaves_the_workspace_folder(computer, requested) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
    ):
        try:
            result = await list_workspace_files(WS_A, OWNER, path=requested)
        except HTTPException as exc:
            assert exc.status_code == 404
        else:
            assert all("secret.txt" not in f for f in result["files"]), result
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested", [f"../{DIR_B}/planted.txt", "work/peek/planted.txt"]
)
async def test_write_never_lands_outside_the_workspace_folder(
    computer, requested
) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
    ):
        with pytest.raises(HTTPException) as exc:
            await write_workspace_file(
                WS_A, OWNER, path=requested, body=WriteFileRequest(content="x")
            )
    assert exc.value.status_code == 404
    assert not Path(computer.sibling, "planted.txt").exists()
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
async def test_write_to_the_shared_scratch_folds_into_the_workspace(computer) -> None:
    """A leading slash is the client's virtual spelling, so ``/tmp/x`` is A's own."""
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
    ):
        await write_workspace_file(
            WS_A, OWNER, path="/tmp/note.txt", body=WriteFileRequest(content="x")
        )
    assert computer.sandbox.writes == [f"{computer.work_dir}/tmp/note.txt"]
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested", [f"../{DIR_B}/secret.txt", "work/peek/secret.txt"]
)
async def test_delete_never_names_a_path_outside_the_workspace_folder(
    computer, requested
) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
    ):
        result = await delete_workspace_files(
            WS_A, OWNER, body=DeleteFilesRequest(paths=[requested])
        )
    assert result["deleted"] == []
    assert result["errors"]
    assert computer.sandbox.bash == []
    assert Path(computer.sibling, "secret.txt").read_bytes() == B_SECRET
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
async def test_delete_unlinks_a_symlink_instead_of_its_target(computer) -> None:
    target = Path(computer.work_dir, "work", "report.html")
    link = Path(computer.work_dir, "work", "report-link.html")
    link.symlink_to("report.html")
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_FILES_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=computer.work_dir),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, _workspace())),
    ):
        result = await delete_workspace_files(
            WS_A, OWNER, body=DeleteFilesRequest(paths=["work/report-link.html"])
        )

    assert result == {"deleted": ["work/report-link.html"], "errors": []}
    assert not link.exists()
    assert target.read_bytes() == A_REPORT


def _renamed(computer: SimpleNamespace) -> tuple[dict, dict]:
    """A's row before and after the acquisition that moves it out of ``old-a``."""
    before = _workspace(dir_name="old-a") | {"computer_root_dir": computer.root}
    after = _workspace() | {
        "computer_root_dir": computer.root,
        "previous_dir_names": ["old-a"],
    }
    return before, after


@pytest.mark.asyncio
async def test_listing_after_a_rename_reads_the_folder_the_acquisition_settled(
    computer,
) -> None:
    before, after = _renamed(computer)
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=before)),
        patch(_FILES_DBWS, AsyncMock(return_value=before)),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, after)),
    ):
        result = await list_workspace_files(
            WS_A, OWNER, path="work", pattern="**/*", wait_for_sandbox=True
        )
    assert "work/report.html" in result["files"]
    _nothing_from_the_sibling(computer)


@pytest.mark.asyncio
async def test_write_after_a_rename_does_not_recreate_the_former_folder(
    computer,
) -> None:
    before, after = _renamed(computer)
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=before)),
        patch(_FILES_DBWS, AsyncMock(return_value=before)),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(computer.sandbox, after)),
    ):
        result = await write_workspace_file(
            WS_A,
            OWNER,
            path="/home/workspace/old-a/work/report.html",
            body=WriteFileRequest(content="x"),
        )
    assert result["path"] == "work/report.html"
    assert computer.sandbox.writes == [f"{computer.work_dir}/work/report.html"]
    assert not Path(computer.root, "old-a").exists()


@pytest.mark.asyncio
async def test_the_workspace_serves_its_own_files(computer) -> None:
    """The containment is a fence, not a wall: A's own report still serves."""
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=computer.work_dir),
        patch(_RECHECK_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, computer.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        response = await serve_workspace_file(
            WS_A, "work/report.html", inject_theme=False
        )
    assert response.status_code == 200
    assert response.body == A_REPORT


# --- a folder that moves while a route reads it ----------------------------
#
# A settle on another worker can move A's folder between the row read and the
# sandbox read, and B can then land on the name A left. The stale row names
# DIR_B: A's folder when it was read, B's by the time the sandbox is. Both
# folders hold ``work/report.html``, so reading the stale one does not fail,
# it answers with B's file as A's.


def _moved(computer: SimpleNamespace) -> tuple[dict, dict]:
    """A's row as read before the move, and as read after it."""
    Path(computer.sibling, "work").mkdir()
    Path(computer.sibling, "work", "report.html").write_bytes(B_SECRET)
    root = {"computer_root_dir": computer.root}
    return _workspace(dir_name=DIR_B) | root, _workspace() | root


def _record_sandbox_calls(sandbox: _ComputerSandbox, events: list[str]) -> None:
    for name in ("aglob_files", "adownload_file_bytes"):
        original = getattr(sandbox, name)

        async def call(*args, _original=original, _name=name, **kwargs):
            events.append(_name)
            return await _original(*args, **kwargs)

        setattr(sandbox, name, call)
    run = sandbox.runtime.exec

    async def exec_(command: str, timeout: int = 60):
        events.append("exec")
        return await run(command, timeout)

    sandbox.runtime.exec = exec_


LIVE_READS = {
    "list": (
        lambda: list_workspace_files(
            WS_A, OWNER, path=".", include_system=False, pattern="**/*",
            wait_for_sandbox=True, auto_start=False,
        ),
        lambda r: "work/report.html" in r["files"] and "secret.txt" not in r["files"],
    ),
    "resolve": (
        lambda: resolve_workspace_file(
            WS_A, OWNER, ResolveFileRefRequest(candidates=["report.html"])
        ),
        lambda r: r["path"] == "work/report.html",
    ),
    "read": (
        lambda: read_workspace_file(
            WS_A, OWNER, path="work/report.html", offset=0, limit=100, unlimited=True
        ),
        lambda r: r["content"] == A_REPORT.decode(),
    ),
    "download-url": (
        lambda: workspace_file_download_url(WS_A, OWNER, path="work/report.html"),
        lambda r: r == {"url": "https://store.example.com/work/report.html"},
    ),
    "backup-status": (
        lambda: get_backup_status(WS_A, OWNER),
        lambda r: r["untracked"] == ["work/report.html"],
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("route", sorted(LIVE_READS))
async def test_live_reads_hold_the_folder_until_the_read_ends(computer, route) -> None:
    """Every sandbox read runs under the folder hold, on the row read under it."""
    before, after = _moved(computer)
    events: list[str] = []
    folders: list[str] = []
    held = False

    @asynccontextmanager
    async def hold(_workspace_id):
        nonlocal held
        events.append("held")
        held = True
        try:
            yield
        finally:
            held = False
            events.append("released")

    async def row(_workspace_id):
        return after if held else before

    async def export(_workspace, _sandbox, rel_path, *, layout):
        events.append("export")
        folders.append(layout.workspace)
        return f"https://store.example.com/{rel_path}"

    async def scan(_sandbox, *, layout):
        events.append("scan")
        folders.append(layout.workspace)
        return {"work/report.html": {"file_size": len(A_REPORT), "mtime": 1.0}}

    manager = MagicMock()
    manager.get_instance.return_value.get_session_for_workspace = AsyncMock(
        return_value=SimpleNamespace(sandbox=computer.sandbox)
    )
    _record_sandbox_calls(computer.sandbox, events)
    call, answered_for_a = LIVE_READS[route]
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=before)),
        patch(_FILES_WSMGR, manager),
        patch(_FILES_HOLD, hold),
        patch(_FILES_DBWS, row),
        patch(_CRUD_VAULT, AsyncMock(return_value=[])),
        patch(_CRUD_LINK, export),
        patch(_CRUD_SCAN, scan),
        patch(_SYNC_META, AsyncMock(return_value={})),
        patch(_TOTAL_SIZE, AsyncMock(return_value=0)),
    ):
        result = await call()

    assert events[0] == "held" and events[-1] == "released", events
    assert len(events) > 2 and events.count("held") == 1, events
    assert folders == [computer.work_dir] * len(folders)
    assert not any(computer.sibling in c for c in computer.sandbox.runtime.commands)
    _nothing_from_the_sibling(computer)
    assert answered_for_a(result), result


SHARED_READS = {
    "read": lambda: read_shared_file("tok", path="work/report.html", offset=0, limit=100),
    "download": lambda: download_shared_file("tok", path="work/report.html"),
    "serve": lambda: serve_shared_file(_json_request(), "tok", path="work/report.html"),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("route", sorted(SHARED_READS))
@pytest.mark.parametrize("row_after", ["moved", "staged", "lookup failed"])
async def test_a_shared_read_that_raced_a_folder_move_is_not_served(
    computer, route, row_after
) -> None:
    """A share route holds no folder, so the row is checked after the read.
    The token resolved while the row named DIR_B; B's file is 404, not A's."""
    before, after = _moved(computer)
    recheck = {
        "moved": AsyncMock(return_value=after),
        "staged": AsyncMock(return_value=after | {"dir_name": moving_path(WS_A)}),
        "lookup failed": AsyncMock(side_effect=RuntimeError("pool exhausted")),
    }[row_after]
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=before)),
        patch(_RECHECK_DBWS, recheck),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SHARE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
    ):
        _warm(mgr, computer.sandbox)
        with pytest.raises(HTTPException) as exc:
            await SHARED_READS[route]()
    assert exc.value.status_code == 404
    recheck.assert_awaited_once_with(WS_A)


@pytest.mark.asyncio
async def test_a_shared_listing_that_raced_a_folder_move_answers_from_the_manifest(
    computer,
) -> None:
    """A moved folder is a sandbox miss like any other on this route."""
    before, after = _moved(computer)
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=before)),
        patch(_RECHECK_DBWS, AsyncMock(return_value=after)),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SHARE_FP_TREE, AsyncMock(return_value=[{"path": "work/report.html"}])),
    ):
        _warm(mgr, computer.sandbox)
        result = await list_shared_files("tok", path=".")
    assert result == {"path": ".", "files": ["work/report.html"], "source": "database"}
