"""Symlink and path-escape containment on the routes that serve a file by path.

The sandbox double reads a real temp tree through the real ``/bin/sh`` probe, so
a missed containment check does not merely return the wrong status: it hands the
test the bytes of a file outside the serve root. The tree mirrors the two escapes
that matter once several workspaces share one filesystem -- a symlink pointing
out of the root entirely, and one pointing at the root's reserved ``_internal``.
"""

from __future__ import annotations

import base64
import math
import os
import re
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.share_access import ShareScope, shared_path_visible
from src.server.app.share_files import (
    download_shared_file,
    list_shared_files,
    read_shared_file,
    serve_shared_file,
)
from src.server.services.workspace_file_containment import (
    PROBE_ESCAPED,
    ContainedSlice,
    FileChangedWhileRead,
    _exec_read_max,
    contained_relative_path,
    contained_sandbox_path,
    contained_sandbox_paths,
    is_within,
    probe_command,
    read_contained_sandbox_range,
    resolve_in_sandbox,
    serving_roots,
)
from src.server.app.workspace_files.crud import (
    RANGE_MAX_BYTES,
    download_workspace_file,
    read_workspace_file,
)
from src.server.utils.http_headers import content_disposition
from src.server.app.workspace_files.serve import serve_workspace_file
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime
from ptc_agent.core.sandbox.runtime import SandboxTransientError

WS_ID = "ws-containment-1"
OWNER = "user-containment-1"
OUTSIDE_SECRET = b"outside-the-serve-root"
INTERNAL_SECRET = b"reserved-runtime-bytes"
REPORT_HTML = b"<html><head></head><body>report</body></html>"
SIBLING_SECRET = b"a file the share token does not cover"
CHART_PNG = b"\x89PNG\r\n\x1a\n\x01\x02"

_SERVE_DBWS = "src.server.app.workspace_files.serve.db_get_workspace"
_SERVE_FP = "src.server.app.workspace_files.serve.FilePersistenceService"
_SERVE_WD = "src.server.app.workspace_files.serve.work_dir_for"
_SERVE_WSMGR = "src.server.app.workspace_files.serve.WorkspaceManager"
_SERVE_VAULT = "src.server.app.workspace_files.serve.get_vault_secrets_for_redaction"
_SHARE_WD = "src.server.app.share_access.work_dir_for"
_SHARE_THREAD = "src.server.app.share_access.get_thread_by_share_token"
_SHARE_DBWS = "src.server.app.share_access.db_get_workspace"
_SHARE_SERVE = "src.server.app.share_files.serve_workspace_file"
_SHARE_FP = "src.server.app.share_files.FilePersistenceService"
_SHARE_VAULT = "src.server.app.share_files.get_vault_secrets_for_redaction"
_SHARE_WSMGR = "src.server.app.workspace_files.serve.WorkspaceManager"
_CRUD_DBWS = "src.server.app.workspace_files.crud.db_get_workspace"
_CRUD_OWNER = "src.server.app.workspace_files.crud.require_workspace_owner"
_CRUD_ACQUIRE_TO_CHANGE = "src.server.app.workspace_files.crud.acquire_sandbox_to_change"
_CRUD_WD = "src.server.app.workspace_files.crud.owner_work_dir"
# The row a URL-only route re-reads after a live read, to prove the folder the
# read went to was still this workspace's.
_RECHECK = "src.server.app.workspace_files._shared.db_get_workspace"
# The row an owner route's guard reads.
_OWNED_DBWS = "src.server.services.workspace_files.db_get_workspace"


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


class _FsSandbox:
    """A sandbox whose reads hit a real directory tree.

    ``validate_and_normalize_path`` is the production one's shape on purpose: a
    lexical prefix test that resolves neither ``..`` nor a symlink, which is the
    hole the containment probe exists to close.
    """

    def __init__(self, root: str) -> None:
        self.root = root
        self.config = SimpleNamespace(
            filesystem=SimpleNamespace(
                allowed_directories=[root],
                denied_directories=[f"{root}/_internal"],
                enable_path_validation=True,
            )
        )
        self.runtime = _ShellRuntime()
        self.reads: list[str] = []

    def validate_path(self, filepath: str) -> bool:
        filesystem = self.config.filesystem
        for denied in filesystem.denied_directories:
            if filepath == denied or filepath.startswith(f"{denied}/"):
                return False
        return any(
            filepath == allowed or filepath.startswith(f"{allowed}/")
            for allowed in filesystem.allowed_directories
        )

    def validate_and_normalize_path(self, path: str) -> tuple[str, str | None]:
        absolute = path if path.startswith("/") else f"{self.root}/{path}"
        if absolute != self.root and not absolute.startswith(f"{self.root}/"):
            return absolute, f"Access denied: {path} is not in allowed directories"
        return absolute, None

    def virtualize_path(self, path: str) -> str:
        if path.startswith(f"{self.root}/"):
            return path[len(self.root) :]
        if path == self.root:
            return "/"
        return path

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        self.reads.append(filepath)
        try:
            return Path(filepath).read_bytes()
        except OSError:
            return None

    async def astream_file_bytes(self, filepath: str):
        self.reads.append(filepath)
        if not Path(filepath).is_file():
            return None

        async def body():
            # Read lazily, like a real stream: what the file holds when the
            # client reads it, not when the download opened.
            data = Path(filepath).read_bytes()
            for start in range(0, len(data), 3):
                yield data[start : start + 3]

        return body()


@pytest.fixture
def tree(tmp_path: Path) -> SimpleNamespace:
    """Serve root with two escaping symlinks and one honest file."""
    base = Path(os.path.realpath(tmp_path))
    root = base / "workspace"
    outside = base / "outside"
    (root / "work").mkdir(parents=True)
    (root / "_internal").mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_bytes(OUTSIDE_SECRET)
    (root / "_internal" / "secret.txt").write_bytes(INTERNAL_SECRET)
    (root / "work" / "report.html").write_bytes(REPORT_HTML)
    (root / "work" / "chart.png").write_bytes(b"\x89PNG\r\n\x1a\n\xff\xfe")
    (root / "work" / "escape").symlink_to(outside)
    (root / "work" / "reserved").symlink_to(root / "_internal")
    # A share token's subtree, the workspace file beside it that the token does
    # not cover, and the link inside the subtree that points at it. The link is
    # the escape a scope check on the requested spelling cannot see.
    (root / "work" / "report" / "charts").mkdir(parents=True)
    (root / "work" / "report" / "index.html").write_bytes(REPORT_HTML)
    (root / "work" / "report" / "charts" / "a.png").write_bytes(CHART_PNG)
    (root / "work" / "other.html").write_bytes(SIBLING_SECRET)
    (root / "work" / "report" / "peek.html").symlink_to(root / "work" / "other.html")
    return SimpleNamespace(
        base=str(base),
        root=str(root),
        outside=str(outside),
        sandbox=_FsSandbox(str(root)),
    )


def _workspace() -> dict:
    return {
        "workspace_id": WS_ID,
        "user_id": OWNER,
        "status": "running",
        "config": None,
        "sandbox_id": "sb-containment",
    }


def _warm(mock_mgr: MagicMock, sandbox: object) -> None:
    mock_mgr.get_instance.return_value.get_session_if_ready.return_value = MagicMock(
        sandbox=sandbox
    )


def _shared_thread(perms: dict | None = None) -> dict:
    return {
        "conversation_thread_id": "thread-containment-1",
        "workspace_id": WS_ID,
        "share_permissions": perms or {"allow_files": True, "allow_download": True},
    }


def _json_request() -> MagicMock:
    request = MagicMock()
    request.headers = {"accept": "application/json"}
    return request


# --- lexical containment --------------------------------------------------


@pytest.mark.parametrize(
    "requested",
    [
        "../etc/passwd",
        "work/../../etc/passwd",
        "..",
        "work/..",
        "//etc/passwd",
        "",
        ".",
        "/",
        "work\\..\\..\\etc",
        "work/report\x00.html",
    ],
)
def test_contained_relative_path_rejects_escapes(requested: str) -> None:
    assert contained_relative_path(requested, "/home/workspace") is None


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("work/report.html", "work/report.html"),
        ("/work/report.html", "work/report.html"),
        ("/home/workspace/work/report.html", "work/report.html"),
        ("./work/report.html", "work/report.html"),
        ("work/./report.html", "work/report.html"),
        ("work//report.html", "work/report.html"),
        ("work/sub/../report.html", "work/report.html"),
        # A leading slash is the client's virtual-absolute spelling of a
        # workspace path, so it lands inside the root rather than at /etc.
        ("/etc/passwd", "etc/passwd"),
    ],
)
def test_contained_relative_path_keeps_workspace_paths(
    requested: str, expected: str
) -> None:
    assert contained_relative_path(requested, "/home/workspace") == expected


def test_is_within() -> None:
    assert is_within("/home/workspace", "/home/workspace")
    assert is_within("/home/workspace", "/home/workspace/work/a.txt")
    assert not is_within("/home/workspace", "/home/workspace-2/a.txt")
    assert not is_within("/home/workspace", "/etc/passwd")


def test_serving_roots_are_the_project_folder_and_the_shared_agent_tier() -> None:
    sandbox = SimpleNamespace(working_dir="/home/workspace")
    assert serving_roots(sandbox, work_dir="/home/workspace/alpha") == (
        "/home/workspace/alpha",
        "/home/workspace/.agents",
    )


def test_serving_roots_do_not_repeat_a_tier_inside_the_folder() -> None:
    """An unsplit computer serves the shared tier out of its own folder."""
    sandbox = SimpleNamespace(working_dir="/home/workspace")
    assert serving_roots(sandbox, work_dir="/home/workspace") == ("/home/workspace",)


# --- in-sandbox resolution ------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_in_sandbox_returns_the_canonical_path(tree) -> None:
    resolved = await resolve_in_sandbox(
        tree.sandbox, f"{tree.root}/work/report.html", roots=(tree.root,)
    )
    assert resolved == f"{tree.root}/work/report.html"


@pytest.mark.asyncio
async def test_resolve_in_sandbox_denies_a_symlink_out_of_the_root(tree) -> None:
    assert (
        await resolve_in_sandbox(
            tree.sandbox, f"{tree.root}/work/escape/secret.txt", roots=(tree.root,)
        )
        is None
    )


@pytest.mark.asyncio
async def test_resolve_in_sandbox_follows_a_symlink_that_stays_inside(tree) -> None:
    """Containment is about the root, not about symlinks: an in-root one resolves."""
    resolved = await resolve_in_sandbox(
        tree.sandbox, f"{tree.root}/work/reserved/secret.txt", roots=(tree.root,)
    )
    assert resolved == f"{tree.root}/_internal/secret.txt"


@pytest.mark.asyncio
async def test_contained_sandbox_path_denies_a_path_outside_every_allowed_dir(
    tree,
) -> None:
    assert (
        await contained_sandbox_path(tree.sandbox, "/etc/passwd", work_dir=tree.root)
        is None
    )


@pytest.mark.asyncio
async def test_contained_sandbox_path_runs_the_deny_list_on_the_canonical_path(
    tree,
) -> None:
    """The deny list names _internal; a symlink is how a clean path reaches it."""
    assert (
        await contained_sandbox_path(
            tree.sandbox, f"{tree.root}/work/reserved/secret.txt", work_dir=tree.root
        )
        is None
    )
    assert (
        await contained_sandbox_path(
            tree.sandbox, f"{tree.root}/work/report.html", work_dir=tree.root
        )
        == f"{tree.root}/work/report.html"
    )


@pytest.mark.asyncio
async def test_contained_sandbox_paths_answers_a_batch_in_one_probe(tree) -> None:
    """One exec for the batch, and each answer still lands on its own request."""
    resolved = await contained_sandbox_paths(
        tree.sandbox,
        [
            f"{tree.root}/work/report.html",
            f"{tree.root}/work/escape/secret.txt",
            f"{tree.root}/work/reserved/secret.txt",
            "/etc/passwd",
            f"{tree.root}/work/report/charts/a.png",
        ],
        work_dir=tree.root,
    )
    assert resolved == [
        f"{tree.root}/work/report.html",
        None,
        None,
        None,
        f"{tree.root}/work/report/charts/a.png",
    ]
    assert len(tree.sandbox.runtime.commands) == 1


@pytest.mark.asyncio
async def test_contained_sandbox_paths_denies_the_batch_on_a_mangled_probe(
    tree,
) -> None:
    """These paths are about to be handed to rm, so a short reply is a refusal."""
    tree.sandbox.runtime.exec = AsyncMock(
        return_value=SimpleNamespace(stdout="", stderr="", exit_code=0)
    )
    resolved = await contained_sandbox_paths(
        tree.sandbox,
        [f"{tree.root}/work/report.html", f"{tree.root}/work/chart.png"],
        work_dir=tree.root,
    )
    assert resolved == [None, None]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply",
    [
        SimpleNamespace(stdout="", stderr="", exit_code=0),
        SimpleNamespace(stdout="", stderr="killed", exit_code=137),
    ],
)
async def test_a_strict_batch_raises_where_a_denial_would_read_as_missing(
    tree, reply
) -> None:
    """A share's file list would otherwise drop every asset without a word."""
    tree.sandbox.runtime.exec = AsyncMock(return_value=reply)
    with pytest.raises(SandboxTransientError):
        await contained_sandbox_paths(
            tree.sandbox,
            [f"{tree.root}/work/report.html", f"{tree.root}/work/chart.png"],
            work_dir=tree.root,
            strict=True,
        )


@pytest.mark.asyncio
async def test_resolve_in_sandbox_reports_a_broken_probe_as_transient(tree) -> None:
    """A provider error has to arrive as a sandbox error, not as "no such file"."""
    tree.sandbox.runtime.exec = AsyncMock(side_effect=ValueError("sdk blew up"))
    with pytest.raises(SandboxTransientError):
        await resolve_in_sandbox(
            tree.sandbox, f"{tree.root}/work/report.html", roots=(tree.root,)
        )


@pytest.mark.asyncio
async def test_serve_falls_back_to_the_mirror_when_the_probe_cannot_run(tree) -> None:
    tree.sandbox.runtime.exec = AsyncMock(side_effect=ValueError("sdk blew up"))
    record = {
        "file_name": "report.html",
        "content_text": "mirrored",
        "content_binary": None,
        "is_binary": False,
        "mime_type": "text/html",
    }
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=record)
        response = await serve_workspace_file(
            WS_ID, "work/report.html", inject_theme=False
        )
    assert response.status_code == 200
    assert response.body == b"mirrored"


def test_probe_exits_with_the_escape_code(tree) -> None:
    done = subprocess.run(
        [
            "/bin/sh",
            "-c",
            probe_command(f"{tree.root}/work/escape/secret.txt", roots=(tree.root,)),
        ],
        capture_output=True,
        text=True,
    )
    assert done.returncode == PROBE_ESCAPED
    assert done.stdout == ""


# --- wsfiles serving ------------------------------------------------------


@pytest.mark.asyncio
async def test_serve_denies_a_symlink_out_of_the_serve_root(tree) -> None:
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await serve_workspace_file(
                WS_ID, "work/escape/secret.txt", inject_theme=False
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []
    assert len(tree.sandbox.runtime.commands) == 1


@pytest.mark.asyncio
async def test_serve_denies_a_symlink_into_the_reserved_runtime_dir(tree) -> None:
    """Inside the root but reserved: the hidden-path gate has to see the canonical path."""
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await serve_workspace_file(
                WS_ID, "work/reserved/secret.txt", inject_theme=False
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_serve_returns_a_contained_file(tree) -> None:
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_RECHECK, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        response = await serve_workspace_file(
            WS_ID, "work/report.html", inject_theme=False
        )
    assert response.status_code == 200
    assert response.body == REPORT_HTML
    assert tree.sandbox.reads == []
    assert len(tree.sandbox.runtime.commands) == 1


@pytest.mark.asyncio
async def test_serve_denies_a_traversal_path_without_touching_the_sandbox(tree) -> None:
    with (
        patch(_SERVE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
    ):
        _warm(mgr, tree.sandbox)
        with pytest.raises(HTTPException) as exc:
            await serve_workspace_file(
                WS_ID, "../outside/secret.txt", inject_theme=False
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.runtime.commands == []


# --- share-token scope ----------------------------------------------------


def test_share_scope_contains() -> None:
    whole = ShareScope(WS_ID, "")
    assert whole.contains("work/anything.html")

    one_report = ShareScope(WS_ID, "work/report")
    assert one_report.contains("work/report")
    assert one_report.contains("work/report/charts/a.png")
    assert not one_report.contains("work/report-2/a.html")
    assert not one_report.contains("work/other.html")


# --- the workspace's own notes file ---------------------------------------
#
# The agent keeps runtime context in ``agent.md`` at the folder root. A share
# carries no folder name, so the shared page cannot tell it from a
# deliverable; the routes leave it out instead. Only the root file: a nested
# ``docs/agent.md`` is a file the user asked for.

_SHARE_WARM = "src.server.app.share_files.warm_sandbox"


def test_share_gate_hides_the_notes_file_at_the_folder_root_only() -> None:
    whole = ShareScope(WS_ID, "")
    assert not shared_path_visible(whole, "agent.md")
    assert shared_path_visible(whole, "docs/agent.md")
    assert shared_path_visible(whole, "work/report.html")
    # A token minted for the notes file itself opens nothing.
    assert not shared_path_visible(ShareScope(WS_ID, "agent.md"), "agent.md")


def test_share_gate_hides_the_users_profile_and_automation_files() -> None:
    # The file panel lists these for their owner; a share never serves them,
    # even when the mount's links put them inside the shared folder.
    whole = ShareScope(WS_ID, "")
    assert not shared_path_visible(whole, ".agents/user/profile/portfolio.json")
    assert not shared_path_visible(whole, ".agents/user/profile")
    assert not shared_path_visible(whole, ".agents/user/automations/daily-brief.json")
    agents = ShareScope(WS_ID, ".agents")
    assert not shared_path_visible(agents, ".agents/user/profile/watchlist.json")


@pytest.mark.asyncio
async def test_shared_listing_leaves_out_the_notes_file(tree) -> None:
    rows = [{"path": p} for p in ("agent.md", "docs/agent.md", "work/report.html")]
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SHARE_WARM, return_value=None),
        patch(_SHARE_FP) as fp,
    ):
        fp.get_file_tree = AsyncMock(return_value=rows)
        listing = await list_shared_files("tok", path=".")
    assert listing["files"] == ["docs/agent.md", "work/report.html"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spelling", ["agent.md", "/agent.md", "./agent.md", "{root}/agent.md"]
)
async def test_shared_read_refuses_the_notes_file_before_any_lookup(
    tree, spelling: str
) -> None:
    """404, the answer an absent path gets, and the manifest is never asked."""
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SHARE_WARM, return_value=None),
        patch(_SHARE_FP) as fp,
    ):
        fp.get_file_content = AsyncMock(return_value={"content": "notes"})
        with pytest.raises(HTTPException) as exc:
            await read_shared_file(
                "tok", path=spelling.format(root=tree.root), offset=0, limit=100
            )
    assert exc.value.status_code == 404
    fp.get_file_content.assert_not_called()


@pytest.mark.asyncio
async def test_shared_serve_denies_a_sibling_outside_the_token_scope(tree) -> None:
    """A workspace file the token does not cover, asked for by its own name."""
    thread = _shared_thread({"allow_files": True, "root_path": "work/report"})
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await serve_shared_file(_json_request(), "tok", path="work/other.html")
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_shared_serve_denies_a_symlink_out_of_the_token_scope(tree) -> None:
    """The escape a scope check on the requested spelling cannot see.

    ``work/report/peek.html`` is inside the token's subtree and resolves to a
    file beside it, so the scope has to be re-decided on the canonical path the
    sandbox read landed on.
    """
    thread = _shared_thread({"allow_files": True, "root_path": "work/report"})
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await serve_shared_file(
                _json_request(), "tok", path="work/report/peek.html"
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_shared_serve_returns_a_file_inside_the_token_scope(tree) -> None:
    thread = _shared_thread({"allow_files": True, "root_path": "work/report"})
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_RECHECK, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        response = await serve_shared_file(
            _json_request(), "tok", path="work/report/charts/a.png"
        )
    assert response.status_code == 200
    assert response.body == CHART_PNG


@pytest.mark.asyncio
async def test_shared_serve_without_a_scope_still_serves_the_workspace(tree) -> None:
    """Every token minted so far carries no root path; those keep today's reach."""
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_RECHECK, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_VAULT, AsyncMock(return_value=[])),
        patch(_SERVE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        response = await serve_shared_file(
            _json_request(), "tok", path="work/other.html"
        )
    assert response.status_code == 200
    assert response.body == SIBLING_SECRET


@pytest.mark.asyncio
async def test_shared_serve_denies_an_unusable_stored_scope(tree) -> None:
    thread = _shared_thread({"allow_files": True, "root_path": "../../etc"})
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SHARE_SERVE, AsyncMock()) as core,
    ):
        with pytest.raises(HTTPException) as exc:
            await serve_shared_file(_json_request(), "tok", path="work/report/a.html")
    assert exc.value.status_code == 404
    core.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_pdf_pulls_subresources_through_the_token(tree) -> None:
    """The renderer carries no token, so the prefix it may fetch under is the gate."""
    thread = _shared_thread({"allow_files": True, "root_path": "work/report"})
    render = AsyncMock(return_value=b"%PDF-1.4")
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_RECHECK, AsyncMock(return_value=_workspace())),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
        patch("src.server.services.pdf_render.render_workspace_pdf", render),
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        response = await serve_shared_file(
            _json_request(), "tok", path="work/report/index.html", format="pdf"
        )
    assert response.status_code == 200
    document_url = render.await_args.args[0]
    prefix = render.await_args.kwargs["workspace_serve_prefix"]
    assert prefix.endswith("/api/v1/public/shared/tok/files/serve/")
    assert document_url == f"{prefix}work/report/index.html"
    assert f"/wsfiles/{WS_ID}/" not in prefix


@pytest.mark.asyncio
async def test_shared_pdf_refuses_a_path_outside_the_token_scope(tree) -> None:
    thread = _shared_thread({"allow_files": True, "root_path": "work/report"})
    render = AsyncMock(return_value=b"%PDF-1.4")
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SERVE_WD, return_value=tree.root),
        patch(_SERVE_WSMGR) as mgr,
        patch(_SERVE_FP) as fp,
        patch("src.server.services.pdf_render.render_workspace_pdf", render),
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await serve_shared_file(
                _json_request(), "tok", path="work/other.html", format="pdf"
            )
    assert exc.value.status_code == 404
    render.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_download_denies_a_sibling_outside_the_token_scope(tree) -> None:
    thread = _shared_thread(
        {"allow_files": True, "allow_download": True, "root_path": "work/report"}
    )
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SHARE_VAULT, AsyncMock(return_value=[])),
        patch(_SHARE_FP) as fp,
    ):
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await download_shared_file("tok", path="work/other.html")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_shared_download_denies_a_symlink_out_of_the_token_scope(tree) -> None:
    """Same escape as the serve route, on the route that hands over the bytes."""
    thread = _shared_thread(
        {"allow_files": True, "allow_download": True, "root_path": "work/report"}
    )
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=thread)),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
        patch(_SHARE_VAULT, AsyncMock(return_value=[])),
        patch(_SHARE_WSMGR) as mgr,
        patch(_SHARE_FP) as fp,
    ):
        _warm(mgr, tree.sandbox)
        fp.get_file_content = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await download_shared_file("tok", path="work/report/peek.html")
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_shared_download_denies_a_traversal_path(tree) -> None:
    with (
        patch(_SHARE_THREAD, AsyncMock(return_value=_shared_thread())),
        patch(_SHARE_DBWS, AsyncMock(return_value=_workspace())),
        patch(_SHARE_WD, return_value=tree.root),
    ):
        with pytest.raises(HTTPException) as exc:
            await download_shared_file("tok", path="../outside/secret.txt")
    assert exc.value.status_code == 404


# --- authenticated download ------------------------------------------------


@pytest.mark.asyncio
async def test_workspace_download_denies_a_symlink_out_of_the_root(tree) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
    ):
        with pytest.raises(HTTPException) as exc:
            await download_workspace_file(
                WS_ID, OWNER, _json_request(), path="work/escape/secret.txt"
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []



def _acquired(sandbox, workspace, events=None):
    """Stands in for the folder-holding acquisition, noting when it lets go."""

    @asynccontextmanager
    async def acquire(*_args):
        try:
            yield sandbox, workspace
        finally:
            if events is not None:
                events.append("released")

    return acquire

_EXEC_CAP = "src.server.services.workspace_file_containment.EXEC_READ_MAX_BYTES"
_CRUD_STORAGE = "src.server.app.workspace_files.crud.is_storage_enabled"


@pytest.mark.asyncio
@pytest.mark.parametrize("storage_on", [False, True])
async def test_workspace_download_past_the_exec_limit_uses_the_provider(
    tree, storage_on
) -> None:
    """A file too large for the exec read streams from the provider, fetched at
    the path the containment check resolved, with or without a store: the
    store's link is only an optimization over this route."""
    request = MagicMock()
    request.headers = {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
        patch(_EXEC_CAP, 4),
        patch(_CRUD_STORAGE, return_value=storage_on),
    ):
        response = await download_workspace_file(WS_ID, OWNER, request, path="work/chart.png")
        body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"\x89PNG\r\n\x1a\n\xff\xfe"
    assert response.headers["content-length"] == str(len(body))
    assert tree.sandbox.reads == [f"{tree.root}/work/chart.png"]


@pytest.mark.asyncio
async def test_a_whole_read_fits_in_what_the_runtime_keeps_of_its_output(tree) -> None:
    """An SSH exec keeps only the first 32 MiB it prints, so a file whose base64
    would pass that streams from the provider instead of arriving cut, which
    would read as a missing file."""
    fits = _exec_read_max(SSHRuntime)
    # The path line ahead of the bytes is a PATH_MAX path in base64 at most.
    path_line = len(base64.b64encode(bytes(4096))) + 1
    assert len(base64.encodebytes(bytes(fits))) + path_line <= SSHRuntime.exec_output_cap
    # Room for five bytes, and chart.png has ten.
    tree.sandbox.runtime.exec_output_cap = 64 * 1024 + 8
    request = MagicMock()
    request.headers = {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
        patch(_CRUD_STORAGE, return_value=False),
    ):
        response = await download_workspace_file(WS_ID, OWNER, request, path="work/chart.png")
        body = b"".join([chunk async for chunk in response.body_iterator])
    assert body == b"\x89PNG\r\n\x1a\n\xff\xfe"
    assert tree.sandbox.reads == [f"{tree.root}/work/chart.png"]


@pytest.mark.asyncio
async def test_a_streamed_download_holds_the_folder_until_the_response_is_sent(tree) -> None:
    """The file is read by path chunk after chunk, so a settle that moved the
    folder mid-send would end the download; the hold goes with the response."""
    request = MagicMock()
    request.headers = {}
    events: list[str] = []
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace(), events)),
        patch(_EXEC_CAP, 4),
        patch(_CRUD_STORAGE, return_value=False),
    ):
        response = await download_workspace_file(WS_ID, OWNER, request, path="work/chart.png")
        assert events == []
        sent = []

        async def send(message):
            sent.append(message)
            if message.get("more_body") is False or message["type"] == "http.response.body":
                assert events == []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert b"".join(m.get("body", b"") for m in sent) == b"\x89PNG\r\n\x1a\n\xff\xfe"
    assert events == ["released"]


@pytest.mark.asyncio
async def test_workspace_download_fails_the_send_when_the_file_grows(tree) -> None:
    """A file that outgrows the size sent as Content-Length breaks the download
    rather than handing the client a silent prefix of it."""
    request = MagicMock()
    request.headers = {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
        patch(_EXEC_CAP, 4),
        patch(_CRUD_STORAGE, return_value=False),
    ):
        response = await download_workspace_file(WS_ID, OWNER, request, path="work/chart.png")
        with Path(tree.root, "work/chart.png").open("ab") as f:
            f.write(b"appended by an agent")
        with pytest.raises(Exception, match="grew past"):
            _ = [chunk async for chunk in response.body_iterator]


@pytest.mark.asyncio
async def test_workspace_download_past_the_exec_limit_still_denies_an_escape(
    tree,
) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
        patch(_EXEC_CAP, 1),
        patch(_CRUD_STORAGE, return_value=False),
    ):
        with pytest.raises(HTTPException) as exc:
            await download_workspace_file(
                WS_ID, OWNER, _json_request(), path="work/escape/secret.txt"
            )
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_workspace_read_denies_a_symlink_out_of_the_root(tree) -> None:
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
    ):
        with pytest.raises(HTTPException) as exc:
            await read_workspace_file(WS_ID, OWNER, path="work/escape/secret.txt")
    assert exc.value.status_code == 404
    assert tree.sandbox.reads == []


@pytest.mark.asyncio
async def test_workspace_download_returns_a_contained_file(tree) -> None:
    request = MagicMock()
    request.headers = {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace())),
    ):
        response = await download_workspace_file(
            WS_ID, OWNER, request, path="work/chart.png"
        )
    assert response.status_code == 200
    assert response.body == b"\x89PNG\r\n\x1a\n\xff\xfe"


# --- ranged download -------------------------------------------------------

_CRUD_VAULT = "src.server.app.workspace_files.crud.get_vault_secrets_for_redaction"
_CRUD_READ_RANGE = "src.server.app.workspace_files.crud.read_contained_sandbox_range"
_RANGE_MAX = "src.server.app.workspace_files.crud.RANGE_MAX_BYTES"
_CRUD_WARM = "src.server.app.workspace_files.crud.warm_sandbox"
VAULT_SECRET = "vault-secret-value-1234"
# A version line as stat prints it (size, mtime, ctime, inode), and the clock.
STAMPED = "10 1700000000.5 1700000000.5 42"
CLOCK = "1700000100"
_VERSION_MIN_AGE = "src.server.services.workspace_file_containment._VERSION_MIN_AGE_S"


@pytest.fixture
def settled():
    """Every file counts as past the two seconds in which a write could still
    share its stamps; os.utime cannot age a ctime, and waiting costs 2 s a test."""
    with patch(_VERSION_MIN_AGE, -math.inf):
        yield


async def _read_range(tree, path: str, offset: int, length: int):
    return await read_contained_sandbox_range(
        tree.sandbox,
        f"{tree.root}/{path}",
        work_dir=tree.root,
        offset=offset,
        length=length,
    )


def _exec_printing(*lines: str) -> SimpleNamespace:
    """A runtime whose read prints ``lines`` after the canonical chart path."""
    return SimpleNamespace(
        exec=AsyncMock(
            return_value=SimpleNamespace(
                stdout="\n".join(lines) + "\n", stderr="", exit_code=0
            )
        )
    )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


async def _ranged_download(
    tree,
    range_header: str | None,
    path="work/chart.png",
    *,
    warm=None,
    folder_now=None,
    events=None,
):
    """The owner route's answer. ``warm`` is the sandbox of a session this
    worker already holds, ``folder_now`` the row the recheck after a warm read
    finds, and ``events`` notes an acquisition."""
    request = MagicMock()
    request.headers = {"range": range_header} if range_header else {}
    with (
        patch(_CRUD_DBWS, AsyncMock(return_value=_workspace())),
        patch(_OWNED_DBWS, AsyncMock(return_value=_workspace())),
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE_TO_CHANGE, _acquired(tree.sandbox, _workspace(), events)),
        patch(_CRUD_VAULT, AsyncMock(return_value={"API_KEY": VAULT_SECRET})),
        patch(_CRUD_WARM, return_value=warm),
        patch(_RECHECK, AsyncMock(return_value=folder_now or _workspace())),
    ):
        return await download_workspace_file(
            WS_ID, OWNER, request, path=path, attachment=False
        )


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
async def test_a_ranged_read_returns_the_slice_and_the_file_it_was_cut_from(
    tree,
) -> None:
    chart = Path(tree.root, "work/chart.png").read_bytes()
    sliced = await _read_range(tree, "work/chart.png", 2, 4)
    assert sliced.canonical == f"{tree.root}/work/chart.png"
    assert (sliced.size, sliced.content, sliced.head) == (10, chart[2:6], chart)
    # mtime, ctime and inode from GNU or BSD stat, with the fraction both give.
    assert re.fullmatch(r"\d+\.\d+-\d+\.\d+-\d+", sliced.version)


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["in-place", "renamed-over"])
async def test_a_same_size_rewrite_that_keeps_the_mtime_moves_the_version(
    tree, how
) -> None:
    """A writer can set the mtime back (``cp -p``, ``touch -r``, an unpacked
    archive), so the version also names what no writer sets: the ctime every
    write moves, and the inode a file renamed over the name brings."""
    chart = Path(tree.root, "work/chart.png")
    before = await _read_range(tree, "work/chart.png", 0, 4)
    assert before.version is not None
    stamp = chart.stat()
    replacement = bytes(reversed(chart.read_bytes()))
    written = chart if how == "in-place" else chart.with_name("chart.png.part")
    written.write_bytes(replacement)
    os.utime(written, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    written.replace(chart)
    assert chart.stat().st_mtime_ns == stamp.st_mtime_ns

    after = await _read_range(tree, "work/chart.png", 0, 4)
    assert (after.size, after.content) == (10, replacement[:4])
    assert after.version != before.version


@pytest.mark.asyncio
async def test_a_ranged_read_stops_at_the_end_of_the_file(tree) -> None:
    chart = Path(tree.root, "work/chart.png").read_bytes()
    sliced = await _read_range(tree, "work/chart.png", 6, 100)
    assert (sliced.size, sliced.content) == (10, chart[6:])


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [10, 10**17])
async def test_a_ranged_read_past_the_end_names_the_size_and_no_bytes(
    tree, offset
) -> None:
    sliced = await _read_range(tree, "work/chart.png", offset, 4)
    assert (sliced.size, sliced.content) == (10, None)


@pytest.mark.asyncio
async def test_a_ranged_read_is_not_bound_by_the_exec_read_limit(tree) -> None:
    """A range is how a file of any size gets read, so the limit that sends a
    large file to the provider's stream never applies to one."""
    chart = Path(tree.root, "work/chart.png").read_bytes()
    with patch(_EXEC_CAP, 1):
        sliced = await _read_range(tree, "work/chart.png", 0, 10)
    assert sliced.content == chart


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested",
    [
        "work/escape/secret.txt",
        "work/reserved/secret.txt",
        "work/../../outside/secret.txt",
    ],
)
async def test_a_ranged_read_still_refuses_an_escape(tree, requested) -> None:
    assert await _read_range(tree, requested, 0, 4) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("after", "sliced"),
    [
        # A same-length rewrite between the two stats that put the mtime back.
        ("10 1700000000.5 1700000001.5 42", b"abcd"),
        # A same-length file renamed over the name between them.
        ("10 1700000000.5 1700000000.5 43", b"abcd"),
        # A slice the size read before it contradicts.
        (STAMPED, b"ab"),
    ],
    ids=["ctime-moved", "inode-moved", "slice-short"],
)
async def test_a_file_that_changes_while_it_is_read_is_refused(
    tree, after, sliced
) -> None:
    """Answering would send a validator or a Content-Range the body belies."""
    canonical = f"{tree.root}/work/chart.png"
    tree.sandbox.runtime = _exec_printing(
        _b64(canonical.encode()),
        STAMPED,
        CLOCK,
        _b64(b"head"),
        _b64(sliced),
        after,
    )
    with pytest.raises(FileChangedWhileRead):
        await _read_range(tree, "work/chart.png", 0, 4)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "line",
    [
        # Every stat failed, and wc gave the size alone.
        "10",
        "10 1700000000.5",
        "10 1700000000.5 1700000000.5",
        "10 1700000000.5 1700000000.5 42 7",
        "10 1700000000.5 1700000000.5 4x",
        "10 1700000000.5 \u0661\u0667\u0660\u0660 42",
        "10 1700000000.5e3 1700000000.5 42",
    ],
)
async def test_a_version_line_without_stat_s_four_stamps_has_no_version(
    tree, line
) -> None:
    """The size alone cannot tell two versions apart, and anything but stat's
    digits would land in the ETag, so the route answers whole."""
    canonical = f"{tree.root}/work/chart.png"
    tree.sandbox.runtime = _exec_printing(
        _b64(canonical.encode()), line, CLOCK, _b64(b"head"), _b64(b"abcd"), line
    )
    sliced = await _read_range(tree, "work/chart.png", 0, 4)
    assert (sliced.size, sliced.version, sliced.content) == (10, None, b"abcd")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ctime", "clock", "versioned"),
    [
        # BusyBox prints whole seconds; GNU on a coarse filesystem, zeros.
        ("1700000000", "1700000001", False),
        ("1700000000.000000000", "1700000001", False),
        # Nanosecond digits, but stamped from a clock that steps by a tick.
        ("1700000000.480762010", "1700000001", False),
        ("1700000000.5", "1700000002", False),
        # A sandbox clock behind the filesystem's, or none at all.
        ("1700000000", "1699999990", False),
        ("1700000000", "", False),
        ("1700000000.5", "", False),
        ("1700000000", "1700000002", True),
        ("1700000000.000000000", "1700000002", True),
        ("1700000000.480762010", "1700000003", True),
    ],
)
async def test_a_ctime_counts_once_the_clock_is_two_seconds_past_it(
    tree, ctime, clock, versioned
) -> None:
    """A write moves the ctime only by the clock's step, a tick or a whole
    second, so until the clock has left it behind, the next same-size write
    may carry every stamp this one does, whatever the ctime's precision."""
    canonical = f"{tree.root}/work/chart.png"
    line = f"10 1700000000.5 {ctime} 42"
    tree.sandbox.runtime = _exec_printing(
        _b64(canonical.encode()), line, clock, _b64(b"head"), _b64(b"abcd"), line
    )
    sliced = await _read_range(tree, "work/chart.png", 0, 4)
    assert sliced.version == (f"1700000000.5-{ctime}-42" if versioned else None)


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
async def test_a_ranged_download_answers_206_with_the_slice(tree) -> None:
    chart = Path(tree.root, "work/chart.png").read_bytes()
    response = await _ranged_download(tree, "bytes=2-5")
    assert response.status_code == 206
    assert response.body == chart[2:6]
    assert response.headers["content-range"] == "bytes 2-5/10"
    assert response.headers["content-length"] == "4"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["cache-control"] == "private, no-store, no-transform"
    assert response.headers["content-type"] == "image/png"
    assert response.headers["content-disposition"] == content_disposition(
        "chart.png", disposition="inline"
    )
    # Size and version from the exec that read the slice, so a rewrite moves it.
    assert re.fullmatch(r'"10-\d+\.\d+-\d+\.\d+-\d+"', response.headers["etag"])


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("header", "cap", "content_range"),
    [
        ("bytes=6-9", RANGE_MAX_BYTES, "bytes 6-9/10"),
        ("bytes=6-99", RANGE_MAX_BYTES, "bytes 6-9/10"),
        ("bytes=1-99", 4, "bytes 1-4/10"),
        ("bytes=0-8", 4, "bytes 0-3/10"),
    ],
)
async def test_a_ranged_download_stops_at_the_end_and_at_the_cap(
    tree, header, cap, content_range
) -> None:
    """A client loops on a short answer, so the cap bounds one answer
    whatever the range asked for."""
    chart = Path(tree.root, "work/chart.png").read_bytes()
    with patch(_RANGE_MAX, cap):
        response = await _ranged_download(tree, header)
    first, last = (int(n) for n in content_range[6:].split("/")[0].split("-"))
    assert response.headers["content-range"] == content_range
    assert response.body == chart[first : last + 1]


@pytest.mark.asyncio
async def test_a_file_written_moments_ago_goes_whole(tree) -> None:
    """Its next write may still share every stamp, so no version names it."""
    chart = Path(tree.root, "work/chart.png").read_bytes()
    # Fresh however long the runner took between the fixture's write and the read.
    with patch(_VERSION_MIN_AGE, math.inf):
        response = await _ranged_download(tree, "bytes=2-5")
    assert response.status_code == 200
    assert response.body == chart
    assert "content-range" not in response.headers


@pytest.mark.asyncio
async def test_a_range_past_the_end_is_416_with_the_size(tree) -> None:
    with pytest.raises(HTTPException) as exc:
        await _ranged_download(tree, "bytes=10-20")
    assert exc.value.status_code == 416
    assert exc.value.headers == {"Content-Range": "bytes */10"}


@pytest.mark.asyncio
async def test_an_empty_file_answers_any_range_with_416(tree) -> None:
    """Decided from the size before the content is looked at, where an empty
    body would pass for text."""
    Path(tree.root, "work/empty.png").write_bytes(b"")
    with pytest.raises(HTTPException) as exc:
        await _ranged_download(tree, "bytes=0-0", path="work/empty.png")
    assert exc.value.status_code == 416
    assert exc.value.headers == {"Content-Range": "bytes */0"}


@pytest.mark.asyncio
async def test_a_range_past_the_end_is_416_without_a_version(tree) -> None:
    """A 416 carries no validator, so a file too fresh to version gets one too."""
    unversioned = ContainedSlice(f"{tree.root}/work/chart.png", 10, None, b"", None)
    with patch(_CRUD_READ_RANGE, AsyncMock(return_value=unversioned)):
        with pytest.raises(HTTPException) as exc:
            await _ranged_download(tree, "bytes=10-20")
    assert exc.value.status_code == 416


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "header"),
    [
        ("work/report.html", "bytes=0-3"),
        ("work/chart.png", "bytes=-4"),
        ("work/chart.png", "bytes=4-"),
        ("work/chart.png", "bytes=0-1,4-5"),
        ("work/chart.png", None),
    ],
)
async def test_a_range_the_route_does_not_honor_goes_whole(
    tree, path, header
) -> None:
    """Text keeps its redaction, which needs the whole body; a suffix or
    several ranges are shapes the route ignores rather than half-serves."""
    response = await _ranged_download(tree, header, path=path)
    assert response.status_code == 200
    assert response.body == Path(tree.root, path).read_bytes()
    assert "content-range" not in response.headers


@pytest.mark.asyncio
async def test_a_binary_name_linked_to_a_text_file_goes_whole_and_redacted(
    tree,
) -> None:
    """The type comes from where the path lands, so a link named like a PDF
    cannot carry a text file out unredacted a range at a time."""
    Path(tree.root, "work/notes.txt").write_text(f"key={VAULT_SECRET}\n")
    Path(tree.root, "work/notes.pdf").symlink_to(Path(tree.root, "work/notes.txt"))
    response = await _ranged_download(tree, "bytes=0-3", path="work/notes.pdf")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.body.startswith(b"key=")
    assert VAULT_SECRET.encode() not in response.body


@pytest.mark.asyncio
async def test_text_under_a_binary_name_goes_whole_and_redacted(tree) -> None:
    """The whole answer redacts any body that decodes as UTF-8, whatever its
    name, so a range of one would carry what that answer hides."""
    Path(tree.root, "work/settings.bin").write_text(f"key={VAULT_SECRET}\n")
    response = await _ranged_download(tree, "bytes=0-3", path="work/settings.bin")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.body.startswith(b"key=")
    assert VAULT_SECRET.encode() not in response.body


@pytest.mark.asyncio
async def test_a_file_without_a_version_goes_whole(tree) -> None:
    chart = Path(tree.root, "work/chart.png").read_bytes()
    unversioned = ContainedSlice(
        f"{tree.root}/work/chart.png", 10, None, chart, chart[:4]
    )
    with patch(_CRUD_READ_RANGE, AsyncMock(return_value=unversioned)):
        response = await _ranged_download(tree, "bytes=0-3")
    assert response.status_code == 200
    assert response.body == chart


@pytest.mark.asyncio
async def test_a_file_that_changed_mid_read_is_a_503_to_retry(tree) -> None:
    """Not the sandbox-unreachable answer: the sandbox is fine, the file moved."""
    changed = AsyncMock(side_effect=FileChangedWhileRead("work/chart.png"))
    with patch(_CRUD_READ_RANGE, changed):
        with pytest.raises(HTTPException) as exc:
            await _ranged_download(tree, "bytes=0-3")
    assert exc.value.status_code == 503
    # A code, since the file panel tells 503s apart by it.
    assert exc.value.detail == {
        "code": "file_changed",
        "message": "File changed while it was read",
    }
    assert exc.value.headers == {"Retry-After": "1"}


# --- ranged download through a warm session ---------------------------------


def _ranged_commands(sandbox) -> list[str]:
    return [c for c in sandbox.runtime.commands if "date +%s" in c]


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
async def test_a_warm_session_answers_a_range_without_an_acquisition(tree) -> None:
    """The viewer sends a request per slice, and an acquisition for each would
    hold the folder nearly all the time a reader scrolls, so a rename's settle
    could never move it."""
    chart = Path(tree.root, "work/chart.png").read_bytes()
    events: list[str] = []
    response = await _ranged_download(
        tree, "bytes=2-5", warm=tree.sandbox, events=events
    )
    assert response.status_code == 206
    assert response.body == chart[2:6]
    assert events == []


@pytest.mark.asyncio
async def test_a_warm_refusal_stands_without_an_acquisition(tree) -> None:
    events: list[str] = []
    with pytest.raises(HTTPException) as exc:
        await _ranged_download(tree, "bytes=10-20", warm=tree.sandbox, events=events)
    assert exc.value.status_code == 416
    assert events == []


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
@pytest.mark.parametrize("header", ["bytes=2-5", "bytes=10-20"])
async def test_a_warm_read_whose_folder_moved_is_read_again_under_the_hold(
    tree, header
) -> None:
    """Read without a hold, the folder may since have gone to a sibling that
    landed on its name, so nothing the read found stands, a refusal included."""
    events: list[str] = []
    moved = {**_workspace(), "dir_name": "renamed"}
    try:
        response = await _ranged_download(
            tree, header, warm=tree.sandbox, folder_now=moved, events=events
        )
    except HTTPException as e:
        assert (header, e.status_code) == ("bytes=10-20", 416)
    else:
        assert (header, response.status_code) == ("bytes=2-5", 206)
    assert events == ["released"]
    assert len(_ranged_commands(tree.sandbox)) == 2


@pytest.mark.asyncio
async def test_a_file_the_warm_read_cannot_find_is_looked_for_under_the_hold(
    tree,
) -> None:
    """A workspace moving to another computer keeps its folder's name while the
    folder is set aside, so a warm miss may be a file away for the move, which
    only the acquisition answers as such."""
    events: list[str] = []
    with pytest.raises(HTTPException) as exc:
        await _ranged_download(
            tree, "bytes=0-3", path="work/gone.png", warm=tree.sandbox, events=events
        )
    assert exc.value.status_code == 404
    assert events == ["released"]
    assert len(_ranged_commands(tree.sandbox)) == 2


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
async def test_a_workspace_without_a_warm_session_is_acquired(tree) -> None:
    events: list[str] = []
    response = await _ranged_download(tree, "bytes=2-5", events=events)
    assert response.status_code == 206
    assert events == ["released"]


@pytest.mark.usefixtures("settled")
@pytest.mark.asyncio
async def test_a_warm_sandbox_that_does_not_answer_is_acquired(tree) -> None:
    """The acquisition is what retires a dead handle and attaches anew."""
    dead = _FsSandbox(tree.root)
    dead.runtime = SimpleNamespace(
        exec=AsyncMock(side_effect=SandboxTransientError("no answer"))
    )
    events: list[str] = []
    response = await _ranged_download(tree, "bytes=2-5", warm=dead, events=events)
    assert response.status_code == 206
    assert events == ["released"]


@pytest.mark.asyncio
async def test_a_file_the_warm_read_sends_whole_is_not_ranged_again(tree) -> None:
    Path(tree.root, "work/settings.bin").write_text(f"key={VAULT_SECRET}\n")
    events: list[str] = []
    response = await _ranged_download(
        tree, "bytes=0-3", path="work/settings.bin", warm=tree.sandbox, events=events
    )
    assert response.status_code == 200
    assert VAULT_SECRET.encode() not in response.body
    assert events == ["released"]
    assert len(_ranged_commands(tree.sandbox)) == 1
