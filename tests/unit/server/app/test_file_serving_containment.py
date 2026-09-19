"""Symlink and path-escape containment on the routes that serve a file by path.

The sandbox double reads a real temp tree through the real ``/bin/sh`` probe, so
a missed containment check does not merely return the wrong status: it hands the
test the bytes of a file outside the serve root. The tree mirrors the two escapes
that matter once several workspaces share one filesystem -- a symlink pointing
out of the root entirely, and one pointing at the root's reserved ``_internal``.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.share_access import ShareScope
from src.server.app.share_files import (
    download_shared_file,
    serve_shared_file,
)
from src.server.app.workspace_files._containment import (
    PROBE_ESCAPED,
    contained_relative_path,
    contained_sandbox_path,
    contained_sandbox_paths,
    is_within,
    probe_command,
    resolve_in_sandbox,
    serving_roots,
)
from src.server.app.workspace_files.crud import (
    download_workspace_file,
    read_workspace_file,
)
from src.server.app.workspace_files.serve import serve_workspace_file
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
_CRUD_ACQUIRE = "src.server.app.workspace_files.crud._acquire_sandbox"
_CRUD_WD = "src.server.app.workspace_files.crud.owner_work_dir"


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
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE, AsyncMock(return_value=tree.sandbox)),
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
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE, AsyncMock(return_value=tree.sandbox)),
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
        patch(_CRUD_OWNER, MagicMock()),
        patch(_CRUD_WD, return_value=tree.root),
        patch(_CRUD_ACQUIRE, AsyncMock(return_value=tree.sandbox)),
    ):
        response = await download_workspace_file(
            WS_ID, OWNER, request, path="work/chart.png"
        )
    assert response.status_code == 200
    assert response.body == b"\x89PNG\r\n\x1a\n\xff\xfe"
