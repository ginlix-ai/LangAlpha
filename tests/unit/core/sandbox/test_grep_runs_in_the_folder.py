"""The agent's Grep, run as the sandbox runs it: rg in the turn's folder,
which is where it matches a ``glob`` filter from, whatever path it searches,
except in the computer root's ``.agents``, whose files it matches from the
root."""

import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ptc_agent.core.paths import grep_working_dir, resolve_agent_path
from ptc_agent.core.sandbox import files
from ptc_agent.core.sandbox.files import agrep_content


class _LocalRuntime:
    """Runs a command from the runtime's own working directory, the computer
    root, finding ``rg`` in ``bin_dir`` first when one is given. Its errors
    land in the output, as the sandbox's exec folds them in."""

    def __init__(self, root: Path, bin_dir: Path | None = None) -> None:
        self.root = root
        self._env = None
        if bin_dir is not None:
            self._env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}

    async def exec(self, command: str, timeout: int = 60):
        done = subprocess.run(
            ["/bin/sh", "-c", command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=self.root,
            env=self._env,
        )
        return SimpleNamespace(stdout=done.stdout, stderr="", exit_code=done.returncode)


def _sandbox(runtime: _LocalRuntime, folder: Path) -> SimpleNamespace:
    async def _call(fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)

    def _normalize_search_path(path: str) -> str:
        if path == ".":
            return str(folder)
        return path if path.startswith("/") else f"{folder}/{path}"

    return SimpleNamespace(
        _wait_ready=AsyncMock(),
        config=SimpleNamespace(
            filesystem=SimpleNamespace(enable_path_validation=False)
        ),
        _work_dir=str(runtime.root),
        _normalize_search_path=_normalize_search_path,
        runtime=runtime,
        _runtime_call=_call,
    )


def _bin_with_rg(tmp_path: Path, script: str) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "rg").write_text(f"#!/bin/sh\n{script}\n")
    (bin_dir / "rg").chmod(0o755)
    return bin_dir


@pytest.mark.asyncio
async def test_rg_runs_in_the_turns_folder(tmp_path):
    """An ``rg`` that prints where it runs prints the folder, not the root
    the runtime starts in."""
    bin_dir = _bin_with_rg(tmp_path, "pwd")
    root = tmp_path / "root"
    folder = root / "Home"
    folder.mkdir(parents=True)

    sandbox = _sandbox(_LocalRuntime(root, bin_dir), folder)

    assert await agrep_content(sandbox, "x", str(root)) == [str(folder)]


@pytest.mark.parametrize(
    ("searched", "runs_in"),
    [
        (".agents", "root"),
        (".agents/skills", "root"),
        (".agents/tmp", "root"),
        (".agents/user/channels", "root"),
        (".agents/user/memory", "root"),
        (".agents/workflows", "root"),
        ("Home/data", "folder"),
        # The folder's own file under a computer-tier name is the folder's.
        ("Home/.agents/tmp", "folder"),
        ("Other", "folder"),
        ("", "folder"),
    ],
)
@pytest.mark.asyncio
async def test_rg_runs_from_the_root_in_its_agents(tmp_path, searched, runs_in):
    """Grep prints a computer-tier file as ``/.agents/...``, spelled from the
    root, so rg runs there for the root's ``.agents`` and anything in it; a
    sibling's folder, the root itself and the folder's own files keep the
    folder."""
    bin_dir = _bin_with_rg(tmp_path, "pwd")
    root = tmp_path / "root"
    folder = root / "Home"
    path = root / searched if searched else root
    for directory in (folder, path):
        directory.mkdir(parents=True, exist_ok=True)

    sandbox = _sandbox(_LocalRuntime(root, bin_dir), folder)

    expected = root if runs_in == "root" else folder
    assert await agrep_content(sandbox, "x", str(path)) == [str(expected)]


@pytest.mark.parametrize(
    ("path", "runs_in"),
    [
        ("/r/.agents", "/r"),
        ("/r/.agents/", "/r"),
        ("/r/.agents/skills", "/r"),
        ("/r/.agents/user/memory/a.md", "/r"),
        ("/r/Home/.agents/memory", "/r/Home"),
        ("/r/Home", "/r/Home"),
        ("/r/Research", "/r/Home"),
        ("/r", "/r/Home"),
        ("/r/.agentsx", "/r/Home"),
        ("/r/x/.agents", "/r/Home"),
    ],
)
def test_the_root_runs_only_its_own_agents(path, runs_in):
    """The root's ``.agents`` and what is below it run from the root; the
    root itself, a name that only starts like it, and any other folder's
    ``.agents`` keep the folder."""
    assert grep_working_dir(path, folder="/r/Home", root="/r/") == runs_in


@pytest.mark.asyncio
async def test_rg_runs_in_the_folder_it_is_handed(tmp_path):
    """A caller pinned to a folder hands it over, and rg runs there rather
    than in the sandbox's own, so the two match a filter from one place."""
    bin_dir = _bin_with_rg(tmp_path, "pwd")
    root = tmp_path / "root"
    pinned = root / "Research"
    for directory in (root / "Home", pinned):
        directory.mkdir(parents=True)

    sandbox = _sandbox(_LocalRuntime(root, bin_dir), root / "Home")

    assert await agrep_content(sandbox, "x", str(root), folder=str(pinned)) == [
        str(pinned)
    ]


@pytest.mark.parametrize(
    ("output_mode", "as_found"),
    [
        pytest.param("files_with_matches", lambda hit: hit, id="files"),
        pytest.param("count", lambda hit: (hit, 1), id="count"),
    ],
)
@pytest.mark.asyncio
async def test_a_missing_folder_is_never_a_hit(
    tmp_path, monkeypatch, output_mode, as_found
):
    """A folder gone mid-move leaves rg in the runtime's own directory, and
    the shell's complaint, which the exec folds into the output, is never
    read as a matching file; the server hears of it instead."""
    bin_dir = _bin_with_rg(
        tmp_path,
        'case "$1" in -c) echo "$(pwd)/a.txt:1" ;; *) echo "$(pwd)/a.txt" ;; esac',
    )
    root = tmp_path / "root"
    (root / "data").mkdir(parents=True)
    logger = Mock()
    monkeypatch.setattr(files, "logger", logger)

    sandbox = _sandbox(_LocalRuntime(root, bin_dir), root / "Gone")
    found = await agrep_content(sandbox, "x", str(root / "data"), output_mode)

    assert found == [as_found(f"{root}/a.txt")]
    logger.warning.assert_called_once()


@pytest.mark.skipif(shutil.which("rg") is None, reason="needs ripgrep")
@pytest.mark.asyncio
async def test_a_filter_matches_from_the_folder(tmp_path):
    """``reports/*.json`` and ``/reports/*.json`` match the folder's
    ``reports`` from the folder or from the root above it, and a path
    outside the folder is matched whole, as Grep prints it."""
    root = tmp_path / "root"
    folder = root / "Home"
    for rel in ("Home/reports/a.json", "Home/top.json", "data/d.json"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text("x\n")
    sandbox = _sandbox(_LocalRuntime(root), folder)

    async def grep(glob: str, path: str = ".") -> list[str]:
        return sorted(await agrep_content(sandbox, "x", path, glob=glob))

    report = str(folder / "reports" / "a.json")
    assert await grep("reports/*.json") == [report]
    assert await grep("/reports/*.json") == [report]
    assert await grep("Home/reports/*.json") == []
    assert await grep("reports/*.json", str(root)) == [report]
    assert await grep("data/*.json", str(root)) == []
    assert await grep("**/data/*.json", str(root)) == [str(root / "data" / "d.json")]


@pytest.mark.skipif(shutil.which("rg") is None, reason="needs ripgrep")
@pytest.mark.asyncio
async def test_a_filter_matches_a_computer_tier_file_as_grep_prints_it(tmp_path):
    """``.agents/tmp/*.csv`` and ``/.agents/tmp/*.csv`` match the computer's
    ``.agents/tmp``, and ``.agents/user/channels/*`` a folder of the user tier
    no route serves, which the file mount reaches through a link."""
    root = tmp_path / "root"
    folder = root / "Home"
    mount = tmp_path / "mnt" / "user"
    for directory in (folder, root / ".agents" / "tmp", mount / "channels"):
        directory.mkdir(parents=True)
    (root / ".agents" / "tmp" / "x.csv").write_text("x\n")
    (mount / "channels" / "c.md").write_text("x\n")
    (root / ".agents" / "user").symlink_to(mount)
    sandbox = _sandbox(_LocalRuntime(root), folder)

    async def grep(glob: str, path: str) -> list[str]:
        searched = resolve_agent_path(
            path, workspace=str(folder), root=str(root), allowed=[str(root)]
        )
        return sorted(await agrep_content(sandbox, "x", searched, glob=glob))

    scratch = str(root / ".agents" / "tmp" / "x.csv")
    assert await grep(".agents/tmp/*.csv", ".agents/tmp") == [scratch]
    assert await grep("/.agents/tmp/*.csv", ".agents/tmp") == [scratch]
    assert await grep(".agents/user/channels/*", ".agents/user") == [
        str(root / ".agents" / "user" / "channels" / "c.md")
    ]
