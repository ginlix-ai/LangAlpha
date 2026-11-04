"""A Grep or Glob of a folder above one a machine holds back from the file
tools (its Claude Code tokens) never reaches into it, run as the sandbox runs
them: real rg and the shipped glob script, against a real tree."""

import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.paths import CLAUDE_CODE_TOKEN_DIR
from ptc_agent.core.project_context import MachinePaths, ProjectContext, project_bound
from ptc_agent.core.sandbox.files import aglob_files, agrep_content

CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
SECRET = "sk-ant-oat01-not-a-real-token"


class _LocalRuntime:
    def __init__(self, cwd: Path) -> None:
        self.cwd = cwd

    async def exec(self, command: str, timeout: int = 60):
        command = command.replace("python3", sys.executable, 1)
        done = subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True, cwd=self.cwd)
        return SimpleNamespace(stdout=done.stdout + done.stderr, stderr="", exit_code=done.returncode)


def _sandbox(home: Path, folder: Path) -> SimpleNamespace:
    async def _call(fn, *args, retry_policy=None, **kwargs):
        return await fn(*args, **kwargs)

    def _normalize_search_path(path: str) -> str:
        return str(folder) if path == "." else path

    return SimpleNamespace(
        _wait_ready=AsyncMock(),
        config=SimpleNamespace(filesystem=SimpleNamespace(enable_path_validation=False)),
        _work_dir=str(home),
        _normalize_search_path=_normalize_search_path,
        runtime=_LocalRuntime(home),
        _runtime_call=_call,
    )


def _home(base: Path, config: str) -> tuple[Path, Path]:
    """A home whose config home is ``config``, holding a token file, and
    a file beside the token folder that the same search finds; the home and
    the folder held back."""
    home = base / "home" / "u"
    tokens = home / config / CLAUDE_CODE_TOKEN_DIR
    tokens.mkdir(parents=True)
    (tokens / CID).write_text(SECRET + "\n")
    (home / config / "other").mkdir()
    (home / config / "other" / "settings").write_text(SECRET + "\n")
    return home, home / config / "langalpha"


def _machine(home: Path, held: Path) -> ProjectContext:
    return ProjectContext(workspace_id="", dir_name=None, machine=MachinePaths(home=str(home), denied=(str(held),)))


@pytest.mark.skipif(shutil.which("rg") is None, reason="needs ripgrep")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config", "searched", "runs_in"),
    [
        (".config", ".config", "home"),
        (".config", ".config", "elsewhere"),
        ("cfg", "", "home"),
        ("cfg", "", "elsewhere"),
        ("cfg", "cfg", "root"),
    ],
)
@pytest.mark.parametrize("output_mode", ["content", "files_with_matches", "count"])
async def test_grep_of_a_folder_above_the_tokens_never_returns_them(tmp_path, config, searched, runs_in, output_mode):
    home, held = _home(tmp_path, config)
    folder = {"home": home, "elsewhere": tmp_path / "elsewhere", "root": tmp_path}[runs_in]
    folder.mkdir(exist_ok=True)
    search = str(home / searched) if searched else str(home)

    with project_bound(_machine(home, held)):
        found = await agrep_content(_sandbox(home, folder), "sk-ant", search, output_mode=output_mode)

    text = "\n".join(map(str, found))
    assert "other/settings" in text
    assert CID not in text and "langalpha" not in text


@pytest.mark.skipif(shutil.which("rg") is None, reason="needs ripgrep")
@pytest.mark.asyncio
async def test_grep_steps_over_a_held_folder_named_with_glob_characters(tmp_path):
    home, held = _home(tmp_path / "a [b]{c,d}*?", "cfg")
    with project_bound(_machine(home, held)):
        found = await agrep_content(_sandbox(home, tmp_path), "sk-ant", str(home), output_mode="content")
    text = "\n".join(map(str, found))
    assert "other/settings" in text
    assert CID not in text


@pytest.mark.asyncio
@pytest.mark.parametrize(("config", "searched"), [(".config", ".config"), ("cfg", ""), ("cfg", "cfg")])
@pytest.mark.parametrize("pattern", ["**/*", "*/*/*", f"langalpha/claude-code/{CID}", f"cfg/langalpha/claude-code/{CID}"])
async def test_glob_of_a_folder_above_the_tokens_never_lists_them(tmp_path, config, searched, pattern):
    home, held = _home(tmp_path, config)
    search = str(home / searched) if searched else str(home)

    with project_bound(_machine(home, held)):
        found = await aglob_files(_sandbox(home, home), pattern, search)

    assert not any(CID in path for path in found)


@pytest.mark.asyncio
async def test_glob_still_lists_whats_beside_the_tokens(tmp_path):
    home, held = _home(tmp_path, ".config")
    with project_bound(_machine(home, held)):
        found = await aglob_files(_sandbox(home, home), "**/*", str(home / ".config"))
    assert found == [str(home / ".config" / "other" / "settings")]
