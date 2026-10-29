"""A create that fails after the layout takes the computer's folder back.

The root's mark names the computer, and a root marked by a computer whose
folder still exists is refused as that computer's, so a failed create left
behind would lock the retry, which comes with a new id, out of the folder the
user chose.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox.providers._ssh_shell import (
    computer_dir,
    computers_dir,
    delete_script,
    layout_script,
    root_dir_script,
)
from ptc_agent.core.sandbox.providers._ssh_transport import HostFacts, Link
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime

HOME = "/home/u"
ID = "c1"


def _runtime(*, fails_with: BaseException) -> tuple[SSHRuntime, Link, AsyncMock]:
    # The constructor needs a target and a key loader; these paths read neither.
    runtime = object.__new__(SSHRuntime)
    runtime._id = ID
    runtime._target = SimpleNamespace(host="m", label="u@m")
    runtime._working_dir = "/home/u/langalpha"
    runtime._bootstrap_timeout = 60
    runtime._login_from = ()
    link = Link(runtime._target, MagicMock())
    link.facts = HostFacts(uid=1000, user="u", home=HOME, os="Linux", arch="x86_64")
    runtime._lifecycle_link = AsyncMock(return_value=link)
    runtime._ensure_root = AsyncMock()
    runtime._ensure_layout = AsyncMock(return_value=False)
    runtime._bootstrap = AsyncMock(side_effect=fails_with)
    runtime._run = AsyncMock(return_value="")
    return runtime, link, runtime._run


def _deletes(run: AsyncMock) -> list[str]:
    script = delete_script(computer_dir(HOME, ID))
    return [c.args[1] for c in run.await_args_list if c.args[1] == script]


@pytest.mark.asyncio
async def test_a_failed_install_removes_the_folder_before_raising():
    runtime, _, run = _runtime(fails_with=RuntimeError("install failed"))

    with pytest.raises(RuntimeError, match="install failed"):
        await runtime.create({})

    assert len(_deletes(run)) == 1


@pytest.mark.asyncio
async def test_a_cancelled_create_still_removes_the_folder():
    runtime, link, run = _runtime(fails_with=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await runtime.create({})
    await asyncio.gather(*link._tasks)

    assert len(_deletes(run)) == 1


@pytest.mark.asyncio
async def test_a_create_failing_right_after_taking_the_root_removes_the_folder():
    """Taking the root made the computer's folder, which keeps the root marked
    as this computer's for the retry."""
    runtime, _, run = _runtime(fails_with=RuntimeError("unused"))
    runtime._ensure_key_line = AsyncMock(side_effect=RuntimeError("connection lost"))

    with pytest.raises(RuntimeError, match="connection lost"):
        await runtime.create({})

    assert len(_deletes(run)) == 1


@pytest.mark.asyncio
async def test_a_root_refused_before_the_layout_removes_nothing():
    runtime, _, run = _runtime(fails_with=RuntimeError("unused"))
    runtime._ensure_root.side_effect = RuntimeError("root refused")

    with pytest.raises(RuntimeError, match="root refused"):
        await runtime.create({})

    assert _deletes(run) == []


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
def test_the_retry_can_claim_the_root_once_the_folder_is_gone(tmp_path):
    home = str(tmp_path.resolve())
    root = f"{home}/langalpha"

    def sh(script: str) -> int:
        return subprocess.run(["bash", "-c", script], check=False).returncode

    def claim(computer_id: str) -> int:
        return sh(
            root_dir_script(
                root, computers=computers_dir(home), computer_id=computer_id
            )
        )

    assert claim("c1") == 0
    assert sh(layout_script(computer_dir(home, "c1"))) == 0
    # The first create's install failed with its folder still there.
    assert claim("c2") == 10

    assert sh(delete_script(computer_dir(home, "c1"))) == 0

    assert claim("c2") == 0


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize("at_once", [True, False], ids=["at once", "in turn"])
def test_two_computers_cannot_both_take_a_root(tmp_path, at_once):
    """Each found the folder free and marked it, and both went on to use it:
    at once, or in turn before the first had made its own folder."""
    home = str(tmp_path.resolve())
    scripts = [
        root_dir_script(
            f"{home}/langalpha", computers=computers_dir(home), computer_id=c
        )
        for c in ("c1", "c2")
    ]

    def claim(script: str) -> subprocess.Popen:
        return subprocess.Popen(["bash", "-c", script], stdout=subprocess.DEVNULL)

    if at_once:
        exits = sorted(p.wait() for p in [claim(s) for s in scripts])
    else:
        exits = [claim(s).wait() for s in scripts]

    assert exits == [0, 10]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize(
    "reclaim,checked", [((), (10, "c1")), (("c1",), (0, ""))]
)
def test_the_create_probe_clears_only_a_removed_computers_mark(
    tmp_path, reclaim, checked
):
    """A removal that never reached the machine left c1's folder and mark,
    which would refuse the folder to every new computer."""
    from ptc_agent.core.sandbox.providers.ssh import _check_root

    home = str(tmp_path.resolve())
    root = f"{home}/langalpha"

    async def run(script: str, *, timeout: float, root: bool = False):
        done = subprocess.run(["bash", "-c", script], capture_output=True)
        return SimpleNamespace(exit_code=done.returncode, stdout=done.stdout)

    claim = root_dir_script(root, computers=computers_dir(home), computer_id="c1")
    for script in (claim, layout_script(computer_dir(home, "c1"))):
        assert subprocess.run(["bash", "-c", script]).returncode == 0

    link = SimpleNamespace(run=run)
    assert (
        asyncio.run(_check_root(link, root, home=home, reclaim=reclaim, timeout=30))
        == checked
    )
    assert (tmp_path / ".langalpha" / "computers" / "c1").exists() is not bool(reclaim)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
def test_taking_a_root_leaves_its_top_writable_by_the_login_alone(tmp_path):
    """Another account in the group could swap in a workspace folder whose
    .agents/tools the agent imports."""
    home = str(tmp_path.resolve())
    root = tmp_path / "langalpha"
    (root / "ws" / ".agents").mkdir(parents=True)
    for folder in (root, root / "ws", root / "ws" / ".agents"):
        folder.chmod(0o775)
    (root / ".langalpha-root").write_text("c1\n")

    script = root_dir_script(str(root), computers=computers_dir(home), computer_id="c1")
    assert subprocess.run(["bash", "-c", script]).returncode == 0

    assert root.stat().st_mode & 0o777 == 0o755
    assert (root / "ws").stat().st_mode & 0o777 == 0o755
    # Below the top the folders are the owner's to keep as they are.
    assert (root / "ws" / ".agents").stat().st_mode & 0o777 == 0o775
