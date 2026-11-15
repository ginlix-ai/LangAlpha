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

from ptc_agent.core.sandbox.providers._ssh_root import root_dir_script
from ptc_agent.core.sandbox.providers._ssh_shell import (
    computer_dir,
    computers_dir,
    delete_script,
    layout_script,
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
        == (*checked, root)
    )
    assert (tmp_path / ".langalpha" / "computers" / "c1").exists() is not bool(reclaim)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
def test_a_take_over_of_a_folder_renamed_since_stops_nothing(tmp_path):
    """The take-over keeps the spelling the create's probe found, so a folder
    renamed since is refused before the computer marking it is stopped."""
    from ptc_agent.core.sandbox.providers.ssh import _check_root

    home = str(tmp_path.resolve())

    async def run(script: str, *, timeout: float, root: bool = False):
        done = subprocess.run(["bash", "-c", script], capture_output=True)
        return SimpleNamespace(exit_code=done.returncode, stdout=done.stdout)

    claim = root_dir_script(
        f"{home}/LangAlpha", computers=computers_dir(home), computer_id="c1"
    )
    for script in (claim, layout_script(computer_dir(home, "c1"))):
        assert subprocess.run(["bash", "-c", script]).returncode == 0
    if not (tmp_path / "langalpha").exists():
        pytest.skip("the disk tells names apart by case")

    checked = asyncio.run(
        _check_root(
            SimpleNamespace(run=run),
            f"{home}/langalpha",
            home=home,
            reclaim=("c1",),
            timeout=30,
            adopt=False,
        )
    )

    assert checked.exit_code == 12
    assert (tmp_path / ".langalpha" / "computers" / "c1").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize(
    "typed,checked",
    [
        ("research", (0, "", "Research")),
        ("Cafe\u0301", (0, "", "Caf\u00e9")),
        ("RESEARCH/new", (0, "", "Research/new")),
        ("link", (12, "", "link")),
    ],
    ids=["case", "normalization", "parent-case", "symlink"],
)
def test_a_folder_typed_in_another_spelling_is_kept_as_the_disk_has_it(
    tmp_path, typed, checked
):
    """A case-insensitive disk takes either spelling for one folder, so a root
    typed in other letter case or Unicode normalization is the folder there,
    kept as the disk spells it; one reached through a symbolic link is still
    refused."""
    from ptc_agent.core.sandbox.providers.ssh import _check_root

    home = str(tmp_path.resolve())
    (tmp_path / "Research").mkdir()
    (tmp_path / "Caf\u00e9").mkdir()
    (tmp_path / "target").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "target")
    if not (tmp_path / "RESEARCH").exists() or not (tmp_path / "Cafe\u0301").exists():
        pytest.skip("the disk tells names apart by case or normalization")

    async def run(script: str, *, timeout: float, root: bool = False):
        done = subprocess.run(["bash", "-c", script], capture_output=True)
        return SimpleNamespace(exit_code=done.returncode, stdout=done.stdout)

    exit_code, owner, root = asyncio.run(
        _check_root(
            SimpleNamespace(run=run), f"{home}/{typed}", home=home, reclaim=(), timeout=30
        )
    )

    assert (exit_code, owner, root) == (checked[0], checked[1], f"{home}/{checked[2]}")


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


_ICLOUD = "Library/Mobile Documents/com~apple~CloudDocs/la"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize(
    "folder,computer_id,system,denied,exit_code",
    [
        ("Projects/existing", None, "Darwin", "Projects", 16),
        ("Documents/existing", "c" * 32, "Darwin", "Documents", 16),
        ("Documents/new", None, "Darwin", "Documents", 16),
        ("Documents/existing", None, "Darwin", None, 16),
        ("Desktop/la", None, "Darwin", None, 16),
        ("Downloads/la", None, "Darwin", None, 16),
        ("Documents/existing", "c" * 32, "Darwin", None, 0),
        (_ICLOUD, None, "Darwin", None, 17),
        (_ICLOUD, "c" * 32, "Darwin", None, 0),
        (_ICLOUD, None, "Linux", None, 0),
        ("Documents/existing", None, "Linux", None, 0),
    ],
    ids=[
        "privacy-elsewhere",
        "privacy-already-working",
        "privacy-new",
        "documents-with-full-disk-access",
        "desktop-new",
        "downloads-new",
        "documents-already-working",
        "icloud-new",
        "icloud-already-working",
        "icloud-path-off-a-mac",
        "documents-off-a-mac",
    ],
)
def test_a_folder_the_mac_keeps_private_or_syncs_is_refused(
    tmp_path, folder, computer_id, system, denied, exit_code
):
    """macOS's privacy controls fail a listing the file modes allow, which
    reads as an empty folder. iCloud can sync Desktop and Documents with no
    sign in the path, and would upload the computer's credentials, so a new
    folder there is refused even where the login may list it, but a working
    one starts. The same paths on another system are only folders."""
    home = tmp_path.resolve() / "home"
    (home / "Documents" / "existing").mkdir(parents=True)
    (home / "Projects" / "existing").mkdir(parents=True)
    (home / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / "la").mkdir(
        parents=True
    )
    shim = tmp_path / "shim"
    shim.mkdir()
    if denied:
        (shim / "ls").write_text(
            f'#!/bin/sh\nfor a; do case $a in */{denied}*) '
            'echo "ls: $a: Operation not permitted" >&2; exit 1;; esac; done\n'
            'exec /bin/ls "$@"\n'
        )
        (shim / "ls").chmod(0o755)
    (shim / "uname").write_text(f"#!/bin/sh\necho {system}\n")
    (shim / "uname").chmod(0o755)
    script = root_dir_script(
        f"{home}/{folder}", computers=computers_dir(str(home)), computer_id=computer_id
    )

    done = subprocess.run(
        ["bash", "-c", script],
        env={"PATH": f"{shim}:/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home)},
        capture_output=True,
    )

    assert done.returncode == exit_code


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize(
    "folder,exit_code",
    [
        (_ICLOUD, 17),
        ("Library/mobile documents/com~apple~CloudDocs/la", 17),
        ("documents/la", 16),
    ],
    ids=["icloud", "icloud-other-case", "documents-other-case"],
)
def test_a_new_folder_in_icloud_is_refused_before_icloud_holds_anything(
    tmp_path, folder, exit_code
):
    """With nothing yet under Mobile Documents the nearest folder that exists
    is outside it, so the folder asked for is what the check reads, in any
    case, since a Mac's disk takes any."""
    home = tmp_path.resolve() / "home"
    (home / "Library").mkdir(parents=True)
    shim = tmp_path / "shim"
    shim.mkdir()
    (shim / "uname").write_text("#!/bin/sh\necho Darwin\n")
    (shim / "uname").chmod(0o755)
    script = root_dir_script(
        f"{home}/{folder}", computers=computers_dir(str(home)), computer_id=None
    )

    done = subprocess.run(
        ["bash", "-c", script],
        env={"PATH": f"{shim}:/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home)},
        capture_output=True,
    )

    assert done.returncode == exit_code

