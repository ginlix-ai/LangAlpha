"""CopyFile, run against two folders standing in for two computers.

The probes and the joining script run in the local bash, so a script that a
Mac's bash 3.2 or BSD ``wc`` would read differently fails here first.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.agent.tools import copy_file as cf
from ptc_agent.agent.tools.computer_reach import ComputerTarget, ComputerUnreachable
from ptc_agent.core.paths import lexical_path
from ptc_agent.core.project_context import ProjectContext

PART = 1000


class LocalMachine:
    """The sandbox calls CopyFile makes, served from a local folder."""

    def __init__(self, root: Path, *, break_upload_at: int | None = None) -> None:
        self.root = str(root)
        self.uploads = 0
        self.break_upload_at = break_upload_at
        self.config = SimpleNamespace(
            filesystem=SimpleNamespace(enable_path_validation=True, allowed_directories=[self.root])
        )

    def normalize_path(self, path: str, project=None) -> str:
        """As a computer's file tools fold a path: an absolute one outside the
        allowed folders is read as a path inside the root."""
        path = path.strip()
        for allowed in self.config.filesystem.allowed_directories:
            if path == allowed or path.startswith(f"{allowed}/"):
                return lexical_path(path)
        return lexical_path(f"{self.root}/{path.lstrip('/')}")

    def validate_path(self, path: str, project=None) -> bool:
        """The allowed folders and below, but not the root's credentials, as a
        computer's file tools allow; matched as a Mac's disk matches names."""
        whole = self.normalize_path(path).casefold()
        internal = f"{self.root}/_internal".casefold()
        if whole == internal or whole.startswith(f"{internal}/"):
            return False
        return any(
            whole == d.casefold() or whole.startswith(f"{d.casefold()}/")
            for d in self.config.filesystem.allowed_directories
        )

    async def execute_bash_command(self, command, working_dir=None, timeout=60):
        done = await asyncio.to_thread(
            subprocess.run,
            ["bash", "-c", command],
            cwd=working_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "success": done.returncode == 0,
            "stdout": done.stdout,
            "stderr": done.stderr,
            "exit_code": done.returncode,
        }

    async def astream_file_bytes(self, path: str):
        if not os.path.isfile(path):
            return None

        async def chunks():
            with open(path, "rb") as f:
                while chunk := f.read(300):
                    yield chunk

        return chunks()

    async def aupload_file_bytes(self, path: str, content: bytes, verify: bool = False) -> bool:
        self.uploads += 1
        if self.break_upload_at is not None and self.uploads >= self.break_upload_at:
            raise RuntimeError("the upload broke")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(content)
        return True


def _computer(computer_id: str, name: str, root: Path, **kw) -> ComputerTarget:
    root.mkdir(parents=True, exist_ok=True)
    return ComputerTarget(
        computer_id,
        name,
        SimpleNamespace(sandbox=LocalMachine(root, **kw)),
        ProjectContext(workspace_id="", dir_name=None),
        str(root),
    )


def _leftovers(*roots: Path) -> list[Path]:
    return [p for root in roots for p in root.rglob("*") if cf._COPY_TAG in p.name]


@pytest.fixture(autouse=True)
def _small_parts(monkeypatch):
    monkeypatch.setattr(cf, "_PART_BYTES", PART)


@pytest.fixture
def machines(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    return _computer("a", "Mac", a), _computer("b", "Cloud", b), a, b


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [0, 1, PART, 3 * PART, 3 * PART + 1])
async def test_a_file_lands_byte_for_byte_and_leaves_no_parts(machines, size):
    mac, cloud, a, b = machines
    data = os.urandom(size)
    (a / "data.bin").write_bytes(data)
    result = await cf.copy_file(mac, "data.bin", cloud, "ws/data.bin")
    assert hashlib.sha256((b / "ws/data.bin").read_bytes()).digest() == hashlib.sha256(data).digest()
    assert result.endswith(f'to {b}/ws/data.bin on computer "Cloud".')
    assert not _leftovers(a, b)


@pytest.mark.asyncio
async def test_a_folder_destination_keeps_the_files_name(machines):
    mac, cloud, a, b = machines
    (a / "notes.txt").write_text("hi\n")
    (b / "existing").mkdir()
    await cf.copy_file(mac, "notes.txt", cloud, "existing")
    # Spelled as a folder, it is made as one.
    await cf.copy_file(mac, "notes.txt", cloud, "new/")
    assert (b / "existing/notes.txt").read_text() == "hi\n"
    assert (b / "new/notes.txt").read_text() == "hi\n"


@pytest.mark.asyncio
async def test_a_file_already_there_is_kept_unless_overwrite(machines):
    mac, cloud, a, b = machines
    (a / "notes.txt").write_text("new\n")
    (b / "notes.txt").write_text("old\n")
    with pytest.raises(ComputerUnreachable, match="already exists.*overwrite=true"):
        await cf.copy_file(mac, "notes.txt", cloud, "notes.txt")
    assert (b / "notes.txt").read_text() == "old\n"
    await cf.copy_file(mac, "notes.txt", cloud, "notes.txt", overwrite=True)
    assert (b / "notes.txt").read_text() == "new\n"


@pytest.mark.asyncio
async def test_the_execute_bit_carries_over(machines):
    mac, cloud, a, b = machines
    (a / "run.sh").write_text("#!/bin/sh\n")
    (a / "run.sh").chmod(0o755)
    (a / "plain.txt").write_text("x")
    await cf.copy_file(mac, "run.sh", cloud, "run.sh")
    await cf.copy_file(mac, "plain.txt", cloud, "plain.txt")
    assert os.access(b / "run.sh", os.X_OK)
    assert not os.access(b / "plain.txt", os.X_OK)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "refusal"),
    [
        ("missing.bin", "There is no file or folder missing.bin"),
        # Read as a path under the root, as the file tools read it.
        ("/etc/hosts", "There is no file or folder /etc/hosts"),
    ],
)
async def test_a_source_that_is_not_here_is_refused(machines, source, refusal):
    mac, cloud, *_ = machines
    with pytest.raises(ComputerUnreachable, match=refusal):
        await cf.copy_file(mac, source, cloud, "x")


@pytest.mark.asyncio
async def test_a_copy_onto_itself_is_refused(machines):
    mac, _, a, _ = machines
    (a / "notes.txt").write_text("hi\n")
    with pytest.raises(ComputerUnreachable, match="are the same"):
        await cf.copy_file(mac, "notes.txt", mac, f"{a}/notes.txt", overwrite=True)


@pytest.mark.asyncio
async def test_a_file_over_the_cap_is_refused_before_a_byte_moves(machines, monkeypatch):
    mac, cloud, a, b = machines
    monkeypatch.setattr(cf, "COPY_MAX_BYTES", 10)
    (a / "data.bin").write_bytes(b"x" * 11)
    with pytest.raises(ComputerUnreachable, match="carries up to"):
        await cf.copy_file(mac, "data.bin", cloud, "data.bin")
    assert cloud.session.sandbox.uploads == 0


@pytest.mark.asyncio
async def test_a_copy_that_breaks_midway_leaves_nothing_behind(machines, tmp_path):
    mac, _, a, b = machines
    broken = _computer("b", "Cloud", b, break_upload_at=2)
    (a / "data.bin").write_bytes(os.urandom(3 * PART))
    with pytest.raises(RuntimeError, match="the upload broke"):
        await cf.copy_file(mac, "data.bin", broken, "data.bin")
    assert not (b / "data.bin").exists()
    assert not _leftovers(a, b)


@pytest.mark.asyncio
async def test_the_tool_answers_a_refusal_as_its_result(machines):
    mac, cloud, *_ = machines

    class Reach:
        async def target(self, computer):
            if computer == "Nowhere":
                raise ComputerUnreachable('The user has no computer named "Nowhere".')
            return cloud if computer else mac

        async def in_use(self, target):
            pass

    tool = cf.create_copy_file_tool(Reach())
    result = await tool.ainvoke(
        {"source": "x", "destination": "y", "destination_computer": "Nowhere"}
    )
    assert result == 'ERROR: The user has no computer named "Nowhere".'


@pytest.fixture
def tmpdir_env(tmp_path, monkeypatch):
    """Where a pack's archive goes, watched for leftovers."""
    archives = tmp_path / "tmp"
    archives.mkdir()
    monkeypatch.setenv("TMPDIR", str(archives))
    return archives


def _tree(root: Path) -> dict[str, str]:
    """Each entry under ``root``: a file's text, a link's target, or a folder."""
    out = {}
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root))
        out[rel] = f"-> {os.readlink(p)}" if p.is_symlink() else ("/" if p.is_dir() else p.read_text())
    return out


def _report(a: Path) -> Path:
    folder = a / "ws/report"
    (folder / "charts").mkdir(parents=True)
    (folder / "empty").mkdir()
    (folder / "summary.md").write_text("# NVDA\n")
    (folder / "charts/rev.csv").write_text("q,rev\n")
    (folder / "run.sh").write_text("#!/bin/sh\n")
    (folder / "run.sh").chmod(0o755)
    (folder / "latest").symlink_to("summary.md")
    (folder / "shared.txt").write_text("group-writable")
    (folder / "shared.txt").chmod(0o666)
    return folder


@pytest.mark.asyncio
async def test_a_folder_lands_whole_with_its_links_modes_and_empty_folders(machines, tmpdir_env):
    mac, cloud, a, b = machines
    folder = _report(a)
    result = await cf.copy_file(mac, "ws/report", cloud, "ws/")
    assert _tree(b / "ws/report") == _tree(folder)
    assert os.access(b / "ws/report/run.sh", os.X_OK)
    assert (b / "ws/report/shared.txt").stat().st_mode & 0o777 == 0o666
    assert result.startswith(f"Copied the folder {folder} on computer \"Mac\" (4 files,")
    assert not _leftovers(a, b)
    assert not list(tmpdir_env.iterdir())


@pytest.mark.asyncio
async def test_a_folder_can_take_a_new_name(machines, tmpdir_env):
    mac, cloud, a, b = machines
    _report(a)
    await cf.copy_file(mac, "ws/report", cloud, "ws/report-2026")
    assert (b / "ws/report-2026/summary.md").read_text() == "# NVDA\n"


@pytest.mark.asyncio
async def test_a_folder_merges_into_one_already_there_only_with_overwrite(machines, tmpdir_env):
    mac, cloud, a, b = machines
    _report(a)
    there = b / "ws/report"
    there.mkdir(parents=True)
    (there / "summary.md").write_text("old\n")
    (there / "notes.md").write_text("the user's own\n")
    with pytest.raises(ComputerUnreachable, match="overwrite=true to merge into it"):
        await cf.copy_file(mac, "ws/report", cloud, "ws/report")
    assert (there / "summary.md").read_text() == "old\n"
    result = await cf.copy_file(mac, "ws/report", cloud, "ws/report", overwrite=True)
    assert result.startswith("Merged the folder")
    assert (there / "summary.md").read_text() == "# NVDA\n"
    assert (there / "notes.md").read_text() == "the user's own\n"
    assert (there / "charts/rev.csv").exists()


@pytest.mark.asyncio
async def test_a_file_where_the_folder_would_go_is_refused(machines, tmpdir_env):
    mac, cloud, a, b = machines
    _report(a)
    (b / "ws").mkdir()
    (b / "ws/report").write_text("a file")
    with pytest.raises(ComputerUnreachable, match="is a file"):
        await cf.copy_file(mac, "ws/report", cloud, "ws/report", overwrite=True)


@pytest.mark.asyncio
async def test_a_workspace_copy_leaves_out_what_the_server_keeps(machines, tmpdir_env):
    mac, cloud, a, b = machines
    ws = a / "ws"
    for kept_elsewhere in (".agents/memory", ".agents/threads", ".agents/tools", ".agents/user"):
        (ws / kept_elsewhere).mkdir(parents=True)
        (ws / kept_elsewhere / "f").write_text("x")
    (ws / ".agents/skills").mkdir(parents=True)
    (ws / ".agents/skills/s.md").write_text("skill")
    (ws / "agent.md").write_text("notebook")
    (ws / ".data.csv.langalpha-copy-0a1b2c3d").mkdir()
    (ws / ".data.copy-20240101").write_text("the user's own")
    await cf.copy_file(mac, "ws", cloud, "ws")
    assert sorted(_tree(b / "ws")) == [
        ".agents",
        ".agents/skills",
        ".agents/skills/s.md",
        ".data.copy-20240101",
        "agent.md",
    ]


@pytest.mark.asyncio
async def test_a_copy_of_the_whole_computer_leaves_out_its_credentials(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "_internal").mkdir()
    (a / "_internal/token").write_text("secret")
    (a / "ws/_internal").mkdir(parents=True)
    (a / "ws/_internal/mine.txt").write_text("the user's own folder of that name")
    await cf.copy_file(mac, str(a), cloud, "everything")
    assert not (b / "everything/_internal").exists()
    assert (b / "everything/ws/_internal/mine.txt").exists()


@pytest.mark.asyncio
async def test_a_folder_that_packs_past_the_cap_is_refused_and_its_archive_removed(
    machines, tmpdir_env, monkeypatch
):
    mac, cloud, a, _ = machines
    (a / "big").mkdir()
    (a / "big/data.bin").write_bytes(os.urandom(8000))
    # Under twice the cap on disk (12 KB on ext4) so the pack runs, and over
    # the cap once packed (10240 bytes in tar records).
    monkeypatch.setattr(cf, "COPY_MAX_BYTES", 9000)
    with pytest.raises(ComputerUnreachable, match="packs to more than"):
        await cf.copy_file(mac, "big", cloud, "big")
    assert cloud.session.sandbox.uploads == 0
    assert not list(tmpdir_env.iterdir())
    assert not _leftovers(a)


@pytest.mark.asyncio
async def test_another_spelling_of_the_computer_still_leaves_out_its_credentials(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "_internal").mkdir()
    (a / "_internal/token").write_text("secret")
    (a / "ws").mkdir()
    (a / "ws/notes.md").write_text("notes")
    (a / "ws/everything").symlink_to(a)
    await cf.copy_file(mac, "ws/everything", cloud, "everything")
    assert (b / "everything/ws/notes.md").exists()
    assert not (b / "everything/_internal").exists()


@pytest.mark.asyncio
async def test_a_link_into_the_credentials_is_refused(machines):
    mac, cloud, a, b = machines
    (a / "_internal").mkdir()
    (a / "_internal/token").write_text("secret")
    (a / "ws").mkdir()
    (a / "ws/token").symlink_to(a / "_internal/token")
    (a / "ws/creds").symlink_to(a / "_internal")
    for source in ("ws/token", "ws/creds"):
        with pytest.raises(ComputerUnreachable, match="outside what the file tools reach"):
            await cf.copy_file(mac, source, cloud, "x")
    assert cloud.session.sandbox.uploads == 0


@pytest.mark.asyncio
async def test_a_destination_through_a_link_into_the_credentials_is_refused(machines):
    mac, cloud, a, b = machines
    (a / "notes.txt").write_text("hi")
    (b / "_internal").mkdir()
    (b / "creds").symlink_to(b / "_internal")
    with pytest.raises(ComputerUnreachable, match="outside what the file tools reach"):
        await cf.copy_file(mac, "notes.txt", cloud, "creds/notes.txt")
    assert not (b / "_internal/notes.txt").exists()


@pytest.mark.asyncio
async def test_nothing_merges_into_the_computer_root(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "kit/_internal").mkdir(parents=True)
    (a / "kit/_internal/token").write_text("not the computer's")
    (b / "_internal").mkdir()
    (b / "_internal/token").write_text("the computer's")
    with pytest.raises(ComputerUnreachable, match="holds the computer's own folders"):
        await cf.copy_file(mac, "kit", cloud, str(b), overwrite=True)
    assert (b / "_internal/token").read_text() == "the computer's"


@pytest.mark.asyncio
async def test_a_private_file_stays_private(machines):
    mac, cloud, a, b = machines
    (a / ".env").write_text("KEY=1")
    (a / ".env").chmod(0o600)
    await cf.copy_file(mac, ".env", cloud, ".env")
    assert (b / ".env").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_an_agents_folder_copied_on_its_own_leaves_out_what_the_server_keeps(machines, tmpdir_env):
    mac, cloud, a, b = machines
    agents = a / "ws/.agents"
    for kept_elsewhere in ("memory", "threads", "tools"):
        (agents / kept_elsewhere).mkdir(parents=True)
        (agents / kept_elsewhere / "f").write_text("x")
    (agents / "skills").mkdir()
    (agents / "skills/s.md").write_text("skill")
    await cf.copy_file(mac, "ws/.agents", cloud, "ws2/.agents")
    assert sorted(_tree(b / "ws2/.agents")) == ["skills", "skills/s.md"]


@pytest.mark.asyncio
async def test_a_socket_in_a_folder_is_left_out(machines, tmpdir_env):
    import socket

    mac, cloud, a, b = machines
    (a / "kit").mkdir()
    (a / "kit/notes.md").write_text("notes")
    # A socket path must be short, so it is made from inside the folder.
    here = os.getcwd()
    os.chdir(a / "kit")
    try:
        with socket.socket(socket.AF_UNIX) as s:
            s.bind("daemon.sock")
            await cf.copy_file(mac, "kit", cloud, "kit")
    finally:
        os.chdir(here)
    assert sorted(_tree(b / "kit")) == ["notes.md"]


@pytest.mark.asyncio
async def test_a_merge_replaces_a_link_rather_than_writing_through_it(machines, tmpdir_env, tmp_path):
    mac, cloud, a, b = machines
    (a / "kit").mkdir()
    (a / "kit/latest.csv").write_text("new")
    outside = tmp_path / "outside.csv"
    outside.write_text("untouched")
    (b / "kit").mkdir()
    (b / "kit/latest.csv").symlink_to(outside)
    await cf.copy_file(mac, "kit", cloud, "kit", overwrite=True)
    assert outside.read_text() == "untouched"
    assert not (b / "kit/latest.csv").is_symlink()
    assert (b / "kit/latest.csv").read_text() == "new"


@pytest.mark.asyncio
async def test_a_read_only_folder_copies_and_leaves_no_staging(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "kit/locked").mkdir(parents=True)
    (a / "kit/locked/f").write_text("x")
    (a / "kit/locked").chmod(0o555)
    try:
        result = await cf.copy_file(mac, "kit", cloud, "kit")
    finally:
        (a / "kit/locked").chmod(0o755)
        if (b / "kit/locked").exists():
            (b / "kit/locked").chmod(0o755)
    assert result.startswith("Copied the folder")
    assert not _leftovers(a, b)


@pytest.mark.asyncio
async def test_a_link_out_of_the_root_is_refused_on_either_side(machines, tmp_path):
    mac, cloud, a, b = machines
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("not the agent's")
    (a / "ws").mkdir()
    (a / "ws/link").symlink_to(outside / "secret.txt")
    with pytest.raises(ComputerUnreachable, match="outside what the file tools reach"):
        await cf.copy_file(mac, "ws/link", cloud, "copy.txt")
    (a / "ws/mine.txt").write_text("the agent's")
    (b / "out").symlink_to(outside)
    with pytest.raises(ComputerUnreachable, match="outside what the file tools reach"):
        await cf.copy_file(mac, "ws/mine.txt", cloud, "out/")
    assert sorted(p.name for p in outside.iterdir()) == ["secret.txt"]
    assert not (b / "copy.txt").exists()


@pytest.mark.asyncio
async def test_an_allowed_folder_reached_through_a_link_is_spelled_as_allowed(machines, tmp_path):
    # A Mac's /tmp is /private/tmp on disk.
    mac, cloud, a, b = machines
    (tmp_path / "private-tmp").mkdir()
    (tmp_path / "tmp").symlink_to(tmp_path / "private-tmp")
    cloud.sandbox.config.filesystem.allowed_directories.append(str(tmp_path / "tmp"))
    (a / "note.txt").write_text("hi")
    result = await cf.copy_file(mac, "note.txt", cloud, f"{tmp_path}/tmp/note.txt")
    assert (tmp_path / "private-tmp/note.txt").read_text() == "hi"
    assert result.endswith(f'to {tmp_path}/tmp/note.txt on computer "Cloud".')


@pytest.mark.asyncio
async def test_a_line_break_in_a_path_never_copies_another_file(machines):
    mac, cloud, a, b = machines
    (a / "report").write_text("the other file")
    (a / "report\nold.txt").write_text("the one named")
    with pytest.raises(ComputerUnreachable, match="line break"):
        await cf.copy_file(mac, "report\nold.txt", cloud, "copy.txt")
    assert not (b / "copy.txt").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("through", ["link", "sub/file"])
async def test_a_line_break_ending_a_link_or_folder_never_copies_another_file(machines, through):
    # The shell drops a trailing line break from what a command prints, so
    # "report\n" would read as its sibling "report".
    mac, cloud, a, b = machines
    for name in ("report", "report\n"):
        (a / name).write_text(f"named {name!r}")
        (a / f"dir{name[6:]}").mkdir()
        (a / f"dir{name[6:]}/file").write_text(f"in dir{name[6:]!r}")
    (a / "link").symlink_to("report\n")
    (a / "sub").symlink_to("dir\n")
    with pytest.raises(ComputerUnreachable, match="could not be checked"):
        await cf.copy_file(mac, through, cloud, "copy.txt")
    assert not (b / "copy.txt").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("overwrite", [False, True])
async def test_a_read_only_file_stays_read_only(machines, overwrite):
    mac, cloud, a, b = machines
    (a / "frozen.txt").write_text("keep")
    (a / "frozen.txt").chmod(0o444)
    await cf.copy_file(mac, "frozen.txt", cloud, "frozen.txt", overwrite=overwrite)
    assert ((b / "frozen.txt").stat().st_mode & 0o777) == 0o444


def _place_file_racing(tmp_path, race: str, *, overwrite: bool = False) -> tuple[int, Path]:
    """The joining script for a file, run with ``race`` defined first."""
    staging, landing = tmp_path / "staging", tmp_path / "landing"
    staging.mkdir()
    (staging / "0").write_text("data")
    source = cf._Source("/src/data.txt", folder=False, size=4, mode="644")
    arrival = cf._streamed(cf._Payload(source.path, 4), 1, folder=False)
    script = race + cf._placing_script(str(staging), str(landing), source, arrival, overwrite=overwrite)
    done = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert not staging.exists()
    return done.returncode, landing


@pytest.mark.parametrize(
    ("race", "overwrite"),
    [
        # The folder appears just before the link is made.
        ('ln(){ mkdir -p "$2"; command ln "$@"; }; ', False),
        # On a disk that keeps no links, just before the move.
        ('ln(){ return 1; }; mv(){ case $1 in -n) mkdir -p "$3";; esac; command mv "$@"; }; ', False),
        # Just before a replacing move.
        ('mv(){ case $1 in -f) mkdir -p "$3";; esac; command mv "$@"; }; ', True),
    ],
)
def test_a_folder_appearing_where_a_file_lands_is_left_as_it_is(tmp_path, race, overwrite):
    code, landing = _place_file_racing(tmp_path, race, overwrite=overwrite)
    assert code == cf._CHANGED
    assert landing.is_dir() and not list(landing.iterdir())


def test_only_the_copy_is_ever_taken_out_of_a_folder_at_the_landing(tmp_path):
    # Another process swaps the placed file for a folder holding its own file.
    race = 'ln(){ command ln "$@" && rm -f "$2" && mkdir "$2" && echo theirs > "$2/whole"; }; '
    code, landing = _place_file_racing(tmp_path, race)
    assert code == 0
    assert (landing / "whole").read_text() == "theirs\n"


@pytest.mark.asyncio
async def test_a_folder_large_only_in_what_it_leaves_out_still_copies(machines, tmpdir_env, monkeypatch):
    mac, cloud, a, b = machines
    (a / "ws/.agents/threads").mkdir(parents=True)
    (a / "ws/.agents/threads/run.log").write_bytes(os.urandom(200_000))
    (a / "ws/report.md").write_text("small")
    monkeypatch.setattr(cf, "COPY_MAX_BYTES", 20_000)
    await cf.copy_file(mac, "ws", cloud, "ws")
    assert (b / "ws/report.md").read_text() == "small"
    assert not (b / "ws/.agents/threads").exists()


@pytest.mark.asyncio
async def test_many_small_files_copy_though_they_take_more_on_disk(machines, tmpdir_env, monkeypatch):
    # 50 one-byte files take a block each on disk (about 200 KB) and pack to
    # about 60 KB, so only the packed size can decide.
    mac, cloud, a, b = machines
    (a / "tiny").mkdir()
    for i in range(50):
        (a / f"tiny/{i}.txt").write_text("x")
    monkeypatch.setattr(cf, "COPY_MAX_BYTES", 70_000)
    await cf.copy_file(mac, "tiny", cloud, "tiny")
    assert len(list((b / "tiny").iterdir())) == 50


@pytest.mark.asyncio
async def test_a_file_that_appears_while_the_copy_runs_is_kept(machines):
    mac, cloud, a, b = machines
    (a / "data.bin").write_bytes(os.urandom(3 * PART))
    sandbox = cloud.session.sandbox
    upload = sandbox.aupload_file_bytes

    async def upload_then_race(path, content, verify=False):
        done = await upload(path, content, verify)
        (b / "data.bin").write_text("written meanwhile")
        return done

    sandbox.aupload_file_bytes = upload_then_race
    with pytest.raises(ComputerUnreachable, match="appeared on"):
        await cf.copy_file(mac, "data.bin", cloud, "data.bin")
    assert (b / "data.bin").read_text() == "written meanwhile"
    assert not _leftovers(a, b)


@pytest.mark.asyncio
async def test_a_link_chain_longer_than_the_probe_follows_is_refused(machines, tmp_path):
    mac, cloud, a, b = machines
    (tmp_path / "secret.txt").write_text("not the agent's")
    (a / "l40").symlink_to(tmp_path / "secret.txt")
    for i in range(40):
        (a / f"l{i}").symlink_to(f"l{i + 1}")
    with pytest.raises(ComputerUnreachable, match="could not be checked"):
        await cf.copy_file(mac, "l0", cloud, "copy.txt")
    assert not (b / "copy.txt").exists()


@pytest.mark.asyncio
async def test_a_folder_taking_the_name_while_a_folder_lands_is_left_as_it_is(tmp_path):
    staging, landing = tmp_path / "staging", tmp_path / "landing"
    (tmp_path / "src/kit").mkdir(parents=True)
    (tmp_path / "src/kit/a.txt").write_text("a")
    staging.mkdir()
    subprocess.run(["tar", "-cf", str(staging / "0"), "kit"], cwd=tmp_path / "src", check=True)
    size = (staging / "0").stat().st_size
    source = cf._Source("/src/kit", folder=True, size=size)
    # Another copy makes the folder just before this one moves in.
    race = f'mv(){{ [ "$2" = {landing} ] && mkdir -p "$2"; command mv "$@"; }}; '
    arrival = cf._streamed(cf._Payload(source.path, size, entries=2), 1, folder=True)
    script = race + cf._placing_script(str(staging), str(landing), source, arrival, overwrite=False)
    done = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert done.returncode == cf._TAKEN
    assert landing.is_dir() and not list(landing.iterdir())
    assert not staging.exists()


@pytest.mark.asyncio
async def test_the_source_is_kept_in_use_while_the_destination_wakes(machines, monkeypatch):
    from ptc_agent.agent.tools import computer_reach

    mac, cloud, a, _ = machines
    monkeypatch.setattr(computer_reach, "_IN_USE_EVERY_S", 0.01)
    (a / "f.txt").write_text("x")
    seen = []

    class Reach:
        async def target(self, computer):
            if computer:
                await asyncio.sleep(0.1)
                seen.append("awake")
                return cloud
            return mac

        async def in_use(self, target):
            seen.append(target.name)

    await cf.create_copy_file_tool(Reach()).ainvoke(
        {"source": "f.txt", "destination": "f.txt", "destination_computer": "Cloud"}
    )
    assert "Mac" in seen[: seen.index("awake")]


@pytest.mark.asyncio
@pytest.mark.parametrize("destination", ["project/backup", "project/sub/"])
async def test_a_folder_is_never_copied_into_itself(machines, tmpdir_env, destination):
    mac, _, a, _ = machines
    (a / "project/sub").mkdir(parents=True)
    (a / "project/a.txt").write_text("a")
    with pytest.raises(ComputerUnreachable, match="inside the folder being copied"):
        await cf.copy_file(mac, "project", mac, destination, overwrite=True)
    assert sorted(_tree(a / "project")) == ["a.txt", "sub"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [0o555, 0o700])
async def test_a_merge_keeps_the_receiving_folders_own_mode(machines, tmpdir_env, mode):
    mac, cloud, a, b = machines
    (a / "kit").mkdir()
    (a / "kit/new.txt").write_text("new")
    (a / "kit").chmod(mode)
    (b / "kit").mkdir()
    (b / "kit").chmod(0o755)
    try:
        await cf.copy_file(mac, "kit", cloud, "kit", overwrite=True)
    finally:
        (a / "kit").chmod(0o755)
    assert ((b / "kit").stat().st_mode & 0o777) == 0o755
    assert (b / "kit/new.txt").read_text() == "new"


@pytest.mark.asyncio
async def test_an_empty_folder_merges(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "kit").mkdir()
    (b / "kit").mkdir()
    (b / "kit/kept.txt").write_text("kept")
    await cf.copy_file(mac, "kit", cloud, "kit", overwrite=True)
    assert (b / "kit/kept.txt").read_text() == "kept"


@pytest.mark.asyncio
async def test_a_file_reached_through_a_link_keeps_the_name_it_was_given(machines):
    mac, cloud, a, b = machines
    (a / "versions").mkdir()
    (a / "versions/nov.csv").write_text("rows")
    (a / "current.csv").symlink_to("versions/nov.csv")
    (b / "exports").mkdir()
    await cf.copy_file(mac, "current.csv", cloud, "exports/")
    assert (b / "exports/current.csv").read_text() == "rows"
    assert not (b / "exports/nov.csv").exists()


@pytest.mark.asyncio
async def test_a_merge_keeps_a_sparse_file_sparse(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "kit").mkdir()
    with open(a / "kit/hole.bin", "wb") as f:
        f.truncate(8 * 1024 * 1024)
    if (a / "kit/hole.bin").stat().st_blocks * 512 > 1024 * 1024:
        pytest.skip("this disk keeps no sparse files")
    (b / "kit").mkdir()
    await cf.copy_file(mac, "kit", cloud, "kit", overwrite=True)
    landed = (b / "kit/hole.bin").stat()
    assert landed.st_size == 8 * 1024 * 1024
    assert landed.st_blocks * 512 < 1024 * 1024


def _join_crafted(tmp_path, entries, *, merge=False, counted=None):
    """The joining script for a folder archive built from ``entries``
    (name, mode, is a folder), as if packed with ``counted`` entries."""
    import io
    import tarfile

    staging, landing = tmp_path / "staging", tmp_path / "landing"
    staging.mkdir()
    if merge:
        landing.mkdir()
    packed = io.BytesIO()
    with tarfile.open(fileobj=packed, mode="w") as archive:
        for name, mode, folder in entries:
            entry = tarfile.TarInfo(name)
            entry.mode = mode
            if folder:
                entry.type = tarfile.DIRTYPE
                archive.addfile(entry)
            else:
                entry.size = 1
                archive.addfile(entry, io.BytesIO(b"x"))
    (staging / "0").write_bytes(packed.getvalue())
    size = len(packed.getvalue())
    if counted is None:
        counted = len(entries)
    source = cf._Source("/src/kit", folder=True, size=size)
    arrival = cf._streamed(cf._Payload(source.path, size, entries=counted), 1, folder=True)
    script = cf._placing_script(str(staging), str(landing), source, arrival, overwrite=merge)
    done = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert not staging.exists()
    return done.returncode, landing


@pytest.mark.parametrize("merge", [False, True])
@pytest.mark.parametrize(
    "pair",
    [("A.txt", "a.txt"), ("\u00c9.txt", "\u00e9.txt"), ("\u00e9.txt", "e\u0301.txt")],
    ids=["ascii-case", "accented-case", "composed-and-decomposed"],
)
def test_names_the_destination_disk_takes_for_one_are_refused(tmp_path, merge, pair):
    first, second = pair
    (tmp_path / first).write_text("")
    if not (tmp_path / second).exists():
        pytest.skip("this disk tells these names apart")
    (tmp_path / first).unlink()
    code, landing = _join_crafted(
        tmp_path,
        [("./kit", 0o755, True), (f"./kit/{first}", 0o644, False), (f"./kit/{second}", 0o644, False)],
        merge=merge,
    )
    assert code == cf._CLASH
    assert not landing.exists() or not list(landing.iterdir())


def test_a_failed_copy_removes_a_folder_it_cannot_enter(tmp_path):
    # Anything that fails after the unpack, here the count, leaves a tree
    # holding a folder of mode 000 for the cleanup.
    code, landing = _join_crafted(
        tmp_path,
        [("./kit", 0o755, True), ("./kit/locked", 0o000, True), ("./kit/locked/f", 0o644, False)],
        counted=99,
    )
    assert code == cf._CLASH
    assert not landing.exists()


@pytest.mark.parametrize("merge", [False, True])
def test_folders_the_destination_disk_takes_for_one_are_refused(tmp_path, merge):
    (tmp_path / "Probe").write_text("")
    if not (tmp_path / "probe").exists():
        pytest.skip("this disk tells names apart by case")
    code, landing = _join_crafted(
        tmp_path,
        [
            ("./kit", 0o755, True),
            ("./kit/A", 0o755, True),
            ("./kit/A/one.txt", 0o644, False),
            ("./kit/a", 0o700, True),
            ("./kit/a/two.txt", 0o644, False),
        ],
        merge=merge,
    )
    assert code == cf._CLASH
    assert not landing.exists() or not list(landing.iterdir())


@pytest.mark.asyncio
async def test_a_users_own_file_named_like_a_copys_is_copied(machines, tmpdir_env):
    mac, cloud, a, b = machines
    (a / "ws").mkdir()
    (a / "ws/report.langalpha-copy-deadbeef").write_text("the user's own")
    (a / "ws/.data.langalpha-copy-notahexx").write_text("the user's own")
    await cf.copy_file(mac, "ws", cloud, "ws")
    assert sorted(_tree(b / "ws")) == [".data.langalpha-copy-notahexx", "report.langalpha-copy-deadbeef"]


@pytest.mark.parametrize("mode", ["0", "4", "40", "644", "4755"])
def test_every_mode_stat_prints_is_taken(tmp_path, mode):
    assert cf._MODE.match(mode)
    staging, landing = tmp_path / "staging", tmp_path / "landing"
    staging.mkdir()
    (staging / "0").write_text("data")
    source = cf._Source("/src/data.txt", folder=False, size=4, mode=mode)
    arrival = cf._streamed(cf._Payload(source.path, 4), 1, folder=False)
    script = cf._placing_script(str(staging), str(landing), source, arrival, overwrite=False)
    assert subprocess.run(["bash", "-c", script]).returncode == 0
    assert (landing.stat().st_mode & 0o7777) == int(mode, 8)
    landing.chmod(0o644)
