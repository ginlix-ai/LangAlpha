"""A package root runs from its own checked copy: the files and versions the
host computes, and the old copies ``boot`` drops when asked to."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys

from ptc_agent.core.sandbox import mount_commands
from ptc_agent.core.sandbox.livefs_runtime import boot, lifecycle
from ptc_agent.core.sandbox.root_package import RootPackage


def test_the_file_mount_package_is_what_its_daemon_computes():
    """The daemon compares itself against these, so a start restarts it only
    when the shipped code changed."""
    livefs = mount_commands.LIVEFS
    assert livefs.code == lifecycle.code_version()
    assert json.loads(livefs.manifest) == lifecycle.code_manifest()
    assert [name for name, _ in livefs.files] == [name for name, _ in lifecycle.package_files()]
    argv = shlex.split(livefs.command("/r/_internal/src", "status")[len("cd / && "):])
    assert argv[4:6] == [lifecycle.Paths().private, "/r/_internal/src/livefs"]
    # The daemon drops its own copies, so its command asks boot for nothing.
    assert argv[8:] == ["status"]


def _package(tmp_path, body: str = "print('ran')\n") -> RootPackage:
    folder = tmp_path / "pkg"
    folder.mkdir(exist_ok=True)
    (folder / "__main__.py").write_text(body)
    return RootPackage(folder, str(tmp_path / "private"), keep_copies=2)


def test_a_new_copy_keeps_its_own_the_newest_others_and_any_being_written(tmp_path):
    package = _package(tmp_path)
    copies = tmp_path / "private" / "code"
    copies.mkdir(parents=True)
    others = [f"{n:016x}" for n in range(1, 6)]
    for age, name in enumerate([*others, f"{others[0]}.123"]):
        (copies / name).mkdir()
        os.utime(copies / name, (1000 - age, 1000 - age))
    manifest = json.loads(package.manifest)

    found = boot.home(str(tmp_path / "private"), str(package.package_dir), package.code, manifest, 2)

    assert found == str(copies / package.code)
    assert sorted(p.name for p in copies.iterdir()) == sorted(
        [package.code, *others[:2], f"{others[0]}.123"]
    )


def test_without_keep_boot_drops_nothing(tmp_path):
    package = _package(tmp_path)
    copies = tmp_path / "private" / "code"
    copies.mkdir(parents=True)
    for n in range(1, 6):
        (copies / f"{n:016x}").mkdir()
    boot.home(str(tmp_path / "private"), str(package.package_dir), package.code, json.loads(package.manifest))
    assert len(list(copies.iterdir())) == 6


def test_boot_runs_the_package_with_the_arguments_after_the_keep_flag(tmp_path):
    package = _package(tmp_path, "import sys\nprint(sys.argv[1:])\n")
    shipped = tmp_path / "src" / "pkg"
    shutil.copytree(package.package_dir, shipped)
    argv = shlex.split(package.command(str(tmp_path / "src"), "status", "--x")[len("cd / && "):])
    assert argv[8] == f"{boot.KEEP_FLAG}2"
    done = subprocess.run([sys.executable, *argv[1:]], capture_output=True, text=True, check=True)
    assert done.stdout.strip() == "['status', '--x']"
