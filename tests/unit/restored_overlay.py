"""A disk that refuses directory renames the way a restored Daytona sandbox does.

A Daytona archive comes back as one read-only overlay layer, and overlayfs
without ``redirect_dir`` refuses to rename any directory it holds there with
EXDEV, while files, and directories made since, rename as usual. ``archive``
marks the directories under a root as restored. The scripts under test run in
a subprocess, so the refusal is Python source run ahead of them, and the marks
are a file, so they hold across passes as the layer does.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_OVERLAY = r'''
import errno as _errno, json as _json, os as _os, stat as _stat

def _overlay(marks):
    with open(marks) as fh:
        lower = {tuple(m) for m in _json.load(fh)}
    rename, replace, rmdir = _os.rename, _os.replace, _os.rmdir

    def key(path, dir_fd):
        st = _os.lstat(path, dir_fd=dir_fd)
        return (st.st_dev, st.st_ino), st

    def refusing(move):
        def moved(src, dst, *, src_dir_fd=None, dst_dir_fd=None):
            k, st = key(src, src_dir_fd)
            if _stat.S_ISDIR(st.st_mode) and k in lower:
                raise OSError(_errno.EXDEV, "Invalid cross-device link", src)
            return move(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
        return moved

    def removed(path, *, dir_fd=None):
        k, _ = key(path, dir_fd)
        rmdir(path, dir_fd=dir_fd)
        if k in lower:
            # A directory made later may be given the freed inode.
            lower.discard(k)
            with open(marks, "w") as fh:
                _json.dump(sorted(lower), fh)

    return refusing(rename), refusing(replace), removed
'''

# No free block left: a copy never fits.
_FULL = r'''
_statvfs = _os.statvfs
def _full(path):
    st = _statvfs(path)
    return _os.statvfs_result((st.f_bsize, st.f_frsize, st.f_blocks, 0, 0,
                               st.f_files, st.f_ffree, st.f_favail, st.f_flag, st.f_namemax))
_os.statvfs = _full
'''


def archive(root: Path) -> Path:
    """Mark every directory now under ``root`` as restored from an archive;
    returns the file of marks that ``prefix`` reads."""
    lower = sorted(
        [st.st_dev, st.st_ino]
        for top, dirs, _files in os.walk(root)
        for st in (os.lstat(os.path.join(top, name)) for name in dirs)
    )
    marks = root.parent / f".{root.name}.restored"
    marks.write_text(json.dumps(lower))
    return marks


def prefix(marks: Path, *, full: bool = False) -> str:
    """Source to run ahead of a script, in the same globals."""
    patch = f"_os.rename, _os.replace, _os.rmdir = _overlay({str(marks)!r})\n"
    return _OVERLAY + patch + (_FULL if full else "")


def pinned(name: str) -> str:
    """Source, after ``prefix``, that fails the rmdir of any dir called
    ``name``, as a delete in place would stop on a dir something still uses."""
    return (
        "_rmdir_pinned = _os.rmdir\n"
        "def _pinned(path, *, dir_fd=None):\n"
        f"    if _os.path.basename(str(path)) == {name!r}:\n"
        "        raise OSError(_errno.EBUSY, 'Device or resource busy', path)\n"
        "    return _rmdir_pinned(path, dir_fd=dir_fd)\n"
        "_os.rmdir = _pinned\n"
    )


def python3_on_path(
    monkeypatch, bin_dir: Path, marks: Path, *, full: bool = False, extra: str = ""
) -> None:
    """Put a ``python3`` first on PATH that runs the commands the host builds,
    ``python3 -I -c CODE ARGS`` or ``python3 -I FILE ARGS``, with ``prefix``
    and ``extra`` ahead of their code."""
    bin_dir.mkdir(parents=True, exist_ok=True)
    source = bin_dir / "restored_overlay.py"
    source.write_text(prefix(marks, full=full) + extra)
    shim = bin_dir / "python3"
    shim.write_text(
        "#!/bin/sh\n"
        f'[ "$1" = -I ] || exec {sys.executable} "$@"\n'
        'if [ "$2" = -c ]; then\n'
        "code=$3; shift 3\n"
        f'exec {sys.executable} -I -c "exec(open({str(source)!r}).read()); $code" "$@"\n'
        "fi\n"
        "shift\n"
        f'exec {sys.executable} -I -c "import sys; exec(open({str(source)!r}).read()); '
        'sys.argv.pop(0); __file__ = sys.argv[0]; '
        'exec(compile(open(__file__).read(), __file__, \'exec\'))" "$@"\n'
    )
    shim.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")


def refuse_here(monkeypatch, marks: Path) -> None:
    """The same refusals in this process, undone with ``monkeypatch``."""
    scope: dict = {}
    exec(_OVERLAY, scope)
    rename, replace, rmdir = scope["_overlay"](str(marks))
    monkeypatch.setattr(os, "rename", rename)
    monkeypatch.setattr(os, "replace", replace)
    monkeypatch.setattr(os, "rmdir", rmdir)
