"""A PATH that behaves like a Mac's userland on Linux, for the scripts PR 3
runs on users' machines.

Every command on this machine's PATH is there, but ``sha256sum`` and
``flock``, which a Mac has not (so ``shasum`` and perl's flock run, as
there), and the tools a script calls are wrapped to parse their arguments as
BSD's do: options only before the first operand, so a ``--`` after an
operand is a file name, and no long options. ``stat -c`` is refused and
``stat -f`` reads BSD's format, so the BSD fallback runs.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

#: On a Mac the plain tests run on its own userland; this one stands in for it on Linux.
on_linux = pytest.mark.skipif(sys.platform != "linux", reason="emulates a Mac's userland with Linux's tools")
#: The commands a Mac does not have.
HIDDEN = ("sha256sum", "flock")
#: The wrapped commands, each with the options that take an argument there.
WRAPPED = {
    "chmod": "", "stat": "ft", "mktemp": "tp", "touch": "trd", "rm": "", "mv": "", "mkdir": "m",
    "cat": "", "ls": "", "tr": "", "ps": "oOpUutGg", "ln": "", "head": "nc", "date": "rvjf",
}

_WRAPPER = '''#!{python}
import os, sys
name, real, takes = {name!r}, {real!r}, {takes!r}
args, options, i = sys.argv[1:], [], 0
while i < len(args):
    a = args[i]
    if a == "--":
        i += 1
        break
    if not a.startswith("-") or a == "-" or (name == "chmod" and not options and a[1:2].isdigit()):
        break
    if a.startswith("--"):
        sys.exit(f"{{name}}: illegal option -- {{a[2:]}}")
    options.append(a)
    for j, letter in enumerate(a[1:]):
        if letter in takes:
            if j == len(a) - 2:
                i += 1
                options.append(args[i])
            break
    i += 1
operands = args[i:]
if name == "stat":
    if any(o.startswith("-") and "c" in o for o in options):
        sys.exit("stat: illegal option -- c")
    options = ["-c" if o == "-f" else o.replace("%z", "%s").replace("%m", "%Y") for o in options]
tail = operands if name in ("ps", "tr", "date") else ["--", *operands]
os.execv(real, [real, *options, *tail])
'''


def bsd_path(where: Path) -> str:
    """A PATH of one folder under ``where``: this machine's commands, as a Mac's userland has them."""
    folder = where / "bsd-bin"
    folder.mkdir(exist_ok=True)
    for directory in os.environ["PATH"].split(os.pathsep):
        if not os.path.isdir(directory):
            continue
        for entry in os.listdir(directory):
            link, target = folder / entry, os.path.join(directory, entry)
            if entry in HIDDEN or entry in WRAPPED or link.exists() or not os.access(target, os.X_OK):
                continue
            link.symlink_to(target)
    for name, takes in WRAPPED.items():
        real = shutil.which(name)
        if real is None:
            continue
        wrapper = folder / name
        wrapper.write_text(_WRAPPER.format(python=sys.executable, name=name, real=real, takes=takes))
        wrapper.chmod(0o755)
    return str(folder)
