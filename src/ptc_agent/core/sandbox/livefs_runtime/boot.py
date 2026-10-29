"""What root runs first for every daemon command.

The host sends this file's text as the command (``python3 -I -c``) instead of
importing the shipped package, which the sandbox's own user can rewrite:
root imports only a copy in its private directory, made once every shipped
file reads as the digest the host's manifest names. A file that does not is
code a sync is still replacing, answered ``stale_code`` with nothing run.

    python3 -I -c <this file> PRIVATE SHIPPED CODE MANIFEST ACTION [ARGS...]
"""

import hashlib
import json
import os
import runpy
import shutil
import stat
import sys


def _read(path: str) -> bytes | None:
    """A regular file's bytes, never through a link or from a pipe that
    would hold the command open."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    with os.fdopen(fd, "rb") as f:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return None
        return f.read()


def home(private: str, shipped: str, code: str, manifest: dict[str, str]) -> str | None:
    """The directory to import the package from, or None when the shipped
    files are not the manifest's. Checked every time, not only when the copy
    is made: a copy of older code must not start while newer code is shipped."""
    files = {}
    for name, digest in manifest.items():
        data = _read(os.path.join(shipped, name))
        if data is None or hashlib.sha256(data).hexdigest() != digest:
            return None
        files[name] = data
    copies = os.path.join(private, "code")
    found = os.path.join(copies, code)
    if os.path.isdir(found):
        return found
    os.makedirs(private, mode=0o700, exist_ok=True)
    os.makedirs(copies, mode=0o700, exist_ok=True)
    staging = f"{found}.{os.getpid()}"
    package = os.path.join(staging, os.path.basename(shipped))
    os.makedirs(package)
    for name, data in files.items():
        with open(os.path.join(package, name), "wb") as f:
            f.write(data)
    try:
        os.rename(staging, found)
    except OSError:  # another command made it first
        shutil.rmtree(staging, ignore_errors=True)
    return found if os.path.isdir(found) else None


def main() -> None:
    private, shipped, code, manifest = sys.argv[1:5]
    found = home(private, shipped, code, json.loads(manifest))
    if found is None:
        print(json.dumps({"ok": False, "error": "stale_code"}))
        sys.exit(1)
    package = os.path.basename(shipped)
    sys.path.insert(0, found)
    sys.argv = [package, *sys.argv[5:]]
    runpy.run_module(package, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
