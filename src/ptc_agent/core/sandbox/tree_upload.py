"""Ship a set of host files to a sandbox as one archive.

A request per file is what made a skills upload slow (dozens of files, each its
own round trip). Here the files go up as one gzipped tar through the runtime's
plain file upload and unpack with the sandbox's own ``tar`` in one command, so
every provider gets the same two calls.

Ownership and modes match what a direct upload produces on each provider: the
archive carries no owner (``--no-same-owner``), so the files belong to the user
that runs commands there, and files are 0644 and folders 0755, as an upload
and ``mkdir -p`` leave them.
"""

from __future__ import annotations

import asyncio
import io
import shlex
import tarfile
import time
import uuid
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from ptc_agent.core.sandbox.retry import RetryPolicy

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

_FILE_MODE = 0o644
_DIR_MODE = 0o755
# Exit code for an archive already gone: a retried command whose first attempt
# ran and cleaned up. Its outcome is unknown, so the call fails and the caller's
# next pass redoes the upload.
_ARCHIVE_GONE = 97


class TreeUploadError(Exception):
    """The archive did not unpack; nothing says the targets are complete.

    Not a ``RuntimeError``: the routes answer one as a refusal, a 400 that
    carries its text and logs nothing. An unpack that fails is a fault on our
    side, so it surfaces as the failed upload it replaced did, a logged 500.
    """


def build_tar_gz(
    files: Sequence[tuple[str | Path | bytes, str]], dirs: Sequence[str] = ()
) -> bytes:
    """A gzipped tar of *files* (source, absolute sandbox path) and *dirs*.

    Member names are the sandbox paths relative to ``/``. Blocking: reads every
    source, so callers run it off the event loop.
    """
    now = int(time.time())
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.PAX_FORMAT) as tar:
        for path in sorted(set(dirs), key=lambda d: PurePosixPath(d).parts):
            info = tarfile.TarInfo(_member_name(path))
            info.type = tarfile.DIRTYPE
            info.mode = _DIR_MODE
            info.mtime = now
            tar.addfile(info)
        for source, dest in files:
            data = source if isinstance(source, bytes) else Path(source).read_bytes()
            info = tarfile.TarInfo(_member_name(dest))
            info.size = len(data)
            info.mode = _FILE_MODE
            info.mtime = now
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _member_name(path: str) -> str:
    pure = PurePosixPath(path)
    if not pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"Not an absolute sandbox path: {path}")
    return str(pure.relative_to("/"))


async def upload_tree(
    sandbox: "PTCSandbox",
    files: Sequence[tuple[str | Path | bytes, str]],
    *,
    dirs: Sequence[str] = (),
) -> None:
    """Write *files* and *dirs* in one upload and one command.

    Nothing is cleared first: a caller that replaces a folder unpacks beside
    it and swaps, so the live one is never seen empty. Raises
    ``TreeUploadError`` when the command fails.
    """
    if not files and not dirs:
        return
    assert sandbox.runtime is not None
    archive = await asyncio.to_thread(build_tar_gz, files, dirs)
    # Under the computer's own files, which the agent's tools never list, and
    # under the working dir, where every provider's upload can write.
    remote = f"{sandbox.layout.internal}/.tree-upload-{uuid.uuid4().hex}.tar.gz"
    await sandbox._runtime_call(
        sandbox.runtime.upload_file, archive, remote, retry_policy=RetryPolicy.SAFE
    )

    quoted = shlex.quote(remote)
    steps = [
        f"[ -f {quoted} ] || exit {_ARCHIVE_GONE}",
        # Quoted whole: under a root with a space the path is in single
        # quotes of its own, which would end a pair around it.
        f"trap {shlex.quote(f'rm -f {quoted}')} EXIT",
        f"tar -xzf {quoted} -C / --no-same-owner",
    ]
    result = await sandbox._runtime_call(
        sandbox.runtime.exec, " && ".join(steps), retry_policy=RetryPolicy.SAFE
    )
    if result.exit_code != 0:
        raise TreeUploadError(
            f"Unpacking {len(files)} files failed (exit {result.exit_code}): "
            f"{(result.stderr or result.stdout or '').strip()[:500]}"
        )
