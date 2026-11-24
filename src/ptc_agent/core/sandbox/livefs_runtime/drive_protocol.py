"""Names the Drive mount's daemon and the server both use.

Stdlib only: it ships into the computer with the daemon, and the server's
mount endpoint imports it for the same names. The wire protocol itself is
written down in the Drive contract ("Mount protocol").
"""

from __future__ import annotations

import errno
import json
from collections.abc import Iterator
from typing import BinaryIO, Literal, NotRequired, TypedDict

#: Each action is a path under the livefs endpoint (``/api/v1/livefs/drive/<action>``),
#: so the Drive mount authenticates with the computer's mount token exactly as
#: the livefs mount does, through the same dependency.
ACTION_PREFIX = "drive"

#: Where the Drive is served, and where each daemon start mounts the fresh
#: generation ``MOUNT`` then points at (a tmpfs each, as for livefs).
MOUNT = "/mnt/drive"
GENERATIONS = "/mnt/.drive"

#: Root's own directory for the Drive daemon: its state, lock, log, code
#: copies and the content store. Inside livefs's, which is already root's
#: alone, so nothing the computer's user can write sits on the path.
PRIVATE = "/var/lib/livefs/drive"

FSNAME = "drivefs"

#: Generated at the root of each space, naming what ``rg`` (and ``fd``) should
#: skip: the space's binaries, which a blind scan would otherwise read in full.
IGNORE_NAME = ".ignore"

#: A file larger than this is never text (the server's rule, mirrored so the
#: daemon can tell a binary from its size alone).
TEXT_MAX_BYTES = 1024 * 1024

#: The largest file a save may carry when the manifest names no limit.
FILE_MAX_BYTES = 50 * 1024 * 1024

#: How long the server holds a ``changes`` request open with nothing to say.
FEED_WAIT_S = 25

#: How long, after a command that names the Drive, its saves still on the
#: way are waited for (``drive-settle``), so a refusal shows in that
#: command's result. Past it they are reported as not saved yet.
SETTLE_WAIT_S = 5.0

#: At most this many ids in one ``texts`` request, and about this many bytes
#: in its answer (the server stops early and the daemon asks for the rest).
TEXTS_MAX_IDS = 1000
TEXTS_MAX_BYTES = 16 * 1024 * 1024

#: Sent with a change from a process no tool call started (nothing in its
#: environment names a call): its session, so one ``rm -r`` or script run by
#: hand still counts as one sitting. The value is ``<sid>-<start time>``.
PROCESS_HEADER = "X-Drive-Process"
#: How a process tag travels where a call id would (``drive_daemon._call_or_process``).
PROCESS_PREFIX = "process:"

#: The fields of a manifest row, in order.
ROW_FIELDS = ("id", "path", "kind", "size", "sha256", "is_text", "modified_ms", "seq")


class WireEntry(TypedDict):
    """One entry as the mount protocol carries it outside a manifest."""

    id: str
    #: Relative to the space root, no leading or trailing slash.
    path: str
    kind: Literal["file", "folder"]
    size: int
    sha256: str | None
    is_text: bool
    modified_ms: int
    #: The space's change sequence at this entry's last change.
    seq: int
    #: Only in a ``changes`` answer: the entry went to the trash.
    deleted: NotRequired[bool]


#: Refusal codes (the service's ``DriveError`` codes) and the errno each
#: reaches the program as.
ERRNO = {
    "not_found": errno.ENOENT,
    "exists": errno.EEXIST,
    "changed": errno.ESTALE,
    "is_folder": errno.EISDIR,
    "not_folder": errno.ENOTDIR,
    "not_empty": errno.ENOTEMPTY,
    "invalid_path": errno.EINVAL,
    "too_large": errno.EFBIG,
    "over_quota": errno.EDQUOT,
    "too_many_entries": errno.EDQUOT,
    "forbidden": errno.EACCES,
    "unavailable": errno.EIO,
    "too_many_uploads": errno.EBUSY,
}

#: The codes the Drive's own handlers refuse with. A body carrying none of
#: them (a proxy's page, another route's 404) is no handler's answer: the
#: request may not have reached one.
CODES = frozenset(ERRNO) | {"upload_ended", "parts_missing", "bad_part", "bad_digest", "mismatch"}


def code_of(body: bytes) -> str | None:
    """A refusal's ``code``; None for a body that is not one (a proxy's page)."""
    try:
        answer = json.loads(body)
    except ValueError:
        return None
    code = answer.get("code") if isinstance(answer, dict) else None
    return code if isinstance(code, str) else None


def errno_for(status: int, body: bytes) -> int:
    if status == 401:
        return errno.EACCES
    code = code_of(body)
    if status in (429, 503) and code in (None, "unavailable"):
        # Nothing was changed; the same call can be made again.
        return errno.EAGAIN
    return ERRNO.get(code or "", errno.EIO)


# --- the ``texts`` answer ------------------------------------------------
#
# A run of frames, each ``<entry id> <sha256> <length>\n`` then that many
# bytes then ``\n``: text bytes need no escaping, and a frame can be checked
# against its digest before it is kept.


def read_frames(stream: BinaryIO) -> Iterator[tuple[str, str, bytes]]:
    """``(entry id, sha256, bytes)`` per frame of a stream, a frame at a time;
    stops at the end or at a frame cut short."""
    while True:
        head = stream.readline(256)
        if not head.endswith(b"\n"):
            return
        try:
            entry_id, sha, size = head[:-1].decode().split(" ")
            n = int(size)
        except ValueError:
            return
        if not 0 <= n <= TEXT_MAX_BYTES:
            return  # a negative read would take the rest of the stream
        data = stream.read(n + 1)
        if len(data) != n + 1 or data[n:] != b"\n":
            return
        yield entry_id, sha, data[:n]


def encode_frame(entry_id: str, sha256: str, data: bytes) -> bytes:
    return b"%s %s %d\n%s\n" % (entry_id.encode(), sha256.encode(), len(data), data)
