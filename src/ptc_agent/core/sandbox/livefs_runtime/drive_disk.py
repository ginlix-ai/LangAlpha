"""File operations the Drive's bytes on disk share: reading a whole range,
hashing a file in chunks, writing that tells a full disk apart, and giving
a sparse file's range back to the disk."""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import hashlib
import os
from collections.abc import Callable
from typing import Any

from .remote import fail

#: What a hash reads at a time.
HASH_CHUNK = 8 << 20

_FALLOC_PUNCH = 0x01 | 0x02  # FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
_libc: Any = None


class DiskFull(Exception):
    """The disk refused a write. Not an ``OSError``, so a fetch writing into
    the cache does not take it for the store failing and read the range
    again elsewhere."""


def pread_exact(fd: int, size: int, offset: int) -> bytes:
    """``size`` bytes from ``offset``; EIO when the file is shorter."""
    parts, at = [], offset
    while at < offset + size:
        chunk = os.pread(fd, min(HASH_CHUNK, offset + size - at), at)
        if not chunk:
            raise fail(errno.EIO)
        parts.append(chunk)
        at += len(chunk)
    return b"".join(parts) if len(parts) != 1 else parts[0]


def sha256_fd(fd: int, size: int, between: Callable[[], None] | None = None) -> str:
    """The sha256 of a file's first ``size`` bytes, read a chunk at a time;
    ``between`` runs before each chunk and may raise to stop."""
    h = hashlib.sha256()
    at = 0
    while at < size:
        if between is not None:
            between()
        chunk = os.pread(fd, min(HASH_CHUNK, size - at), at)
        if not chunk:
            raise fail(errno.EIO)
        h.update(chunk)
        at += len(chunk)
    return h.hexdigest()


def pwrite_all(fd: int, data: memoryview | bytes, offset: int) -> None:
    """All of ``data`` at ``offset``: a pwrite may take only part of it. The
    disk's own error when it takes none."""
    view = memoryview(data)
    while view:
        n = os.pwrite(fd, view, offset)
        if not n:
            raise fail(errno.EIO)
        view, offset = view[n:], offset + n


def write_at(fd: int, data: memoryview | bytes, offset: int) -> None:
    """All of ``data`` at ``offset``; ``DiskFull`` when the disk has no room."""
    view = memoryview(data)
    try:
        while view:
            n = os.pwrite(fd, view, offset)
            view, offset = view[n:], offset + n
    except OSError as exc:
        if exc.errno in (errno.ENOSPC, errno.EDQUOT):
            raise DiskFull from exc
        raise


def punch(fd: int, offset: int, length: int) -> bool:
    """Give a range of a sparse file's disk back; False where the filesystem
    or the platform cannot, and the range still holds its disk."""
    global _libc
    if _libc is None:
        try:
            _libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
            _libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_long, ctypes.c_long]
        except (OSError, AttributeError):
            _libc = False
    return bool(_libc) and _libc.fallocate(fd, _FALLOC_PUNCH, offset, length) == 0
