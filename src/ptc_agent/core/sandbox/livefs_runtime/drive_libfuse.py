"""What the Drive mount relies on in libfuse beyond mfusepy's bindings: every
ctypes call and private layout in one place, so a libfuse upgrade has one
file to check.

- ``Kernel``: the invalidation calls (a name, and a path's attributes and
  data), bound against the running mount.
- ``keep_listings``: libfuse 3.14's high-level library drops
  ``cache_readdir`` on the way to the kernel; this wraps the session's own
  ``opendir`` to set it, after checking the session's private layout.
- ``fuse_class``: mfusepy's FUSE with a Python thread state kept per libfuse
  worker, and ``opendir`` answering with the flags that keep listings.
- ``without_atomic_trunc``: the capability that would hide an O_TRUNC open
  from a mount that answers no opens.
"""

from __future__ import annotations

import ctypes
import os
import threading
from collections import Counter

#: libfuse's FUSE_CAP_ATOMIC_O_TRUNC. With it the kernel leaves an O_TRUNC
#: open's truncation to the open request, which ``no_open`` never sends, so
#: the daemon would never learn the file was emptied and a shorter rewrite
#: would keep the old tail. Off, the kernel sends the truncation as its own
#: setattr.
_CAP_ATOMIC_O_TRUNC = 1 << 3

#: Where libfuse 3.14's ``struct fuse_session`` keeps its low-level operations
#: on a 64-bit build (after mountpoint, exited, fd, io, mo, debug and
#: deny_others), the slot of ``opendir`` in them, and the slots that tell the
#: table is the high-level library's: set, except ``write`` and
#: ``retrieve_reply``, which it never sets.
_SESSION_FD = 12
_SESSION_OPS = 40
_OPENDIR = 20
_OPS_SET = (*range(16), *range(17, 31), 36, 38, 41)
_OPS_UNSET = (16, 37)

#: The callback put in ``opendir``'s slot, kept alive for the mount's life.
_kept_callbacks: list = []


def without_atomic_trunc(conn) -> None:
    """From the FUSE ``init`` callback's connection info."""
    if conn is None:
        return
    conn.want &= ~_CAP_ATOMIC_O_TRUNC
    if hasattr(conn, "want_ext"):
        conn.want_ext &= ~_CAP_ATOMIC_O_TRUNC


class Kernel:
    """libfuse's invalidation calls, bound against the running mount.

    The high-level API exports ``fuse_invalidate_path`` (a path's attributes
    and data) but nothing that drops a cached name, which a file moved or
    deleted elsewhere needs: without it the kernel keeps answering the old
    name for a day. That is ``fuse_lowlevel_notify_inval_entry``, which wants
    the parent directory's node id; the high-level library reports each
    node's id as its ``st_ino`` (unless ``use_ino`` is set, which this mount
    does not set), so an ``lstat`` of the parent through the mount names it.
    """

    def __init__(self, lib: ctypes.CDLL, fuse_ptr: int, mountpoint: str) -> None:
        self._lib = lib
        self._mount = mountpoint
        lib.fuse_get_session.argtypes = [ctypes.c_void_p]
        lib.fuse_get_session.restype = ctypes.c_void_p
        lib.fuse_invalidate_path.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        lib.fuse_invalidate_path.restype = ctypes.c_int
        lib.fuse_lowlevel_notify_inval_entry.argtypes = [
            ctypes.c_void_p, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_size_t,
        ]
        lib.fuse_lowlevel_notify_inval_entry.restype = ctypes.c_int
        self._fuse = ctypes.c_void_p(fuse_ptr)
        self._session = ctypes.c_void_p(lib.fuse_get_session(self._fuse))

    def forget_attrs(self, path: str) -> int:
        """Drop ``path``'s cached attributes and data (a directory's: its listing)."""
        return self._lib.fuse_invalidate_path(self._fuse, path.encode("utf-8", "surrogateescape"))

    def forget_name(self, parent: str, name: str) -> int:
        """Drop the cached entry (or absence) for ``name`` in ``parent``, and
        everything cached below it."""
        if parent == "/":
            ino = 1
        else:
            try:
                ino = os.lstat(self._mount + parent).st_ino
            except OSError:
                return 0  # gone too; its own parent's entry covers it
        raw = name.encode("utf-8", "surrogateescape")
        return self._lib.fuse_lowlevel_notify_inval_entry(self._session, ino, raw, len(raw))


def keep_listings(fuse_module, fuse_ptr: int) -> bool:
    """Have the kernel keep every directory listing until it is told it
    changed, as it keeps names and attributes.

    The FUSE class's ``opendir`` asks for that (``cache_readdir``), but
    libfuse 3.14's high-level library drops the flag on the way to the
    kernel (later releases pass it), so every search listed every folder
    through the daemon again: one Python call per folder per search, all
    taking turns on one interpreter lock. This wraps the session's own
    ``opendir`` to set it where the reply is made from. The session is
    private to libfuse, so its layout is checked against what the library
    is known to put there first (the mountpoint's fd, and which operation
    slots it fills); on any other layout this does nothing and listings
    stay uncached. Returns whether they are kept.
    """
    if ctypes.sizeof(ctypes.c_void_p) != 8:
        return False
    lib = fuse_module._libfuse
    lib.fuse_get_session.argtypes = [ctypes.c_void_p]
    lib.fuse_get_session.restype = ctypes.c_void_p
    lib.fuse_session_fd.argtypes = [ctypes.c_void_p]
    lib.fuse_session_fd.restype = ctypes.c_int
    session = lib.fuse_get_session(ctypes.c_void_p(fuse_ptr))
    if not session:
        return False
    if ctypes.c_int.from_address(session + _SESSION_FD).value != lib.fuse_session_fd(session):
        return False
    slots = (ctypes.c_void_p * 44).from_address(session + _SESSION_OPS)
    if any(slots[i] for i in _OPS_UNSET) or not all(slots[i] for i in _OPS_SET):
        return False
    proto = ctypes.CFUNCTYPE(
        None, ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(fuse_module.fuse_file_info)
    )
    original = proto(slots[_OPENDIR])

    def opendir(req, ino, fip):
        fi = fip.contents
        fi.cache_readdir = 1
        fi.keep_cache = 1
        original(req, ino, fip)

    wrapped = proto(opendir)
    _kept_callbacks.extend((original, wrapped))
    slots[_OPENDIR] = ctypes.cast(wrapped, ctypes.c_void_p).value
    return True


def fuse_class(fuse_module, counts: Counter[str]) -> type:
    """mfusepy's FUSE class as the Drive mount runs it."""
    kept = threading.local()

    class FUSE(fuse_module.FUSE):
        def _wrapper(self, func, *args, **kwargs):
            if not getattr(kept, "state", False):
                # libfuse's workers are C threads, so every callback would
                # make and drop a Python thread state, doubling what a cached
                # request costs. One hold never released keeps the state for
                # the thread's life (the idle threads are kept too).
                ctypes.pythonapi.PyGILState_Ensure()
                kept.state = True
            return super()._wrapper(func, *args, **kwargs)

        def opendir(self, path, fip):
            # The kernel keeps each listing until the feed says it changed
            # (``keep_listings`` carries these flags where libfuse drops them).
            counts["opendir"] += 1
            fi = fip.contents
            if hasattr(fi, "cache_readdir"):
                fi.cache_readdir = 1
            fi.keep_cache = 1
            return 0

    return FUSE
