"""The symlinks that put the mount where the agent's paths are."""

from __future__ import annotations

import contextlib
import errno
import os
import re
import stat
import tempfile
from collections.abc import Callable

from . import tree_runtime


def swap_link(value: str, path: str) -> None:
    """Make ``path`` a symlink to ``value`` in one step."""
    tmp = f"{path}.livefs.tmp"
    with contextlib.suppress(FileNotFoundError):
        os.unlink(tmp)
    os.symlink(value, tmp)
    try:
        os.replace(tmp, path)
    except OSError:
        os.unlink(tmp)
        raise


def finish_set_aside(target: str, copied: dict) -> str | None:
    """Finish the delete of a folder set aside whose copy ``copied`` records:
    where the copy is, or None, dropping the record, when it holds no copy of
    what is at ``target`` now. That is asked of every file left there, as a
    folder whose delete failed stays in use until the link goes in, and what
    was written to it since is set aside anew rather than deleted. A link no
    longer asked for finishes it too, or the folder half deleted would stay
    beside its whole copy."""
    try:
        st = os.lstat(target)
    except FileNotFoundError:
        st = None
    held = copied.pop(target, None)
    if not (
        st is not None
        and isinstance(held, list)
        and len(held) == 2
        and isinstance(held[0], str)
        and re.fullmatch(re.escape(target) + r"\.local(\.[0-9]+)?", held[0])
        and os.path.isdir(held[0])
        and not os.path.islink(held[0])
        and stat.S_ISDIR(st.st_mode)
        and st.st_ino == held[1]
    ):
        return None
    copied[target] = held
    if not tree_runtime.holds(held[0], target):
        del copied[target]
        return None
    tree_runtime.remove_tree(target)
    del copied[target]
    return held[0]


def _set_aside(target: str, copied: dict, keep: Callable[[], None]) -> str:
    """Keep what was already where a link goes, under a name beside it.

    A folder a Daytona sandbox restored from its archive sits in a read-only
    overlay layer that refuses to rename it (EXDEV), so it is copied there, as
    the folder script copies, and deleted where it is instead; the copy, made
    since, renames. ``copied`` holds a copy whose delete failed or was cut
    short, as [the copy, the folder's inode], which ``keep`` saves before the
    delete starts, so a retry of the link finishes the delete rather than
    taking the size of what it cannot remove again on every pass.
    """
    held = finish_set_aside(target, copied)
    if held:
        return held
    aside, n = f"{target}.local", 0
    while os.path.lexists(aside):
        n += 1
        aside = f"{target}.local.{n}"
    try:
        os.rename(target, aside)
    except OSError as exc:
        if not tree_runtime.cannot_rename(exc):
            raise
        # A name of its own, so no folder the agent made beside it is taken
        # for a copy cut short. One a pass killed midway left is ours, as
        # passes take turns under the lock.
        prefix, parent = f".{os.path.basename(aside)}.copying-", os.path.dirname(aside)
        for name in os.listdir(parent):
            left = os.path.join(parent, name)
            if re.fullmatch(re.escape(prefix) + "[a-z0-9_]{8}", name) and not os.path.islink(left):
                with contextlib.suppress(OSError):
                    tree_runtime.remove_tree(left)
        ino = os.lstat(target).st_ino
        copying = tempfile.mkdtemp(prefix=prefix, dir=parent)
        try:
            # Whole only once a walk finds nothing more to write, as a process
            # at work in the folder can change what an earlier walk passed,
            # and the folder goes next. One still at it waits for the next link.
            if tree_runtime.sync_tree(target, copying) is None:
                raise OSError(errno.EBUSY, "still changing as it was copied", target)
            os.rename(copying, aside)
        except BaseException as exc:
            with contextlib.suppress(OSError):
                tree_runtime.remove_tree(copying)
            if isinstance(exc, tree_runtime.NoRoom):
                raise OSError(errno.ENOSPC, str(exc), target) from exc
            raise
        copied[target] = [aside, ino]
        keep()
        tree_runtime.remove_tree(target)
        del copied[target]
    return aside


def link(
    value: str,
    target: str,
    copied: dict | None = None,
    keep: Callable[[], None] | None = None,
) -> str | None:
    """Point ``target`` at ``value``; returns where anything already there
    went. Made with the caller's rights, as whoever owns the folders."""
    aside = None
    copied = {} if copied is None else copied
    keep = keep or (lambda: None)
    if os.path.islink(target):
        if os.readlink(target) == value:
            return None
    elif os.path.isdir(target):
        try:
            os.rmdir(target)
        except OSError:
            aside = _set_aside(target, copied, keep)
    elif os.path.lexists(target):
        aside = _set_aside(target, copied, keep)
    else:
        os.makedirs(os.path.dirname(target), exist_ok=True)
    swap_link(value, target)
    return aside


def unlink(target: str, mount: str) -> None:
    """Remove a link into ``mount``, and nothing that has since replaced it."""
    with contextlib.suppress(OSError):
        if os.readlink(target).startswith(mount + "/"):
            os.unlink(target)
