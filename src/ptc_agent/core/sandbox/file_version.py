"""A file's version as one stat on the machine sees it, and its spellings.

A read prints a stat line, a download hands out an ETag, a save names the
ETag back and a guarded write checks the line on the machine: one
:class:`FileStamp` behind all four.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal


def version_line(path: str) -> str:
    """Shell that sets ``v`` to the version line of ``path`` (a shell word):
    size, mtime, ctime and inode from one stat, GNU's nanoseconds, then its
    seconds, then BSD's fraction.

    Each attempt fills ``v`` on its own, since a failed one can still print
    (GNU reads ``-f`` as file system status). ``wc`` is the last resort, for
    the size alone, as BusyBox's reads every byte; BSD's pads it with spaces.
    """
    return (
        f"v=$(stat -c '%s %.9Y %.9Z %i' -- {path} 2>/dev/null) "
        f"|| v=$(stat -c '%s %Y %Z %i' -- {path} 2>/dev/null) "
        f"|| v=$(stat -f '%z %Fm %Fc %i' -- {path} 2>/dev/null) "
        f"|| v=$(wc -c < {path} | tr -d ' '); "
    )


_TIMESTAMP = re.compile(r"\d+(\.\d+)?", re.ASCII)
# How far the sandbox clock must have left a file's ctime behind before no
# write still to come can share it. A write moves the ctime only by the clock's
# step, a kernel tick or a whole second; the second beyond that covers a
# filesystem clock a little off the sandbox's.
_SETTLED_AFTER_S = 2


# Marks an edit version whose stamp had not settled.
_UNSETTLED = "+u"


def _digits(value: str) -> bool:
    return value.isascii() and value.isdigit()


def line_size(line: str) -> int | None:
    """The size a version line starts with, which a size-only line has too."""
    fields = line.split()
    return int(fields[0]) if fields and _digits(fields[0]) else None


@dataclass(frozen=True)
class FileStamp:
    """Size, mtime, ctime and inode as stat printed them.

    They land in an ETag, so anything but stat's digits makes no stamp.
    """

    size: int
    mtime: str
    ctime: str
    inode: str

    @classmethod
    def _of(cls, fields: list[str]) -> FileStamp | None:
        if len(fields) != 4:
            return None
        size, mtime, ctime, inode = fields
        if not (
            _digits(size)
            and _TIMESTAMP.fullmatch(mtime)
            and _TIMESTAMP.fullmatch(ctime)
            and _digits(inode)
        ):
            return None
        return cls(int(size), mtime, ctime, inode)

    @classmethod
    def from_line(cls, line: str) -> FileStamp | None:
        return cls._of(line.split())

    @classmethod
    def from_etag(cls, tag: str) -> FileStamp | None:
        """From an ETag's value, unquoted; None for any tag no read gave."""
        return cls._of(tag.split("-"))

    def edit_version(self, *, settled: bool) -> str:
        """The version a read or a save gives an editor to name: the ETag,
        marked when the stamp has not :meth:`settled`, so a save naming it
        knows the stamp alone may not tell its file from a later write."""
        return self.etag if settled else self.etag + _UNSETTLED

    @classmethod
    def from_edit_version(cls, version: str) -> tuple[FileStamp | None, bool]:
        """The stamp an :meth:`edit_version` names, and whether it had
        settled; no stamp for any version no read or save gave."""
        settled = not version.endswith(_UNSETTLED)
        return cls.from_etag(version.removesuffix(_UNSETTLED)), settled

    @property
    def line(self) -> str:
        return f"{self.size} {self.mtime} {self.ctime} {self.inode}"

    @property
    def etag(self) -> str:
        return f"{self.size}-{self.mtime}-{self.ctime}-{self.inode}"

    def settled(self, clock: str) -> bool:
        """Whether the stamp tells the file from its next write, by the
        sandbox ``clock`` read after the stat.

        mtime alone cannot, since a writer can set it (``cp -p``, ``touch
        -r``, an unpacked archive). No writer sets the ctime, and a file
        renamed over the name brings its own inode; but a write moves the
        ctime only by the clock's step, so two same-size writes within one
        step share every stamp, however many digits the ctime shows. A stamp
        counts only once the clock has left the last write behind, as
        Apache's ETags do.
        """
        clock = clock.strip()
        if not _digits(clock):
            return False
        # Exact, so a fractional ctime is held to the same two seconds.
        return Decimal(clock) - Decimal(self.ctime) >= _SETTLED_AFTER_S
