"""What a Drive path may be, and the key two paths collide on.

Rules a copy of the tree to a POSIX disk keeps: a segment fits a file name
there (at most 255 bytes, never ``.`` or ``..``), with no control character
and no space at either end. Names Windows refuses (``?``, ``:``, ``<`` and
its reserved names) are allowed. The service checks what a caller sends
(``normalize``); the writes check the names they build themselves (a
restored entry set beside a taken one).
"""

from __future__ import annotations

import unicodedata

from src.server.database.drive.records import DriveError

#: Bytes of UTF-8, so a path under a computer's mount prefix stays well inside
#: Linux's 4096-byte path limit.
MAX_PATH_BYTES = 1024
MAX_DEPTH = 32
MAX_SEGMENT_BYTES = 255


def split_path(path: str) -> tuple[str, str]:
    """``path``'s parent folder and its name."""
    parent, _, name = path.rpartition("/")
    return parent, name


def path_depth(path: str) -> int:
    return path.count("/") + 1


def path_bytes(path: str) -> int:
    return len(path.encode("utf-8"))


def _fold(segment: str) -> str:
    return unicodedata.normalize("NFC", unicodedata.normalize("NFC", segment).casefold())


def path_key(path: str) -> str:
    """What a case-insensitive disk would take ``path`` for: full case
    folding, so ``straße`` meets ``STRASSE`` and ``ς`` meets ``σ``, which
    ``lower()`` keeps apart. Folded a segment at a time, so the key of a
    path under a folder starts with the folder's key and a slash."""
    return "/".join(_fold(s) for s in path.split("/")) if path else ""


def _invalid(message: str) -> DriveError:
    return DriveError("invalid_path", message)


def check(path: str) -> None:
    """Raise ``invalid_path`` unless ``path`` (already NFC, no leading or
    trailing slash) keeps every rule."""
    if not path:
        raise _invalid("A path is required")
    if any("\ud800" <= ch <= "\udfff" for ch in path):
        # A lone surrogate has no UTF-8: refused here, before anything encodes it.
        raise _invalid("A path is not valid Unicode")
    if len(path.encode("utf-8")) > MAX_PATH_BYTES:
        raise _invalid(f"A path is at most {MAX_PATH_BYTES} bytes")
    segments = path.split("/")
    if len(segments) > MAX_DEPTH:
        raise _invalid(f"A path is at most {MAX_DEPTH} levels deep")
    for segment in segments:
        if not segment:
            raise _invalid("A path has no empty segments")
        if segment in (".", ".."):
            raise _invalid("A path segment cannot be . or ..")
        if len(segment.encode("utf-8")) > MAX_SEGMENT_BYTES:
            raise _invalid(f"A name is at most {MAX_SEGMENT_BYTES} bytes")
        if segment != segment.strip(" "):
            raise _invalid("A name cannot start or end with a space")
        if any(unicodedata.category(ch) == "Cc" for ch in segment):
            raise _invalid("A name cannot hold control characters")


def normalize(path: str, *, root: bool = False) -> str:
    """``path`` NFC-normalised, with a leading or trailing slash dropped.
    ``root`` admits ``''``, the space's top folder, for the calls that read
    a folder.

    Raises:
        DriveError: ``invalid_path``.
    """
    path = unicodedata.normalize("NFC", path or "")
    path = path.removeprefix("/").removesuffix("/")
    if not path and root:
        return ""
    check(path)
    return path
