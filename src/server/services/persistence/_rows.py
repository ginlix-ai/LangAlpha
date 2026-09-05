"""Pure helpers shared by the backup and restore paths.

Manifest row shape and the field conversions that feed it in both
directions, plus the sandbox transfer mode both paths branch on. Nothing
here touches the database, the sandbox or object storage.
"""

import codecs
import mimetypes
import os
from datetime import datetime
from typing import Any

from src.server.database.workspace_file import micros_to_datetime
from src.server.services.persistence.transfer import ScanEntry
from src.utils.storage import get_blob_transfer_mode

# Extensions treated as binary before any bytes are read, for rows whose
# content this process never sees (the sandbox sniffs the rest).
_BINARY_EXTENSIONS = frozenset(
    {
        ".pdf",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".bmp",
        ".ico",
        ".tiff",
        ".zip",
        ".tar",
        ".gz",
        ".bz2",
        ".7z",
        ".rar",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".mp3",
        ".mp4",
        ".wav",
        ".avi",
        ".mov",
        ".mkv",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".sqlite",
        ".db",
        ".pickle",
        ".pkl",
    }
)


def _ns_to_datetime(mtime_ns: int) -> datetime:
    # Epoch-zero and pre-1970 mtimes are legitimate (``touch -d @0`` is a
    # common "mark as stale" idiom); treating them as absent makes the row
    # look changed on every sync and restores the file with a fresh mtime.
    return micros_to_datetime(mtime_ns // 1000)


def _mode_string(mode: int | None) -> str | None:
    # Zero is a real mode (a file made unreadable on purpose), so only an
    # absent value reads as "no mode recorded".
    return None if mode is None else f"{mode:04o}"


def _entry_mode_string(entry: ScanEntry) -> str | None:
    # A symlink's mode is never applied on restore, so none is recorded.
    return None if entry.kind == "symlink" else _mode_string(entry.mode)


def _mode_int(permissions: str | None, kind: str) -> int:
    if permissions:
        try:
            return int(permissions, 8)
        except ValueError:
            pass
    return 0o755 if kind == "dir" else 0o644


_UTF8_SLICE = 1 << 20


def _is_binary_extension(file_path: str) -> bool:
    """Check if file extension indicates binary content."""
    _, ext = os.path.splitext(file_path)
    return ext.lower() in _BINARY_EXTENSIONS


def _detect_is_binary(file_path: str, content: bytes) -> bool:
    """Detect whether file content is binary."""
    if _is_binary_extension(file_path):
        return True
    # A NUL anywhere, not only in a leading window: text goes to a column
    # that cannot hold one, so a NUL further in is dropped on the way to
    # the row and the stored bytes stop hashing to the row's own digest.
    # Restore verifies that digest, and a file that fails it is never placed.
    if b"\x00" in content:
        return True
    # The whole content decides, in slices so no full-size str is built: a
    # blob row is never re-read on the way out, so a bad sequence after the
    # first pages would otherwise be served as text with replacement marks.
    decoder = codecs.getincrementaldecoder("utf-8")()
    view = memoryview(content)
    try:
        for start in range(0, len(view), _UTF8_SLICE):
            decoder.decode(view[start : start + _UTF8_SLICE])
        decoder.decode(b"", final=True)
        return False
    except UnicodeDecodeError:
        return True


def _transfer_mode(sandbox: Any) -> str:
    # PTCSandbox holds the whole CoreConfig; the provider name is on its
    # sandbox section. Anything else (a mock, a foreign runtime) reads as
    # an unknown provider and relays.
    config = getattr(sandbox, "config", None)
    section = getattr(config, "sandbox", None)
    provider = getattr(section, "provider", None)
    if not isinstance(provider, str):
        provider = None
    return get_blob_transfer_mode(provider)


def _row_base(entry: ScanEntry) -> dict[str, Any]:
    return {
        "file_path": entry.path,
        "file_name": os.path.basename(entry.path),
        "file_size": entry.size if entry.kind == "file" else 0,
        "permissions": _entry_mode_string(entry),
        "sandbox_modified_at": _ns_to_datetime(entry.mtime_ns),
        "kind": entry.kind,
        "symlink_target": entry.symlink_target,
        "content_hash": entry.sha256 if entry.kind == "file" else None,
        "content_text": None,
        "content_binary": None,
        "blob_sha256": None,
        "pack_sha256": None,
        "pack_offset": None,
        "mime_type": None,
        "is_binary": False,
    }


def _blob_row(
    entry: ScanEntry, *, is_binary: bool | None = None
) -> dict[str, Any]:
    row = _row_base(entry)
    mime, _ = mimetypes.guess_type(entry.path)
    if is_binary is None:
        # The scan sniffs content the way the relay path does; the
        # extension list is the other half of that rule.
        sniffed = entry.is_binary
        is_binary = _is_binary_extension(entry.path) or bool(sniffed)
    row.update(
        {
            "blob_sha256": entry.sha256,
            "mime_type": mime,
            "is_binary": is_binary,
        }
    )
    return row


def _pack_row(
    entry: ScanEntry,
    pack_sha256: str,
    pack_offset: int,
    *,
    is_binary: bool | None = None,
) -> dict[str, Any]:
    row = _blob_row(entry, is_binary=is_binary)
    row.update(
        {
            "blob_sha256": None,
            "pack_sha256": pack_sha256,
            "pack_offset": int(pack_offset),
        }
    )
    return row


def _stamp_matches(db: dict[str, Any], entry: ScanEntry) -> bool:
    """True when a manifest row's mode and mtime already describe this entry.

    The comparison is truncated to microseconds because the column is a
    TIMESTAMPTZ: the nanoseconds the scan reports cannot survive the round
    trip, so a full-precision compare would read every row as moved.
    """
    return (
        db.get("mtime_ns") is not None
        and db["mtime_ns"] // 1000 == entry.mtime_ns // 1000
        and db.get("permissions") == _entry_mode_string(entry)
    )


def _content_matches(db: dict[str, Any] | None, entry: ScanEntry) -> bool:
    """True when a manifest row's bytes are the ones this entry holds."""
    return bool(
        db
        and db.get("kind", "file") == "file"
        and db.get("content_hash") == entry.sha256
        and db.get("file_size") == entry.size
    )


def _has_inline_bytes(row: dict[str, Any]) -> bool:
    """True when a row carries its own bytes, so nothing has to be fetched."""
    return (
        row.get("content_text") is not None
        or row.get("content_binary") is not None
    )
