"""A skill's folder as its page reads it: every file listed, one file shown.

A user skill is a stored zip and a platform skill is a folder on disk. The
page asks both the same two things, what is there and what one file says, so
each storage answers through one small interface and the response is built
once.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any, Protocol

from src.server.services.os_litter import is_os_litter

# A skill folder is instructions plus the references and scripts they name;
# a page lists what is there and shows what reads as text.
_LISTED = 500
_SHOWN_BYTES = 512 * 1024


def _listed(rel: str) -> bool:
    """Hidden files, bytecode and OS litter are packaging residue, not the skill."""
    parts = rel.split("/")
    return not (
        is_os_litter(parts)
        or any(part.startswith(".") or part == "__pycache__" for part in parts)
    )


class SkillFolder(Protocol):
    # Listed files and their sizes, sorted by path.
    files: dict[str, int]

    def read(self, path: str) -> bytes: ...


class ZipFolder:
    """A stored archive, whose files sit under one ``<name>/`` folder. An
    archive that does not open lists nothing."""

    def __init__(self, data: bytes, name: str) -> None:
        self._prefix = f"{name}/"
        self.files: dict[str, int] = {}
        try:
            self._zip = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile:
            return
        for info in sorted(self._zip.infolist(), key=lambda i: i.filename):
            rel = info.filename[len(self._prefix) :]
            if (
                info.is_dir()
                or not info.filename.startswith(self._prefix)
                or not rel
                or not _listed(rel)
            ):
                continue
            self.files[rel] = info.file_size
            if len(self.files) >= _LISTED:
                break

    def read(self, path: str) -> bytes:
        return self._zip.read(self._prefix + path)


class DiskFolder:
    """A shipped skill's folder, walked on construction. A symlink is skipped
    rather than followed, so nothing outside the folder is listed or served."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self.files: dict[str, int] = {}
        for entry in sorted(root.rglob("*")):
            rel = entry.relative_to(root).as_posix()
            if entry.is_symlink() or not entry.is_file() or not _listed(rel):
                continue
            self.files[rel] = entry.stat().st_size
            if len(self.files) >= _LISTED:
                break

    def read(self, path: str) -> bytes:
        return (self._root / path).read_bytes()


def _as_text(data: bytes) -> str | None:
    if b"\x00" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def skill_content_response(
    folder: SkillFolder, name: str, path: str
) -> dict[str, Any] | None:
    """One file of the folder with the listing beside it; None when ``path``
    is not listed, which is what keeps a path from reaching outside the
    skill. ``content`` is None for a file that is not text, or too large to
    show on a page."""
    size = folder.files.get(path)
    if size is None:
        return None
    return {
        "name": name,
        "path": path,
        "content": _as_text(folder.read(path)) if size <= _SHOWN_BYTES else None,
        "size": size,
        "files": [{"path": p, "size": n} for p, n in folder.files.items()],
    }
