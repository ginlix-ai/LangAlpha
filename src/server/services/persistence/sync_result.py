"""What one sync pass saved, and every file it could not.

A caller acts differently per reason, which is why failures carry one rather
than being counted: some are refused the same way by every later sync on
this deployment, while the rest can save on the next pass. A caller about to
destroy the sandbox waits on every one whose only copy is still in it, which
is all of them but a repository's history past its limit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from src.server.services.persistence.transfer import ScanMark

UnsavedReason = Literal[
    "too_large",
    "path_too_long",
    "bad_name",
    "unreadable",
    "changed",
    "failed",
    "history_too_large",
    "history_held",
]

@dataclass(frozen=True, slots=True)
class _Reason:
    #: For a log line or an operator's error.
    phrase: str
    #: For the person who asked for a change, after the file's path.
    user: str
    #: The next pass tries the file again, so it holds the scan mark back.
    retried: bool
    #: Its only copy is still in the sandbox, so a teardown waits on it.
    at_risk: bool = True


_REASONS: dict[UnsavedReason, _Reason] = {
    "too_large": _Reason("too large for this deployment's transfer path", "too large to back up", False),
    "path_too_long": _Reason("path too long to record", "path too long to back up", False),
    "bad_name": _Reason("name not UTF-8", "name isn't valid text", False),
    "unreadable": _Reason("unreadable in the sandbox", "unreadable", True),
    "changed": _Reason("changed while being saved", "changed while saving", True),
    "failed": _Reason("failed to save", "didn't upload", True),
    # A history past the limit was never promised; its files are backed up.
    "history_too_large": _Reason(
        "git history over the size limit", "Git history too large to back up", False, at_risk=False
    ),
    # Never alone: the file that held the repository has its own line, which
    # says whether a later pass can save it.
    "history_held": _Reason(
        "git history held at its last complete backup", "Git history not backed up this time", False
    ),
}


@dataclass(frozen=True, slots=True)
class UnsavedFile:
    path: str
    reason: UnsavedReason
    size: int | None = None


@dataclass(slots=True)
class SyncResult:
    synced: int = 0
    skipped: int = 0
    deleted: int = 0
    total_size: int = 0
    #: The largest file this deployment's transfer path can store, or ``None``
    #: when nothing but the workspace disk bounds it.
    max_file_bytes: int | None = None
    #: The project's folder is absent from this sandbox, so the pass mirrored
    #: nothing and pruned nothing; the manifest stands as the record.
    root_missing: bool = False
    #: The folder's restore waits on its owner's choice, so the pass read
    #: nothing; the manifest stands as the record, as the files to restore.
    held: bool = False
    #: A routine pass found its computer's backups turned off, so it read
    #: nothing; the last backup stays as it was.
    backups_off: bool = False
    #: The pass pruned and refreshed stamps; one that withheld both leaves
    #: work a later pass has to repeat.
    pruned: bool = False
    unsaved: list[UnsavedFile] = field(default_factory=list)
    #: What this pass's scan can vouch for, recorded only if the pass leaves
    #: nothing a later pass could still save.
    scan_mark: ScanMark | None = None

    @property
    def at_risk(self) -> list[UnsavedFile]:
        """Unsaved files whose only copy is still in the sandbox."""
        return [f for f in self.unsaved if _REASONS[f.reason].at_risk]

    @property
    def errors(self) -> int:
        """Unsaved files the next sync may still save."""
        return sum(1 for f in self.unsaved if _REASONS[f.reason].retried)

    @property
    def oversized(self) -> int:
        """Unsaved files no sync on this deployment can save."""
        return sum(1 for f in self.unsaved if not _REASONS[f.reason].retried)

    def describe_unsaved(self, examples: int = 3) -> str:
        """Each reason with its count and a few paths, for a log line or an error."""
        by_reason: dict[UnsavedReason, list[str]] = {}
        for f in self.unsaved:
            by_reason.setdefault(f.reason, []).append(f.path)
        parts = []
        for reason, paths in by_reason.items():
            shown = ", ".join(paths[:examples])
            more = f", +{len(paths) - examples} more" if len(paths) > examples else ""
            parts.append(f"{len(paths)} {_REASONS[reason].phrase}: {shown}{more}")
        return "; ".join(parts)


_SHOWN_PATH_CHARS = 120


def _shown_path(path: str) -> str:
    """A path short enough for a sentence; the ends are what identify it."""
    if len(path) <= _SHOWN_PATH_CHARS:
        return path
    half = (_SHOWN_PATH_CHARS - 3) // 2
    return f"{path[:half]}...{path[-half:]}"


class BackupIncomplete(RuntimeError):
    """A strict backup left files whose only copy is in the sandbox.

    ``str()`` is the operator's account, with ids and causes; ``user_message``
    is what the person who asked for the change can act on, naming the files.
    """

    def __init__(self, detail: str, unsaved: list[UnsavedFile] | None = None) -> None:
        super().__init__(detail)
        self.unsaved = list(unsaved or [])

    @property
    def user_message(self) -> str:
        if not self.unsaved:
            return (
                "Nothing was changed: this computer's files could not be "
                "backed up first. Try again in a moment."
            )
        shown = ", ".join(
            f"{_shown_path(f.path)} ({_REASONS[f.reason].user})" for f in self.unsaved[:3]
        )
        more = len(self.unsaved) - 3
        tail = f", and {more} more" if more > 0 else ""
        count = len(self.unsaved)
        noun = "file" if count == 1 else "files"
        return (
            f"Nothing was changed: {count} {noun} could not be backed up "
            f"first: {shown}{tail}. Move or delete them, or try again."
        )
