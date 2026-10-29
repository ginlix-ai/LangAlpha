"""A workspace's folder on its computer is its name, so names are the folder rules.

``workspace_folder_name`` is the one spelling of a name as a folder, and
``workspace_name_key`` is what two names must not share per user: the folded
spelling, case-insensitively, because a folder is the thing that collides and
a case-insensitive disk (a Docker bind mount on macOS) cannot hold ``Research``
and ``research`` side by side. Migration 054 carries a frozen copy of these
rules; edit both or the backfill and the app disagree about what is taken.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Iterable, Optional

from ptc_agent.core.paths import COMPUTER_ROOT_ENTRIES, LEGACY_ROOT_DIRS

WORKSPACE_NAME_MAX_CHARS = 80
# NAME_MAX on Linux counts bytes, and a CJK character is three of them.
FOLDER_NAME_MAX_BYTES = 255

# A folder beside the workspaces that belongs to the computer or an older layout.
_RESERVED_FOLDERS = frozenset(
    name.casefold() for name in (*COMPUTER_ROOT_ENTRIES, *LEGACY_ROOT_DIRS)
)

#: The folder of the user's Home, the agent that works across every workspace.
HOME_FOLDER = "Home"
# Names a new workspace may not take, though the folder is an ordinary one:
# Home is placed under its own name, and a workspace already holding it keeps
# Home on a placeholder until that workspace is renamed or deleted.
_RESERVED_NAMES = frozenset({HOME_FOLDER.casefold()})
# Characters that break a path, split a PATH-style list (PYTHONPATH joins
# folders with ":"), or survive badly inside a double-quoted shell word.
_UNSAFE = re.compile(r'[/\\:$`"\x00-\x1f\x7f]')


class WorkspaceNameInvalid(ValueError):
    """The name has no usable folder spelling.

    ``reason`` is ``empty``, ``reserved`` or ``too_long``, so a client words the
    refusal in its own language; ``name`` is the reserved folder it hit.
    """

    def __init__(self, message: str, *, reason: str = "invalid", name: Optional[str] = None):
        super().__init__(message)
        self.reason = reason
        self.name = name


class WorkspaceNameTaken(Exception):
    """Another live workspace of this user already folds to the same folder."""

    def __init__(self, name: str, workspace_id: Optional[str] = None):
        self.name = name
        self.workspace_id = workspace_id
        super().__init__(f'A workspace named "{name}" already exists.')


def _fit_bytes(text: str, limit: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode("utf-8", errors="ignore").rstrip()


def _folder_spelling(name: Optional[str]) -> str:
    text = unicodedata.normalize("NFC", name or "")
    return _UNSAFE.sub("-", " ".join(text.split())).lstrip(".- ")


def workspace_folder_name(name: Optional[str]) -> str:
    """The folder a workspace named ``name`` lives in.

    Spaces and non-ASCII stay, so the folder reads as the name the user typed;
    only what breaks a path or a quoted shell word is replaced.
    """
    text = _fit_bytes(_folder_spelling(name), FOLDER_NAME_MAX_BYTES)
    if not text:
        raise WorkspaceNameInvalid(
            "A workspace name needs at least one letter or digit.", reason="empty"
        )
    if text.casefold() in _RESERVED_FOLDERS:
        raise WorkspaceNameInvalid(
            f'"{text}" is reserved; choose another name.', reason="reserved", name=text
        )
    return text


def workspace_name_key(name: Optional[str]) -> str:
    return workspace_folder_name(name).casefold()


def checked_workspace_name(name: Optional[str]) -> str:
    """``name`` as a workspace is stored under it: trimmed, and one that fits a folder."""
    text = (name or "").strip()
    if len(text) > WORKSPACE_NAME_MAX_CHARS:
        raise WorkspaceNameInvalid(
            f"A workspace name can be at most {WORKSPACE_NAME_MAX_CHARS} characters.",
            reason="too_long",
        )
    folder = workspace_folder_name(text)
    if folder.casefold() in _RESERVED_NAMES:
        raise WorkspaceNameInvalid(
            f'"{folder}" is reserved; choose another name.', reason="reserved", name=folder
        )
    return text


def placeholder_dir_name(name: Optional[str], workspace_id: str, *, hex_chars: int = 4) -> str:
    """A folder for a workspace whose own is still held, until the next settle moves it."""
    digest = hashlib.md5(str(workspace_id).encode("utf-8")).hexdigest()[:hex_chars]
    try:
        base = workspace_folder_name(name)
    except WorkspaceNameInvalid:
        base = "workspace"
    return f"{_fit_bytes(base, FOLDER_NAME_MAX_BYTES - 1 - hex_chars)}-{digest}"


#: Placeholder spellings a new folder tries after the name's own, each with a
#: longer suffix, before the caller reports the folder taken.
PLACEHOLDER_ATTEMPTS = 3


def candidate_dir_name(name: Optional[str], workspace_id: str, attempt: int) -> str:
    """The folder to try on ``attempt``: the name's own first, then placeholders."""
    if attempt == 0:
        try:
            return workspace_folder_name(name)
        except WorkspaceNameInvalid:
            pass
    return placeholder_dir_name(name, workspace_id, hex_chars=4 * max(1, attempt))


def candidate_dir_names(
    name: Optional[str], workspace_id: str, held: Iterable[str] = (), *, own_folder: bool = True
) -> list[str]:
    """Every candidate in order, less any folder ``held`` has under any case.

    The folder index compares case, but a case-insensitive disk (a Docker work
    dir bind-mounted from macOS) keeps "Research" and "research" as one folder,
    so two rows would share its files and cleaning up one would take both.
    Without ``own_folder`` only placeholders are offered.
    """
    folded = {folder.casefold() for folder in held}
    candidates = (
        candidate_dir_name(name, workspace_id, attempt)
        for attempt in range(0 if own_folder else 1, PLACEHOLDER_ATTEMPTS + 1)
    )
    return [folder for folder in candidates if folder.casefold() not in folded]


def suffixed_name(name: str, suffix: str) -> str:
    """``name`` plus ``suffix``, trimming the name so the result still fits.

    The fit is measured before the folder's byte cut: after it, a suffix the cut
    dropped looks like a fit, and every numbered spelling keys as the name.
    """
    base = name.strip()
    while base and (
        len(base) + len(suffix) > WORKSPACE_NAME_MAX_CHARS
        or len(_folder_spelling(base + suffix).encode("utf-8")) > FOLDER_NAME_MAX_BYTES
    ):
        base = base[:-1].rstrip()
    return f"{base}{suffix}" if base else suffix.strip()


_COPY_SUFFIX = re.compile(r" \(copy(?: \d+)?\)$")


def copy_name(name: str, taken_keys: Iterable[str]) -> str:
    """``name (copy)``, then ``name (copy 2)`` and on; a copy of a copy counts on."""
    taken = set(taken_keys)
    base = _COPY_SUFFIX.sub("", name.strip()) or name.strip()
    n = 1
    while True:
        candidate = suffixed_name(base, " (copy)" if n == 1 else f" (copy {n})")
        if workspace_name_key(candidate) not in taken:
            return candidate
        n += 1


def first_free_name(name: str, taken_keys: Iterable[str], *, pattern: str = " ({n})") -> str:
    """``name`` if free, else the first ``name (2)``-style spelling nobody holds."""
    taken = set(taken_keys)
    if workspace_name_key(name) not in taken:
        return name
    n = 2
    while True:
        candidate = suffixed_name(name, pattern.format(n=n))
        if workspace_name_key(candidate) not in taken:
            return candidate
        n += 1
