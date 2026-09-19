"""Write admission for the files the baseline context block freezes.

The baseline reads agent.md and the memory indexes under a size cap and cuts
what is past it, and the block is only rebuilt at compaction, far from the
write that crossed the line. So the Write/Edit result says where the file
stands the moment it is written, while the model still holds the file.
"""

from __future__ import annotations

from typing import NamedTuple

from ptc_agent.core.paths import (
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    WorkspaceLayout,
    workspace_relative_path,
)

# Read-time ceilings. The block is cached per epoch, so a generous cap costs
# cache reads, not rewrites; it is a safety ceiling most threads never reach,
# not a budget. 32 KB is about 200 index lines at 150 characters, which is what
# the memory steering already promises.
MAX_AGENT_MD_SIZE = 32768
MAX_MEMORY_BLOCK_SIZE = 32768
MEMORY_FILL_WARN_RATIO = 0.85

# Two tables because the caps sit on two tiers, and a path names its file
# only relative to the tier that owns it.
WORKSPACE_CAPPED_FILES: dict[str, int] = {
    "agent.md": MAX_AGENT_MD_SIZE,
    f"{WorkspaceLayout.MEMORY_DIR}/{MEMORY_INDEX_FILENAME}": MAX_MEMORY_BLOCK_SIZE,
}

COMPUTER_CAPPED_FILES: dict[str, int] = {
    f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}": MAX_MEMORY_BLOCK_SIZE,
}


class CappedFile(NamedTuple):
    """A capped file: the name the model sees in the note, and its cap."""

    name: str
    cap: int


def capped_file(
    path: str, *, workspace_dir: str, computer_root: str
) -> CappedFile | None:
    """The cap on an absolute sandbox path, or None when the file is uncapped.

    Both bases are tried because the user memory index is shared by every
    workspace on the computer while agent.md and the workspace index sit inside
    one folder: strip the other tier's base and the leftover prefix matches no
    key. An unsplit computer passes the same base twice, which is why the caps
    worked before a computer could be split at all.
    """
    for base, table in (
        (workspace_dir, WORKSPACE_CAPPED_FILES),
        (computer_root, COMPUTER_CAPPED_FILES),
    ):
        key = workspace_relative_path(path, base)
        cap = table.get(key)
        if cap is not None:
            return CappedFile(key.rsplit("/", 1)[-1], cap)
    return None


def fill_note(name: str, size: int, cap: int) -> str | None:
    """The sentence appended to a Write/Edit result once a capped file is near full.

    Silent under the warn ratio: a file at 20% needs no nudge, and a note on
    every write would be read as noise by the tenth one.
    """
    if size < cap * MEMORY_FILL_WARN_RATIO:
        return None
    if size > cap:
        return (
            f"Note: {name} is at {size:,} of {cap:,} characters. The part past "
            f"the cap is not visible in your context block. Consolidate now: "
            f"merge overlapping entries, retire stale ones, move detail into "
            f"the files the index points at."
        )
    percent = int(size * 100 / cap)
    return (
        f"Note: {name} is at {size:,} of {cap:,} characters ({percent}% full). "
        f"Consolidate before adding more: merge overlapping entries, retire "
        f"stale ones, move detail into the files the index points at."
    )


def over_cap_refusal(name: str, size: int, cap: int) -> str | None:
    """The result for a Write that would land past the cap, when set to refuse.

    Only a Write can be refused: its size is known before anything is written,
    while an Edit's is only known once it is applied, and undoing an applied
    edit would be a second write the model never asked for.
    """
    if size <= cap:
        return None
    return (
        f"ERROR: {name} would be {size:,} characters, past its cap of {cap:,}. "
        f"Nothing was written. Consolidate first: merge overlapping entries, "
        f"retire stale ones, move detail into the files the index points at."
    )
