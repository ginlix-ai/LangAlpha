"""The harness-authored blocks: text the harness wrote, frozen like a file.

Each block's identity is the template it renders through, the element a change
row names, and the words that row uses for what moved. They live together here,
below baseline, epoch and durable in the import graph, so the block renderer,
the retiring row and the change row all read one entry, and a block a build
does not have is never named by a row that build files.
"""

from __future__ import annotations

from dataclasses import dataclass

_CHANGED = "_changed"


@dataclass(frozen=True, slots=True)
class HarnessBlock:
    """One harness block: its baseline template, its element, and its row subject."""

    template: str
    label: str
    subject: str


#: Every harness block, keyed by read kind, in the order the baseline renders
#: them. Adding one is an entry here plus its template.
HARNESS_BLOCKS: dict[str, HarnessBlock] = {
    "mcp_servers": HarnessBlock(
        template="envelope/baseline_mcp_servers.md.j2",
        label="<mcp-servers>",
        subject="MCP server list",
    ),
    "skills": HarnessBlock(
        template="envelope/baseline_skills.md.j2",
        label="<skills>",
        subject="skills manifest",
    ),
    # Here rather than in the static prompt because its path names the
    # thread; it is absolute, so a rename of the workspace's folder moves it
    # and files a row.
    "scratchpad": HarnessBlock(
        template="envelope/baseline_scratchpad.md.j2",
        label="<scratchpad>",
        subject="scratchpad folder",
    ),
    "activity": HarnessBlock(
        template="envelope/baseline_activity.md.j2",
        label="<activity>",
        subject="user's activity",
    ),
}


def harness_update_kind(kind: str) -> str:
    """The change-row kind a harness block's reads file under."""
    return f"{kind}{_CHANGED}"


def harness_block_for(update_kind: str) -> HarnessBlock | None:
    """The block a change row of this kind speaks for, or None for a non-harness kind."""
    if not update_kind.endswith(_CHANGED):
        return None
    return HARNESS_BLOCKS.get(update_kind.removesuffix(_CHANGED))
