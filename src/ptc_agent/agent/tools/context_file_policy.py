"""Write admission for the files the baseline context block freezes.

The baseline reads agent.md and the memory indexes under a size cap and cuts
what is past it, and the block is only rebuilt at compaction, far from the
write that crossed the line. So the Write/Edit result says where the file
stands the moment it is written, while the model still holds the file, and a
write that would take the file well past its budget is refused outright: a
warning alone let notebooks grow to the cap and stay there. The baseline cuts
its copy at the same limit, here too, so what a write is let through is what
the agent reads.
"""

from __future__ import annotations

import re
from typing import NamedTuple

from ptc_agent.agent.roles import AgentRole
from ptc_agent.core.paths import (
    MEMORY_INDEX_FILENAME,
    MEMORY_USER_DIR,
    WorkspaceLayout,
    workspace_relative_path,
)

#: What a Chinese, Japanese or Korean character counts for. The files are
#: injected into every call, so what matters is their token cost, and one of
#: these characters costs about 2.5 (o200k) to 3.5 (cl100k) times what any
#: other character does. Counted raw, a notebook in Chinese at the cap cost
#: three times the tokens of one in English.
WIDE_CHAR_WEIGHT = 3

_WIDE = re.compile(
    "["
    "ᄀ-ᇿ"  # Hangul Jamo
    "⺀-⿟"  # CJK radicals
    "　-〿"  # CJK symbols and punctuation
    "぀-ヿ"  # Hiragana, Katakana
    "㄀-ㄯ"  # Bopomofo
    "㄰-㆏"  # Hangul compatibility Jamo
    "㐀-䶿"  # CJK extension A
    "一-鿿"  # CJK unified ideographs
    "가-힯"  # Hangul syllables
    "豈-﫿"  # CJK compatibility ideographs
    "＀-￯"  # Fullwidth forms
    "\U00020000-\U0003134f"  # CJK extensions B onward
    "]"
)


def weighted_size(text: str) -> int:
    """The size every cap here is measured in (see ``WIDE_CHAR_WEIGHT``).

    Counted with ``subn`` rather than ``findall``: a notebook written through
    Bash can be megabytes, and a list of one string per CJK character cost
    eight times the file's size before it could be cut.
    """
    return len(text) + (WIDE_CHAR_WEIGHT - 1) * _WIDE.subn("", text)[1]


def weighted_prefix(text: str, limit: int) -> int:
    """The length of the longest prefix of ``text`` within ``limit``."""
    total = 0
    for i, ch in enumerate(text):
        total += WIDE_CHAR_WEIGHT if _WIDE.match(ch) else 1
        if total > limit:
            return i
    return len(text)


#: How far past its budget a file may go before a write is refused. The
#: headroom lets one task's lines land before the agent consolidates.
HARD_LIMIT_RATIO = 1.25
FILL_WARN_RATIO = 0.85
#: An entry longer than this is content, not a pointer.
INDEX_LINE_LIMIT = 300

_UNIT = "a CJK character counts as 3"


class CappedFile(NamedTuple):
    """A file the baseline injects: the name the model sees, its budget, how
    to shorten it, and the ``##`` sections it must keep, when it has a fixed
    layout."""

    name: str
    budget: int
    advice: str
    sections: tuple[str, ...] | None = None

    @property
    def limit(self) -> int:
        """Past this a write is refused and the read is cut."""
        return int(self.budget * HARD_LIMIT_RATIO)


# Budgets, in weighted characters. agent.md is a map, one line per entry, so
# 10k is about 60 lines (some 2.5k tokens); a memory index is 200 pointers of
# about 120.
AGENT_MD = CappedFile(
    "agent.md",
    10_000,
    "keep each entry to one line, leave findings in the deliverables, and drop "
    "the oldest deliverable lines",
)
#: An analyst's agent.md, in the sections Workspace Notes gives, under these
#: English headings whatever language the entries are in. Home's is laid out
#: by the Chief of Staff role instead.
WORKSPACE_AGENT_MD = AGENT_MD._replace(sections=("Purpose", "Reusable", "Open", "Deliverables"))
MEMORY_INDEX = CappedFile(
    "memory.md",
    24_000,
    "merge overlapping entries, retire stale ones, move detail into the files "
    "the index points at",
)

AGENT_MD_BUDGET = AGENT_MD.budget
MEMORY_INDEX_BUDGET = MEMORY_INDEX.budget
MAX_AGENT_MD_SIZE = AGENT_MD.limit
MAX_MEMORY_BLOCK_SIZE = MEMORY_INDEX.limit

_WORKSPACE_MEMORY_INDEX = f"{WorkspaceLayout.MEMORY_DIR}/{MEMORY_INDEX_FILENAME}"
_USER_MEMORY_INDEX = f"{MEMORY_USER_DIR}/{MEMORY_INDEX_FILENAME}"


def agent_md_for(role: AgentRole) -> CappedFile:
    """The agent.md an agent in ``role`` keeps."""
    return WORKSPACE_AGENT_MD if role == "analyst" else AGENT_MD


def capped_file(
    path: str, *, workspace_dir: str, computer_root: str, agent_md: CappedFile = AGENT_MD
) -> CappedFile | None:
    """The cap on an absolute sandbox path, or None when the file is uncapped.

    Both bases are tried because the user memory index is shared by every
    workspace on the computer while agent.md and the workspace index sit inside
    one folder: strip the other tier's base and the leftover prefix matches no
    key. An unsplit computer passes the same base twice, which is why the caps
    worked before a computer could be split at all.
    """
    workspace_key = workspace_relative_path(path, workspace_dir)
    if workspace_key == "agent.md":
        return agent_md
    if workspace_key == _WORKSPACE_MEMORY_INDEX:
        return MEMORY_INDEX
    if workspace_relative_path(path, computer_root) == _USER_MEMORY_INDEX:
        return MEMORY_INDEX
    return None


class Admission(NamedTuple):
    """What a write to a capped file gets: refused or not, and the text the
    tool result carries, either the refusal or a note after the result."""

    refused: bool
    text: str | None


def admit(capped: CappedFile, after: str, before: str | None) -> Admission:
    """Whether a write taking ``capped`` from ``before`` to ``after`` goes
    through, and what its result says.

    A write that does not grow the file goes through even past the limit, so a
    file already over it, written before the limit existed, can be consolidated
    in steps rather than in one rewrite.
    """
    size = weighted_size(after)
    layout = _layout_note(capped, after)
    if size > capped.limit and (before is None or size > weighted_size(before)):
        refusal = (
            f"ERROR: {capped.name} would be {size:,}, past its limit of {capped.limit:,} "
            f"({_UNIT}). Nothing was written. Shorten it first: {capped.advice}."
        )
        return Admission(True, _join(refusal, layout))
    return Admission(False, _join(_size_note(capped, size), _line_note(capped, after), layout))


def visible(capped: CappedFile, text: str) -> str:
    """``text`` as the baseline injects it: whole up to the limit, else cut
    at the last line break before it, with a line saying the rest is hidden.

    Cut at a newline so the slice never lands mid-fence and tempts the model
    to "repair" content it did not write.
    """
    if weighted_size(text) <= capped.limit:
        return text
    end = weighted_prefix(text, capped.limit)
    cut = text.rfind("\n", 0, end)
    if cut <= 0:
        cut = end
    return (
        f"{text[:cut]}\n\n[... truncated: {capped.name} is past its size limit and "
        f"the rest is not shown. Shorten it before you add to it; a write that "
        f"grows it is refused.]"
    )


def _join(*notes: str | None) -> str | None:
    return "\n\n".join(n for n in notes if n) or None


def _size_note(capped: CappedFile, size: int) -> str | None:
    """Silent under the warn ratio: a file at 20% needs no nudge, and a note on
    every write would be read as noise by the tenth one."""
    if size < capped.budget * FILL_WARN_RATIO:
        return None
    if size > capped.limit:
        return (
            f"Note: {capped.name} is at {size:,}, past its limit of {capped.limit:,} "
            f"({_UNIT}). The part past the limit is not visible in your context "
            f"block, and a write that grows it is refused. Shorten it now: {capped.advice}."
        )
    if size > capped.budget:
        return (
            f"Note: {capped.name} is at {size:,}, over its budget of {capped.budget:,} "
            f"({_UNIT}). Shorten it now: {capped.advice}. A write that takes it past "
            f"{capped.limit:,} is refused."
        )
    percent = int(size * 100 / capped.budget)
    return (
        f"Note: {capped.name} is at {size:,} of {capped.budget:,} ({percent}% full; "
        f"{_UNIT}). Shorten it before adding more: {capped.advice}."
    )


def _line_note(capped: CappedFile, text: str) -> str | None:
    long_lines = [
        i for i, line in enumerate(text.splitlines(), 1) if weighted_size(line) > INDEX_LINE_LIMIT
    ]
    if not long_lines:
        return None
    shown = ", ".join(str(i) for i in long_lines[:5]) + (", ..." if len(long_lines) > 5 else "")
    return (
        f"Note: {len(long_lines)} line(s) of {capped.name} run past {INDEX_LINE_LIMIT} "
        f"(line {shown}). Each entry is one line; move the rest into the file it points at."
    )


_H2 = re.compile(r"^##[ \t]+(.+?)[ \t#]*\r?$", re.MULTILINE)
#: A fenced block, whose ``##`` lines are code (a shell comment), not headings.
_FENCE = re.compile(r"^(`{3,}|~{3,}).*?^\1", re.MULTILINE | re.DOTALL)


def _layout_note(capped: CappedFile, text: str) -> str | None:
    """The ask to move a notebook kept in another layout into its sections.

    Said here rather than in the static prompt, where a sentence about
    migrating would ride every turn long after the last notebook moved: the
    write result asks while the agent holds the file, and stops once it is done.
    """
    if capped.sections is None:
        return None
    if sorted(_H2.findall(_FENCE.sub("", text))) == sorted(capped.sections):
        return None
    headings = ", ".join(f"## {name}" for name in capped.sections)
    return (
        f"Note: {capped.name} is not in the layout Workspace Notes gives. Rewrite it "
        f"into {headings}, under those exact headings and no others, keeping only "
        f"what still matters: one line per entry, findings left in the deliverables."
    )
