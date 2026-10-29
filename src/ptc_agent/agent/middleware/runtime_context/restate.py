"""Rows that state sections of the baseline block whole.

A block Claude's thinking is bound to stands even when a rebuild is due, since
rebuilding it would invalidate every thinking block after it. What the rebuild
would have said goes into a row instead, which states whole the sections a
delta against the frozen block cannot describe. ``render`` gives each section
of an epoch's block, keyed as ``SECTION_*`` and by source kind.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.middleware.runtime_context.changes import (
    UPDATE_SCHEMA_VERSION,
    sha256_text,
)
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.harness_blocks import (
    harness_block_for,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.runtime_context.epoch import (
        BaselineEpoch,
        ObservationCursor,
        Observations,
    )

# The sections of the block that are not a read source, which renders key by
# its kind. Keyed so a row can state one section of the block whole.
SECTION_WORKSPACE = "workspace"
SECTION_PROFILE = "user_profile"
SECTION_IDENTITY = "identity"
SECTION_FILES = "files"
SECTION_MEMO = "memo"

#: The row that states sections of the block whole, where the frozen block
#: stands but a delta against it cannot say what moved.
RESTATED_ROW_KIND = "baseline_restated"

# The cursor's mark for a source the frozen block holds and this build no
# longer reads, once a row has said so. Never a content hash.
_GONE = ""

#: The block a change row of each non-harness kind speaks for, as the retiring
#: row names it. The harness kinds name their block from ``HARNESS_BLOCKS``.
_ROW_BLOCKS: dict[str, str] = {
    "agent_md_changed": "<agentmd>",
    "memo_changed": "the memo count",
    "profile_changed": "<user_profile> and <user_identity>",
    "workspace_changed": "<workspace>",
}


def row_block(kind: str) -> str:
    # The two memory tiers are two blocks with one tag, and a rebuild can fold
    # one while the other read fails, so the exception has to name the tier.
    if kind.startswith("memory_changed:"):
        return f"<memory> for the {kind.split(':', 1)[1]} tier"
    harness = harness_block_for(kind)
    if harness is not None:
        return harness.label
    return _ROW_BLOCKS.get(kind, kind)


def restate(
    epoch: BaselineEpoch,
    rebuilt: BaselineEpoch,
    obs: Observations,
    render: Callable[[BaselineEpoch], dict[str, str]],
) -> tuple[BaselineEpoch, list[DurableUpdate]]:
    """The epoch after a summary the block's thinking survives, and the row restating it.

    The block stands, because the kept turns' thinking was produced in front
    of it. The rows that said what moved since it froze may have gone with
    the summarized turns, so one row states whole every section that no longer
    matches its frozen copy, as ``rebuilt`` (this turn's reads frozen) renders
    it, and the cursor moves to what that row says. A source whose read did
    not answer keeps the frozen copy in the row and is measured from it next
    turn, as a rebuild carries it.
    """
    now, then = render(rebuilt), render(epoch)
    sections = [text for key, text in now.items() if then.get(key) != text]
    gone = [key for key in then if key not in now]
    observed = dict(rebuilt.cursor.observed)
    if obs.workspace_available:
        observed["workspace"] = obs.workspace.sha()
    if obs.memo_configured and obs.memo_count is not None:
        observed["memo"] = str(obs.memo_count)
    for kind in epoch.source_kinds() - {read.kind for read in obs.reads}:
        observed[kind] = _GONE
    restated = replace(
        epoch,
        compaction_seen=obs.compaction,
        cursor=replace(rebuilt.cursor, observed=observed),
    )
    if not sections and not gone and not obs.compaction_note:
        return restated, []
    return restated, [restated_row(obs, sections, gone, after_summary=True)]


def stated_sources(
    epoch: BaselineEpoch,
    rebuilt: BaselineEpoch,
    obs: Observations,
    cursor: ObservationCursor,
    render: Callable[[BaselineEpoch], dict[str, str]],
) -> tuple[DurableUpdate | None, ObservationCursor]:
    """The row for sources the frozen block cannot be measured against, and the cursor after it.

    A source with no frozen copy (a deploy added it, or its read failed as the
    epoch froze) has no delta to file, and one this build no longer reads has
    nothing left to measure. A rebuild would take the first in and drop the
    second; with the block standing, the row states each added section whole
    and names the dropped ones, once: the cursor keeps the hash it stated, so
    only a later change states a section again.
    """
    observed = cursor.observed
    added = {
        read.kind: sha
        for read in obs.reads
        if read.available
        and epoch.source_entry(read.kind) is None
        and observed.get(read.kind) != (sha := sha256_text(read.content))
    }
    dropped = sorted(
        kind
        for kind in epoch.source_kinds() - {read.kind for read in obs.reads}
        if observed.get(kind) != _GONE
    )
    if not added and not dropped:
        return None, cursor
    now, then = render(rebuilt), render(epoch)
    sections = [
        text
        for key, text in now.items()
        if key in added or (key == SECTION_FILES and key not in then and added)
    ]
    stated = {**observed, **added, **dict.fromkeys(dropped, _GONE)}
    return (
        restated_row(obs, sections, dropped, after_summary=False),
        replace(cursor, observed=stated),
    )


def restated_row(
    obs: Observations, sections: list[str], gone: list[str], *, after_summary: bool
) -> DurableUpdate:
    """The row stating ``sections`` of the block whole, and naming the ``gone`` ones.

    After a summary it also carries the summary's note, which has to reach the
    turns after it without sitting between the summary and the turns it kept.
    """
    provenance: dict[str, Any] = {
        "source": "harness",
        "after": "summary" if after_summary else "source",
    }
    labels = [label for key in gone if (label := _section_label(key)) is not None]
    if labels:
        provenance["gone"] = labels
    if after_summary and obs.compaction_note:
        provenance["note"] = obs.compaction_note
    if after_summary and obs.compaction_note_sent:
        provenance["note_sent"] = True
    return DurableUpdate(
        kind=RESTATED_ROW_KIND,
        schema_version=UPDATE_SCHEMA_VERSION,
        text="\n\n".join(sections),
        provenance=provenance,
        created_at=obs.now,
    )


def summary_note_row(note: str, summary_id: str | None, now: datetime) -> DurableUpdate:
    """The row taking a summary's note to the call that answers from it.

    Claude's block is sent alone, so the note follows the turns it kept; sent
    with that call and kept in history ahead of its answer, it reaches the
    model at once and is sent the same way on every later call. It reads as
    the restatement row a summary with nothing to restate files.
    """
    return DurableUpdate(
        kind=RESTATED_ROW_KIND,
        schema_version=UPDATE_SCHEMA_VERSION,
        text="",
        provenance={"source": "harness", "after": "summary", "note": note, "summary": summary_id},
        created_at=now,
    )


def note_sent(messages: Sequence[Any], summary_id: str | None) -> bool:
    """Whether ``messages`` already carry the note of the summary ``summary_id``."""
    if summary_id is None:
        return False
    return any(
        (update := runtime_update_from_message(message)) is not None
        and update.kind == RESTATED_ROW_KIND
        and update.provenance.get("summary") == summary_id
        for message in messages
    )


def _section_label(key: str) -> str | None:
    """The block a section is, as a row names it; None for the files preface,
    which governs the file blocks rather than saying anything itself."""
    if key == SECTION_FILES:
        return None
    if key == "agent_md":
        return row_block("agent_md_changed")
    if key.startswith("memory:"):
        return row_block(f"memory_changed:{key.split(':', 1)[1]}")
    harness = harness_block_for(f"{key}_changed")
    if harness is not None:
        return harness.label
    return {
        SECTION_WORKSPACE: "<workspace>",
        SECTION_PROFILE: "<user_profile>",
        SECTION_IDENTITY: "<user_identity>",
        SECTION_MEMO: "<memo-index>",
    }.get(key, key)
