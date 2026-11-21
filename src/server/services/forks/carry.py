"""What a fork's first turn carries over that the copy could not.

The copy is made on the server, where no computer need be awake, so what
lives on the computer (the scratchpad folder) goes at the fork's first turn,
which has the computer. That turn's row also tells the agent it is in a fork,
and the fork is titled after its first message.

The fork's first turn, whatever it starts from (the cut, an edit of a copied
turn, or a regenerate of a copied reply), claims the carry once by stamping ``carried_at``
in the lineage, so a later edit back into the copied turns states and copies
nothing again, and work done to the thread before any message (a model
switch's hand-off, a compaction) does not hide its first turn. A first turn
that fails after the claim keeps what it copied but not its notice. The title
is apart from the claim: the first turn with words in it retitles the fork, a
regenerate before it having none to title by.
"""

from __future__ import annotations

import dataclasses
import logging
import shlex
from dataclasses import dataclass
from typing import Any, Optional

from ptc_agent.agent.middleware.runtime_context.turn_context import ForkNotice, TurnContext
from ptc_agent.core.paths import WorkspaceLayout
from src.server.database import thread_fork
from src.server.services.forks.model import ForkLineage
from src.server.utils.checkpoint_helpers import FORK_ROOT_CHECKPOINT_ID

logger = logging.getLogger(__name__)

#: The copy script's exit when the source has no scratchpad folder.
_NOTHING_TO_COPY = 3
#: The first token waits on the copy; a folder slower than this to copy
#: starts the fork without it.
_COPY_TIMEOUT_S = 20


@dataclass(frozen=True)
class ForkTurnStart:
    """What a fork's turn still owes it: the carry, on its first turn, and the
    title, until a message retitles it. Nothing on any other thread."""

    lineage: Optional[ForkLineage] = None
    #: The carry is unclaimed: any turn but a resume, a new message or a
    #: regenerate of a copied reply.
    carry: bool = False
    #: The turn starts on history the copy made, which the notice speaks of;
    #: false for a fork with none and for an edit of its first turn.
    on_copied_history: bool = False
    #: The fork still has the title it was made with.
    retitle: bool = False

    @classmethod
    def read(cls, lineage: Optional[ForkLineage], request: Any, title: Optional[str]) -> ForkTurnStart:
        if lineage is None or request.hitl_response:
            return cls()
        return cls(
            lineage,
            carry=lineage.carried_at is None,
            on_copied_history=lineage.checkpoint_id is not None
            and request.checkpoint_id != FORK_ROOT_CHECKPOINT_ID,
            retitle=title == lineage.title,
        )

    async def begin(
        self,
        turn_context: TurnContext,
        *,
        sandbox: Any,
        layout: Optional[WorkspaceLayout],
        scratchpad: bool,
        thread_id: str,
        user_id: str,
        message: str,
        timezone: Optional[str],
    ) -> TurnContext:
        """Retitle the fork, claim the carry, seed the scratchpad (the user's
        ``scratchpad`` feature on, and the source still theirs to read) and
        return the turn context with the notice stating what was carried.
        Never fails the turn."""
        lineage = self.lineage
        if lineage is None:
            return turn_context
        if self.retitle:
            _retitle(lineage, thread_id=thread_id, user_id=user_id, message=message, timezone=timezone)
        if not self.carry or not await _claim(thread_id):
            return turn_context
        if not self.on_copied_history:
            return turn_context
        copied = bool(
            scratchpad
            and lineage.copy_scratchpad
            and sandbox is not None
            and layout is not None
            and await _can_read(lineage.thread_id, user_id)
            and await _copy_scratchpad(sandbox, layout, lineage.thread_id, thread_id)
        )
        notice = ForkNotice(tasks_copied=lineage.tasks > 0, scratchpad_copied=copied)
        return dataclasses.replace(turn_context, forked=notice)


async def _claim(thread_id: str) -> bool:
    try:
        return await thread_fork.claim_carry(thread_id)
    except Exception:
        logger.warning("[fork] carry claim failed thread=%s", thread_id, exc_info=True)
        return False


async def _can_read(source_id: str, user_id: str) -> bool:
    """Whether the source is still the sender's to read: in a shared
    workspace its starter may have made it private since the fork."""
    from src.server.services.shared_workspaces.access import thread_access

    try:
        await thread_access(source_id, user_id)
        return True
    except Exception:
        logger.info("[fork] source no longer readable, scratchpad not copied source=%s", source_id)
        return False


async def _copy_scratchpad(sandbox: Any, layout: WorkspaceLayout, source_id: str, thread_id: str) -> bool:
    """Copy the source's scratchpad folder into the fork's, keeping any file
    the fork's already has. True when there was a folder and it was copied."""
    if source_id[:8] == thread_id[:8]:
        return False
    src = shlex.quote(layout.join(WorkspaceLayout.scratchpad_subdir(source_id[:8])))
    dst = shlex.quote(layout.join(WorkspaceLayout.scratchpad_subdir(thread_id[:8])))
    script = f"[ -d {src} ] || exit {_NOTHING_TO_COPY}; mkdir -p {dst} && cp -Rpn {src}/. {dst}/"
    try:
        result = await sandbox.execute_bash_command(script, timeout=_COPY_TIMEOUT_S)
    except Exception:
        logger.warning("[fork] scratchpad copy failed thread=%s", thread_id, exc_info=True)
        return False
    code = (result or {}).get("exit_code")
    if code not in (0, _NOTHING_TO_COPY):
        logger.warning("[fork] scratchpad copy failed thread=%s result=%s", thread_id, result)
    return code == 0


def _retitle(lineage: ForkLineage, *, thread_id: str, user_id: str, message: str, timezone: Optional[str]) -> None:
    """Title the fork after its first message, as a new thread is titled
    after its first. Swapped only against the title the fork was made with,
    so a rename by the user, or an earlier retitle, wins."""
    if not message.strip():
        return
    from src.server.services.thread_title import schedule_title_generation

    schedule_title_generation(
        thread_id=thread_id,
        user_id=user_id,
        first_query=message,
        expected_title=lineage.title,
        timezone=timezone,
    )
