"""Fill ``thread_search_turns``: each settled turn's message and answer.

The answer is the main agent's committed text (``committed_texts``), the same
reading the Chief of Staff's ``agent_output`` takes, so tool output, code and
subagent work are left out by construction. A failed or cancelled turn keeps
whatever text it committed. The user's side is the turn's message, except for
a turn the server started (a report-back, a wakeup, a price watch), whose
message is a notice nobody typed.

A turn is indexed when its run settles (``schedule_search_index``), newest two
turns as the replay refresh reprojects them, since a turn's answered
interrupts reach its text only at the next turn's resume. That trigger lives in
this process and is lost with it; the sweep (``sweep.py``) finds what it
missed.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass

from src.server.contracts.turn_kinds import NOTIFICATION_QUERY_TYPES
from src.server.database.search import index as db
from src.server.utils.pg_sanitize import strip_pg_nul_str
from src.utils.concurrency import CoalescingRunner

logger = logging.getLogger(__name__)

# Per side, in characters (code points): enough for a long answer, and it
# bounds what a search's match and snippet read per row.
MAX_CHARS = 32_000


@dataclass(frozen=True)
class Indexed:
    #: Rows written or changed.
    written: int
    #: Turns whose answer exists but could not be read: their rows hold the
    #: message alone, marked for the sweep to read again.
    unread: int


def _clean(text: str | None) -> str:
    """The text as a TEXT column takes it: no NUL, and half a surrogate pair
    (which cannot be encoded) replaced, so one bad turn never fails the
    statement that writes its thread's turns."""
    text = strip_pg_nul_str(text) or ""
    text = text.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    return text[:MAX_CHARS]


async def index_turns(
    thread_id: str,
    *,
    turn_indexes: Collection[int] | None = None,
    newest: int | None = None,
) -> Indexed:
    """Index the thread's settled turns at ``turn_indexes``, or its ``newest``
    few. A database fault raises. A turn whose answer cannot be read is
    indexed with its message alone and counted ``unread``; its row keeps an
    answer read before."""
    from src.server.services.history.committed import read_committed_texts

    sources, read_at = await db.turns_to_index(
        thread_id, turn_indexes=turn_indexes, newest=newest
    )
    if not sources:
        return Indexed(0, 0)
    # Never the whole-thread claims pass: a turn waiting on it is read from
    # its checkpoints, and the sweep that calls this paces only its own reads.
    read = await read_committed_texts(
        thread_id,
        {s.response_id: s.turn_index for s in sources},
        schedule_claims_pass=False,
    )
    written = await db.write_turns(
        thread_id,
        [
            db.IndexedTurn(
                response_id=s.response_id,
                user_text=""
                if s.query_type in NOTIFICATION_QUERY_TYPES
                else _clean(s.user_text),
                answer_text=_clean(read.texts.get(s.response_id)),
                answer_read=s.response_id not in read.unread,
                turn_at=s.turn_at,
            )
            for s in sources
        ],
        read_at=read_at,
    )
    return Indexed(written, len(read.unread))


async def _index_newest(thread_id: str, _: None) -> None:
    from src.server.services.history.replay.refresh import REFRESHES

    try:
        # The refresh scheduled beside this trigger projects the same turns;
        # reading after it finds their slices stored instead of projecting
        # them a second time.
        await REFRESHES.wait(thread_id)
        await index_turns(thread_id, newest=2)
    except Exception:
        logger.warning(
            "[SEARCH] indexing failed for thread %s", thread_id, exc_info=True
        )


# Execution context only: coalesces this process's turn-end triggers per
# thread. A trigger landing on another worker, or lost with this one, is
# found by the sweep.
_INDEXING: CoalescingRunner[None] = CoalescingRunner(_index_newest, lambda: None)


def schedule_search_index(thread_id: str) -> None:
    """Fire-and-forget: index the thread's newest two turns. At most one
    runner per thread; a trigger landing while it runs folds into one more
    pass."""
    _INDEXING.schedule(thread_id)
