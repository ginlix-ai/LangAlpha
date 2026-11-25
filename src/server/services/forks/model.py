"""What a fork is asked for: where to cut, what kind of fork, and what it records.

A fork is three steps, and every feature built on it shares the first two:

1. Snapshot (``snapshot.py``): resolve a ``Cut`` against the source thread's
   current branch to one checkpoint and the turns up to it.
2. Materialize (``service.py``): write the agent's state at that checkpoint
   into a new thread as one checkpoint. The turns before it stay the
   source's: the fork shows them from there (``BorrowedSegment``) and holds
   none of their rows.
3. Kind: what the copy is for. The kind decides which cuts it accepts and what
   it carries beyond the conversation; a new kind of fork (a side chat beside a
   running turn, a hidden reflection run) is a new ``ForkKind`` and, where it
   needs one, a new cut, not a new copy path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Optional, Union

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from ptc_agent.agent.transcript.identity import DIGEST_VERSION, Window
from src.server.services.history.branch import stale_fork_detail

logger = logging.getLogger(__name__)

CutName = Literal["through_turn", "before_turn"]


@dataclass(frozen=True)
class ThroughTurn:
    """Everything up to and including turn ``turn_index``'s reply."""

    turn_index: int
    name: CutName = "through_turn"


@dataclass(frozen=True)
class BeforeTurn:
    """Everything before turn ``turn_index``, whose message the caller sends
    again, changed or not, as the fork's first turn."""

    turn_index: int
    name: CutName = "before_turn"


Cut = Union[ThroughTurn, BeforeTurn]
CUTS: dict[str, type[ThroughTurn] | type[BeforeTurn]] = {
    "through_turn": ThroughTurn,
    "before_turn": BeforeTurn,
}


@dataclass(frozen=True)
class ForkKind:
    """One kind of fork: the cuts it accepts and what it carries besides the
    conversation, which every kind gets."""

    name: str
    cuts: tuple[type, ...]
    #: Copy the finished background task runs the copied turns launched, so
    #: the forked agent can resume a task its history names.
    copy_task_runs: bool
    #: Seed the fork's own scratchpad folder with the source's at its first turn.
    copy_scratchpad: bool


#: A thread of its own that the user keeps working in, beside the source.
DURABLE = ForkKind(
    name="durable",
    cuts=(ThroughTurn, BeforeTurn),
    copy_task_runs=True,
    copy_scratchpad=True,
)


@dataclass(frozen=True)
class ForkRequest:
    source_thread_id: str
    cut: Cut
    #: Who asks: the fork is theirs, and they must be able to read the source.
    user_id: str
    kind: ForkKind = DURABLE
    #: The fork's title; the source's when None.
    title: Optional[str] = None
    #: The surface the fork is made from ("web", a channel), as a thread records it.
    platform: Optional[str] = None


#: The key in a fork's thread metadata that records where it came from.
FORK_META = "fork"


class BorrowedSegment(BaseModel):
    """Turns a fork shows from the thread that holds them rather than holding
    them itself. A fork of a fork lists its source's segments before its own,
    so each is read from the thread that made it and deleting a middle fork
    hides only that fork's turns."""

    model_config = ConfigDict(frozen=True)

    thread_id: str
    #: The first and last turn shown from it, inclusive.
    first_turn: int
    through_turn: int
    #: The response that ended ``through_turn`` when the fork was made. An
    #: edit or a regenerate there deletes it.
    response_id: str
    #: The newest run the fork read of the thread (``run_seq``). A response
    #: past it on a turn shown is not the one the fork saw: an edit or a
    #: regenerate wrote it, or a retry, which keeps the attempt before it.
    run_horizon: int
    #: The last transcript file (``turn-NNNN``) of the turns shown from it;
    #: its first is the previous segment's last plus one, or 1.
    runs_through: int


class ForkHead(BaseModel):
    """The transcript window at the cut (``transcript.identity.Window``)
    taken over every message there, the source's trimmed ones and the ones
    its checkpoint still held: ``runs`` is the last transcript file the fork
    borrows, and its own files number from ``runs + 1``. Empty for a fork of
    a thread with no history."""

    model_config = ConfigDict(frozen=True)

    runs: int = 0
    messages: int = 0
    digest: str = ""
    version: int = DIGEST_VERSION

    @classmethod
    def of(cls, window: Window) -> ForkHead:
        return cls(
            runs=window.runs, messages=window.messages, digest=window.digest, version=window.version
        )

    def window(self) -> Window:
        """As a ``Window``, which the fork's trims and transcript count on from."""
        return Window(self.runs, self.messages, self.digest, {}, self.version)


#: What a refused edit, regenerate or rating of a borrowed turn says.
CHANGE_BORROWED = (
    "This message is from the original chat, so it can't be changed here. "
    "Open the original to change it."
)
#: What a refused fork at a borrowed turn says.
FORK_BORROWED = (
    "This message is from the original chat, so a new chat can't start from "
    "it here. Open the original to start one from there."
)


class Borrowing(BaseModel):
    """How a snapshot fork stands on the threads it was made from. It holds
    one checkpoint, the state at the cut, and no rows for the turns before
    ``turn_base``, which it shows from ``ancestors`` and refuses to edit,
    regenerate, rate or fork (``require_own``)."""

    model_config = ConfigDict(frozen=True)

    #: The first turn the fork holds; every turn before it is borrowed.
    turn_base: int
    #: The fork's one checkpoint, the source's state at the cut; None when
    #: the source had no history.
    checkpoint_id: Optional[str]
    #: The borrowed turns, oldest first, contiguous up to ``turn_base``.
    ancestors: tuple[BorrowedSegment, ...] = ()
    #: The transcript window at the cut.
    head: ForkHead = ForkHead()

    def owns(self, turn_index: int) -> bool:
        return turn_index >= self.turn_base

    def require_own(self, turn_index: int, message: str = CHANGE_BORROWED) -> None:
        """Refuse ``turn_index`` when it is borrowed: the fork holds no rows
        or state there to change."""
        if not self.owns(turn_index):
            raise BorrowedTurn(message)


class ForkLineage(BaseModel):
    """Where a fork came from, kept in its thread's ``metadata.fork``. Its
    first turn reads it to carry what the copy could not (``carry.py``).

    A fork with ``borrow`` is a snapshot fork (``Borrowing``). One without was
    made by copying the source's whole history, and holds every turn itself."""

    model_config = ConfigDict(frozen=True)

    kind: str
    #: The source thread.
    thread_id: str
    cut: CutName
    turn_index: int
    #: The checkpoint the fork starts from; None for a fork with no history.
    checkpoint_id: Optional[str]
    copy_scratchpad: bool
    #: How many finished subagent tasks came with the copy.
    tasks: int
    #: The title the fork was made with, which its first message retitles from.
    title: Optional[str]
    at: datetime
    by: str
    #: When the fork's first turn carried the scratchpad and stated the
    #: notice; None until then (``carry.py``).
    carried_at: Optional[datetime] = None
    #: A snapshot fork's standing on its sources; None for a copied fork.
    borrow: Optional[Borrowing] = None

    @classmethod
    def of(cls, metadata: Optional[Mapping[str, Any]]) -> Optional[ForkLineage]:
        return cls.parse((metadata or {}).get(FORK_META))

    @classmethod
    def parse(cls, raw: Any) -> Optional[ForkLineage]:
        """The lineage from its stored value (``metadata->'fork'``)."""
        if raw is None:
            return None
        try:
            return cls.model_validate(raw)
        except ValidationError:
            logger.warning("[fork] unreadable lineage %r", raw)
            return None

    def as_metadata(self) -> dict[str, Any]:
        return {FORK_META: self.model_dump(mode="json")}


class ForkRefused(HTTPException):
    """A fork that cannot be made, with the code and message a client shows.
    Messages describe what happened in the user's terms, never the mechanics
    behind it."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(status_code=status, detail={"code": code, "message": message})


def stale_refusal() -> ForkRefused:
    return ForkRefused(409, **stale_fork_detail())


class BorrowedTurn(ForkRefused):
    """A turn a snapshot fork shows from the chat it was made from, which it
    cannot change or fork (``Borrowing.require_own``)."""

    def __init__(self, message: str = CHANGE_BORROWED):
        super().__init__(409, "borrowed_turn", message)
