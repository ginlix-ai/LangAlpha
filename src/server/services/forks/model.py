"""What a fork is asked for: where to cut, what kind of fork, and what it records.

A fork is three steps, and every feature built on it shares the first two:

1. Snapshot (``snapshot.py``): resolve a ``Cut`` against the source thread's
   current branch to one checkpoint and the turns up to it.
2. Materialize (``service.py``): copy that history into a new thread.
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


class ForkLineage(BaseModel):
    """Where a fork came from, kept in its thread's ``metadata.fork``. Its
    first turn reads it to carry what the copy could not (``carry.py``)."""

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

    @classmethod
    def of(cls, metadata: Optional[Mapping[str, Any]]) -> Optional[ForkLineage]:
        raw = (metadata or {}).get(FORK_META)
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
