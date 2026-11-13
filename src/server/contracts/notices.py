"""What the user is told of a run, as the types the feed and the push share.

The wording that fills them, and the decision whether a settle is news at
all, live in ``services/notices``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, get_args

from src.server.contracts.turn_kinds import RunOrigin

#: What a settle's news is about; a phone can switch each off.
PushKind = Literal["turn", "needs_input", "wakeup", "automation"]
PUSH_KINDS: tuple[PushKind, ...] = get_args(PushKind)

#: A background run's start notice, which only the desktop shows, besides.
NoticeKind = PushKind | Literal["started"]


@dataclass(frozen=True)
class Notice:
    kind: NoticeKind
    title: str
    body: str


@dataclass(frozen=True)
class SettleNews:
    """A settle judged news: what it says, and where the run came from."""

    notice: Notice
    origin: RunOrigin
