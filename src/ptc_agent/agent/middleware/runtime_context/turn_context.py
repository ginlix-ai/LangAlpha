"""What a turn knows about itself, carried whole from the request to the stack.

These values are read when the turn opens, mostly off its request, and are used
at the turn boundary: the anchor row, and the zones its clock is stamped in and
its tools read. Threading them one by one made every builder between the
handler and the middleware restate a list it has no other interest in, and
adding one meant editing every signature between them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from datetime import datetime


@dataclass(frozen=True, slots=True)
class HomeAway:
    """A Home turn running in Home's stand-in rather than on Home's own
    computer. The thread, its admission and its records stay Home's."""

    #: The stand-in's workspace, whose computer and folder the turn runs in.
    workspace_id: str
    #: The computer Home lives on.
    computer_id: str
    computer_name: str
    #: The cloud computer the stand-in is on.
    cloud_computer_id: str
    cloud_name: str
    #: When the turn was admitted, ISO 8601: a file card of the turn finds the
    #: hand-back that carried its file by it (the first one after).
    at: str = ""
    #: ``unreachable`` (Home's computer can't be reached) or ``command`` (the
    #: turn reports on a background command that ran in the stand-in, whose log
    #: and files are there, though Home's computer is back).
    reason: str = "unreachable"

    def as_metadata(self) -> dict[str, str]:
        """What the run and query rows, and the client, record of it."""
        return {
            "computer_id": self.computer_id,
            "computer_name": self.computer_name,
            "cloud_computer_id": self.cloud_computer_id,
            "cloud_name": self.cloud_name,
            "at": self.at,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ForkNotice:
    """A forked thread's first turn: the history before it was copied from
    another thread, whose live work stays there."""

    #: Finished subagent tasks came with the copy, resumable by their ids.
    tasks_copied: bool
    #: The source's scratchpad files were copied into this thread's folder.
    scratchpad_copied: bool


@dataclass(frozen=True, slots=True)
class TurnContext:
    """Per-turn runtime context for the turn anchor row, and the previous
    run's settle time for compaction's cache clock. Every field is optional.

    A context-free build (thread maintenance, a subagent stack, a test) passes
    no context at all, which is the same thing as passing one whose fields are
    all None: the row states the market unconditionally and drops the gap line.
    """

    last_turn_at: datetime | None = None
    # When the thread's previous run settled, by the run ledger, which a call
    # that was cancelled or failed still counts toward: it refreshed the
    # provider's prompt cache. Read by the main agent's compaction to judge
    # whether that cache has expired at this turn's start.
    previous_run_settled_at: datetime | None = None
    # The send asked to keep its whole context: no summary for a cold cache
    # alone at this turn's start (see ``compaction.rebuild``).
    keep_full_context: bool = False
    # When the server received the turn's send (epoch seconds), when that
    # summary is judged; None at the turn start (see ``compaction.rebuild``).
    sent_at: float | None = None
    platform: str | None = None
    origin: str | None = None
    # The agent that sent an agent-origin turn (``chief_of_staff``),
    # which the turn row names as acting for the user.
    origin_agent: str | None = None
    surface_rules: str | None = None
    # A turn nobody sent (the harness reporting finished background work) runs
    # under the last rules stated, the way a resumed interrupt does, rather
    # than taking back the rules of the conversation it reports into.
    inherits_rules: bool = False
    # Nobody is watching the turn as it runs (an automation's, a wake, a
    # report-back the server posts, a hand-off), so nobody can wake a machine
    # that stops answering: it ends on the first tool call that finds so.
    unwatched: bool = False
    # Set only once the shared disk is low enough to change what the agent
    # should do; None states nothing. A subagent's stack never carries it.
    disk_free_mb: int | None = None
    # Whether a current reading backs disk_free_mb being None; only then may a
    # turn take back an earlier low-disk line.
    disk_known: bool = False
    # A Home turn in Home's stand-in on a cloud computer, rather than on the
    # machine Home lives on. None on every other turn.
    home_away: HomeAway | None = None
    # On a Home turn back on Home's own computer: that computer's name, and
    # the last hand-back of what the stand-in's turns wrote (``folder``,
    # ``files``), or ``pending`` while it is still owed. Stated only after a
    # row that said Home was away.
    home_back: dict[str, Any] | None = None
    # On a forked thread's first turn (``services.forks``), what the fork
    # carried. None on every other turn.
    forked: ForkNotice | None = None
    # The zone the profile or the request names. The stamp, the identity block
    # and a subagent's row state it, so "9am tomorrow" means the same thing to
    # the model and a tool. None when neither names one: the stamp then keeps
    # the frozen identity's zone, never a locale default.
    timezone: str | None = None
    # The zone the turn's tools read a local time in, and the run records:
    # ``timezone``, else the request locale's default, since a tool needs a
    # clock even when nothing named one.
    tool_timezone: str = "UTC"
