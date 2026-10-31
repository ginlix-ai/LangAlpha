"""When a summary rebuilds a pinned thread's system prompt.

A due thread (see ``frozen_prompt.due``) takes the new prompt only through a
summary that keeps no turns, cut where nothing after it holds thinking: at a
turn start, or between turns. Under its threshold that summary would run only
for this, so the main agent forces it only where it costs little: the prompt
changed by a deploy, never by a per-turn input that may flip back, the context
is at least ``floor_tokens``, and the provider's prompt cache has expired, so
the turn's first call would write the whole context again in any case.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

from langchain.agents.middleware.types import ModelRequest
from langchain_core.messages import AIMessage, AnyMessage

from src.llms.cache_markers import route_ttl_seconds, written_ttl_seconds

from ptc_agent.agent.middleware.compaction.utils import settled_end, turn_start_cut
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import (
    due,
    rebuild_pending,
    sources_changed,
)


def _claude(message: Any) -> bool:
    return isinstance(message, AIMessage) and "claude" in str(
        (message.response_metadata or {}).get("model_name") or ""
    ).lower()


def cache_ttl(view: Sequence[AnyMessage], model: Any) -> int:
    """The life in seconds of the cache entries the thread's calls write: as
    the newest Claude answer in ``view`` that wrote any reports them
    (``written_ttl_seconds``); with none recorded, what the route asks for
    (``route_ttl_seconds``)."""
    for message in reversed(view):
        if _claude(message) and (ttl := written_ttl_seconds(message.usage_metadata)):
            return ttl
    return route_ttl_seconds(model)


@dataclass(frozen=True)
class RebuildPlan:
    """What a due prompt asks of one model call.

    Attributes:
        due: The thread's kept prompt is one the build would not send.
        cut: Where a summary keeps no turns, so the call after it takes the
            new prompt; 0 keeps the tail.
        reason: Logged as ``rebuild=yes`` once a summary at ``cut`` lands.
        forced: Summarize at ``cut`` even under the threshold.
        waits: Why a due turn start under the threshold does not, logged
            once its call lands.
        cold: The thread's prompt cache has expired.
    """

    due: bool = False
    cut: int = 0
    reason: str = ""
    forced: bool = False
    waits: str | None = None
    cold: bool = False


class StalePinRebuilds:
    """A subagent's rebuilds: at a turn start, once its pin is stale. Its run
    is bounded, and its pin is never forced."""

    #: Whether ``plan`` reads ``first_call``, which a run's start then records
    #: (see ``CompactionMiddleware.abefore_agent``).
    reads_first_call: ClassVar[bool] = False

    def due(self, request: ModelRequest, view: Sequence[AnyMessage]) -> bool:
        return rebuild_pending(request.state)

    def plan(
        self,
        request: ModelRequest,
        view: Sequence[AnyMessage],
        tokens: int,
        threshold: int,
        now: float,
        *,
        first_call: bool,
    ) -> RebuildPlan:
        """What a call of a turn does for a due prompt. ``first_call`` says the
        call is its run's first (see ``turn_start_cut``)."""
        if not self.due(request, view):
            return RebuildPlan()
        return RebuildPlan(due=True, cut=turn_start_cut(view), reason="turn_start")

    def between_turns(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> RebuildPlan:
        """What a manual compaction does for a due prompt: all of the view, once
        its last step is complete; until then, the tail stays."""
        if not self.due(request, view):
            return RebuildPlan()
        cut = settled_end(view)
        if not cut:
            return RebuildPlan(due=True, waits="unanswered_tool_call")
        return RebuildPlan(due=True, cut=cut, reason="manual", cold=self._cold(request, view, now))

    def _cold(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> bool:
        return False


@dataclass(frozen=True)
class TurnStartRebuilds(StalePinRebuilds):
    """The main agent's rebuilds: due on the pin's digest, at a run's first
    call after a turn that ended on tool results too, and forced under the
    threshold as above.

    Attributes:
        floor_tokens: The least context a deploy's rebuild is forced at
            under the threshold; 0 never forces one.
        settled_at: When the thread's previous run settled, by the run
            ledger, a naive one read as UTC; None when it has none.
    """

    reads_first_call: ClassVar[bool] = True

    floor_tokens: int
    settled_at: datetime | None = None

    def due(self, request: ModelRequest, view: Sequence[AnyMessage]) -> bool:
        return due(request, view)

    def plan(
        self,
        request: ModelRequest,
        view: Sequence[AnyMessage],
        tokens: int,
        threshold: int,
        now: float,
        *,
        first_call: bool,
    ) -> RebuildPlan:
        if not self.due(request, view):
            return RebuildPlan()
        cut = turn_start_cut(view, after_tools=first_call)
        if not cut:
            return RebuildPlan(due=True)
        cold = self._cold(request, view, now)
        if tokens >= threshold:
            return RebuildPlan(due=True, cut=cut, reason="digest", cold=cold)
        waits = self._waits(request, view, tokens, cold=cold)
        if waits is None:
            return RebuildPlan(due=True, cut=cut, reason="deploy_cold", forced=True, cold=cold)
        return RebuildPlan(due=True, cut=cut, reason="digest", waits=waits, cold=cold)

    def _waits(self, request: ModelRequest, view: Sequence[AnyMessage], tokens: int, *, cold: bool) -> str | None:
        """Why a due turn start under the threshold is not forced, or None.

        A thread whose last answer came from a fallback of another provider
        waits: the pin only follows Claude, so a summary that a fallback
        answers after would leave the thread due, and force again next turn.
        """
        if not _claude(next((m for m in reversed(view) if isinstance(m, AIMessage)), None)):
            return "fallback"
        if not sources_changed(request.state):
            return "inputs_only"
        if not self.floor_tokens:
            return "off"
        if tokens < self.floor_tokens:
            return "under_floor"
        if not cold:
            return "warm"
        return None

    def _cold(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> bool:
        """Whether the thread's prompt cache has expired: longer than its ttl since the later of the last answer compaction stamped and
        the previous run's settling. Both are read at a response's end while
        an entry's life counts from its request's start, so the estimate errs
        towards warm: it skips a rebuild it could have taken rather than force
        one on a warm cache."""
        settled = self.settled_at
        settled_ts = settled.replace(tzinfo=settled.tzinfo or UTC).timestamp() if settled else 0.0
        last = max(request.state.get("_last_model_response_at") or 0.0, settled_ts)
        return now - last > cache_ttl(view, request.model)
