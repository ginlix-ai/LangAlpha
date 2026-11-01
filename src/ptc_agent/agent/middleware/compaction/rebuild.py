"""When a summary rebuilds a pinned thread's system prompt.

A due thread (see ``frozen_prompt.due``) takes the new prompt only through a
summary that keeps no turns, cut where nothing after it holds thinking: at a
turn start, or between turns. Under its threshold that summary would run only
for this, so the main agent forces it only where it costs little: the prompt
changed by a deploy, never by a per-turn input that may flip back, the context
is at least ``floor_tokens``, and the provider's prompt cache has expired, so
the turn's first call would write the whole context again in any case. A
thread pinned below the build's ``rebuild_before`` is forced whatever its
size or cache, and a build older than the pin, a rollback, forces nothing.

In the middle of a turn, the main agent's summary on a due thread keeps no
turn either: it ends right after the step under way, whose calls and results
the call after it is sent as a row (see ``carry``). With notes on, the
compaction that would run it first asks for one more step, on the notes, and
arms the thread, so the next call summarizes whatever its size: mid-turn, or
at the turn start when that step was the turn's answer. Any call after it
spends the arm (``CompactionMiddleware._answered``), and so does a summary in
between.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

from langchain.agents.middleware.types import ModelRequest
from langchain_core.messages import AIMessage, AnyMessage

from src.llms.cache_markers import CacheScope, route_ttl_seconds, written_ttl_seconds

from ptc_agent.agent.middleware.compaction.notes import summary_id
from ptc_agent.agent.middleware.compaction.types import RebuildArm
from ptc_agent.agent.middleware.compaction.utils import settled_end, step_cut, turn_start_cut
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import (
    STATE_SYSTEM_PIN,
    due,
    rebuild_pending,
    sources_changed,
)
from ptc_agent.agent.middleware.runtime_context.state import as_dict, state_get
from ptc_agent.agent.prompts import release


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


def arm(state: Any) -> RebuildArm:
    """The arm a call that asks for the notes ahead of a summary that keeps no
    turn stores, so the next call writes that summary (``_armed``)."""
    return RebuildArm(summary=summary_id(state))


def _armed(state: Any) -> bool:
    """Whether the call before this one asked for the notes ahead of a summary
    that keeps no turn, under the summary that still stands; another summary
    since, a manual compaction, already rebuilt the thread or kept its tail."""
    stored = state.get("_rebuild_armed")
    return isinstance(stored, dict) and stored.get("summary") == summary_id(state)


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
        scope: The cache markers of the summary's own native call:
            ``last_tool`` once the main agent's prompt cache has expired.
        carry: ``cut`` ends mid-turn, after the results of the step under
            way, which the call after the summary is sent as a row.
    """

    due: bool = False
    cut: int = 0
    reason: str = ""
    forced: bool = False
    waits: str | None = None
    scope: CacheScope | None = None
    carry: bool = False


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
        return RebuildPlan(due=True, cut=cut, reason="manual", scope=self._scope(request, view, now))

    def _scope(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> CacheScope | None:
        return None


@dataclass(frozen=True)
class TurnStartRebuilds(StalePinRebuilds):
    """The main agent's rebuilds: due on the pin's digest, at a run's first
    call after a turn that ended on tool results too, in the middle of a turn
    after the step under way, and forced under the threshold as above, or
    once armed.

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
        armed = _armed(request.state)
        scope = self._scope(request, view, now)
        cut = turn_start_cut(view, after_tools=first_call)
        if not cut:
            step = step_cut(view)
            if not step:
                return RebuildPlan(due=True)
            return RebuildPlan(due=True, cut=step, reason="mid_turn", forced=armed, scope=scope, carry=True)
        if armed:
            return RebuildPlan(due=True, cut=cut, reason="armed", forced=True, scope=scope)
        if tokens >= threshold:
            return RebuildPlan(due=True, cut=cut, reason="digest", scope=scope)
        forced, reason = self._under_threshold(request, view, tokens, cold=scope is not None)
        if forced:
            return RebuildPlan(due=True, cut=cut, reason=reason, forced=True, scope=scope)
        return RebuildPlan(due=True, cut=cut, reason="digest", waits=reason, scope=scope)

    def _under_threshold(
        self, request: ModelRequest, view: Sequence[AnyMessage], tokens: int, *, cold: bool
    ) -> tuple[bool, str]:
        """Whether a due turn start under the threshold is forced, and why:
        ``urgent`` or ``deploy_cold``, or why it waits.

        A thread whose last answer came from a fallback of another provider
        waits unless urgent: the pin only follows Claude, so a summary that a
        fallback answers after would leave the thread due, and force again
        next turn. An urgent one cannot loop: the summary leaves no bound
        thinking in view, and a thread without any is not due.
        """
        pinned = as_dict(state_get(request.state, STATE_SYSTEM_PIN)).get("prompt_version") or 0
        if release.prompt_version() < pinned:
            return False, "rollback"
        if pinned < release.rebuild_before():
            return True, "urgent"
        if not _claude(next((m for m in reversed(view) if isinstance(m, AIMessage)), None)):
            return False, "fallback"
        if not sources_changed(request.state):
            return False, "inputs_only"
        if not self.floor_tokens:
            return False, "off"
        if tokens < self.floor_tokens:
            return False, "under_floor"
        if not cold:
            return False, "warm"
        return True, "deploy_cold"

    def _scope(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> CacheScope | None:
        """``last_tool`` once the thread's prompt cache has expired, since the
        call after the summary sends neither the old prompt nor the history
        again: longer than its ttl since the later of the last answer compaction stamped and
        the previous run's settling. Both are read at a response's end while
        an entry's life counts from its request's start, so the estimate errs
        towards warm: it skips a rebuild it could have taken rather than force
        one on a warm cache."""
        settled = self.settled_at
        settled_ts = settled.replace(tzinfo=settled.tzinfo or UTC).timestamp() if settled else 0.0
        last = max(request.state.get("_last_model_response_at") or 0.0, settled_ts)
        return "last_tool" if now - last > cache_ttl(view, request.model) else None
