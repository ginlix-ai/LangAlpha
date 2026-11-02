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

A cold Claude thread's turn start summarizes at ``cold_tokens`` when that is
under its threshold (``cold_threshold``), due or not: otherwise each turn
after a pause writes the whole context to the cache again. One that is not
due keeps its tail, as at the threshold, and one that is due keeps none, as a
due thread's threshold summary does. A send that asks to keep the full context
keeps the threshold its own for its turn alone. Between turns,
``next_summary`` tells the composer what the next send will summarize, from
what the last answer left in state (``answer_reading``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal, NotRequired, TypedDict

from langchain.agents.middleware.types import ModelRequest
from langchain_core.messages import AIMessage, AnyMessage

from src.llms.cache_markers import CacheScope, route_ttl_seconds, written_ttl_seconds

from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.agent.middleware.compaction.measure import is_claude, measured_tokens
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

logger = logging.getLogger(__name__)


def _claude(message: Any) -> bool:
    return isinstance(message, AIMessage) and "claude" in str(
        (message.response_metadata or {}).get("model_name") or ""
    ).lower()


def _claude_answered_last(view: Sequence[AnyMessage]) -> bool:
    return _claude(next((m for m in reversed(view) if isinstance(m, AIMessage)), None))


def cold_threshold(threshold: int, cold_tokens: int) -> int:
    """The threshold a Claude thread's turn start summarizes at once its
    prompt cache has expired: ``cold_tokens`` when under its own, 0 never."""
    return min(threshold, cold_tokens) if cold_tokens > 0 else threshold


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
        scope: The cache markers of the summary's own native call, once
            the main agent's prompt cache has expired: ``last_tool`` for
            one that keeps no turns, ``system_and_tools`` for one that
            keeps the tail.
        threshold: The threshold the call summarizes at in place of its
            own, a cold turn start's ``cold_threshold``; None keeps its own.
        carry: ``cut`` ends mid-turn, after the results of the step under
            way, which the call after the summary is sent as a row.
    """

    due: bool = False
    cut: int = 0
    reason: str = ""
    forced: bool = False
    waits: str | None = None
    scope: CacheScope | None = None
    threshold: int | None = None
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
        return RebuildPlan(
            due=True, cut=cut, reason="manual", scope="last_tool" if self._cold(request, view, now) else None
        )

    def _cold(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> bool:
        return False


@dataclass(frozen=True)
class TurnStartRebuilds(StalePinRebuilds):
    """The main agent's rebuilds: due on the pin's digest, at a run's first
    call after a turn that ended on tool results too, in the middle of a turn
    after the step under way, and forced under the threshold as above, or
    once armed, as is a cold thread's summary at its turn start.

    Attributes:
        floor_tokens: The least context a deploy's rebuild is forced at
            under the threshold; 0 never forces one.
        settled_at: When the thread's previous run settled, by the run
            ledger, a naive one read as UTC; None when it has none.
        cold_tokens: The threshold a cold thread's turn start summarizes at
            when under its own (``cold_threshold``); 0 never lowers it.
        keep_full_context: The turn's send asked not to be summarized for a
            cold cache alone; the threshold and a deploy's rebuild still are.
    """

    reads_first_call: ClassVar[bool] = True

    floor_tokens: int
    settled_at: datetime | None = None
    cold_tokens: int = 0
    keep_full_context: bool = False

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
        due = self.due(request, view)
        # Only a run's first call starts a turn, the one call a pause can have
        # left on an expired cache; the run's later calls keep it warm.
        at_turn_start = turn_start_cut(view, after_tools=first_call)
        if not at_turn_start:
            step = step_cut(view) if due else 0
            if not step:
                return RebuildPlan(due=due)
            scope: CacheScope | None = "last_tool" if self._cold(request, view, now) else None
            return RebuildPlan(
                due=True, cut=step, reason="mid_turn", forced=_armed(request.state), scope=scope, carry=True
            )
        cold = self._cold(request, view, now)
        claude = _claude_answered_last(view)
        scope = "last_tool" if cold else None
        reason = None
        if due:
            if _armed(request.state):
                return RebuildPlan(due=True, cut=at_turn_start, reason="armed", forced=True, scope=scope)
            if tokens >= threshold:
                return RebuildPlan(due=True, cut=at_turn_start, reason="digest", scope=scope)
            forced, reason = forced_under_threshold(request.state, tokens, self.floor_tokens, claude=claude, cold=cold)
            if forced:
                return RebuildPlan(due=True, cut=at_turn_start, reason=reason, forced=True, scope=scope)
        # The call's own model too: another provider's pays no premium to
        # write its cache again, so a summary would cost it context for nothing.
        lowered = self._lowered(threshold, tokens) if cold and claude and is_claude(request.model) else None
        if not due:
            return RebuildPlan(threshold=lowered, scope="system_and_tools" if cold and claude else None)
        return RebuildPlan(due=True, cut=at_turn_start, reason="digest", waits=reason, scope=scope, threshold=lowered)

    def _lowered(self, threshold: int, tokens: int) -> int | None:
        """``cold_threshold`` where it is under the call's own, unless the
        turn's send asked to keep the full context, which is logged where it
        spared a summary."""
        lowered = cold_threshold(threshold, self.cold_tokens)
        if lowered >= threshold:
            return None
        if not self.keep_full_context:
            return lowered
        if lowered <= tokens < threshold:
            thread_id, agent = run_ids()
            logger.info(
                "[Compaction] trigger=cold skipped=user tokens=%d threshold=%d thread_id=%s agent=%s",
                tokens,
                threshold,
                thread_id,
                agent,
            )
        return None

    def _cold(self, request: ModelRequest, view: Sequence[AnyMessage], now: float) -> bool:
        return now > cache_expires_at(
            cache_ttl(view, request.model), request.state.get("_last_model_response_at"), self.settled_at
        )


def forced_under_threshold(
    state: Any, tokens: int, floor_tokens: int, *, claude: bool, cold: bool
) -> tuple[bool, str]:
    """Whether a due turn start under the threshold is forced, and why:
    ``urgent`` or ``deploy_cold``, or why it waits. ``claude``: Claude gave
    the last answer; ``cold``: the prompt cache has expired.

    A thread whose last answer came from a fallback of another provider
    waits unless urgent: the pin only follows Claude, so a summary that a
    fallback answers after would leave the thread due, and force again
    next turn. An urgent one cannot loop: the summary leaves no bound
    thinking in view, and a thread without any is not due.
    """
    pinned = as_dict(state_get(state, STATE_SYSTEM_PIN)).get("prompt_version") or 0
    if release.prompt_version() < pinned:
        return False, "rollback"
    if pinned < release.rebuild_before():
        return True, "urgent"
    if not claude:
        return False, "fallback"
    if not sources_changed(state):
        return False, "inputs_only"
    if not floor_tokens:
        return False, "off"
    if tokens < floor_tokens:
        return False, "under_floor"
    if not cold:
        return False, "warm"
    return True, "deploy_cold"


def cache_expires_at(ttl: int, last_answer_at: float | None, settled_at: datetime | None) -> float:
    """When the thread's prompt cache expires: ``ttl`` after the later of
    the last answer compaction stamped and the previous run's settling, a
    naive ``settled_at`` read as UTC. Both are read at a response's end
    while an entry's life counts from its request's start, so the estimate
    errs towards warm: it skips a summary it could have taken rather than
    force one on a warm cache."""
    settled_ts = settled_at.replace(tzinfo=settled_at.tzinfo or UTC).timestamp() if settled_at else 0.0
    return max(last_answer_at or 0.0, settled_ts) + ttl


#: The state key holding ``answer_reading``.
LAST_ANSWER_KEY = "_last_answer"


def answer_reading(
    message: AIMessage, threshold: int, model: Any, previous: Mapping[str, Any] | None, rebuilds: TurnStartRebuilds
) -> dict[str, Any]:
    """What ``next_summary`` reads of the main agent's newest answer, kept in
    state so a read between turns replays no messages and resolves no
    settings: the threshold it was measured against, whether Claude gave it
    and whether the call was to a Claude model (a fallback's answer can be
    Claude's on a call to another), what ``cache_ttl`` reads from the view
    and the route (the ttl the newest Claude answer that wrote the cache
    wrote, and the route's), and the turn's own ``cold_tokens`` and
    ``floor_tokens``, the user's settings included."""
    claude = _claude(message)
    written = written_ttl_seconds(message.usage_metadata) if claude else None
    return {
        "threshold": threshold,
        "claude": claude,
        "claude_model": is_claude(model),
        "written_ttl": written or as_dict(previous).get("written_ttl"),
        "route_ttl": route_ttl_seconds(model),
        "cold_tokens": rebuilds.cold_tokens,
        "floor_tokens": rebuilds.floor_tokens,
    }


def offloaded_reading(state: Any, tokens: int) -> dict[str, Any] | None:
    """The stamp with what an offload between turns leaves of the context,
    ``tokens`` as the next turn start reads it, which ``next_summary`` reads
    while the offload has zeroed the measurement. It holds under the summary
    the offload saw: a summary since, a manual compaction, has cut the
    context again, and the next answer stamps afresh. None without a stamp."""
    reading = as_dict(state_get(state, LAST_ANSWER_KEY))
    if not reading:
        return None
    return {**reading, "offloaded": {"tokens": tokens, "summary": summary_id(state)}}


class NextSummary(TypedDict):
    """What the next turn start summarizes: ``threshold`` now, ``cold`` or
    ``rebuild`` from ``at``, when the cache expires (epoch seconds), an
    urgent ``rebuild`` now, without one. Only ``cold`` gives way to a send
    that keeps the full context."""

    reason: Literal["threshold", "cold", "rebuild"]
    at: NotRequired[float]


def next_summary(state: Any, *, settled_at: datetime | None) -> NextSummary | None:
    """What the main agent's next turn start summarizes, read between turns
    so the composer can say so before the user sends, from the turn start's
    own rules. A pin marked stale or built from other sources may be due,
    which the state alone cannot tell, so its cold summary reads as the
    rebuild a due thread would be forced into, if any (``rebuild``), whatever
    the user's ``cold_tokens``: a send cannot skip that one. Only a call to a
    Claude model keeps a pin, or summarizes a cold thread early. The context
    is the last call's measurement, or after an offload that zeroed it, what
    the offload left (``offloaded_reading``).
    """
    reading = as_dict(state_get(state, LAST_ANSWER_KEY))
    tokens = measured_tokens(as_dict(state))
    offloaded = as_dict(reading.get("offloaded"))
    if tokens is None and offloaded and offloaded.get("summary") == summary_id(as_dict(state)):
        tokens = offloaded.get("tokens")
    threshold = reading.get("threshold")
    if tokens is None or not threshold:
        return None
    if tokens >= threshold:
        return {"reason": "threshold"}
    if not reading.get("claude_model"):
        return None
    claude = bool(reading.get("claude"))
    ttl = reading.get("written_ttl") or reading.get("route_ttl") or route_ttl_seconds(None)
    at = cache_expires_at(ttl, state_get(state, "_last_model_response_at"), settled_at)
    if rebuild_pending(state) or sources_changed(state):
        forced, why = forced_under_threshold(state, tokens, reading.get("floor_tokens") or 0, claude=claude, cold=True)
        if forced:
            return {"reason": "rebuild"} if why == "urgent" else {"reason": "rebuild", "at": at}
    if not claude or tokens < cold_threshold(threshold, reading.get("cold_tokens") or 0):
        return None
    return {"reason": "cold", "at": at}
