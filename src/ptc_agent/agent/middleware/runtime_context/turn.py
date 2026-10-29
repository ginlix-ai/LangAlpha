"""The turn's opening context, written once into history.

This used to be four sections of the per-call envelope, re-rendered on every
model call of the turn. That was wrong twice over: the values are frozen for
the turn anyway, so the re-render cost tokens without changing a byte, and a
block that never survives the call left the model with no time line at all
across turns.

Written instead as one durable row at the turn boundary, the stamp becomes what
it always was: the turn's anchor, sitting at the point in the conversation where
the turn began, and readable from there for the rest of the thread. That is also
why every line is a fact rather than a countdown. "next open in 13h" is true for
one minute; "next open Thu 09:30 ET" is true forever.

The row also carries the delivery rules for the surface the turn arrived on.
Those used to be a static table of every surface in the cached system prefix,
which spent the rules for five surfaces on every turn of every thread so that
one of them could apply, and still could not say anything about the surface in
hand. Carried here they arrive on demand and only when they are news: a thread
that never leaves the web app states the web rules once, and a thread that
moves to another surface states that surface's rules on the turn it moves.

Which rules those are depends on who renders the reply. langalpha has a line of
its own for the surfaces it draws itself; for any other surface the client that
posted the turn sends its own text (``ChatRequest.surface_rules``) and that text
is the paragraph, because the client is what has to display the answer.

This module also owns the turn's end. The server stops a turn at its wall
clock, and a stop is an error the user reads as nothing found, however much
the agent had: the agent cannot see the clock, so it cannot plan around it.
So when the end is near the agent gets one row asking for its answer, and in
the last minutes its tools stop running, which leaves the next call an answer
to give. The server's stop stays as the backstop for a call that never
returns. A subagent outlives the turn that started it, so it runs on a clock
of its own, set when its run starts. The row rides a hook this middleware
already runs and the stop is a tool wrapper of its own
(``TimeUpToolStopMiddleware``), so neither costs an extra step per round.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from ptc_agent.agent.limits import REFUSAL_KEY
from ptc_agent.agent.middleware.runtime_context import clock
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    last_stated,
    rows_in_view,
)
from ptc_agent.agent.middleware.runtime_context.state import STATE_BASELINE, state_get
from ptc_agent.agent.middleware.runtime_context.surface import Surface, parse_surface
from ptc_agent.agent.middleware.runtime_context.templates import render_template

logger = logging.getLogger(__name__)

#: Row kind of the turn anchor. Rendered verbatim by ``update_row.md.j2``,
#: which hardcodes the same string.
TURN_ROW_KIND = "turn_opened"

#: Row kind of the subagent-switch notice, which ``update_row.md.j2`` hardcodes
#: the same way.
SUBAGENTS_ROW_KIND = "subagents_switched"

#: Row kind of the scratchpad notes reminder (``compaction/notes.py``), which
#: ``update_row.md.j2`` hardcodes the same way.
NOTES_DUE_ROW_KIND = "notes_due"

#: Row kind of the scratchpad notes check-in (``compaction/notes.py``), which
#: ``update_row.md.j2`` hardcodes the same way.
NOTES_CHECK_IN_ROW_KIND = "notes_check_in"

#: Row kind of the ask to answer before the turn's time runs out, which
#: ``update_row.md.j2`` hardcodes the same way.
TIME_UP_ROW_KIND = "turn_time_up"

#: Row kind that takes back a time-up ask once a later run of the thread is on
#: a new clock, which ``update_row.md.j2`` hardcodes the same way.
TIME_UP_WITHDRAWN_ROW_KIND = "turn_time_up_withdrawn"

#: Row kind of a live-price stamp (``middleware/market_watch.py``), which
#: ``update_row.md.j2`` hardcodes the same way.
MARKET_WATCH_ROW_KIND = "market_watch"

#: Row kind of the user's cancel of wakeups the agent set
#: (``middleware/wakeups.py``), which ``update_row.md.j2`` hardcodes the same
#: way.
WAKEUP_CANCELLED_ROW_KIND = "wakeup_cancelled"

#: Rows that speak for no block, so a rebuild neither folds them in nor
#: retires them, but a summary carries them: nobody states them again, and
#: the user's cancel of a wakeup holds whatever the blocks say.
NOTICE_ROW_KINDS = frozenset({WAKEUP_CANCELLED_ROW_KIND})

#: Rows that are no change under the baseline: their writers state them again
#: once a compaction takes them from view (the notes reminder before the next
#: summary, the check-in after the next run of tool calls, the time-up ask on
#: the next call, the live prices on the next fetch), so a rebuild does not
#: count them and a summary does not carry them. The withdrawal of a time-up ask answers only that ask, which a
#: summary does not carry either, so it has nothing to answer once both are
#: out of view.
NON_CHANGE_ROW_KINDS = frozenset(
    {
        TURN_ROW_KIND,
        SUBAGENTS_ROW_KIND,
        NOTES_DUE_ROW_KIND,
        NOTES_CHECK_IN_ROW_KIND,
        TIME_UP_ROW_KIND,
        TIME_UP_WITHDRAWN_ROW_KIND,
        MARKET_WATCH_ROW_KIND,
    }
)

#: The configurable key a run's wall-clock end rides under, in epoch seconds:
#: the server's stop for a turn, ``arun_subagent_streaming``'s for a subagent.
#: Absent for a run nothing times (a test, thread maintenance).
TURN_DEADLINE_KEY = "turn_deadline"
#: The run's whole wall clock in seconds, beside its end.
TURN_TIME_LIMIT_KEY = "turn_time_limit"

# How long before the end the agent is asked for its answer, and when its tools
# stop. Tunable: late in a long turn one call of a slow model at a large
# context has taken over six minutes, so the ask leaves room for two or three.
# A clock under an hour scales both down, to its last third and sixth, so a run
# timed in minutes still gets to work before it is asked to finish.
TIME_UP_ASK_BEFORE = timedelta(minutes=20)
TIME_UP_TOOLS_STOP_BEFORE = timedelta(minutes=10)

TIME_UP_REFUSAL = (
    "Not run: this turn is out of time. Answer the user now with what you have, "
    "say what is left undone, and that they can reply to have you continue."
)
#: The ``refusal`` tag on a call the tool stop refused.
TIME_UP_REFUSAL_TAG = "time_up"

# The key a manual follow-up in an automation's thread states under when it
# has no surface of its own: the handoff is a rule, and a rule needs a key so
# the next attended turn does not say it again.
ATTENDED_KEY = "attended"
#: The key of a turn that arrived with no rules after one that stated some: the
#: return to the default shape is itself a rule, stated once.
DEFAULT_KEY = "default"
# The surfaces `surface_rules.md.j2` carries a line for itself.
BUILT_IN_SURFACES = frozenset({"web", "market_view"})

TURN_SCHEMA_VERSION = 1

# A gap shorter than this is normal conversational pacing and says nothing the
# model can act on; only a real absence is worth a line. Tunable: the threshold
# is a judgement call, not a measured constant.
ELAPSED_MIN_GAP = timedelta(minutes=15)


def _minutes(span: timedelta) -> str:
    """``span`` to the nearest minute, never "0 minutes" for time that is left."""
    n = max(round(span.total_seconds() / 60), 1)
    return f"{n} minute" if n == 1 else f"{n} minutes"


def _run_clock() -> tuple[datetime, timedelta, timedelta] | None:
    """When this run's time is up, and how long before that the agent is
    asked to finish and its tools stop; None when nothing times it."""
    try:
        configurable = get_config().get("configurable") or {}
    except RuntimeError:  # outside a runnable context
        return None
    return run_clock(configurable)


def run_clock(
    configurable: Mapping[str, Any] | None,
) -> tuple[datetime, timedelta, timedelta] | None:
    """``_run_clock`` read off a config's ``configurable``, for a caller that
    holds the config rather than running inside it."""
    configurable = configurable or {}
    end = configurable.get(TURN_DEADLINE_KEY)
    if not isinstance(end, int | float):
        return None
    ask = TIME_UP_ASK_BEFORE
    limit = configurable.get(TURN_TIME_LIMIT_KEY)
    if isinstance(limit, int | float) and limit > 0:
        ask = min(ask, timedelta(seconds=limit) / 3)
    stop = min(TIME_UP_TOOLS_STOP_BEFORE, ask / 2)
    return datetime.fromtimestamp(end, tz=UTC), ask, stop


class TimeUpToolStopMiddleware(AgentMiddleware):
    """Refuses every tool call once the run is inside its tool stop.

    It goes outside every other middleware that wraps tool calls, because
    whatever wraps it has already acted on the call by the time the refusal
    comes back: the background interceptor has spawned the subagent a refused
    ``Task`` asked for, the file-op middleware has streamed a refused ``Write``
    as done, and the repeat guard has counted the call. Out here a refused call
    reaches none of them. The ``refusal`` tag tells the harness's own refusal
    from a tool that failed. A subagent's result capture alone goes further
    out: it acts only on the result, and has to stream the refusal too.
    """

    # Async only, as ``SubagentSwitchMiddleware``'s is: the async agent never
    # calls the sync hook.
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        clock = _run_clock()
        if clock is None or clock[0] - datetime.now(tz=UTC) > clock[2]:
            return await handler(request)
        return ToolMessage(
            content=TIME_UP_REFUSAL,
            tool_call_id=request.tool_call.get("id"),
            name=request.tool_call.get("name", ""),
            status="error",
            additional_kwargs={REFUSAL_KEY: TIME_UP_REFUSAL_TAG},
        )


class TurnContextMiddleware(AgentMiddleware):
    """Writes one ``turn_opened`` row into history at the turn boundary.

    Sits ahead of :class:`BaselineContextMiddleware` in the stack so the anchor
    lands first and the change rows follow it, all of them after the turn's own
    user message, which is the placement every carrier shape needs.

    Args:
        now: The turn's instant. The main stacks pass the request time so the
            row agrees with ``current_time``; a stack that is built once and
            run many times (a subagent's) passes nothing, and the row reads
            the clock at its own turn boundary instead of the parent's.
        timezone: IANA zone the stamp is rendered in.
        preferred_market: Market the session line reports on.
        last_turn_at: When the previous turn in this thread ran. None on a
            thread's first turn, which drops the gap line and states the
            market unconditionally.
        platform: Surface this turn arrived on (web, slack, market_view, ...).
        origin: Who started the thread (agent, automation, system).
        origin_agent: Which agent, for an ``agent`` origin.
        surface_rules: Delivery rules the posting client wrote for its own
            surface. They stand in for langalpha's built-in line, because the
            client that renders the reply is the authority on what it accepts.
        inherits_rules: Whether the turn runs under the last rules stated
            instead of its own, because nobody sent it: the harness reporting
            finished background work into the thread.
        disk_free_mb: Free space on the computer's shared disk, passed only
            when it is low enough that the agent should work around it.
        disk_known: Whether a current reading stands behind ``disk_free_mb``
            being None, as opposed to no reading at all.
        is_subagent: Whether this stack belongs to a subagent.
    """

    def __init__(
        self,
        *,
        now: datetime | None = None,
        timezone: str | None = None,
        preferred_market: str | None = None,
        last_turn_at: datetime | None = None,
        platform: str | None = None,
        origin: str | None = None,
        origin_agent: str | None = None,
        surface_rules: str | None = None,
        inherits_rules: bool = False,
        disk_free_mb: int | None = None,
        disk_known: bool = False,
        is_subagent: bool = False,
    ) -> None:
        super().__init__()
        self._now = now
        self._timezone = timezone or None
        self._preferred_market = preferred_market
        self._last_turn_at = last_turn_at
        self._platform = platform
        self._origin = origin
        self._origin_agent = origin_agent
        self._surface_rules = surface_rules
        self._inherits_rules = inherits_rules
        self._disk_free_mb = disk_free_mb
        self._disk_known = disk_known
        self._is_subagent = is_subagent
        # Set once the turn-open hook has run on this instance; see
        # ``abefore_model``.
        self._opened = False

    # -- middleware hooks ---------------------------------------------------

    async def abefore_agent(
        self, state: Any, runtime: Any = None, *, resumed: bool = False
    ) -> dict[str, Any] | None:
        self._opened = True
        try:
            now = self._now or datetime.now(tz=UTC)
            surface = parse_surface(self._platform)
            market = self._market(state)
            zone = self._zone(state)
            key = rules_key(
                surface.name,
                self._origin,
                self._is_subagent,
                self._surface_rules,
                symbol=surface.symbol,
            )
            last = _last_rules_key(state)
            if resumed or self._inherits_rules:
                # An attempt-only continuation (a resumed interrupt, a retry)
                # arrives without the surface of the turn it completes, so
                # the rules it runs under are the last ones stated, whatever
                # this request did or did not say. A notification is the same
                # case: it reports into the conversation the work was started
                # from, so a channel's rules stay in force rather than being
                # taken back, and since nobody sent it, it is not the attended
                # turn an automation's handoff waits for either.
                key = last
            # An automation's thread can take a manual follow-up. The last
            # rules the model can read then say nobody is waiting, and a turn
            # with no rules of its own would leave that standing, so the
            # handoff is stated as this turn's rule and keyed so it is said once.
            # Only a turn with no origin is a person's: an agent or system
            # origin changes the key and restates the surface, without the line.
            handoff = (
                _is_automation_key(last)
                and self._origin is None
                and not self._is_subagent
            )
            if handoff and key is None:
                key = ATTENDED_KEY
            # A turn with no rules of its own (the web app sends no surface
            # for a plain turn; a channel may send its name and drop its
            # text) after a turn that stated some would leave that paragraph
            # as the last rule in view, so the return to the default shape is
            # stated as this turn's rule, keyed so that it too is said once.
            # The handoff row already left the model on that shape, so it is
            # not a rule to take back.
            reset = (
                not _states_rules(key)
                and key != last
                and _states_rules(last)
                and not handoff
            )
            if key is None and (reset or last == DEFAULT_KEY):
                key = DEFAULT_KEY
            rules = (
                ""
                if key is None or key == last
                else render_surface_rules(
                    surface,
                    self._origin,
                    self._is_subagent,
                    self._surface_rules,
                    handoff=handoff,
                    reset=reset,
                )
            )
            provenance: dict[str, Any] = {
                "source": "harness",
                "surface": surface.name,
                "symbol": surface.symbol,
                "origin": self._origin,
                # The market and zone this row's session line is about, so the
                # next row can tell a market or clock change from a session
                # that did not move.
                "market": market,
                "zone": zone,
            }
            # Absent rather than None when there are no rules for this turn, so
            # a turn that stated nothing does not read as the last word on what
            # the model was told.
            if key is not None:
                provenance["rules_key"] = key
            # The low-disk line is a present-tense instruction in a row nothing
            # rewrites, so the first turn a reading shows it no longer holds
            # takes it back. A turn without a reading records nothing, leaving
            # the last word to the rows that had one. A subagent never carries
            # the line.
            disk_recovered = False
            if not self._is_subagent:
                if self._disk_free_mb is not None:
                    provenance["disk_low"] = True
                elif self._disk_known:
                    provenance["disk_low"] = False
                    disk_recovered = _last_disk_low(state)
            row = DurableUpdate(
                kind=TURN_ROW_KIND,
                schema_version=TURN_SCHEMA_VERSION,
                text=self._render(
                    now,
                    surface,
                    rules,
                    zone,
                    market,
                    _last_turn_market(state),
                    disk_recovered=disk_recovered,
                ),
                provenance=provenance,
                created_at=now,
            )
            # No ids: the row is returned from a middleware hook, so the Pregel
            # path mints them, and minting one here would re-roll a different
            # uuid on every replay.
            return {"messages": [build_update_message(row)]}
        except Exception:  # noqa: BLE001 - context is never worth failing a turn for
            logger.warning(
                "[Turn] anchor row failed; the turn opens without one", exc_info=True
            )
            return None

    def before_agent(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    async def abefore_model(
        self, state: Any, runtime: Any = None
    ) -> dict[str, Any] | None:
        # A turn that resumes an interrupt re-enters the graph at the
        # interrupted node, never at the entry node, so ``abefore_agent`` does
        # not run for it and the model would read the interrupted turn's clock.
        # The instance is built per request (the subagent stack is the one
        # exception, and every subagent run enters through ``abefore_agent``),
        # so a first model call on an instance that never opened a turn is that
        # resume: the row is written here, after the resumed tool's result.
        # The resume is also a new run on a new clock, so the call reads the
        # time-up rows against it as every later call does, rather than acting
        # on an ask that ran out with the interrupted run.
        if self._opened:
            return self._time_up(state)
        opened = await self.abefore_agent(state, runtime, resumed=True)
        ending = self._time_up(state)
        if opened is None or ending is None:
            return opened or ending
        return {"messages": [*opened["messages"], *ending["messages"]]}

    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

    # -- the turn's end -------------------------------------------------------

    def _time_up(self, state: Any) -> dict[str, Any] | None:
        """One ``turn_time_up`` row once the end is near, and again only if a
        compaction took it from view.

        A row nothing rewrites outlives its clock: the next turn of the thread
        runs on a new one, while the ask in view still says time is up and the
        tools have stopped. So the first call of a run whose clock is not the
        one the last ask in view was about takes the ask back, once.
        """
        clock = _run_clock()
        if clock is None:
            return None
        deadline, ask_before, stop_before = clock
        now = datetime.now(tz=UTC)
        stamp = deadline.isoformat()
        if deadline - now > ask_before:
            return _withdrawn(state, stamp, now)
        if last_stated(state, TIME_UP_ROW_KIND, "deadline") == stamp:
            return None
        left = _minutes(deadline - now)
        # A long tool call can carry the run past the ask into the stop window,
        # so the first row may land after the tools have already stopped.
        if deadline - now <= stop_before:
            text = f"It ends in {left}, and your tools have stopped running."
        else:
            text = (
                f"It ends in {left}, and your tools stop running "
                f"{_minutes(stop_before)} before that."
            )
        row = DurableUpdate(
            kind=TIME_UP_ROW_KIND,
            schema_version=TURN_SCHEMA_VERSION,
            text=text,
            provenance={"source": "harness", "deadline": stamp},
            created_at=now,
        )
        return {"messages": [build_update_message(row)]}

    # -- rendering ----------------------------------------------------------

    def _zone(self, state: Any) -> str:
        """The zone the stamp is rendered in.

        Resolved the way ``_market`` is: a build that could not read the
        profile passes None, and the main agent then keeps the zone the frozen
        identity block states rather than reading the failed read as UTC.
        """
        if self._timezone is not None or self._is_subagent:
            return self._timezone or "UTC"
        baseline = state_get(state, STATE_BASELINE)
        identity = baseline.get("identity") if isinstance(baseline, dict) else None
        stated = identity.get("timezone") if isinstance(identity, dict) else None
        return stated if isinstance(stated, str) and stated else "UTC"

    def _market(self, state: Any) -> str | None:
        """The market this turn reports on.

        A build that could not read the profile passes None; the main agent
        then keeps the market the frozen identity block already states, so the
        stamp and the block agree, and falls to the product default only when
        there is no epoch yet. A subagent has no market line at all.
        """
        if self._preferred_market is not None or self._is_subagent:
            return self._preferred_market
        baseline = state_get(state, STATE_BASELINE)
        identity = baseline.get("identity") if isinstance(baseline, dict) else None
        stated = identity.get("preferred_market") if isinstance(identity, dict) else None
        return stated if isinstance(stated, str) and stated else "US"

    def _render(
        self,
        now: datetime,
        surface: Surface,
        surface_rules: str,
        zone: str,
        market: str | None,
        last_market: LastSession | None,
        *,
        disk_recovered: bool = False,
    ) -> str:
        from ptc_agent.agent.prompts import format_current_time

        opened = format_current_time(now, zone)
        elapsed_human, sessions_closed = self._elapsed(now, market)
        fields: dict[str, Any] = {
            "opened": opened,
            # ``opened`` already carries the zone abbreviation, so the IANA name
            # is only worth adding when it says something the stamp does not.
            "local_tz": zone if zone not in opened else None,
            "market_line": self._market_line(
                now, zone, market, last_market, gap_reported=elapsed_human is not None
            ),
            "elapsed_human": elapsed_human,
            "sessions_closed": sessions_closed,
            "market": market,
            "surface": surface.name,
            "symbol": surface.symbol,
            "started_by": started_by(self._origin, self._origin_agent),
            "is_subagent": self._is_subagent,
            "surface_rules": surface_rules,
            "disk_free_mb": self._disk_free_mb,
            "disk_recovered": disk_recovered,
        }
        return render_template("envelope/turn.md.j2", **fields)

    def _market_line(
        self,
        now: datetime,
        zone: str,
        market: str | None,
        last_market: LastSession | None,
        *,
        gap_reported: bool,
    ) -> str | None:
        """The session line, kept only when it says something the last row did not.

        The clock is a pure function of the instant, so rendering it at the
        previous turn's time answers whether the state moved; an identical line
        is a repeat and drops. It comes back whenever the gap line fires, so the
        cue to re-fetch and the state it should be re-fetched against sit
        together, whenever the last row in view spoke for another market or
        another zone, since a line that matches by phase is still about a
        different market and a line rendered in another zone puts the local
        clock at another hour, and whenever no row is in view at all, because a
        repeat of a line the model can no longer read is not a repeat.
        """
        line = clock.market_status_line(market, now, zone, relative=False)
        if line is None or self._last_turn_at is None or gap_reported:
            return line
        if last_market is None:
            return line
        known, stated, stated_zone = last_market
        if known and stated != market:
            return line
        if stated_zone is not None and stated_zone != zone:
            return line
        previous = clock.market_status_line(
            market, self._last_turn_at, zone, relative=False
        )
        return None if previous == line else line

    def _elapsed(self, now: datetime, market: str | None) -> tuple[str | None, int]:
        """The gap since the previous turn, or nothing when it is not worth a line."""
        if self._last_turn_at is None or now - self._last_turn_at < ELAPSED_MIN_GAP:
            return None, 0
        return clock.elapsed_summary(market, self._last_turn_at, now)


def _withdrawn(state: Any, stamp: str, now: datetime) -> dict[str, Any] | None:
    """A ``turn_time_up_withdrawn`` row when the last time-up row in view was
    about another clock and nothing has taken it back yet."""
    last = next(
        (
            row
            for row in reversed(rows_in_view(state))
            if row.kind in (TIME_UP_ROW_KIND, TIME_UP_WITHDRAWN_ROW_KIND)
        ),
        None,
    )
    if (
        last is None
        or last.kind != TIME_UP_ROW_KIND
        or last.provenance.get("deadline") == stamp
    ):
        return None
    row = DurableUpdate(
        kind=TIME_UP_WITHDRAWN_ROW_KIND,
        schema_version=TURN_SCHEMA_VERSION,
        text="",
        provenance={"source": "harness", "deadline": stamp},
        created_at=now,
    )
    return {"messages": [build_update_message(row)]}


# -- the run line -----------------------------------------------------------

#: Agents that start a turn for the user, as the run line names them.
_FOR_THE_USER = {"chief_of_staff": "the Chief of Staff"}


def started_by(origin: str | None, agent: str | None) -> str | None:
    """Who the run line says started the thread: an agent that acts for the
    user is named as doing so, since the analyst otherwise reads its hand-off
    as the user's own words."""
    if origin == "agent" and agent in _FOR_THE_USER:
        return f"{_FOR_THE_USER[agent]}, on the user's behalf"
    return origin


# -- the delivery rules -----------------------------------------------------


def rules_key(
    surface: str | None,
    origin: str | None,
    is_subagent: bool,
    surface_rules: str | None = None,
    *,
    symbol: str | None = None,
) -> str | None:
    """One comparable token for the delivery rules this turn needs.

    A subagent has no surface, so its key stands alone. Everything else is the
    surface, its symbol when it has one (the market_view paragraph names the
    chart the user has open, so a change of chart is a change of rules), and
    whether an automation started the thread, because those are the inputs the
    rules paragraph reads. Caller-supplied rules add a digest of the text: the
    gateway may reword them without changing surface, and the turn after it
    does is the one that has to restate them.
    """
    if is_subagent:
        return "subagent"
    caller = (surface_rules or "").strip()
    head = f"{surface}:{symbol}" if surface and symbol else surface
    if caller:
        digest = hashlib.sha256(caller.encode("utf-8")).hexdigest()[:12]
        head = f"{surface or ''}#{digest}"
    parts = [
        part
        for part in (head, "automation" if origin == "automation" else None)
        if part
    ]
    return "+".join(parts) or None


def _is_automation_key(key: str | None) -> bool:
    return key is not None and key.split("+")[-1] == "automation"


def _states_rules(key: str | None) -> bool:
    """Whether the rules paragraph behind this key says anything.

    The template renders a paragraph for a subagent, an automation, caller
    text (the digest) and the two built-in surfaces. A bare surface with none
    of those names itself on the run line and states no rules, the same as no
    surface at all, so a turn that moves between the two has nothing to take
    back; a turn that moves onto either from a stated rule does.
    """
    if key is None or key in (DEFAULT_KEY, ATTENDED_KEY):
        return False
    if key == "subagent" or _is_automation_key(key):
        return True
    head = key.split("+")[0]
    return "#" in head or head.split(":")[0] in BUILT_IN_SURFACES


def render_surface_rules(
    surface: Surface,
    origin: str | None,
    is_subagent: bool,
    surface_rules: str | None = None,
    *,
    handoff: bool = False,
    reset: bool = False,
) -> str:
    return render_template(
        "envelope/surface_rules.md.j2",
        surface=surface.name,
        symbol=surface.symbol,
        origin=origin,
        is_subagent=is_subagent,
        surface_rules=surface_rules,
        handoff=handoff,
        reset=reset,
    )


#: ``(known, market, zone)`` from the last turn row the model can still read.
#: ``known`` is False when the row predates the market stamp; ``zone`` is None
#: when it predates the zone stamp.
LastSession = tuple[bool, str | None, str | None]


def _last_turn_market(state: Any) -> LastSession | None:
    """The session the last turn row in view spoke for.

    None when no turn row is in view: a compaction that took every row leaves
    the model with no session line to compare against, whatever the clock said
    at the previous instant. A row from before the market stamp reads as
    unknown, and the session line then falls back to comparing the clock at
    the two instants, which is what it did before rows carried a market.
    """
    for update in reversed(rows_in_view(state)):
        if update.kind != TURN_ROW_KIND:
            continue
        zone = update.provenance.get("zone")
        if "market" in update.provenance:
            return True, update.provenance["market"], zone
        return False, None, zone
    return None


def _last_rules_key(state: Any) -> str | None:
    """The key of the last rules the model can still read, or None.

    A row from a build that stamped no ``rules_key`` never counts as having
    stated anything, which is what makes the first turn after a deploy state
    its rules.
    """
    return last_stated(state, TURN_ROW_KIND, "rules_key")


def _last_disk_low(state: Any) -> bool:
    """Whether the last row in view that stated the disk said it was low.

    Rows behind the compaction cutoff do not count: a low-disk line the model
    can no longer read needs no taking back.
    """
    return last_stated(state, TURN_ROW_KIND, "disk_low", False) is True
