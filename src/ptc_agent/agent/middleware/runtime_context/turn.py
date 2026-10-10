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
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware

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

#: Rows that are no change under the baseline: their writers state them again
#: once a compaction takes them from view (the notes reminder before the next
#: summary, the check-in after the next run of tool calls), so a rebuild does
#: not count them and a summary does not carry them.
NON_CHANGE_ROW_KINDS = frozenset(
    {TURN_ROW_KIND, SUBAGENTS_ROW_KIND, NOTES_DUE_ROW_KIND, NOTES_CHECK_IN_ROW_KIND}
)

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
        if self._opened:
            return None
        return await self.abefore_agent(state, runtime, resumed=True)

    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return None

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
            "origin": self._origin,
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
