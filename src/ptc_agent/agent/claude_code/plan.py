"""What a turn's CLI reports of the Claude plan, and the stop at its limit.

The CLI reports the plan in a ``rate_limit_event`` each time a response moves
it. Each report is kept on the row of the credential the run signed in with
(``PlanLog``, the server's store), off the stream's path, so admission,
Settings and every other run on that credential read the same state. A
turn also keeps what its own launches said (``PlanWatch``): under Stop, that
is what stops its CLI before the next request once the plan is used up,
along with the row, read again before each request the turn makes, so a
sibling run that saw the limit first stops this one too.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, Protocol

from langchain_core.messages import AIMessage
from langgraph.config import get_stream_writer

from src.llms.claude_code import PLAN_NOTICE_KEY, ClaudeTurnFailed, PlanLimit, PlanOut, PlanReading, PlanStopped

if TYPE_CHECKING:
    from claude_agent_sdk import RateLimitEvent

    from ptc_agent.agent.claude_code.launcher import Launcher

logger = logging.getLogger(__name__)

#: Which of the user's credentials a run signed in with: the setup token
#: they saved or stored, or the computer's own token file.
SignedIn = Literal["saved", "machine"]


class PlanLog(Protocol):
    """The user's plan state, kept per credential."""

    async def open(self) -> None:
        """Fence the writes that follow to the credentials as they stand now,
        so a report a run makes after its token was replaced is dropped.
        Called before the run reads a token."""

    async def launching(self) -> None:
        """A CLI is starting and is about to read the computer's token file:
        fence the computer's reports to its row as it stands, so a token
        placed since ``open`` is the one they go to."""

    async def took(self, token: str) -> None:
        """The run signs in with the saved setup token ``token``, read since
        ``open``: where the saved row is that token's now, a replacement in
        between was this one, so its reports go to the row as it stands."""

    async def out(self) -> PlanOut:
        """What the user chose a used-up plan does."""

    def record(self, signed_in: SignedIn, reading: PlanReading) -> None:
        """Keep ``reading`` on the credential's row, in the background; a
        failed write is logged, never raised."""

    async def limit(self, signed_in: SignedIn, model: str) -> PlanLimit | None:
        """The credential's limit that holds ``model`` now, or None."""

    async def written(self) -> None:
        """Wait, briefly, for the reports kept so far to be written."""


@dataclass
class PlanWatch:
    """What a turn's launches said of the plan, and, under Stop
    (``stop_at_limit``), the stop at its limit.

    One watch spans the turn: a compaction's launch, then each run's, so
    what the compaction's request said stops the run that follows it.
    ``rejected`` is this query's (a response refused for the plan),
    ``billed`` the turn's (a response billed to usage credits). Either means
    the next request would be billed too, so the turn stops before it
    (``gate``), as the credit gate stops one; so does the credential's row
    holding a limit for this model, which another run may have seen first.
    A turn's watch (``enforce``) reads the user's choice again at each
    gate, so a switch to Stop or back holds from the turn's next request,
    read from its row each time, since it decides what is billed.
    """

    source: Launcher
    model: str
    stop_at_limit: bool
    #: Whether the user's choice is read again at each gate: a turn's watch, not a token's check.
    enforce: bool = False
    rejected: bool = False
    billed: bool = False
    #: The limit the turn knows of: the last refused or billed response's,
    #: or the row's where that stopped it. A response Claude said no reset
    #: for keeps one still ahead that has a reset, and never one that passed.
    limit: PlanLimit | None = None
    #: Whether the gate stopped the turn; once it has, it stays stopped.
    stopped: bool = field(default=False, init=False)

    @classmethod
    async def open(cls, source: Launcher, model: str, *, enforce: bool) -> PlanWatch:
        """A watch over ``source``'s launches on ``model``, opened before the
        first reads its token. ``enforce`` stops them at the limit where the
        user chose Stop; a token's check only records."""
        out: PlanOut = "stop"
        if source.plan is not None:
            try:
                await source.plan.open()
                if enforce:
                    out = await source.plan.out()
            except Exception:  # noqa: BLE001 - Stop is the default and the safe reading
                logger.warning("[ClaudeCode] could not read the plan setting; the turn stops at the limit", exc_info=True)
        return cls(source, model, stop_at_limit=enforce and out == "stop", enforce=enforce)

    def seen(self, event: RateLimitEvent) -> None:
        reading = PlanReading.of(event.rate_limit_info.raw or {}, datetime.now(UTC), self.model)
        self.rejected = self.rejected or reading.refused
        self.billed = self.billed or reading.billed
        if reading.refused or reading.billed:
            held = self.limit if self.limit is not None and self.limit.current() else None
            told = held is not None and held.known and not (reading.limit and reading.limit.known)
            self.limit = held if told else reading.limit
        if (signed_in := self.source.signed_in) is not None and self.source.plan is not None:
            self.source.plan.record(signed_in, reading)

    def reopen(self) -> None:
        """A new query: whether one was refused is that query's own."""
        self.rejected = False

    @property
    def over_plan(self) -> bool:
        """Whether the turn's own responses say its next request would not run
        on the plan, under Stop: one was refused or billed, and the limit it
        named has not reset yet."""
        return self.stop_at_limit and (self.rejected or self.billed) and (self.limit is None or self.limit.current())

    async def gate(self) -> bool:
        """Whether the turn stops before its next request: its own responses
        say so, or the credential's row does for this model."""
        if self.stopped:
            return True
        if self.enforce:
            self.stop_at_limit = await self._stops()
        if not self.stop_at_limit:
            return False
        if not self.over_plan:
            if (row := await self._row_limit()) is None:
                return False
            self.limit = row
        self.stopped = True
        return True

    async def halts(self, stop_reason: str | None) -> bool:
        """Whether the CLI is stopped as a reply it just streamed ends: one cut
        at the output limit, which it continues with no batch end where the
        gate could stop it, once the gate says the plan is used up."""
        return stop_reason == "max_tokens" and await self.gate()

    async def _stops(self) -> bool:
        """Whether the user's choice is Stop now; as before where it can't be read."""
        if self.source.plan is None:
            return self.stop_at_limit
        try:
            return await self.source.plan.out() == "stop"
        except Exception:  # noqa: BLE001 - the choice the turn started on stands
            logger.warning("[ClaudeCode] could not read the plan setting again", exc_info=True)
            return self.stop_at_limit

    async def _row_limit(self) -> PlanLimit | None:
        signed_in, log = self.source.signed_in, self.source.plan
        if signed_in is None or log is None:
            return None
        try:
            return await log.limit(signed_in, self.model)
        except Exception:  # noqa: BLE001 - the turn's own responses still stop it
            logger.warning("[ClaudeCode] could not read the plan row before a request", exc_info=True)
            return None

    def failure(self, failed: ClaudeTurnFailed | None) -> ClaudeTurnFailed | None:
        """Why the query did not do its turn, as the plan says it: the stop
        where the gate stopped it, and a used-up plan's refusal wherever its
        reports said so, naming the window and until when where Claude said.
        A caller raising a ``PlanStopped`` awaits ``settled`` first."""
        if not (self.stopped or (failed is not None and failed.kind == "plan_exhausted")):
            return failed
        if self.limit is not None and self.limit.known:
            return PlanStopped(self.limit.window, self.limit.until, credits_billed=self.billed)
        if self.stopped or self.limit is not None:
            return PlanStopped(credits_billed=self.billed)
        return failed

    async def settled(self) -> None:
        """Wait, briefly, for what the turn's responses said to be kept, so a
        client that reads the plan again on the stop reads it."""
        if self.source.plan is None:
            return
        try:
            await self.source.plan.written()
        except Exception:  # noqa: BLE001 - the stop stands without it
            logger.debug("[ClaudeCode] plan reports not settled", exc_info=True)

    async def stamp(self, reply: AIMessage) -> AIMessage | None:
        """``reply``, the one the turn ended on, with the notice it carries
        under Stop where the plan ran out during it: its last request was
        billed to usage credits, and Claude Code turns wait for the reset.
        Shown live as a ``claude_code_plan`` event, once what the turn's
        responses said is kept (``settled``), and on replay by the stamp;
        None where there is none. The choice is read again first, as a gate
        reads it: a switch to Stop during the reply holds for it.
        """
        if not (self.billed and self.limit is not None):
            return None
        if self.enforce:
            self.stop_at_limit = await self._stops()
        if not self.stop_at_limit:
            return None
        await self.settled()
        notice: dict[str, str | None] = {"reason": "billed_credits"}
        if self.limit.known:
            notice.update(window=self.limit.window, until=self.limit.until.isoformat())
        try:
            get_stream_writer()({"type": "claude_code_plan", **notice})
        except Exception:  # noqa: BLE001 - the stamp still shows it on reload
            logger.debug("[ClaudeCode] plan notice not emitted", exc_info=True)
        return reply.model_copy(update={"response_metadata": {**reply.response_metadata, PLAN_NOTICE_KEY: notice}})
