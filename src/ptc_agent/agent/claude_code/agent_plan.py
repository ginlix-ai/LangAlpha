"""The agent's plan (``UpdatePlan``) on a Claude Code turn: its counts, its
check-in, its nudge and its close-out. Not the Claude plan a login runs on
(``plan.py``).

On a langalpha turn all of it is model-call hooks (``middleware/plan.py``,
``middleware/plan_close_out.py``), which a Claude Code turn never runs, since
the CLI owns the model loop. The runtime asks the same functions here where
its steps meet the loop instead: each reply is counted as it is committed, a
check-in or a nudge rides the hook the CLI calls as a batch's last result
comes back, so it costs no request, and the close-out is one more query on
the same CLI after the answer. This is the one place the runtime knows the
plan.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.types import Command

from ptc_agent.agent.claude_code.mid_turn import ORDINARY, Query
from ptc_agent.agent.claude_code.runtime import node_updates
from ptc_agent.agent.middleware.plan import PLAN_CALLS_KEY, PlanCalls, counted, due_row
from ptc_agent.agent.middleware.plan_close_out import PlanCloseOutMiddleware, log_ask, plan_calls_only, tools_stopped
from ptc_agent.agent.middleware.runtime_context.durable import written_row
from ptc_agent.agent.tools.plan import PLAN_STATE_KEY, PLAN_TOOL_NAME

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.runtime import ClaudeTurn, _Run


class AgentPlan:
    """The plan as a Claude Code turn asks it. ``close_out`` is the turn's
    ``PlanCloseOutMiddleware``, which reads what will report back; None (a
    turn with no thread to read) asks no close-out."""

    def __init__(self, close_out: PlanCloseOutMiddleware | None = None) -> None:
        self._close_out = close_out

    @staticmethod
    def counted(state: Any, reply: AIMessage) -> dict[str, Any]:
        """The counts after ``reply``, for the step that commits it."""
        held: PlanCalls = state.get(PLAN_CALLS_KEY) or PlanCalls()
        calls = counted(held, reply)
        return {PLAN_CALLS_KEY: calls} if calls != held else {}

    @staticmethod
    def rows(state: Any, outs: Sequence[Any]) -> tuple[tuple[HumanMessage, ...], dict[str, Any]]:
        """The row due before Claude is asked again after a batch, read
        against the plan the batch's outputs ``outs`` left, and the counts
        it leaves."""
        steps = state.get(PLAN_STATE_KEY) or []
        for update in (u for out in outs for u in node_updates(out)):
            steps = update.get(PLAN_STATE_KEY, steps)
        calls, row = due_row(state.get(PLAN_CALLS_KEY) or PlanCalls(), steps)
        return ((written_row(row),), {PLAN_CALLS_KEY: calls}) if row is not None else ((), {})

    async def close_out_step(self, turn: ClaudeTurn, state: Any, run: _Run) -> Command | None:
        """The step that sends the close-out (``PlanCloseOutMiddleware``) as
        the turn's next query, after a query that ended on an answer leaving
        plan steps unfinished; None when none is sent. Only an ordinary
        query's end is followed by one: any other was the close-out. It goes
        only outside the turn's tool stop (``tools_stopped``), where the turn
        would send another query (``ClaudeTurn.sendable``),
        and the thread is read last, once nothing else holds it back.

        Its reply is never streamed and is kept for its plan calls alone;
        every other call is refused, and the CLI stops once the batch is
        done. One kept as nothing takes the ask out with it, as does a
        close-out that fails: the next turn's prompt would otherwise hand
        Claude an ask its session already holds."""
        if self._close_out is None or run.query is not ORDINARY:
            return None
        if (row := self._close_out.due(state)) is None or tools_stopped():
            return None
        ask = written_row(row)
        prompt = await turn.sendable(state, run, [ask])
        if prompt is None or not await self._close_out.nothing_pending(state):
            return None
        log_ask(row)
        await run.again(prompt, Query(
            admits=frozenset({PLAN_TOOL_NAME}),
            last_batch=True,
            streamed=False,
            keep=plan_calls_only,
            dropped=(RemoveMessage(id=ask.id),),
        ))
        return Command(update={"messages": [ask], **turn.commit(run.store.drain())}, goto="model")
