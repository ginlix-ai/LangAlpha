"""Thread state for the agent's plan (``UpdatePlan``), and its check-ins.

``advance`` names no steps, so the tool resolves it against the plan it last
wrote. Keeping that plan in state, not only in a tool call's arguments, is
what lets it outlive compaction and lets replay read the resolved list.

The plan is the reader's view of the work, so one that stops moving while the
work goes on misleads them. A model that does the steps without advancing
them is the common failure, so a run of tool calls with a step still shown
running gets a durable row saying so, as the notes check-in does. A request
that runs a while with no plan gets one row asking for one, whether to show
it left to the model, since long work can still be one step.

A plan belongs to the request that set it, which runs from the user's message
through the turns that carry it on (a report-back, a wake, a resumed
interrupt). A new request starts with no plan, so ``advance`` cannot move one
left over from work the user has moved past.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated, Any, NotRequired, TypedDict

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import (
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
)
from langchain_core.messages import AIMessage
from langgraph.types import Command

from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.agent.middleware.plan_close_out import (
    PLAN_REQUEST_AT_KEY,
    close_out_due,
    plan_calls_only,
)
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import (
    PLAN_CHECK_IN_ROW_KIND,
    PLAN_NUDGE_ROW_KIND,
)
from ptc_agent.agent.tools.plan import (
    PLAN_STATE_KEY,
    PLAN_TOOL_NAME,
    PlanStepDict,
    current_index,
)

logger = logging.getLogger(__name__)

#: Tool calls a step may show as running, since the plan last moved or was
#: checked in on, before the check-in. Short enough that the reader does not
#: watch a stale step for long, long enough that a step with real work in it
#: is not interrupted.
PLAN_CHECK_IN_CALLS = 6

#: Tool calls a request may make with no plan before it is asked for one.
#: Past what a short answer takes, so a quick request is never asked.
PLAN_NUDGE_CALLS = 8

#: State key of the request's call counts (``PlanCalls``).
PLAN_CALLS_KEY = "_plan_calls"


class PlanCalls(TypedDict, total=False):
    """The request's tool calls as the check-in and the nudge count them."""

    #: Since the plan last moved or was checked in on.
    since_touch: int
    #: Before the request first called ``UpdatePlan``; None once it has, or
    #: was asked for a plan, so no nudge is due again.
    without_plan: int | None


class PlanState(AgentState):
    """``plan`` holds the steps in order. The tool applies one call per
    message (``_superseded``), so a step writes it once."""

    plan: Annotated[NotRequired[list[PlanStepDict]], PrivateStateAttr]
    _plan_calls: Annotated[NotRequired[PlanCalls], PrivateStateAttr]
    _plan_request_at: Annotated[NotRequired[float], PrivateStateAttr]


def counted(calls: PlanCalls, message: AIMessage) -> PlanCalls:
    """``calls`` with the calls of one model message counted. One that calls
    ``UpdatePlan`` moves the plan, so its other calls do not count against
    the step."""
    if any(call.get("name") == PLAN_TOOL_NAME for call in message.tool_calls):
        return PlanCalls(since_touch=0, without_plan=None)
    n = len(message.tool_calls)
    without = calls.get("without_plan", 0)
    return PlanCalls(
        since_touch=calls.get("since_touch", 0) + n,
        without_plan=None if without is None else without + n,
    )


def plan_nudge(steps: Sequence[PlanStepDict], calls: PlanCalls) -> DurableUpdate | None:
    count = calls.get("without_plan", 0)
    if steps or count is None or count < PLAN_NUDGE_CALLS:
        return None
    return DurableUpdate(
        kind=PLAN_NUDGE_ROW_KIND,
        schema_version=UPDATE_SCHEMA_VERSION,
        text=f"You have made {count} tool calls on this request without a plan.",
        provenance={"source": "harness", "calls": count},
    )


def plan_check_in(steps: Sequence[PlanStepDict], calls: PlanCalls) -> DurableUpdate | None:
    cur = current_index(steps)
    if cur is None:
        return None
    count = calls.get("since_touch", 0)
    if count < PLAN_CHECK_IN_CALLS:
        return None
    return DurableUpdate(
        kind=PLAN_CHECK_IN_ROW_KIND,
        schema_version=UPDATE_SCHEMA_VERSION,
        text=(
            f"Step {cur + 1} of {len(steps)}, “{steps[cur]['title']}”, has been "
            f"the step in progress for your last {count} tool calls."
        ),
        provenance={"source": "harness", "step": cur, "calls": count},
    )


def due_row(calls: PlanCalls, steps: Sequence[PlanStepDict]) -> tuple[PlanCalls, DurableUpdate | None]:
    """The row due before the model is next asked, given the request's
    counts and the plan ``steps`` now, and the counts after it: a check-in
    restarts the count, and a nudge is not asked again. A runtime that runs
    no model-call hooks (Claude Code) asks this where its next request
    starts."""
    row = plan_check_in(steps, calls)
    if row is not None:
        calls = PlanCalls({**calls, "since_touch": 0})
    elif (row := plan_nudge(steps, calls)) is not None:
        calls = PlanCalls({**calls, "without_plan": None})
    if row is not None:
        thread_id, agent = run_ids()
        logger.info(
            "[Plan] %s calls=%s thread_id=%s agent=%s",
            row.kind,
            row.provenance["calls"],
            thread_id,
            agent,
        )
    return calls, row


class PlanStateMiddleware(AgentMiddleware):
    """Declares the ``plan`` key, which the tool writes through its Command,
    clears it when a new request starts, checks in when a running step stops
    moving, and asks once for a plan when a request runs long without one.

    ``continues`` says this request's turn carries an earlier one's on
    (``TurnContext.continues_request``). A resumed interrupt re-enters at the
    interrupted node and never reaches ``before_agent``, so it keeps the plan
    too. The instance is built per request, so a second pass through
    ``before_agent`` on it is the orchestrator re-entering the same request.
    Where a request starts is known only here, so its time is kept beside
    the plan (``PLAN_REQUEST_AT_KEY``).

    The calls are counted where each model response comes back, so only what
    the model sent counts: a call another middleware put in the thread (a
    steering resume) is not the model's step.
    """

    state_schema = PlanState

    def __init__(self, *, continues: bool = False) -> None:
        super().__init__()
        self._continues = continues
        self._entered = False

    async def abefore_agent(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        first, self._entered = not self._entered, True
        if self._continues or not first:
            return None
        return {**(cleared(state) or {}), PLAN_REQUEST_AT_KEY: time.time()}

    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        self._entered = True
        held: PlanCalls = state.get(PLAN_CALLS_KEY) or PlanCalls()
        calls, row = due_row(held, state.get(PLAN_STATE_KEY) or [])
        if row is None:
            return None
        return {PLAN_CALLS_KEY: calls, "messages": [build_update_message(row)]}

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ExtendedModelResponse:
        response = await handler(request)
        if isinstance(response, AIMessage):
            response = ModelResponse(result=[response])
        state = request.state or {}
        # The close-out's reply is kept for its plan calls alone, so only
        # those count.
        keep = plan_calls_only if close_out_due(state.get("messages") or ()) else None
        held: PlanCalls = state.get(PLAN_CALLS_KEY) or PlanCalls()
        calls = held
        for message in response.result:
            if isinstance(message, AIMessage):
                calls = counted(calls, keep(message) if keep else message)
        command = Command(update={PLAN_CALLS_KEY: calls}) if calls != held else None
        return ExtendedModelResponse(model_response=response, command=command)


def cleared(state: Any) -> dict[str, Any] | None:
    """What takes a plan and its counts out of the thread, or None when it
    holds neither."""
    update: dict[str, Any] = {}
    if state.get(PLAN_STATE_KEY):
        update[PLAN_STATE_KEY] = []
    if state.get(PLAN_CALLS_KEY):
        update[PLAN_CALLS_KEY] = PlanCalls()
    return update or None


class PlanOffMiddleware(AgentMiddleware):
    """With the plan turned off, takes away the one an earlier turn left, so
    a summary or a brief never lists a plan the agent can no longer move."""

    state_schema = PlanState

    async def abefore_agent(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        return cleared(state)

