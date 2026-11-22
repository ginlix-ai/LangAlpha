"""The plan's close-out: one silent model call after a final answer that left
plan steps unfinished, so a plan the agent forgot to advance does not end the
request reading as undone work.

It runs only when nothing the request started will report back: a subagent,
a command, a wakeup or an Analyst still out, or ended with its report not yet
in, brings the agent back to the plan in a later turn, which reads as waiting
until then. What an earlier request set is no work of this plan, and a
standing wakeup from one would otherwise hold every later plan open. An
automation does not count: its run is a request of its own, which starts
with no plan. Nor is it asked where it cannot be answered or should not be
paid for: past the credit gate's stop, for an account that may no longer
work, or once the turn's tools have stopped, which refuse ``UpdatePlan`` too.

The call is never streamed, and its reply keeps only its ``UpdatePlan``
calls, so the answer the user was shown stays the turn's last text: the chat
shows a folded turn's last text as its answer, and a trailing text here would
put a reply about the plan there instead. It is best-effort: the user
already has the answer, so a call that fails ends the turn as if none had
been asked.

Whether one was asked, and whether the next call is it, are read off the
thread: the ask is a row, and the turn's own row (``turn_opened``) bounds the
turn. Not forced with ``tool_choice``: Claude with adaptive thinking drops a
forced choice, and other providers differ, so the ask does the work and the
reply is cut to its plan calls whatever the model sends.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import (
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
    hook_config,
)
from langchain_core.messages import AIMessage, RemoveMessage
from langgraph.errors import GraphBubbleUp, NodeCancelledError, NodeTimeoutError
from langgraph.types import Command

from ptc_agent.agent.middleware._utils import run_ids, unstreamed
from ptc_agent.agent.middleware.credit_gate import account_ends, credit_stops
from ptc_agent.agent.middleware.repeat_guard import tool_rounds
from ptc_agent.agent.middleware.runtime_context.changes import UPDATE_SCHEMA_VERSION
from ptc_agent.agent.middleware.runtime_context.durable import (
    DurableUpdate,
    build_update_message,
    runtime_update_from_message,
)
from ptc_agent.agent.middleware.runtime_context.turn import (
    PLAN_CLOSE_OUT_ROW_KIND,
    TURN_ROW_KIND,
    _run_clock,
)
from ptc_agent.agent.tools.plan import PLAN_STATE_KEY, PLAN_TOOL_NAME
from ptc_agent.agent.transcript.classify import is_run_boundary_message

logger = logging.getLogger(__name__)

#: Marks the close-out's reply, which the end and the trim key on.
CLOSE_OUT_KEY = "plan_close_out"

#: State key of when the request began, epoch seconds, which the plan's
#: state middleware writes where a request starts: what was set to report
#: back before then is no work of this request's plan.
PLAN_REQUEST_AT_KEY = "_plan_request_at"

#: What can bring the agent back to this request's plan: an automation's run
#: is a new request, which clears it, so one set to run is no reason to wait.
REPORTS_BACK = ("command", "task", "wakeup", "analyst")

#: Content blocks that are what the model wrote to the user.
_TEXT_BLOCKS = frozenset({"text", "output_text"})
#: Content blocks that carry a call the tool node runs. A provider-executed
#: call (``server_tool_use``) is not among the message's tool calls, and its
#: result block sits beside it, so it stays with its result.
_CALL_BLOCKS = frozenset({"tool_use", "function_call"})

#: What a close-out call raises that is no failure of the call: LangGraph's
#: own control flow (an interrupt, a node it cancelled or timed out), which
#: must stop the run whatever the call was. Cancellation itself is a
#: ``BaseException`` and is never caught.
_NOT_A_FAILURE = (GraphBubbleUp, NodeCancelledError, NodeTimeoutError)


def is_close_out_reply(message: Any) -> bool:
    return isinstance(message, AIMessage) and bool(message.additional_kwargs.get(CLOSE_OUT_KEY))


def _is_ask(message: Any) -> bool:
    row = runtime_update_from_message(message)
    return row is not None and row.kind == PLAN_CLOSE_OUT_ROW_KIND


def _this_turn(messages: Sequence[Any]) -> Iterator[Any]:
    """The turn's messages, newest first, back to its own row or, where none
    was written, its input."""
    for message in reversed(messages):
        row = runtime_update_from_message(message)
        if (row is not None and row.kind == TURN_ROW_KIND) or is_run_boundary_message(message):
            return
        yield message


def asked_this_turn(messages: Sequence[Any]) -> bool:
    return any(_is_ask(m) for m in _this_turn(messages))


def close_out_due(messages: Sequence[Any]) -> bool:
    """Whether the next model call is the close-out: the newest message that
    is not a row is the call-less answer an ask follows. A steer delivered
    since is not a row, so the call is then an ordinary one, which answers
    it."""
    asked = False
    for message in reversed(messages):
        if runtime_update_from_message(message) is None:
            return asked and isinstance(message, AIMessage) and not message.tool_calls
        asked = asked or _is_ask(message)
    return False


def unasked(messages: Sequence[Any]) -> list[RemoveMessage]:
    """What takes out this turn's asks no model call has answered yet: for a
    call that turned out to be an ordinary one, or a close-out that ended
    without a reply."""
    removed: list[RemoveMessage] = []
    for message in _this_turn(messages):
        if isinstance(message, AIMessage):
            break
        if _is_ask(message) and message.id:
            removed.append(RemoveMessage(id=message.id))
    return removed


def close_out_ask(state: Any) -> DurableUpdate | None:
    """The ask after a final answer, or None when the plan has no unfinished
    step. Whether one was asked this turn, and whether anything will report
    back, are the caller's to read."""
    messages = state.get("messages") or ()
    last = messages[-1] if messages else None
    if not isinstance(last, AIMessage) or last.tool_calls:
        return None
    left = [s for s in state.get(PLAN_STATE_KEY) or () if s["status"] != "completed"]
    if not left:
        return None
    titles = ", ".join(f"“{s['title']}”" for s in left)
    noun = "plan step" if len(left) == 1 else "plan steps"
    return DurableUpdate(
        kind=PLAN_CLOSE_OUT_ROW_KIND,
        schema_version=UPDATE_SCHEMA_VERSION,
        text=f"Your turn ended with {len(left)} {noun} not marked finished: {titles}.",
        provenance={"source": "harness", "left": len(left)},
    )


def request_began(state: Any) -> datetime | None:
    """When the request ``state`` is in began, or None for one that began
    before it was kept, which then bounds nothing."""
    at = (state or {}).get(PLAN_REQUEST_AT_KEY)
    return datetime.fromtimestamp(at, tz=UTC) if isinstance(at, int | float) else None


def turn_began(state: Any) -> datetime | None:
    """When the turn ``state`` ends on began, by its own row, or None where
    it wrote none."""
    for message in reversed((state or {}).get("messages") or ()):
        row = runtime_update_from_message(message)
        if row is not None and row.kind == TURN_ROW_KIND:
            at = row.created_at
            return at if at.tzinfo is not None else at.replace(tzinfo=UTC)
        if is_run_boundary_message(message):
            return None
    return None


def tools_stopped() -> bool:
    """Whether the run is inside its tool stop, where every call is refused
    (``TimeUpToolStopMiddleware``)."""
    clock = _run_clock()
    return clock is not None and clock[0] - datetime.now(tz=UTC) <= clock[2]


def log_ask(row: DurableUpdate) -> None:
    """Log a close-out ask being sent."""
    thread_id, agent = run_ids()
    logger.info(
        "[Plan] close-out left=%s thread_id=%s agent=%s",
        row.provenance["left"],
        thread_id,
        agent,
    )


def plan_calls_only(message: AIMessage) -> AIMessage:
    """``message`` with its text and every call but ``UpdatePlan`` taken out,
    marked as the close-out's reply. Reasoning stays, since a provider may
    require it beside the calls it led to. A Responses ``function_call``
    block's ``id`` is its item id; ``call_id`` is the call's."""
    calls = [c for c in message.tool_calls if c.get("name") == PLAN_TOOL_NAME]
    kept = {c.get("id") for c in calls}
    content: Any = ""
    if isinstance(message.content, list):
        content = [
            block
            for block in message.content
            if not (
                isinstance(block, dict)
                and (
                    block.get("type") in _TEXT_BLOCKS
                    or (
                        block.get("type") in _CALL_BLOCKS
                        and (block.get("call_id") or block.get("id")) not in kept
                    )
                )
            )
            and not isinstance(block, str)
        ]
    return message.model_copy(
        update={
            "content": content,
            "tool_calls": calls,
            "invalid_tool_calls": [],
            "additional_kwargs": {**message.additional_kwargs, CLOSE_OUT_KEY: True},
        }
    )


def _response(result: Any) -> ModelResponse:
    return ModelResponse(result=[result]) if isinstance(result, AIMessage) else result


class PlanCloseOutMiddleware(AgentMiddleware):
    """Asks for the close-out after a final answer, makes its call silent,
    and ends the run once its plan calls have run.

    The end is before the model rather than from the tool, as the quiet guard
    does, so the middleware is mounted with that guard, ahead of steering,
    the credit gate and the context rows: a model call that never happens
    takes in no steer and writes no row. That also makes it the outermost
    after-model hook, which it must be: approvals and orders read the model's
    own message as the newest, and its jump back to the model skips none.

    ``work_pending`` says whether anything the request in the state it is
    given set (None: any) is to report back to the thread; a read that fails
    should say yes, since the agent may then be coming back to the plan on
    its own.
    """

    def __init__(self, work_pending: Callable[[Any], Awaitable[bool]]) -> None:
        super().__init__()
        self._work_pending = work_pending

    @classmethod
    def for_thread(
        cls, thread_id: str, user_id: str | None, *, analysts: bool
    ) -> PlanCloseOutMiddleware:
        """Reads what is pending as ThreadStatus lists it, an Analyst still
        out among it for a Chief of Staff: what the request set, and of that
        what ended since the turn began with its report still to come. A
        task's report-back leaves its run unstamped, so one that ended
        earlier may well have been told already, in this turn or another."""
        # Imported here: the thread status tool reaches back into the
        # middleware package.
        from src.tools.thread_status.tool import pending_work

        async def work_pending(state: Any) -> bool:
            try:
                return (
                    await pending_work(
                        thread_id,
                        user_id,
                        analysts=analysts,
                        kinds=REPORTS_BACK,
                        since=request_began(state),
                        ended_since=turn_began(state),
                    )
                    is not None
                )
            except Exception:  # noqa: BLE001 - unread counts as pending: no close-out
                logger.warning("[Plan] pending read failed; no close-out", exc_info=True)
                return True

        return cls(work_pending)

    def due(self, state: Any) -> DurableUpdate | None:
        """The ask after the answer ``state`` ends on, unless one was asked
        this turn or the plan has nothing unfinished. What will report back
        is read apart (``nothing_pending``), last, since it reads the
        thread."""
        row = close_out_ask(state)
        if row is None or asked_this_turn(state.get("messages") or ()):
            return None
        return row

    async def nothing_pending(self, state: Any = None) -> bool:
        """Whether nothing the request in ``state`` set will report back;
        without a state, nothing at all."""
        return not await self._work_pending(state)

    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        latest = next(tool_rounds(state.get("messages") or ()), None)
        if latest is not None and is_close_out_reply(latest[0]):
            return {"jump_to": "end"}
        return None

    @hook_config(can_jump_to=["model", "end"])
    async def aafter_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        messages = state.get("messages") or ()
        last = messages[-1] if messages else None
        if is_close_out_reply(last) and not last.tool_calls:
            # Nothing to apply: the reply and its ask leave no trace, and the
            # run ends. A later turn would otherwise read an ask nothing answered.
            removed = [RemoveMessage(id=last.id)] if last.id else []
            return {"messages": [*removed, *unasked(messages[:-1])], "jump_to": "end"}
        row = self.due(state)
        if row is None or tools_stopped():
            return None
        # The jump back to the model passes the credit gate again, which
        # would pause or fail a turn whose answer is in only to buy this call.
        if credit_stops() or await account_ends() or not await self.nothing_pending(state):
            return None
        log_ask(row)
        return {"messages": [build_update_message(row)], "jump_to": "model"}

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> Any:
        messages = (request.state or {}).get("messages") or ()
        if close_out_due(messages):
            return await self._close_out(request, handler, messages)
        stale = unasked(messages)
        if not stale:
            return await handler(request)
        # A steer came in after the ask, so this call answers it: the ask
        # would tell the model to discard the very answer the user is owed.
        gone = {r.id for r in stale}
        response = await handler(request.override(messages=[m for m in request.messages if m.id not in gone]))
        return ExtendedModelResponse(
            model_response=_response(response), command=Command(update={"messages": stale})
        )

    async def _close_out(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        messages: Sequence[Any],
    ) -> Any:
        with unstreamed():
            try:
                response = _response(await handler(request))
            except _NOT_A_FAILURE:
                raise
            except Exception:  # noqa: BLE001 - a provider's refusal, rate limit or outage
                # The answer is the user's already: the turn ends on it, with
                # the ask taken out, as if none had been asked. The empty
                # reply is dropped as one with no plan call is.
                logger.warning("[Plan] close-out call failed; the turn ends on its answer", exc_info=True)
                nothing = AIMessage(content="", additional_kwargs={CLOSE_OUT_KEY: True})
                return ExtendedModelResponse(
                    model_response=ModelResponse(result=[nothing]),
                    command=Command(update={"messages": unasked(messages)}),
                )
        return dataclasses.replace(
            response,
            result=[plan_calls_only(m) if isinstance(m, AIMessage) else m for m in response.result],
        )
