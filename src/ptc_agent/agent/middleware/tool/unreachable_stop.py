"""End a turn nobody is watching once its computer stops answering.

A tool hands a failed call back to the model as text, so an interactive turn
lets the agent decide what to do about a machine that went to sleep. On a turn
nobody is watching (an automation's run, a wake, a report-back, a hand-off),
nobody can wake the machine, and every further call fails the same way while
each model call in between is paid for. So such a turn ends on the first tool
call that found its machine not answering, finalizing as a failed run whose
``error_kind`` is ``computer_unreachable``, as a turn that could not reach the
machine at its start does.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from ptc_agent.core.sandbox.providers.ssh_errors import connect_failure
from ptc_agent.core.sandbox.unreachable import watch_unreachable


class ComputerUnreachableError(RuntimeError):
    """The turn's computer did not answer a tool call on a turn nobody watches.

    Raised from the connect failure, which the run's failure classifiers find
    on the cause chain for its code. The message is the sentence its owner can
    act on, the one a turn that failed at its start shows.
    ``unreachable_computer_id`` names the computer that did not answer, when
    the caller knows it (``unreachable.unreachable_computer_id``).
    """

    def __init__(self, failure: BaseException, *, computer_id: str | None = None) -> None:
        connect = connect_failure(failure)
        super().__init__(connect.for_owner if connect is not None else str(failure))
        self.unreachable_computer_id = computer_id


class ComputerUnreachableStopMiddleware(AgentMiddleware):
    """Raise :class:`ComputerUnreachableError` after a tool call whose calls on
    ``sandbox`` ended with its machine not answering.

    Must sit outside ``ToolErrorHandlingMiddleware``, which would turn the
    raise back into a tool result. In a subagent it ends the subagent's run,
    whose failure its parent reads; the parent's own next call on the machine
    then ends the turn.
    """

    def __init__(self, sandbox: Any, *, computer_id: str | None = None) -> None:
        super().__init__()
        self._sandbox = sandbox
        self._computer_id = computer_id

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        # Sandbox calls are async; a sync tool never reaches the machine.
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        with watch_unreachable(self._sandbox) as watch:
            result = await handler(request)
        if watch.failure is not None:
            raise ComputerUnreachableError(
                watch.failure, computer_id=self._computer_id
            ) from watch.failure
        return result
