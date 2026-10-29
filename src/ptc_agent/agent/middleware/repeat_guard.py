"""Refuses a tool call that keeps checking on work that has not finished.

With no step cap on a turn, the one thing that stopped a stuck agent early is
gone. The stuck agents seen in prod were all waiting: polling a run that had
not finished, with no way to sleep and nothing telling them to end the turn,
dozens of times in a row. Real work never looks like that, so two patterns are
refused, each with a refusal that says why, which is what the stuck agent was
missing:

- a call that has returned the identical result three times in a row;
- a status read after three rounds in a row made only of reads that found
  their work still running. Those reads never come back identical (elapsed
  time, growing output), so the tool marks each one on its artifact
  (``limits.pending_read``) and the guard counts the marks.

A model that keeps calling after three rounds in a row had every call
refused, by this guard or by the turn's clock, is no longer reading its
results, so the run ends there, before the next model call.

Read off the turn's own history, never kept on the instance, so a resumed or
re-routed turn counts the same calls.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable, Iterator, Sequence
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ToolCallRequest, hook_config
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from ptc_agent.agent.limits import REFUSAL_KEY, is_pending_read, is_refusal
from ptc_agent.agent.middleware.tool.argument_parsing import parse_tool_args

# How many times in a row the same call may return the same result before the
# next identical call is refused.
REPEAT_LIMIT = 3

REPEAT_REFUSAL = (
    "Not run: this exact call has returned the same result {count} times in a "
    "row, so another call will not change it. If you are waiting on work that "
    "reports back when it finishes, end your turn now and it will. Otherwise "
    "do something different, or answer with what you have."
)

# How many rounds in a row may be made only of reads that found their work
# still running before the next read of that work is refused. A round is one
# model message's calls, so reads batched into one message count once.
POLL_LIMIT = 3

POLL_REFUSAL = (
    "Not run: the last {count} checks found this work still running, and "
    "checking again will not make it finish sooner. If it reports back when it "
    "finishes, end your turn now and it will. Otherwise do something "
    "different, or answer with what you have."
)


def _call_key(name: str, args: Any) -> str:
    # Through the parse argument parsing applies, on both sides. That
    # middleware rewrites a call's args in place on the AIMessage, so live
    # history holds the parsed form, while the request reaching this guard
    # (outer to the parser) and a thread reloaded from its checkpoint hold the
    # form the model wrote. Parsing is idempotent, so either order matches.
    args = parse_tool_args(args, name)
    try:
        return name + "\x00" + json.dumps(args, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return name + "\x00" + repr(args)


def _content_key(message: ToolMessage) -> str:
    content = message.content
    if isinstance(content, str):
        return content
    try:
        return json.dumps(content, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return repr(content)


def tool_rounds(messages: Iterable[Any]) -> Iterator[tuple[AIMessage, list[ToolMessage]]]:
    """The turn's latest rounds, newest first: each AIMessage with the results after it.

    Only the tail since the last message that is neither the model's nor a
    tool's can hold a streak, and only as much of it as the caller reads is
    walked, in place: with no step cap a turn's tail can run to thousands of
    rounds, and this runs on the event loop before every tool call.
    """
    if not isinstance(messages, Sequence):
        messages = list(messages)
    results: list[ToolMessage] = []
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if isinstance(message, ToolMessage):
            results.append(message)
        elif isinstance(message, AIMessage):
            results.reverse()
            yield message, results
            results = []
        else:
            return


def _call_for(message: AIMessage, call_id: str) -> dict[str, Any] | None:
    for call in message.tool_calls or ():
        if call.get("id") == call_id:
            return call
    return None


def repeat_streak(messages: Iterable[Any], key: str) -> int:
    """How many of the latest tool results, back to back, answered ``key`` alike.

    Walks back from the newest message. A result from any other call, or any
    message from the user or the harness, ends the streak; the model's own
    messages between the calls do not. A refusal neither counts nor ends it, so
    a refused call stays refused until the agent does something else. Only the
    calls the walk reaches are serialized.
    """
    streak = 0
    result: str | None = None
    for message, results in tool_rounds(messages):
        for tool_message in reversed(results):
            call = _call_for(message, tool_message.tool_call_id)
            if call is None or _call_key(call.get("name", ""), call.get("args")) != key:
                return streak
            if is_refusal(tool_message):
                continue
            content = _content_key(tool_message)
            if result is None:
                result = content
            elif content != result:
                return streak
            streak += 1
    return streak


def poll_streak(messages: Iterable[Any]) -> tuple[int, list[dict[str, Any]]]:
    """How many of the latest rounds, back to back, were made only of reads that
    found their work still running, and the calls those reads were.

    A round with any other result ends it. A refusal neither counts nor ends
    it, as in ``repeat_streak``, and neither does the round being run, whose
    results are not in yet.
    """
    rounds = 0
    reads: list[dict[str, Any]] = []
    for message, results in tool_rounds(messages):
        counted = False
        for tool_message in results:
            if is_refusal(tool_message):
                continue
            call = _call_for(message, tool_message.tool_call_id)
            if call is None or not is_pending_read(tool_message):
                return rounds, reads
            reads.append(call)
            counted = True
        rounds += counted
    return rounds, reads


# How many rounds in a row may have every call refused before the run ends.
END_LIMIT = 3


def refused_streak(messages: Iterable[Any]) -> int:
    """How many of the latest rounds, back to back, had every call refused."""
    rounds = 0
    for _, results in tool_rounds(messages):
        if not results or not all(is_refusal(result) for result in results):
            return rounds
        rounds += 1
    return rounds


def _end_if_refusals_ignored(state: Any) -> dict[str, Any] | None:
    # Every call this round was answered, refusals included, so the history
    # is as valid to end on as to send to the model, and a resumed thread
    # carries no unanswered call. The jump is create_agent's own exit (as
    # ModelCallLimitMiddleware's), which still runs the after_agent hooks.
    # It adds no message: the refusals already told the model why, and the
    # end of the run is what leaves the waiting to the work that reports back.
    messages = state.get("messages") if isinstance(state, dict) else None
    if messages and refused_streak(messages) >= END_LIMIT:
        return {"jump_to": "end"}
    return None


def _refusal(call: dict[str, Any], content: str, kind: str) -> ToolMessage:
    return ToolMessage(
        content=content,
        tool_call_id=call.get("id"),
        name=call.get("name", ""),
        status="error",
        additional_kwargs={REFUSAL_KEY: kind},
    )


class RepeatCallGuardMiddleware(AgentMiddleware):
    """Refuses a poll of unfinished work past ``POLL_LIMIT`` rounds, and an
    identical call once it has answered alike ``REPEAT_LIMIT`` times running.
    Ends the run once ``END_LIMIT`` rounds in a row had every call refused."""

    # Before the model rather than from the tool wrapper: a Command(goto=END)
    # from a tool is one branch beside the tools node's own edge back to the
    # model, so the model is called anyway.
    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        return _end_if_refusals_ignored(state)

    @hook_config(can_jump_to=["end"])
    def before_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        return _end_if_refusals_ignored(state)

    # Async only, as ``SubagentSwitchMiddleware``'s is: the async agent never
    # calls the sync hook.
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        call = request.tool_call
        state = request.state if isinstance(request.state, dict) else {}
        messages = state.get("messages") or ()
        key = _call_key(call.get("name", ""), call.get("args"))
        # Only a read the streak already made is refused: a read of other work
        # has not been found running, and a stop or any other call is the agent
        # doing something different, which is what the refusal asks for.
        polls, reads = poll_streak(messages)
        if polls >= POLL_LIMIT and any(
            _call_key(read.get("name", ""), read.get("args")) == key for read in reads
        ):
            return _refusal(call, POLL_REFUSAL.format(count=POLL_LIMIT), "poll")
        streak = repeat_streak(messages, key)
        if streak >= REPEAT_LIMIT:
            return _refusal(call, REPEAT_REFUSAL.format(count=streak), "repeat")
        return await handler(request)
