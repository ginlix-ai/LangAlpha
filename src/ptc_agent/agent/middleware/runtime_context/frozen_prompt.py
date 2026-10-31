"""The system prompt as the thread first sent it.

The static prompt is built with each turn's agent, from what holds at the
build, and it is the cached prefix. A value it states that can flip between
two turns (whether the file mount serves) is frozen into the epoch instead,
and the prompt is sent rendered with the frozen value whenever the build saw
another, so the prefix changes only when the epoch is rebuilt.

A deploy changes the build itself, and Claude's thinking is bound to the
system prompt in front of it: a block stays valid only while that prompt is
sent byte for byte as it was. So the whole system message, as the stack
composed it, is kept in checkpointed state and sent again for as long as any
of that thinking is in view. A build whose message differs marks the kept one
stale, and the next summary at a turn start keeps no turns, so the call after
it takes the new message (see ``rebuild_pending``).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from langchain.agents.middleware import AgentState
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
)
from langchain_core.messages import AIMessage, SystemMessage
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.agent.middleware.runtime_context.epoch import BaselineEpoch
from ptc_agent.agent.middleware.runtime_context.state import (
    STATE_BASELINE,
    as_dict,
    state_get,
)
from ptc_agent.agent.middleware.thinking_binding import holds_bound_thinking

logger = logging.getLogger(__name__)


class FrozenPromptMiddleware(AgentMiddleware):
    """Send the static prompt with the epoch's ``files_mounted``.

    Outermost on the system message: it replaces the prompt the agent was
    built with, and every middleware inside it appends to the replacement.

    Args:
        files_mounted: The value the built prompt states.
        render: The static prompt stating the value given, built the way the
            agent's own was.
    """

    def __init__(self, *, files_mounted: bool, render: Callable[[bool], str]) -> None:
        super().__init__()
        self._built = files_mounted
        self._render = render
        self._rendered: dict[bool, str] = {}

    def _frozen_prompt(self, request: ModelRequest) -> str | None:
        epoch = BaselineEpoch.from_state(
            state_get(getattr(request, "state", None), STATE_BASELINE)
        )
        frozen = epoch.files_mounted
        if frozen is None or frozen == self._built:
            return None
        if frozen not in self._rendered:
            self._rendered[frozen] = self._render(frozen)
        return self._rendered[frozen]

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        try:
            prompt = self._frozen_prompt(request)
        except Exception:  # noqa: BLE001 - the built prompt is still a whole prompt
            logger.warning("[FrozenPrompt] render failed; sending the built prompt", exc_info=True)
            return await handler(request)
        if prompt is None:
            return await handler(request)
        return await handler(request.override(system_message=SystemMessage(content=prompt)))


#: The system message a thread's thinking is bound to, the model it was sent
#: to, and ``stale`` once a build composed another.
STATE_SYSTEM_PIN = "runtime_system_pin"


def rebuild_pending(state: Any) -> bool:
    """Whether the thread's kept system message is one a build no longer
    composes, which only a summary that keeps no thinking can replace."""
    return bool(as_dict(state_get(state, STATE_SYSTEM_PIN)).get("stale"))

# Cache markers are outside the prefix check, so they are outside the
# comparison too.
_MARKER_KEYS = frozenset({"cache_control", "prompt_cache_breakpoint"})


class SystemPinState(AgentState):
    runtime_system_pin: NotRequired[Annotated[dict[str, Any], PrivateStateAttr]]


class SystemPinMiddleware(AgentMiddleware):
    """Send the system message Claude's thinking in view was produced under.

    Innermost on the system message, so what it keeps is everything the stack
    put there: the static prompt, what middleware appended to it, and the
    baseline block. The stack's message replaces the kept one only where no
    thinking is lost by it: nothing the request sends holds Claude's thinking
    (the call after a summary that kept none, a model that does not think), or
    the model changed, which drops the thinking the new model cannot read in
    any case. So a deploy reaches a thread at such a point, and every new
    thread at once. Read off the request, so a summary's own request, which
    sends the thinking it summarizes, gets the kept message, and the call
    after it, in the same model call, already the new one.
    """

    state_schema = SystemPinState

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback: the async agent won't call this but the protocol requires it.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse | ExtendedModelResponse:
        try:
            pinned, update, change = _pin(request)
        except Exception:  # noqa: BLE001 - the stack's message is still a whole one
            logger.warning("[SystemPin] pin failed; sending the stack's message", exc_info=True)
            return await handler(request)
        if pinned is not None:
            request = request.override(system_message=pinned)
        response = await handler(request)
        if update is None:
            return response
        if change is not None:
            update = {**update, "change": change, "changed_at": _answer_id(response)}
        return ExtendedModelResponse(
            model_response=response, command=Command(update={STATE_SYSTEM_PIN: update})
        )

    def after_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        _log_committed_change(state)
        return None

    async def aafter_model(self, state: Any, runtime: Any = None) -> dict[str, Any] | None:
        _log_committed_change(state)
        return None


def _answer_id(response: Any) -> str | None:
    """The id of the answer a call returned, which marks the pin change it made."""
    result = getattr(response, "result", None) or ()
    return next((m.id for m in reversed(result) if isinstance(m, AIMessage)), None)


def _log_committed_change(state: Any) -> None:
    """Log the pin change the model step just stored, once.

    Logged after the step rather than inside the call: a call can be made and
    its update thrown away (a summary's own request, a response the outer
    middleware refuses and retries), and only the step's stored pin says which
    change happened. The change is the step's when it names the step's answer.
    """
    pin = as_dict(state_get(state, STATE_SYSTEM_PIN))
    change = pin.get("change")
    messages = state_get(state, "messages") or []
    answer = next((m.id for m in reversed(messages) if isinstance(m, AIMessage)), None)
    if change is None or answer is None or pin.get("changed_at") != answer:
        return
    thread_id, agent = run_ids()
    logger.info("[SystemPin] change=%s model=%s thread_id=%s agent=%s", change, pin.get("model"), thread_id, agent)


def _pin(
    request: ModelRequest,
) -> tuple[SystemMessage | None, dict[str, Any] | None, str | None]:
    """The kept message to send instead of the stack's, the pin to store, and
    which change storing it makes: ``stale`` (the build differs while thinking
    bound to the kept message is in view), ``cleared`` (the build matches it
    again), or what let the build's message replace it: ``first``,
    ``model_changed``, ``no_bound_thinking``.

    Kept only for Claude, whose thinking is the only kind bound to the system
    message. A call to another model leaves the stored pin as it is: it may be
    a fallback answering for Claude, whose thinking is still bound to the pin.
    """
    from ptc_agent.agent.middleware.compaction.measure import is_claude
    from ptc_agent.agent.middleware.compaction.summarize import model_label

    built = request.system_message
    if built is None:
        return None, None, None
    stored = as_dict(state_get(getattr(request, "state", None), STATE_SYSTEM_PIN))
    if not is_claude(request.model):
        return None, None, None
    model = model_label(request.model) or ""
    content = stored.get("content")
    if content is not None and stored.get("model") == model:
        if _unmarked(content) == _unmarked(built.content):
            if not stored.get("stale"):
                return None, None, None
            return None, {"model": model, "content": content}, "cleared"
        if holds_bound_thinking(request.messages):
            if stored.get("stale"):
                return SystemMessage(content=content), None, None
            return SystemMessage(content=content), {**stored, "stale": True}, "stale"
        change = "no_bound_thinking"
    else:
        change = "first" if content is None else "model_changed"
    return None, {"model": model, "content": built.content}, change


def _unmarked(content: Any) -> list[Any]:
    """``content`` as blocks without cache markers, a string read as one text block."""
    blocks = content if isinstance(content, list) else [content]
    return [
        {"type": "text", "text": block}
        if isinstance(block, str)
        else {k: v for k, v in block.items() if k not in _MARKER_KEYS}
        if isinstance(block, dict)
        else block
        for block in blocks
    ]
