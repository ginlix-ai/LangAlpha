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
from langchain_core.messages import SystemMessage
from langgraph.types import Command
from typing_extensions import NotRequired

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
            pinned, update = _pin(request)
        except Exception:  # noqa: BLE001 - the stack's message is still a whole one
            logger.warning("[SystemPin] pin failed; sending the stack's message", exc_info=True)
            return await handler(request)
        if pinned is not None:
            request = request.override(system_message=pinned)
        response = await handler(request)
        if update is None:
            return response
        return ExtendedModelResponse(
            model_response=response, command=Command(update={STATE_SYSTEM_PIN: update})
        )


def _pin(request: ModelRequest) -> tuple[SystemMessage | None, dict[str, Any] | None]:
    """The kept message to send instead of the stack's, and the pin to store.

    Kept only for Claude, whose thinking is the only kind bound to the system
    message. A call to another model leaves the stored pin as it is: it may be
    a fallback answering for Claude, whose thinking is still bound to the pin.
    """
    from ptc_agent.agent.middleware.compaction.measure import is_claude
    from ptc_agent.agent.middleware.compaction.summarize import model_label

    built = request.system_message
    if built is None:
        return None, None
    stored = as_dict(state_get(getattr(request, "state", None), STATE_SYSTEM_PIN))
    if not is_claude(request.model):
        return None, None
    model = model_label(request.model) or ""
    content = stored.get("content")
    if content is not None and stored.get("model") == model:
        if _unmarked(content) == _unmarked(built.content):
            return None, {"model": model, "content": content} if stored.get("stale") else None
        if holds_bound_thinking(request.messages):
            return SystemMessage(content=content), None if stored.get("stale") else {**stored, "stale": True}
    return None, {"model": model, "content": built.content}


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
