"""The system prompt as the thread first sent it.

The static prompt is built with each turn's agent, from what holds at the
build, and it is the cached prefix. A value it states that can flip between
two turns (whether the file mount serves) is frozen into the epoch instead,
and the prompt is sent rendered with the frozen value whenever the build saw
another, so the prefix changes only when the epoch is rebuilt.

A deploy changes the build itself, and a model that binds its reasoning to
the prefix (Claude, GPT-6.2 on; see ``thinking_binding``) keeps a payload only
while the system prompt in front of it is sent byte for byte as it was. So the
whole system message, as the stack composed it, is kept in checkpointed state
and sent again for as long as any of that reasoning is in view. On Claude a
build whose message differs marks the kept one stale, and the next summary at
a turn start keeps no turns, so the call after it takes the new message (see
``rebuild_pending``). OpenAI drops the reasoning on its own rather than refuse
it, so a deploy reaches an OpenAI thread at its next call, at the price of
the reasoning in view once, and only a per-turn input waits for a summary.

The pin also records a digest of the prompt's sources, so a thread is known
to be due for that summary before the call that would mark it stale, and a
deploy is told apart from a per-turn input (see ``due`` and
``sources_changed``).
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated, Any

from langchain.agents.middleware import AgentState
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
)
from langchain_core.messages import AIMessage, AnyMessage, SystemMessage
from langgraph.types import Command
from typing_extensions import NotRequired

from src.llms.model_identity import api_model_id, is_claude
from ptc_agent.agent.middleware._utils import run_ids
from ptc_agent.agent.middleware.runtime_context.epoch import BaselineEpoch
from ptc_agent.agent.middleware.runtime_context.state import (
    STATE_BASELINE,
    as_dict,
    state_get,
)
from ptc_agent.agent.middleware.model_resilience import answering_as_fallback
from ptc_agent.agent.middleware.thinking_binding import binds_reasoning, holds_bound_thinking
from ptc_agent.agent.prompts import release

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


def _static_text(content: Any) -> str | None:
    """The static prompt in a system message's ``content``: its first text
    block, or all of it when it is a string. FrozenPrompt sets that block and
    every writer inside it appends after it, so middleware at any depth reads
    the same one."""
    first = content[0] if isinstance(content, list) and content else content
    text = first.get("text") if isinstance(first, dict) else first
    return text if isinstance(text, str) else None


def static_digest(content: Any) -> str | None:
    """sha256 of the static prompt in a system message's ``content``."""
    text = _static_text(content)
    return hashlib.sha256(text.encode()).hexdigest() if text is not None else None


def _kept(state: Any, model: Any) -> dict[str, Any] | None:
    """The pin kept for ``model``, or None. Only a model that binds its
    reasoning to the prefix keeps one, and a call to another model takes the
    build's message at once."""
    pin = as_dict(state_get(state, STATE_SYSTEM_PIN))
    if pin.get("content") is None or not binds_reasoning(model):
        return None
    return pin if pin.get("model") == (api_model_id(model) or "") else None


def _holding(state: Any, model: Any) -> dict[str, Any] | None:
    """The pin that still decides what a call to ``model`` sends, or None:
    kept for it, and not one a deploy replaces at once (``_takes_deploy``)."""
    pin = _kept(state, model)
    return None if pin is None or _takes_deploy(model, state) else pin


def pinned_static(state: Any, model: Any, view: Sequence[AnyMessage]) -> str | None:
    """The static prompt a call to ``model`` sending ``view`` goes out with
    in place of the build's: the kept message's, while thinking bound to it is
    in view (see ``_pin``). None where the build's goes out."""
    pin = _holding(state, model)
    if pin is None or not holds_bound_thinking(view):
        return None
    return _static_text(pin["content"])


def due(request: ModelRequest, view: Sequence[AnyMessage]) -> bool:
    """Whether the thread's kept system message is one this build would not
    send and only a summary can replace: marked stale, or its static prompt
    differs from the build's while thinking bound to it is in ``view``, what
    the call sends. Read before the call that would mark it stale, so a deploy
    that landed while the thread was idle counts from its next first call. A
    thread with no bound thinking takes the build's message on its own call
    (see ``_pin``), and so does an OpenAI thread a deploy reached."""
    state = getattr(request, "state", None)
    pin = _holding(state, request.model)
    if pin is None or _superseded(state):
        return False
    if pin.get("stale"):
        return True
    built = request.system_message
    return (
        built is not None
        and static_digest(pin["content"]) != static_digest(built.content)
        and holds_bound_thinking(view)
    )


def sources_changed(state: Any) -> bool:
    """Whether the prompt's sources changed since the kept message's build,
    which a pin without ``source_digest`` counts as. Only a due thread whose
    sources changed may be forced to rebuild: a due that per-turn inputs
    alone caused would force one on every turn an input flips."""
    return as_dict(state_get(state, STATE_SYSTEM_PIN)).get("source_digest") != release.source_digest()


def _build_sources() -> dict[str, Any]:
    """What a pin records of the build whose static prompt it holds: the
    digest of its sources and its hand-set ``prompt_version``."""
    return {"source_digest": release.source_digest(), "prompt_version": release.prompt_version()}


# Cache markers are outside the prefix check, so they are outside the
# comparison too.
_MARKER_KEYS = frozenset({"cache_control", "prompt_cache_breakpoint"})


class SystemPinState(AgentState):
    runtime_system_pin: NotRequired[Annotated[dict[str, Any], PrivateStateAttr]]


class SystemPinMiddleware(AgentMiddleware):
    """Send the system message the bound reasoning in view was produced under.

    Innermost on the system message, so what it keeps is everything the stack
    put there: the static prompt, what middleware appended to it, and the
    baseline block. The stack's message replaces the kept one only where no
    thinking is lost by it: nothing the request sends holds bound reasoning
    (the call after a summary that kept none, a model that does not think), or
    the model changed, which drops the thinking the new model cannot read in
    any case. So a deploy reaches a thread at such a point, and every new
    thread at once. Read off the request, so a summary's own request, which
    sends the thinking it summarizes, gets the kept message, and the call
    after it, in the same model call, already the new one. A model that drops
    rather than refuses unbound reasoning takes a deploy at its next call
    instead (``_takes_deploy``).
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

    Kept only for a model that binds its reasoning to the prefix. A call to
    another model leaves the stored pin as it is: it may be a fallback
    answering for one, whose reasoning is still bound to the pin. So does a
    fallback to a binding model (``answering_as_fallback``), however many
    calls it answers; a model the thread was switched to takes the pin. On
    OpenAI a deploy is taken at once (``deployed``, see ``_takes_deploy``).
    """
    built = request.system_message
    if built is None or not binds_reasoning(request.model):
        return None, None, None
    replaced: dict[str, Any] = {}
    stored = _kept(getattr(request, "state", None), request.model)
    if stored is not None:
        content = stored["content"]
        if _unmarked(content) == _unmarked(built.content):
            # The build composes the kept message again: the pin takes its
            # sources, so a later due on this thread is read against them.
            current = {**{k: v for k, v in stored.items() if k != "stale"}, **_build_sources()}
            if current == stored:
                return None, None, None
            return None, current, "cleared" if stored.get("stale") else None
        if _takes_deploy(request.model, getattr(request, "state", None)):
            change = "deployed"
            replaced = {"replaced": [*stored.get("replaced", ()), stored.get("source_digest")][-_REPLACED_KEPT:]}
        elif holds_bound_thinking(request.messages):
            if stored.get("stale") or _superseded(getattr(request, "state", None)):
                return SystemMessage(content=content), None, None
            return SystemMessage(content=content), {**stored, "stale": True}, "stale"
        else:
            change = "no_bound_thinking"
    else:
        previous = as_dict(state_get(getattr(request, "state", None), STATE_SYSTEM_PIN))
        if previous.get("content") is None:
            change = "first"
        elif answering_as_fallback():
            return None, None, None
        else:
            change = "model_changed"
    model = api_model_id(request.model) or ""
    return None, {"model": model, "content": built.content, **_build_sources(), **replaced}, change


#: How many builds a pin remembers having replaced (``_takes_deploy``).
_REPLACED_KEPT = 8


def _takes_deploy(model: Any, state: Any) -> bool:
    """Whether a call to ``model`` takes a deploy's system message at once
    rather than at a summary: on a binding model other than Claude, whose API
    drops the reasoning the change unbinds where Anthropic's may refuse it,
    once the prompt's sources changed. A per-turn input that may flip back
    still waits, as on Claude.

    A build the pin already replaced is not a deploy: while a cutover drains,
    the old stack still runs turns, and taking its prompt back would cost the
    thread a drop each way. It keeps the newer message, as Claude does."""
    return not is_claude(model) and sources_changed(state) and not _superseded(state)


def _superseded(state: Any) -> bool:
    """Whether this build is one a deploy replaced on the thread's pin: an
    old stack still running while a cutover drains, which sends the kept
    message as it is and asks for no summary to replace it."""
    replaced = as_dict(state_get(state, STATE_SYSTEM_PIN)).get("replaced") or ()
    return release.source_digest() in replaced


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
