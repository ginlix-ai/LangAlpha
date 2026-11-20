"""The tools array an Anthropic thread first sent, kept as it was sent.

A model that binds its thinking to the prefix (Claude Fable 5.2 and the 5.5
models) loses every earlier thinking block once ``tools`` differs between two
calls of one conversation, and ours differs routinely: a skill's tools are
offered once the skill is loaded, a notification turn is built without the
subagent tools, a directly bound MCP tool arrives with the next turn's build,
a deploy rewords a tool's description.

Where the API takes mid-conversation tool changes, the array the thread first
sent is kept, every entry as it was first sent, and a tool is turned on or off
by a ``tool_addition`` or ``tool_removal`` block in a system message written
into history once, before the reply of the call that first carried it. A tool
the array declares is named by reference; one it lacks is defined in the block
itself, and so is a tool whose definition changed since the model was given it
(a deploy that reworded it), which replaces the earlier definition from there
on. So the array, which leads the prompt cache's key, never changes, the call
that brings a tool in still reads the cache, and the model always has the
schema the tool runs with. A tool that left the build keeps its entry, so the
array never loses one.

The array is checkpointed state rather than process memory: the agent is built
again every turn, and a thread's calls land on any worker. It is rendered
afresh only where no thinking can depend on it: a thread's first call, and the
first call after a compaction that kept no thinking. What the model is offered
is read back from the request itself: the array's undeferred entries, the net
changes a compaction block records, then every change message the request
still carries, so a change no longer sent is made again rather than assumed.

OpenAI's Responses API binds GPT-6.2's reasoning items to ``tools`` the same
way, on the API and the Codex backend, and takes an added tool as an
``additional_tools`` item at its place in the input. So on a model that binds
its reasoning there, the array is kept the same way, and a tool turned on or
redefined arrives in such an item. It has no item that takes a tool away, and
narrowing ``tool_choice`` to ``allowed_tools`` drops the reasoning as surely as
a new array does (measured on gpt-6.2-sol), so a tool turned off keeps its
entry and a call to it is refused, as a call to a tool never offered is. A
change message names the wire it was written for, and a request sends only
its own target's.

Every other target gets the tools the stack chose, as it always has, and none
of the change messages, which no other wire form can carry.
"""

from __future__ import annotations

import abc
import dataclasses
import logging
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import Annotated, Any

from langchain.agents.middleware import AgentMiddleware, AgentState
from langchain.agents.middleware.types import (
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
    PrivateStateAttr,
)
from langchain_anthropic.chat_models import convert_to_anthropic_tool
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.utils.function_calling import convert_to_openai_tool
from langgraph.prebuilt.tool_node import INVALID_TOOL_NAME_ERROR_TEMPLATE
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.middleware.reasoning_compat import model_lineage
from ptc_agent.agent.middleware.thinking_binding import holds_bound_thinking
from ptc_agent.agent.transcript.classify import is_summary_message
from src.llms.operator_channel import (
    OPERATOR_CHANNEL_KEY,
    carries_tool_changes,
    operator_request,
    tool_change_betas,
)
from src.llms.provider_compaction import is_compaction_request, replayed_tool_changes
from src.llms.reasoning_lineage import reasoning_binds_prefix

logger = logging.getLogger(__name__)

TOOL_SURFACE_KEY = "tool_surface"

#: ``lc_source`` of a change message.
TOOL_CHANGE_SOURCE = "tool_change"

#: The ``additional_kwargs`` key naming the wire a change message was written
#: for. Anthropic's carry none: theirs were written before OpenAI's existed.
TOOL_WIRE_KEY = "tool_wire"


class ToolSurfaceState(AgentState):
    """``tool_surface`` holds each wire's kept array as ``tools``, the array
    as declared, and ``epoch``, the summary it was rendered under: Anthropic's
    at the top level, where it was kept before OpenAI's had one, OpenAI's
    under ``openai``. One slot each, so a thread that moves between them finds
    each array as it left it. ``offered`` is the names the model had on the
    last call, None once a call went out on a target that keeps no array."""

    tool_surface: NotRequired[Annotated[dict[str, Any], PrivateStateAttr]]


def is_tool_change_message(message: Any) -> bool:
    return (
        isinstance(message, SystemMessage)
        and (message.additional_kwargs or {}).get("lc_source") == TOOL_CHANGE_SOURCE
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _Prepared:
    request: ModelRequest
    change: SystemMessage | None
    update: dict[str, Any] | None
    #: Whether the call's change messages go through the operator bridge.
    bridged: bool


class _Wire(abc.ABC):
    """A wire form that takes mid-conversation tool changes: how it declares
    the tools, where its kept array is stored, and how it states a change."""

    #: ``TOOL_WIRE_KEY`` on its change messages, None on Anthropic's.
    tag: str | None = None
    bridged = False

    def owns(self, message: Any) -> bool:
        return (message.additional_kwargs or {}).get(TOOL_WIRE_KEY) == self.tag

    @abc.abstractmethod
    def slot(self, surface: dict[str, Any]) -> dict[str, Any]:
        """Where *surface* keeps this wire's ``tools`` and ``epoch``."""

    @abc.abstractmethod
    def stored(
        self, surface: dict[str, Any], tools: list[dict[str, Any]], epoch: str | None, offered: list[str]
    ) -> dict[str, Any]:
        ...

    @abc.abstractmethod
    def definitions(self, tools: list[Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """This build's tools in the wire's array form, and the cache marker
        the stack put on them, if any."""

    @abc.abstractmethod
    def render(self, stored: list[dict[str, Any]], wanted: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """A fresh array, made where no reasoning can depend on the stored one."""

    @abc.abstractmethod
    def diff(
        self,
        declared: list[dict[str, Any]],
        wanted: list[dict[str, Any]],
        messages: list[Any],
        model: Any,
    ) -> tuple[SystemMessage | None, list[str]]:
        """The change message that gives the model *wanted* at the end of
        *messages*, None when nothing changes, and the names it is offered
        after it."""

    @abc.abstractmethod
    def dress(
        self,
        request: ModelRequest,
        declared: list[dict[str, Any]],
        marker: dict[str, Any] | None,
        messages: list[Any],
    ) -> ModelRequest:
        ...


class _AnthropicWire(_Wire):
    bridged = True

    def slot(self, surface: dict[str, Any]) -> dict[str, Any]:
        return surface

    def stored(self, surface, tools, epoch, offered):
        return {**surface, "tools": tools, "epoch": epoch, "offered": offered}

    def definitions(self, tools):
        wanted: list[dict[str, Any]] = []
        marker: dict[str, Any] | None = None
        for tool in tools:
            definition = dict(convert_to_anthropic_tool(tool))
            marker = definition.pop("cache_control", None) or marker
            wanted.append(definition)
        return wanted, marker

    def render(self, stored, wanted):
        """This build's tools, then every stored entry it lacks, deferred.

        The stored entries stay because a compaction block may still turn one
        on or off by name, and a name the array lacks is refused.
        """
        names = {definition["name"] for definition in wanted}
        return [
            *wanted,
            *({**entry, "defer_loading": True} for entry in stored if entry["name"] not in names),
        ]

    def diff(self, declared, wanted, messages, model):
        offered, current = _known(declared, messages, model)
        names = {definition["name"] for definition in wanted}
        added = [definition["name"] for definition in wanted if definition["name"] not in offered]
        changed = [
            definition["name"]
            for definition in wanted
            if definition["name"] in offered and _redefined(current.get(definition["name"]), definition, messages)
        ]
        removed = [name for name in offered if name not in names]
        if not (added or changed or removed) or not _may_change(messages):
            return None, offered
        change = _change_message(added, changed, removed, declared, wanted)
        return change, [name for name in offered if name not in removed] + added

    def dress(self, request, declared, marker, messages):
        # A change on the wire needs its beta, a compaction block's included.
        settings = request.model_settings
        if any(is_tool_change_message(m) or replayed_tool_changes(m, request.model) for m in messages):
            settings = {**settings, "betas": tool_change_betas(settings.get("betas"))}
        return request.override(
            tools=_with_marker(declared, marker), messages=messages, model_settings=settings
        )


class _OpenAIWire(_Wire):
    """The array is kept in the chat form langchain-openai converts from;
    the tools a change gives are compared in the Responses form its item
    carries them in."""

    tag = "openai"

    def slot(self, surface: dict[str, Any]) -> dict[str, Any]:
        return surface.get(self.tag) or {}

    def stored(self, surface, tools, epoch, offered):
        return {**surface, self.tag: {"tools": tools, "epoch": epoch}, "offered": offered}

    def definitions(self, tools):
        return [convert_to_openai_tool(tool) for tool in tools], None

    def render(self, stored, wanted):
        return wanted

    def diff(self, declared, wanted, messages, model):
        """An ``additional_tools`` item for every tool in *wanted* the model
        lacks or holds under another definition. A tool *wanted* lacks is not
        stated, since no item takes one away: it is left out of what the
        model is offered, so a call to it is refused (``_refusal``)."""
        current = {tool.get("name"): tool for tool in map(_as_responses_tool, declared)}
        for message in messages:
            if is_tool_change_message(message):
                for block in message.content:
                    for tool in block.get("tools") or ():
                        current[tool.get("name")] = tool
        given = [_as_responses_tool(definition) for definition in wanted]
        added = [tool for tool in given if tool.get("name") not in current]
        changed = [
            tool
            for tool in given
            if tool.get("name") in current and _redefined(current[tool.get("name")], tool, messages)
        ]
        if not (added or changed) or not _may_change(messages):
            return None, [tool.get("name") for tool in given if tool.get("name") in current]
        change = SystemMessage(
            content=[{"type": "additional_tools", "role": "developer", "tools": [*added, *changed]}],
            additional_kwargs={"lc_source": TOOL_CHANGE_SOURCE, TOOL_WIRE_KEY: self.tag},
        )
        return change, [tool.get("name") for tool in given]

    def dress(self, request, declared, marker, messages):
        return request.override(tools=declared, messages=messages)


_ANTHROPIC = _AnthropicWire()
_OPENAI = _OpenAIWire()


def _wire_for(model: Any) -> _Wire | None:
    """The wire whose array a call to *model* keeps, None for the stack's tools."""
    if carries_tool_changes(model):
        return _ANTHROPIC
    # The lineage's own flag, not ``binds_reasoning``: that one also counts
    # Claude behind an OpenAI-compatible proxy, which need not take the item.
    if getattr(model, "output_version", None) == "responses/v1" and reasoning_binds_prefix(model_lineage(model)):
        return _OPENAI
    return None


class ToolSurfaceMiddleware(AgentMiddleware):
    """Keeps the tools array stable on targets that take mid-conversation tool changes.

    Sits inside every middleware that chooses the tools (the skill filter),
    inside compaction, whose own request must carry the same array as the
    calls whose thinking it keeps, and inside the provider caching middleware,
    whose tools breakpoint it moves off a deferred entry, which may not carry
    one. It sits outside the tail, which composes the rows a change message
    may stand beside.
    """

    state_schema = ToolSurfaceState

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> Any:
        prepared = _prepare(request)
        with operator_request("system") if prepared.bridged else nullcontext():
            return _finish(handler(prepared.request), prepared)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> Any:
        prepared = _prepare(request)
        # Every change message goes through the bridge, which joins it with a
        # neighbouring row and keeps it off a place the wire refuses.
        with operator_request("system") if prepared.bridged else nullcontext():
            return _finish(await handler(prepared.request), prepared)

    def wrap_tool_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        refused = _refusal(request)
        return refused if refused is not None else handler(request)

    async def awrap_tool_call(
        self, request: Any, handler: Callable[[Any], Awaitable[Any]]
    ) -> Any:
        refused = _refusal(request)
        return refused if refused is not None else await handler(request)


def _prepare(request: ModelRequest) -> _Prepared:
    surface = (request.state or {}).get(TOOL_SURFACE_KEY) or {}
    wire = _wire_for(request.model)
    if wire is None:
        return _plain(request, surface)

    messages = [m for m in request.messages or [] if not is_tool_change_message(m) or wire.owns(m)]
    wanted, marker = wire.definitions(request.tools or [])
    slot = wire.slot(surface)
    stored = slot.get("tools") or []
    if not stored and not wanted:
        return _Prepared(_with_messages(request, messages), None, None, bridged=False)

    epoch = _epoch(messages)
    if not stored or (slot.get("epoch") != epoch and not holds_bound_thinking(messages)):
        declared = wire.render(stored, wanted)
    else:
        declared = stored
        epoch = slot.get("epoch")

    # A compaction request summarizes what was sent, so it carries the array
    # and nothing new, and records nothing.
    if is_compaction_request(request.model_settings):
        return _Prepared(wire.dress(request, declared, marker, messages), None, None, bridged=wire.bridged)

    change, offered = wire.diff(declared, wanted, messages, request.model)
    if change is not None:
        messages.append(change)
    stored_now = wire.stored(surface, declared, epoch, offered)
    return _Prepared(
        wire.dress(request, declared, marker, messages),
        change,
        stored_now if stored_now != surface else None,
        bridged=wire.bridged,
    )


def _may_change(messages: list[Any]) -> bool:
    """Whether a change may go out on this call: a system entry may follow a
    user turn or tool results only, so a call that ends on an assistant turn
    leaves the change to the next call."""
    return bool(messages) and isinstance(messages[-1], (HumanMessage, ToolMessage))


def _redefined(given: dict[str, Any] | None, wanted: dict[str, Any], messages: list[Any]) -> bool:
    """Whether the model holds a tool under a definition other than *wanted*
    and may be given the new one now. A redefinition waits for a turn start:
    right after tool results a model may pass over the message it rides."""
    return given != wanted and _at_turn_start(messages)


def _as_responses_tool(definition: dict[str, Any]) -> dict[str, Any]:
    """A chat-format function tool as the Responses API declares it, the way
    langchain-openai converts the ``tools`` array; any other tool as it is."""
    function = definition.get("function")
    if definition.get("type") != "function" or not isinstance(function, dict):
        return definition
    extra = {k: v for k, v in definition.items() if k not in ("type", "function")}
    return {"type": "function", **function, **extra}


def _with_messages(request: ModelRequest, messages: list[Any]) -> ModelRequest:
    if len(messages) == len(request.messages or []):
        return request
    return request.override(messages=messages)


def _plain(request: ModelRequest, surface: dict[str, Any]) -> _Prepared:
    """The request as the stack built it, without the change messages."""
    messages = [m for m in request.messages or [] if not is_tool_change_message(m)]
    update = {**surface, "offered": None} if surface.get("offered") is not None else None
    return _Prepared(_with_messages(request, messages), None, update, bridged=False)


def _finish(response: ModelResponse, prepared: _Prepared) -> Any:
    if prepared.change is not None:
        response = dataclasses.replace(response, result=[prepared.change, *response.result])
    if prepared.update is None:
        return response
    return ExtendedModelResponse(
        model_response=response, command=Command(update={TOOL_SURFACE_KEY: prepared.update})
    )


def _epoch(messages: list[Any]) -> str | None:
    """The summary the request opens on, the boundary a fresh render may sit at."""
    if messages and is_summary_message(messages[0]):
        return messages[0].id
    return None


def _at_turn_start(messages: list[Any]) -> bool:
    """Whether the newest message past the runtime rows is the user's: a row
    (a market-watch stamp, a notes check-in) is written before calls mid-turn
    too, so it says nothing about where the turn is."""
    from ptc_agent.agent.middleware.runtime_context.durable import is_runtime_update_message

    newest = next((m for m in reversed(messages) if not is_runtime_update_message(m)), None)
    return isinstance(newest, HumanMessage)


def _known(
    declared: list[dict[str, Any]], messages: list[Any], model: Any
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """The names the model has at the end of *messages*, and the definition
    it has for every tool it was given, offered or not."""
    offered = [entry["name"] for entry in declared if not entry.get("defer_loading")]
    current = {entry["name"]: _bare(entry) for entry in declared}
    for message in messages:
        if is_tool_change_message(message):
            blocks = message.content
        else:
            blocks = replayed_tool_changes(message, model)
        for block in blocks:
            tool = block.get("tool") or {}
            definition = tool.get("definition") if tool.get("type") == "tool_definition" else None
            name = tool.get("name") or (definition or {}).get("name")
            if definition is not None:
                current[name] = _bare(definition)
            if block.get("type") == "tool_addition" and name not in offered:
                offered.append(name)
            elif block.get("type") == "tool_removal" and name in offered:
                offered.remove(name)
    return offered, current


def _bare(definition: dict[str, Any]) -> dict[str, Any]:
    """*definition* without what only says how it is declared."""
    return {k: v for k, v in definition.items() if k not in ("defer_loading", "cache_control")}


def _change_message(
    added: list[str],
    changed: list[str],
    removed: list[str],
    declared: list[dict[str, Any]],
    wanted: list[dict[str, Any]],
) -> SystemMessage:
    """The change turning *added* on, *changed* to their definitions in
    *wanted*, and *removed* off: a tool *declared* holds as wanted is named
    by reference, any other is defined.

    Only a declared tool may be referenced, so a tool once defined in a
    message is defined again when it comes back."""
    declared_as = {entry["name"]: _bare(entry) for entry in declared}
    definitions = {definition["name"]: definition for definition in wanted}

    def given(name: str) -> dict[str, Any]:
        if declared_as.get(name) == definitions[name] and name not in changed:
            return {"type": "tool_reference", "name": name}
        return {"type": "tool_definition", "definition": definitions[name]}

    blocks = [
        *({"type": "tool_removal", "tool": {"type": "tool_reference", "name": n}} for n in removed),
        *({"type": "tool_addition", "tool": given(n)} for n in [*added, *changed]),
    ]
    return SystemMessage(
        content=blocks,
        additional_kwargs={OPERATOR_CHANNEL_KEY: "system", "lc_source": TOOL_CHANGE_SOURCE},
    )


def _with_marker(
    declared: list[dict[str, Any]], marker: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """*declared* with the tools breakpoint on its last undeferred entry.

    A deferred entry is outside the cached prefix and the API refuses a marker
    on one; the last undeferred entry is where the prefix it caches ends.
    """
    tools = [dict(entry) for entry in declared]
    if marker is None:
        return tools
    for entry in reversed(tools):
        if not entry.get("defer_loading"):
            entry["cache_control"] = marker
            break
    return tools


def _refusal(request: Any) -> ToolMessage | None:
    """The answer to a call for a tool the model was not offered, or None.

    Worded as the tool node words a name it does not know, which to the model
    is what a withdrawn tool is.
    """
    offered = ((getattr(request, "state", None) or {}).get(TOOL_SURFACE_KEY) or {}).get("offered")
    name = request.tool_call.get("name")
    if offered is None or name in offered:
        return None
    logger.info("[ToolSurface] refused %r: not offered to the model", name)
    return ToolMessage(
        content=INVALID_TOOL_NAME_ERROR_TEMPLATE.format(
            requested_tool=name, available_tools=", ".join(offered)
        ),
        name=name,
        tool_call_id=request.tool_call["id"],
        status="error",
    )
