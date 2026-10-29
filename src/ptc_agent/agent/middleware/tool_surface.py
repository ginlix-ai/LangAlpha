"""The tools array an Anthropic thread first sent, kept as it was sent.

A model that binds its thinking to the prefix (Claude Fable 5.1 and the 5.5
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

Every other target gets the tools the stack chose, as it always has, and none
of the change messages, which no other wire form can carry.
"""

from __future__ import annotations

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
from langgraph.prebuilt.tool_node import INVALID_TOOL_NAME_ERROR_TEMPLATE
from langgraph.types import Command
from typing_extensions import NotRequired

from ptc_agent.agent.middleware.thinking_binding import holds_bound_thinking
from ptc_agent.agent.transcript.classify import is_summary_message
from src.llms.operator_channel import (
    OPERATOR_CHANNEL_KEY,
    carries_tool_changes,
    operator_request,
    tool_change_betas,
)
from src.llms.provider_compaction import is_compaction_request, replayed_tool_changes

logger = logging.getLogger(__name__)

TOOL_SURFACE_KEY = "tool_surface"

#: ``lc_source`` of a change message.
TOOL_CHANGE_SOURCE = "tool_change"

class ToolSurfaceState(AgentState):
    """``tool_surface`` holds ``tools``, the array as declared; ``epoch``, the
    summary it was rendered under; and ``offered``, the names the model had on
    the last call, None once a call went out on another target."""

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
    surface: bool


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
        with operator_request("system") if prepared.surface else nullcontext():
            return _finish(handler(prepared.request), prepared)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> Any:
        prepared = _prepare(request)
        # Every change message goes through the bridge, which joins it with a
        # neighbouring row and keeps it off a place the wire refuses.
        with operator_request("system") if prepared.surface else nullcontext():
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
    model = request.model
    if not carries_tool_changes(model):
        return _plain(request, surface)

    wanted: list[dict[str, Any]] = []
    marker: dict[str, Any] | None = None
    for tool in request.tools or []:
        definition = dict(convert_to_anthropic_tool(tool))
        marker = definition.pop("cache_control", None) or marker
        wanted.append(definition)
    stored = surface.get("tools") or []
    if not stored and not wanted:
        return _Prepared(request, None, None, surface=False)

    messages = list(request.messages or [])
    epoch = _epoch(messages)
    if not stored or (surface.get("epoch") != epoch and not holds_bound_thinking(messages)):
        declared = _render(stored, wanted)
    else:
        declared = stored
        epoch = surface.get("epoch")

    compacting = is_compaction_request(request.model_settings)
    change, offered = _diff(declared, wanted, messages, model, compacting=compacting)
    if change is not None:
        messages.append(change)

    # A change on the wire needs its beta, a compaction block's included.
    settings = request.model_settings
    if any(is_tool_change_message(m) or replayed_tool_changes(m, model) for m in messages):
        settings = {**settings, "betas": tool_change_betas(settings.get("betas"))}
    stored_now = {"tools": declared, "epoch": epoch, "offered": offered}
    return _Prepared(
        request.override(
            tools=_with_marker(declared, marker), messages=messages, model_settings=settings
        ),
        change,
        stored_now if stored_now != surface and not compacting else None,
        surface=True,
    )


def _plain(request: ModelRequest, surface: dict[str, Any]) -> _Prepared:
    """The request as the stack built it, without the change messages."""
    messages = [m for m in request.messages or [] if not is_tool_change_message(m)]
    if len(messages) != len(request.messages or []):
        request = request.override(messages=messages)
    update = {**surface, "offered": None} if surface.get("offered") is not None else None
    return _Prepared(request, None, update, surface=False)


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


def _diff(
    declared: list[dict[str, Any]],
    wanted: list[dict[str, Any]],
    messages: list[Any],
    model: Any,
    *,
    compacting: bool,
) -> tuple[SystemMessage | None, list[str]]:
    """The change message that brings what the model has at the end of
    *messages* to *wanted*, None when nothing changes or it has to wait, and
    the names the model is offered after it."""
    offered, current = _known(declared, messages, model)
    names = {definition["name"] for definition in wanted}
    added = [definition["name"] for definition in wanted if definition["name"] not in offered]
    # A redefinition waits for a turn start: right after tool results a
    # model may pass over the system message it rides.
    changed = [
        definition["name"]
        for definition in wanted
        if definition["name"] in offered
        and current.get(definition["name"]) != definition
        and _at_turn_start(messages)
    ]
    removed = [name for name in offered if name not in names]

    # A compaction request summarizes what was sent, so it carries the array
    # and nothing new. A system entry may follow a user turn only, so a call
    # that ends on an assistant turn leaves the change to the next call.
    if (
        not (added or changed or removed)
        or compacting
        or not messages
        or not isinstance(messages[-1], (HumanMessage, ToolMessage))
    ):
        return None, offered
    change = _change_message(added, changed, removed, declared, wanted)
    return change, [name for name in offered if name not in removed] + added


def _at_turn_start(messages: list[Any]) -> bool:
    """Whether the newest message past the runtime rows is the user's: a row
    (a market-watch stamp, a notes check-in) is written before calls mid-turn
    too, so it says nothing about where the turn is."""
    from ptc_agent.agent.middleware.runtime_context.durable import is_runtime_update_message

    newest = next((m for m in reversed(messages) if not is_runtime_update_message(m)), None)
    return isinstance(newest, HumanMessage)


def _render(stored: list[dict[str, Any]], wanted: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A fresh array: this build's tools, then every stored entry it lacks, deferred.

    The stored entries stay because a compaction block may still turn one on
    or off by name, and a name the array lacks is refused.
    """
    names = {definition["name"] for definition in wanted}
    return [
        *wanted,
        *({**entry, "defer_loading": True} for entry in stored if entry["name"] not in names),
    ]


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
