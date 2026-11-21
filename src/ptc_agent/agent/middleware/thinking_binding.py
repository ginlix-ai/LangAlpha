"""Which reasoning is bound to what was sent before it, and the record of
the messages that are sent without theirs.

A model with preserved reasoning checks each of its reasoning payloads against
everything sent ahead of it, the system prompt, the tools and every earlier
message, and drops or refuses the payload once any of that changed. Claude's
thinking is bound so, and OpenAI's reasoning items from GPT-6.2 on, on the API
and the Codex backend alike: a lineage binds when its provider declares
``reasoning_binds_prefix``. Another lineage's reasoning reaches a model
stripped, so it binds nothing.

Anthropic refuses or drops a block whose prefix changed, so an answer whose
thinking an edit invalidated is sent without it (``thinking_after``). OpenAI
drops such an item on its own, and refuses a function call sent without the
reasoning item it followed, so its reasoning is never left out here: binding
only asks the stack to keep the prefix as it was sent.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from typing import Annotated, Any

from langchain.agents.middleware.types import AgentState, PrivateStateAttr
from langchain_core.messages import AIMessage, AnyMessage
from typing_extensions import NotRequired

from ptc_agent.agent.middleware.reasoning_compat import message_lineage, model_lineage
from src.llms.model_identity import is_claude
from src.llms.reasoning_lineage import ANTHROPIC_LINEAGE, reasoning_binds_prefix
from src.llms.reasoning_payload import is_reasoning_block, strip_all_reasoning

UNSENT_THINKING_KEY = "_unsent_thinking_ids"

def _union(held: Iterable[str] | None, new: Iterable[str] | None) -> set[str]:
    return set(held or ()) | set(new or ())


class UnsentThinkingState(AgentState):
    """``_unsent_thinking_ids`` holds the messages sent without their
    reasoning, by id, which every later call sends the same way: an answer's
    thinking is bound to the history it was sent with, so putting reasoning
    back would edit that history. Compaction records the answers an edit
    ahead of them invalidated (``thinking_after``), model resilience the
    messages a retry was answered without.

    Both can write in one model call, and a key without a reducer refuses a
    second write in a step, so writes are a union; the window trim, at a turn
    start, is what shrinks the set (with ``Overwrite``).
    """

    _unsent_thinking_ids: Annotated[NotRequired[set[str]], PrivateStateAttr, _union]


def _carries_reasoning(message: Any) -> bool:
    return (
        isinstance(message, AIMessage)
        and isinstance(message.content, list)
        and any(is_reasoning_block(block) for block in message.content)
    )


def is_bound_answer(message: Any) -> bool:
    """Whether ``message`` is an answer carrying reasoning bound to its prefix."""
    return _carries_reasoning(message) and reasoning_binds_prefix(message_lineage(message))


def _left_out_when_invalid(message: Any) -> bool:
    """Whether ``message`` carries thinking that is sent without it once an
    edit ahead of it invalidated it: Claude's only (see the module docstring)."""
    return _carries_reasoning(message) and message_lineage(message) == ANTHROPIC_LINEAGE


def binds_reasoning(model: Any) -> bool:
    """Whether a call to ``model`` binds the reasoning it writes to the prefix
    it was sent, so the stack keeps that prefix as it was: Claude on any route,
    and every model on a lineage that declares ``reasoning_binds_prefix``."""
    return is_claude(model) or reasoning_binds_prefix(model_lineage(model))


def holds_bound_thinking(messages: Iterable[Any]) -> bool:
    return any(is_bound_answer(message) for message in messages)


def checked_thinking_end(messages: Sequence[AnyMessage]) -> int:
    """Where what can be edited without invalidating thinking starts in
    ``messages``: right after the last bound answer, 0 when there is none."""
    return next(
        (i + 1 for i in range(len(messages) - 1, -1, -1) if is_bound_answer(messages[i])),
        0,
    )


def thinking_after(messages: Sequence[AnyMessage], edited: Collection[str]) -> set[str]:
    """The ids of the answers in ``messages`` whose thinking an edit of the
    ``edited`` ones invalidates and is sent without: every one of Claude's
    after the first of them (see the module docstring).

    Leaving thinking out from the end of the history is valid, so leaving it
    out from the first edited message on keeps what comes before it.
    """
    first = next((i for i, msg in enumerate(messages) if msg.id in edited), None)
    if first is None:
        return set()
    return {msg.id for msg in messages[first + 1 :] if msg.id and _left_out_when_invalid(msg)}


def unsent_thinking(state: Mapping[str, Any] | None) -> set[str]:
    state = state or {}
    return set(state.get(UNSENT_THINKING_KEY) or ())


def without_thinking(messages: list[AnyMessage], ids: Collection[str]) -> list[AnyMessage]:
    """``messages`` with the ones ``ids`` names sent as a strip sends them,
    the original list when nothing changed: an answer without its reasoning,
    gone once nothing is left, and a summary as its text."""
    if not ids:
        return messages
    result: list[AnyMessage] = []
    changed = False
    for msg in messages:
        if getattr(msg, "id", None) not in ids:
            result.append(msg)
            continue
        alone = [msg]
        stripped = strip_all_reasoning(alone)
        changed = changed or stripped is not alone
        result.extend(stripped)
    return result if changed else messages
