"""Compaction between turns, sent as the agent's own model call.

A provider checks a compaction request against the conversation's own: the
system prompt, tools and thinking it was sent with, which its cache and the
thinking in the kept turns are verified against. Between turns no such
request exists, so manual /compact and the hand-off at a model switch run the
agent's model node outside a graph run, on the thread's state, and the
``CompactionMiddleware`` in its chain does the work where it does it mid-turn
(:func:`asked`) instead of calling the model for an answer.

Nothing is written here: the caller installs the answer, as manual /compact
always has.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from langchain_core.messages import HumanMessage
from langchain_core.runnables.config import ensure_config, var_child_runnable_config
from langgraph._internal._constants import CONFIG_KEY_RUNTIME
from langgraph.runtime import Runtime

if TYPE_CHECKING:
    from langgraph.graph.state import CompiledStateGraph

    from ptc_agent.agent.middleware.compaction.compact import Compaction


@dataclass(frozen=True)
class Compact:
    """Summarize all but the tail automatic compaction keeps."""


@dataclass(frozen=True)
class HandOff:
    """Have the model whose provider wrote the view's opening summary, which
    only that provider can read, write it out as text for a model of another
    provider."""


Ask = Compact | HandOff


class NoCompaction(RuntimeError):
    """The agent's model chain holds no ``CompactionMiddleware`` to answer."""


@dataclass
class Asked:
    """An ask on its way through the model node, and what answered it."""

    ask: Ask
    answer: Compaction | HumanMessage | None = None
    answered: bool = False


_asked: ContextVar[Asked | None] = ContextVar("compaction_asked", default=None)


def asked() -> Asked | None:
    """The ask the model call in progress carries, or None for a call of a
    turn."""
    return _asked.get()


async def ask_between_turns(
    agent: CompiledStateGraph,
    state: Mapping[str, Any],
    ask: Ask,
    *,
    thread_id: str,
    store: Any | None = None,
) -> Compaction | HumanMessage | None:
    """``ask`` answered by the ``CompactionMiddleware`` in ``agent``'s model
    chain, ``state`` being the thread's checkpoint values.

    A ``Compact`` answers with the compaction to install, a ``HandOff`` with
    the summary message to put in place of the view's opening one, or None
    when that summary is not one only its provider can read. The calls made
    for it report to the callbacks of the caller's runnable config, which bill
    and trace them, as a turn's summarizer call reports to the turn's.

    Raises:
        ValueError: When the view leaves nothing to summarize, or the model
            wrote no hand-off.
        NoCompaction: When ``agent`` compacts nothing.
    """
    # The middleware's hook is a node of its own, named after it, so an agent
    # built without it is told apart before its model is called as if for a
    # turn. Imported here: the middleware imports this module.
    from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware

    prefix = f"{CompactionMiddleware.__name__}."
    if not any(name.startswith(prefix) for name in agent.builder.nodes):
        raise NoCompaction("the agent has no compaction to ask")
    node = agent.builder.nodes["model"].runnable
    config = ensure_config(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                # What a graph run would hand the node. Nothing streams the
                # summarize signals between turns: the caller reports.
                CONFIG_KEY_RUNTIME: Runtime(
                    context=None, store=store, stream_writer=_unheard
                ),
            },
        }
    )
    slot = Asked(ask)
    asked_token = _asked.set(slot)
    # The node reads its config the way a graph run's would find it.
    config_token = var_child_runnable_config.set(config)
    try:
        await node.ainvoke(dict(state), config)
    finally:
        var_child_runnable_config.reset(config_token)
        _asked.reset(asked_token)
    if not slot.answered:
        raise NoCompaction("the agent has no compaction to ask")
    return slot.answer


def _unheard(_chunk: Any) -> None:
    pass
