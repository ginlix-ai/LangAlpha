"""The main agent's one schema declares what every middleware keeps, so a
graph built on it (a turn, thread maintenance, the history reader) writes no
checkpoint without one of them. ``test_main_state_db`` shows the erasure on
Postgres."""

from __future__ import annotations

from typing import Annotated, get_args, get_origin, get_type_hints

from langchain.agents.middleware.types import AgentMiddleware, PrivateStateAttr
from langgraph.channels.delta import DeltaChannel
from langgraph.checkpoint.memory import InMemorySaver
from typing_extensions import NotRequired, Required

import ptc_agent.agent.middleware  # noqa: F401  (imports every middleware)
from ptc_agent.agent.main_state import MainAgentState
from src.server.services.history.reader import CheckpointHistoryReader


def _subclasses(cls: type) -> set[type]:
    out = set()
    for sub in cls.__subclasses__():
        out |= {sub, *_subclasses(sub)}
    return out


def _private(annotation: object) -> bool:
    if get_origin(annotation) in (Required, NotRequired):
        (annotation,) = get_args(annotation)
    return get_origin(annotation) is Annotated and PrivateStateAttr in annotation.__metadata__


def test_it_declares_every_private_field_a_middleware_keeps() -> None:
    declared = get_type_hints(MainAgentState, include_extras=True)
    kept = {
        (middleware.__name__, name): annotation
        for middleware in _subclasses(AgentMiddleware)
        if middleware.__module__.startswith("ptc_agent.")
        for name, annotation in get_type_hints(
            middleware.state_schema, include_extras=True
        ).items()
        if _private(annotation)
    }
    assert {"_subagents_trimmed", "_notes_calls_trimmed"} <= {name for _, name in kept}
    for (middleware, name), annotation in kept.items():
        assert declared.get(name) == annotation, (middleware, name)


def test_messages_keep_their_delta_channel() -> None:
    (annotated,) = get_args(get_type_hints(MainAgentState, include_extras=True)["messages"])
    assert any(isinstance(meta, DeltaChannel) for meta in annotated.__metadata__)


def test_the_reader_writes_with_the_main_agents_schema() -> None:
    reader = CheckpointHistoryReader(InMemorySaver())
    for graph in (reader.graph, reader._updater):
        assert graph.builder.state_schema is MainAgentState
