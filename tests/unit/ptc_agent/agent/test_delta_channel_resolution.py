"""Channel-resolution proof for the DeltaChannel adoption linchpin.

Passing ``state_schema=DeltaAgentState`` to ``langchain.agents.create_agent``
must make the compiled graph's ``messages`` channel a ``DeltaChannel`` — winning
the schema merge against the ``add_messages`` (``BinaryOperatorAggregate``)
annotation declared by the base ``AgentState`` and by middleware state schemas.

These tests build a real agent through the same factory the production
agent/flash/subagent factories use, with a representative subset of the real
middleware stack (the full PTC/Flash stack needs sandbox/MCP/network infra that
a unit test can't construct). One middleware in the subset *explicitly*
re-declares ``messages: add_messages`` so the override genuinely wins a conflict,
not just a no-op.
"""

from __future__ import annotations

from typing import Annotated

from langchain.agents import AgentState, create_agent
from langchain.agents.middleware import AgentMiddleware, TodoListMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langgraph.channels import DeltaChannel
from langgraph.channels.binop import BinaryOperatorAggregate
from langgraph.graph.message import add_messages

from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware
from ptc_agent.agent.state import DeltaAgentState


class _ConflictState(AgentState):
    """State schema that re-declares ``messages`` with ``add_messages``.

    Stands in for the ~25 middleware state subclasses that declare
    ``messages: add_messages`` in the real stack, so the merge has a genuine
    conflict for ``DeltaAgentState`` to win.
    """

    messages: Annotated[list, add_messages]


class _ConflictMiddleware(AgentMiddleware):
    state_schema = _ConflictState


def _fake_model() -> GenericFakeChatModel:
    """Stub chat model so the build never touches the network."""
    return GenericFakeChatModel(messages=iter([]))


def _representative_middleware() -> list[AgentMiddleware]:
    """A real-middleware subset that includes a ``messages: add_messages`` conflict.

    ``TodoListMiddleware`` and ``PatchToolCallsMiddleware`` are both present in
    the real PTC middleware stack and construct without infra. The conflict
    middleware forces the schema merge the linchpin must win.
    """
    return [_ConflictMiddleware(), TodoListMiddleware(), PatchToolCallsMiddleware()]


def test_messages_channel_is_delta_channel_with_state_schema():
    """WITH ``state_schema=DeltaAgentState`` the messages channel is a DeltaChannel."""
    agent = create_agent(
        _fake_model(),
        tools=[],
        middleware=_representative_middleware(),
        state_schema=DeltaAgentState,
    )

    channel = agent.channels["messages"]
    assert isinstance(channel, DeltaChannel), (
        f"expected DeltaChannel, got {type(channel).__name__} — the "
        "state_schema override lost the messages schema merge"
    )
    assert not isinstance(channel, BinaryOperatorAggregate)


def test_messages_channel_is_binop_without_state_schema():
    """WITHOUT ``state_schema`` the SAME build falls back to BinaryOperatorAggregate.

    Proves the assertion above is meaningful: the DeltaChannel result is caused
    by the override, not by the middleware subset or the fake model.
    """
    agent = create_agent(
        _fake_model(),
        tools=[],
        middleware=_representative_middleware(),
    )

    channel = agent.channels["messages"]
    assert isinstance(channel, BinaryOperatorAggregate)
    assert not isinstance(channel, DeltaChannel)


def test_delta_channel_uses_vendored_reducer():
    """The resolved DeltaChannel carries our vendored ``messages_delta_reducer``."""
    from ptc_agent.agent.state import messages_delta_reducer

    agent = create_agent(
        _fake_model(),
        tools=[],
        middleware=_representative_middleware(),
        state_schema=DeltaAgentState,
    )

    channel = agent.channels["messages"]
    assert isinstance(channel, DeltaChannel)
    assert channel.reducer is messages_delta_reducer
