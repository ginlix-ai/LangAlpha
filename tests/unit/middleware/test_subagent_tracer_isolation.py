"""Subagent callback-shape regression tests + astream driver shape tests.

The subagent's outbound config inherits whatever the parent runtime put on
``callbacks``. LangChain hands that slot in as a ``BaseCallbackManager``
once handlers have been composed (this is what production saw with
``LANGSMITH_TRACING=true``), so the subagent path must drop parent callbacks
without attempting to iterate the manager.

LangSmith tracing rides on the SDK's ambient auto-tracer (ContextVar-propagated),
not on an explicit ``LangChainTracer`` per subagent.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.mark.asyncio
async def test_parent_callbacks_as_callback_manager_does_not_crash(monkeypatch):
    """Regression: ``callbacks`` arrives as a ``BaseCallbackManager`` (not a list)
    when LangSmith tracing is enabled. ``list(manager)`` raises TypeError because
    managers are not iterable; production should drop the parent callback slot
    before composing the subagent config.
    """
    from langchain_core.callbacks import AsyncCallbackManager
    from ptc_agent.agent.middleware.background_subagent import subagent as sa

    parent_handler = MagicMock(name="parent-handler")
    parent_manager = AsyncCallbackManager(handlers=[parent_handler])
    parent_config = {
        "configurable": {"thread_id": "parent-thread"},
        "callbacks": parent_manager,
    }
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    seen_configs: list[dict] = []

    async def fake_astream(state, config, stream_mode=None):
        seen_configs.append(config)
        # When stream_mode is a list, langgraph yields (mode, data) tuples.
        yield ("values", {"messages": [MagicMock(text="ok")]})

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=None,
        checkpointer=None,
    )
    coroutine = tool.coroutine

    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars.get("subagent_graphs")
    assert sg is not None
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-cbmgr"

    # Must not raise — ``list(manager)`` raises TypeError if production tries
    # to normalize inherited callbacks instead of dropping them.
    await coroutine(
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        action="init",
        task_id=None,
        runtime=runtime,
    )

    assert len(seen_configs) == 1
    # Parent's handler is intentionally NOT inherited — the workflow's
    # PerCallTokenTracker would double-count subagent LLM calls. Ambient
    # LangSmith tracing rides on the SDK's ContextVar-propagated tracer,
    # not on this callbacks list.
    cbs = seen_configs[0].get("callbacks", [])
    assert parent_handler not in cbs

@pytest.mark.asyncio
async def test_subagent_does_not_inherit_parent_token_tracker(monkeypatch):
    """Subagent invocations DROP parent's callbacks — the workflow's
    PerCallTokenTracker would double-count subagent LLM tokens (parent
    tracker logs the call into its records AND the per-subagent tracker
    logs the same call into task.per_call_records, so we'd persist both
    against the same usage row). The per-subagent tracker is wired in
    via the current_background_token_tracker ContextVar."""
    from ptc_agent.agent.middleware.background_subagent import subagent as sa
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        current_background_token_tracker,
    )
    from src.utils.tracking.per_call_token_tracker import PerCallTokenTracker

    parent_tracker = PerCallTokenTracker()  # pretend this is the workflow tracker
    parent_handler_other = MagicMock(name="other-parent-handler")
    parent_config = {
        "configurable": {"thread_id": "parent-thread"},
        "callbacks": [parent_tracker, parent_handler_other],
    }
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    bg_tracker = PerCallTokenTracker()
    token = current_background_token_tracker.set(bg_tracker)

    seen_configs: list[dict] = []

    async def fake_astream(state, config, stream_mode=None):
        seen_configs.append(config)
        # When stream_mode is a list, langgraph yields (mode, data) tuples.
        yield ("values", {"messages": [MagicMock(text="ok")]})

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=None,
        checkpointer=None,
    )
    coroutine = tool.coroutine

    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars.get("subagent_graphs")
    assert sg is not None
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-x"

    try:
        await coroutine(
            description="d",
            prompt="p",
            subagent_type="general-purpose",
            action="init",
            task_id=None,
            runtime=runtime,
        )
    finally:
        current_background_token_tracker.reset(token)

    assert len(seen_configs) == 1
    cbs = seen_configs[0].get("callbacks", [])
    # Parent's tracker MUST NOT leak through — would double-bill
    assert parent_tracker not in cbs
    assert parent_handler_other not in cbs
    # Per-subagent tracker IS attached so on_llm_end fires on it
    assert bg_tracker in cbs


@pytest.mark.asyncio
async def test_subagent_uses_astream_with_values_messages_and_custom_modes(
    monkeypatch,
):
    """The Task tool drives the subagent through
    ``astream(stream_mode=["values", "messages", "custom"])``. The LAST
    ``values`` yield is the tool's return; ``messages`` yields are forwarded
    as per-token captured events on the registry; ``custom`` yields surface
    compaction's ``get_stream_writer`` events (token_usage / summarize /
    offload) so they reach the per-task SSE consumer. Earlier ``values``
    yields are intermediate snapshots that must NOT propagate."""
    from ptc_agent.agent.middleware.background_subagent import subagent as sa

    parent_config = {"configurable": {"thread_id": "t1"}}
    monkeypatch.setattr(
        "ptc_agent.agent.middleware.background_subagent.subagent.get_config",
        lambda: parent_config,
    )

    seen_stream_modes: list[Any] = []

    async def fake_astream(state, config, stream_mode=None):
        seen_stream_modes.append(stream_mode)
        # Multi-mode list → langgraph yields (mode, data) tuples.
        yield ("values", {"messages": [MagicMock(text="step-1")]})
        yield ("values", {"messages": [MagicMock(text="step-2")]})
        yield ("values", {"messages": [MagicMock(text="final")], "extra": "carried"})

    fake_subagent = MagicMock()
    fake_subagent.astream = fake_astream

    tool = sa._create_task_tool(
        default_model=MagicMock(),
        default_tools=[],
        default_middleware=[],
        default_interrupt_on=None,
        subagents=[],
        general_purpose_agent=False,
        registry=None,
        checkpointer=None,
    )
    coroutine = tool.coroutine

    closure_vars = {
        cell_name: cell.cell_contents
        for cell_name, cell in zip(
            coroutine.__code__.co_freevars,
            coroutine.__closure__ or (),
        )
    }
    sg = closure_vars.get("subagent_graphs")
    assert sg is not None
    sg["general-purpose"] = fake_subagent

    runtime = MagicMock()
    runtime.state = {"messages": []}
    runtime.tool_call_id = "tc-final"

    cmd = await coroutine(
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        action="init",
        task_id=None,
        runtime=runtime,
    )

    # Driver requests three modes — values for the final-state return,
    # messages for per-token forwarding, custom for get_stream_writer events.
    assert seen_stream_modes == [["values", "messages", "custom"]]

    # Only the LAST values yield propagates as the tool's return.
    msg = cmd.update["messages"][-1]
    assert msg.content == "final"
    assert cmd.update.get("extra") == "carried"
