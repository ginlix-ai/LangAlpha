"""Subagent callback-shape regression tests.

The subagent's outbound config inherits whatever the parent runtime put on
``callbacks``. LangChain hands that slot in as a ``BaseCallbackManager``
once handlers have been composed (this is what production saw with
``LANGSMITH_TRACING=true``), so the subagent path must drop parent callbacks
without attempting to iterate the manager.

LangSmith tracing rides on the SDK's ambient auto-tracer (ContextVar-propagated),
not on an explicit ``LangChainTracer`` per subagent.
"""

from __future__ import annotations

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

    async def fake_ainvoke(state, config):
        seen_configs.append(config)
        return {"messages": [MagicMock(text="ok")]}

    fake_subagent = MagicMock()
    fake_subagent.ainvoke = fake_ainvoke

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

    async def fake_ainvoke(state, config):
        seen_configs.append(config)
        return {"messages": [MagicMock(text="ok")]}

    fake_subagent = MagicMock()
    fake_subagent.ainvoke = fake_ainvoke

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
