"""Tests for filtering streaming chunks in the CLI executor."""

from __future__ import annotations

from unittest.mock import Mock

from typing import Any

import pytest
from rich.panel import Panel

import ptc_cli.streaming.executor as executor_module
from ptc_cli.streaming.executor import execute_task


class FakeSSEClient:
    """Minimal SSE client that yields predefined SSE events."""

    def __init__(self, events: list[tuple[str, dict]]) -> None:
        self._events = events
        self.thread_id = "thread_1"

    async def stream_chat(self, **_kwargs: object):
        for event in self._events:
            yield event

    async def soft_interrupt(self, _thread_id: str) -> dict:
        return {"active_subagents": [], "completed_subagents": []}


@pytest.mark.asyncio
async def test_execute_task_hides_subagent_message_chunks(session_state, monkeypatch):
    """Chunks from subagent (unified identity) are not shown."""
    mock_console = Mock()
    mock_status = Mock()
    mock_console.status.return_value = mock_status
    mock_console.print = Mock()

    monkeypatch.setattr(executor_module, "console", mock_console)
    monkeypatch.setattr(
        executor_module,
        "COLORS",
        {"agent": "#10b981", "tool": "#fbbf24", "thinking": "#6b7280"},
    )

    client = FakeSSEClient(
        events=[
            (
                "tool_calls",
                {
                    "thread_id": "thread_1",
                    "agent": "general-purpose:02cd4745-f5f9-41b0-8a93-393d90e7ede0",
                    "tool_calls": [{"id": "c1", "name": "Task", "args": {"x": 1}}],
                },
            ),
            (
                "message_chunk",
                {
                    "thread_id": "thread_1",
                    "agent": "general-purpose:02cd4745-f5f9-41b0-8a93-393d90e7ede0",
                    "content": "SUBAGENT SHOULD NOT DISPLAY",
                    "content_type": "text",
                },
            ),
            (
                "tool_call_result",
                {
                    "thread_id": "thread_1",
                    "agent": "general-purpose:02cd4745-f5f9-41b0-8a93-393d90e7ede0",
                    "tool_call_id": "c1",
                    "status": "success",
                    "content": "SUBAGENT TOOL RESULT SHOULD NOT DISPLAY",
                },
            ),
            (
                "message_chunk",
                {
                    "thread_id": "thread_1",
                    "agent": "model:a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "content": "FINAL REPORT",
                    "content_type": "text",
                    "finish_reason": "stop",
                },
            ),
            ("done", {"thread_id": "thread_1"}),
        ]
    )

    client_any: Any = client
    await execute_task(
        "hi",
        client_any,
        assistant_id=None,
        session_state=session_state,
    )

    # StreamingState.append_text prints raw text strings, not Markdown objects
    all_printed_text = " ".join(
        str(call.args[0])
        for call in mock_console.print.call_args_list
        if call.args
    )

    assert "FINAL REPORT" in all_printed_text
    assert "SUBAGENT SHOULD NOT DISPLAY" not in all_printed_text
    assert "SUBAGENT TOOL RESULT SHOULD NOT DISPLAY" not in all_printed_text


@pytest.mark.asyncio
async def test_execute_task_renders_background_task_panel(session_state, monkeypatch):
    """Successful task/Wait/TaskOutput results are shown in a Panel."""
    mock_console = Mock()
    mock_status = Mock()
    mock_console.status.return_value = mock_status
    mock_console.print = Mock()

    monkeypatch.setattr(executor_module, "console", mock_console)
    monkeypatch.setattr(
        executor_module,
        "COLORS",
        {"agent": "#10b981", "tool": "#fbbf24", "thinking": "#6b7280"},
    )

    client = FakeSSEClient(
        events=[
            (
                "tool_calls",
                {
                    "thread_id": "thread_1",
                    "agent": "model:a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "tool_calls": [{"id": "c1", "name": "Task", "args": {}}],
                },
            ),
            (
                "tool_call_result",
                {
                    "thread_id": "thread_1",
                    "agent": "model:a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "tool_call_id": "c1",
                    "status": "success",
                    "content": "hello from subagent",
                },
            ),
            ("done", {"thread_id": "thread_1"}),
        ]
    )

    client_any: Any = client
    await execute_task(
        "hi",
        client_any,
        assistant_id=None,
        session_state=session_state,
    )

    panel_calls = [
        call for call in mock_console.print.call_args_list if call.args and isinstance(call.args[0], Panel)
    ]
    assert panel_calls


@pytest.mark.asyncio
async def test_execute_task_renders_TodoWrite(session_state, monkeypatch):
    """TodoWrite tool args trigger todo list rendering."""
    mock_console = Mock()
    mock_status = Mock()
    mock_console.status.return_value = mock_status
    mock_console.print = Mock()

    mock_render = Mock()

    monkeypatch.setattr(executor_module, "console", mock_console)
    monkeypatch.setattr(executor_module, "render_todo_list", mock_render)
    monkeypatch.setattr(
        executor_module,
        "COLORS",
        {"agent": "#10b981", "tool": "#fbbf24", "thinking": "#6b7280"},
    )

    todos = [
        {"status": "pending", "content": "one"},
        {"status": "in_progress", "content": "two", "activeForm": "working"},
    ]

    client = FakeSSEClient(
        events=[
            (
                "tool_calls",
                {
                    "thread_id": "thread_1",
                    "agent": "model:a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                    "tool_calls": [{"id": "t1", "name": "TodoWrite", "args": {"todos": todos}}],
                },
            ),
            ("done", {"thread_id": "thread_1"}),
        ]
    )

    client_any: Any = client
    await execute_task(
        "hi",
        client_any,
        assistant_id=None,
        session_state=session_state,
    )

    mock_render.assert_called_once_with(todos)
