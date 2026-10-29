"""A subagent archive records each lane's provenance for the tool calls its
task's checkpoints saved, and nothing else of the captured events.

A subagent that failed streamed what it fetched in its last step, which its
transcript never shows; a lane that settled normally keeps every source, and
so does a lane whose checkpoint cannot be read.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from src.server.services.runs import subagent_archive


def _source(agent: str, tool_call_id: str, identifier: str) -> dict:
    return {
        "event": "provenance",
        "data": {
            "agent": agent,
            "tool_call_id": tool_call_id,
            "source_type": "web",
            "identifier": identifier,
            "timestamp": "2026-10-13T10:00:00+00:00",
        },
    }


def _chunk(agent: str) -> dict:
    return {
        "event": "message_chunk",
        "data": {"agent": agent, "content_type": "text", "content": "done"},
    }


def _saved(*tool_call_ids: str) -> list:
    messages: list = []
    for tc in tool_call_ids:
        messages.append(
            AIMessage(content="", tool_calls=[{"name": "WebFetch", "args": {}, "id": tc}])
        )
        messages.append(ToolMessage(content="page", tool_call_id=tc))
    return messages


async def _persist(events, task_messages, *, write=None):
    async def task_history(thread_id, task_id):
        if isinstance(task_messages[task_id], Exception):
            raise task_messages[task_id]
        return SimpleNamespace(messages=task_messages[task_id])

    reader = MagicMock()
    reader.aget_task_history = AsyncMock(side_effect=task_history)
    write = write or AsyncMock(return_value=True)
    with (
        patch(
            "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
            return_value=reader,
        ),
        patch.object(subagent_archive, "replace_lane_provenance", write),
        patch.object(subagent_archive.asyncio, "sleep", AsyncMock()),
    ):
        ok = await subagent_archive.persist_collected_events(
            events, "resp-1", "thread-1", "ws-1"
        )
    return ok, write, reader


@pytest.mark.asyncio
async def test_a_failed_subagent_keeps_only_its_saved_calls_sources():
    failed_saved = _source("task:k1", "tc-a", "https://a.test")
    failed_unsaved = _source("task:k1", "tc-b", "https://b.test")
    normal = [
        _source("task:k2", "tc-c", "https://c.test"),
        _source("task:k2", "tc-d", "https://d.test"),
    ]
    events = [failed_saved, failed_unsaved, _chunk("task:k1"), *normal, _chunk("task:k2")]

    ok, write, reader = await _persist(
        events, {"k1": _saved("tc-a"), "k2": _saved("tc-c", "tc-d")}
    )

    assert ok is True
    write.assert_awaited_once_with(
        "resp-1", [failed_saved, *normal], {"task:k1": "k1", "task:k2": "k2"}
    )
    assert sorted(c.args for c in reader.aget_task_history.await_args_list) == [
        ("thread-1", "k1"),
        ("thread-1", "k2"),
    ]


@pytest.mark.asyncio
async def test_a_lane_without_provenance_is_rewritten_without_a_checkpoint_read():
    # Its lane still names the rows the write replaces, so an earlier write's
    # rows for it go.
    ok, write, reader = await _persist([_chunk("task:k1")], {})

    assert ok is True
    write.assert_awaited_once_with("resp-1", [], {"task:k1": "k1"})
    reader.aget_task_history.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_lane_whose_checkpoint_read_fails_keeps_every_source():
    unread = [
        _source("task:k1", "tc-a", "https://a.test"),
        _source("task:k1", "tc-b", "https://b.test"),
    ]
    read = [
        _source("task:k2", "tc-c", "https://c.test"),
        _source("task:k2", "tc-d", "https://d.test"),
    ]

    ok, write, _ = await _persist(
        [*unread, *read], {"k1": RuntimeError("pool closed"), "k2": _saved("tc-c")}
    )

    assert ok is True
    write.assert_awaited_once_with(
        "resp-1", [*unread, read[0]], {"task:k1": "k1", "task:k2": "k2"}
    )


@pytest.mark.asyncio
async def test_a_failed_write_is_retried_then_keeps_the_streams():
    events = [_source("task:k1", "tc-a", "https://a.test")]

    ok, write, _ = await _persist(
        events, {"k1": _saved("tc-a")}, write=AsyncMock(side_effect=RuntimeError("down"))
    )

    assert ok is False
    assert write.await_count == 2


@pytest.mark.asyncio
async def test_events_outside_a_task_lane_write_nothing():
    ok, write, _ = await _persist([_chunk("main")], {})

    assert ok is True
    write.assert_not_awaited()
