"""Durable TaskOutput derivation from a ``task:{id}`` checkpoint namespace.

Two task kinds share one reader: a subagent's answer IS its last AI message,
while a workflow run leaves no transcript and archives its result explicitly.
Presence of an archive decides which applies — never the task's kind.
"""

from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from src.server.services import background_registry_store as store
from src.server.services.background_registry_store import (
    resolve_archived_result_text,
    resolve_task_result_text,
)


def _archive(task_run_id: str, text: str) -> dict:
    return {
        "type": "ui",
        "id": f"task-result-{task_run_id}",
        "name": "task_result",
        "props": {"task_run_id": task_run_id, "text": text, "truncated": False},
    }


@pytest.fixture
def history(monkeypatch):
    """Install a fake task-history reader; returns a setter for its content."""
    state = SimpleNamespace(messages=[], new_ui_records=[])

    class FakeReader:
        @classmethod
        def get_instance(cls):
            return cls()

        async def aget_task_history(self, thread_id, task_id):
            return state

    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader", FakeReader
    )
    return state


@pytest.mark.asyncio
async def test_archived_result_answers_for_the_run_that_wrote_it(history):
    history.new_ui_records = [_archive("run-1", "workflow summary")]

    assert await resolve_task_result_text("t1", "k7Xm2p", "run-1") == (
        "workflow summary"
    )


@pytest.mark.asyncio
async def test_a_predecessors_archive_is_never_served(history):
    """``task:*`` checkpoints outlive the app rows that cascade on delete, so a
    namespace can hold an older run's archive when a new run reuses the id."""
    history.new_ui_records = [_archive("run-1", "stale summary")]

    assert await resolve_task_result_text("t1", "k7Xm2p", "run-2") is None


@pytest.mark.asyncio
async def test_without_a_run_id_only_the_transcript_derivation_applies(history):
    history.new_ui_records = [_archive("run-1", "workflow summary")]
    history.messages = [AIMessage(content="subagent answer")]

    assert await resolve_task_result_text("t1", "k7Xm2p") == "subagent answer"


@pytest.mark.asyncio
async def test_falls_back_to_the_last_ai_message_when_nothing_is_archived(history):
    history.messages = [
        AIMessage(content="first pass"),
        HumanMessage(content="follow-up"),
        AIMessage(content="final answer"),
    ]

    assert await resolve_task_result_text("t1", "k7Xm2p", "run-1") == "final answer"


@pytest.mark.asyncio
async def test_an_empty_namespace_resolves_to_none(history):
    assert await resolve_task_result_text("t1", "k7Xm2p", "run-1") is None


@pytest.mark.asyncio
async def test_block_list_content_is_flattened(history):
    history.messages = [
        AIMessage(content=[{"type": "text", "text": "block answer"}])
    ]

    assert await resolve_task_result_text("t1", "k7Xm2p") == "block answer"


@pytest.mark.asyncio
async def test_new_registries_get_the_resolver_injected():
    """The registry holds no checkpoint access of its own — losing this
    injection silently downgrades every cross-turn TaskOutput to "no longer
    available"."""
    registry = await store.BackgroundRegistryStore().get_or_create_registry("t1")

    assert registry.result_resolver is resolve_task_result_text


# ---------------------------------------------------------------------------
# The archive-only resolver: the strict source for callers holding a
# failed/unknown verdict, where the transcript derivation above would fabricate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_only_never_derives_from_the_transcript(history):
    """The whole point of the split. A genuinely-errored subagent archives
    nothing but still leaves mid-work text — serving that as its result turns
    a clear failure into a plausible-looking fabricated answer."""
    history.messages = [
        AIMessage(content="Step 1 done. Now fetching the second dataset…")
    ]

    assert await resolve_archived_result_text("t1", "k7Xm2p", "run-9") is None
    # ...while the permissive resolver, by design, would have served it.
    assert await resolve_task_result_text("t1", "k7Xm2p", "run-9") == (
        "Step 1 done. Now fetching the second dataset…"
    )


@pytest.mark.asyncio
async def test_archive_only_is_scoped_to_the_requested_run(history):
    """A ``task:*`` namespace outlives the rows that cascade on delete, so a
    predecessor's archive can sit beside a re-dispatched run's id."""
    history.new_ui_records = [_archive("run-7", "predecessor summary")]
    history.messages = [AIMessage(content="leftover text from an earlier attempt")]

    assert await resolve_archived_result_text("t1", "k7Xm2p", "run-8") is None
    assert await resolve_archived_result_text("t1", "k7Xm2p", "run-7") == (
        "predecessor summary"
    )


@pytest.mark.asyncio
async def test_new_registries_get_the_archived_resolver_injected():
    """Injected separately from ``result_resolver``, so it can be forgotten
    separately — and a missing one silently reopens the lost-result defect."""
    registry = await store.BackgroundRegistryStore().get_or_create_registry("t1")

    assert registry.archived_result_resolver is resolve_archived_result_text
