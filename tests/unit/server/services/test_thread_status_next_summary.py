"""What a thread's status tells the composer of its next send's summary
(``read_next_summary``): the turn start's own reading, from the state the
last answer left, the user's ``cold_compaction`` included."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ptc_agent.agent.middleware.compaction.rebuild import LAST_ANSWER_KEY
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import STATE_SYSTEM_PIN
from ptc_agent.agent.prompts import release
from src.server.services import thread_status

COLD = 7_000
THRESHOLD = 10_000
#: The server's clock at the read, in epoch seconds.
READ_AT = 1_250.0


@pytest.fixture(autouse=True)
def _server_clock(monkeypatch):
    monkeypatch.setattr(thread_status.time, "time", lambda: READ_AT)


@pytest.fixture(autouse=True)
def _on_the_branch(monkeypatch):
    """The next turn starts from the newest checkpoint unless a test says not."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(thread_status, "get_checkpointer", lambda: object())
    monkeypatch.setattr(thread_status, "turn_base_checkpoint_id", AsyncMock(return_value=None))


def _values(tokens: int, cold: int = COLD) -> dict:
    return {
        "_cached_input_tokens": tokens,
        "_last_model_response_at": 1_000.0,
        LAST_ANSWER_KEY: {
            "threshold": THRESHOLD,
            "claude": True,
            "claude_model": True,
            "written_ttl": 300,
            "route_ttl": 300,
            "cold_tokens": cold,
            "floor_tokens": 5_000,
        },
        STATE_SYSTEM_PIN: {"source_digest": release.source_digest()},
    }


async def _read(tokens: int, cold: int = COLD) -> dict | None:
    return await thread_status.read_next_summary("t-1", latest={}, values=_values(tokens, cold))


@pytest.mark.asyncio
async def test_a_cold_thread_is_told_with_the_time_its_cache_expires():
    """With the server's own clock beside it, so a client waits ``at - now``
    whatever its clock says."""
    assert await _read(COLD) == {"reason": "cold", "at": 1_300_000, "now": 1_250_000}


@pytest.mark.asyncio
async def test_with_cold_compaction_off_no_cold_summary_is_told():
    assert await _read(COLD, cold=0) is None
    assert await _read(THRESHOLD, cold=0) == {"reason": "threshold"}


@pytest.mark.asyncio
async def test_an_attempt_whose_input_never_reached_the_graph_leaves_the_cache_clock(monkeypatch):
    """It made no model call, so the cache expires from the attempt before."""
    from datetime import UTC, datetime

    from src.server.database.runs import lifecycle as tl_db

    called = {"status": "completed", "usage_settled_at": datetime.fromtimestamp(900, UTC)}
    refused = {
        "status": "error",
        "usage_settled_at": datetime.fromtimestamp(1_200, UTC),
        "metadata": {"pending_input": {"checkpoint_id": None}},
    }

    async def latest(thread_id, *, without_key=None, **kwargs):
        return called if without_key == "pending_input" else refused

    monkeypatch.setattr(tl_db, "get_latest_attempt", latest)

    assert await thread_status.read_next_summary("t-1", latest=refused, values=_values(COLD)) == {"reason": "cold", "at": 1_300_000, "now": 1_250_000}
    assert await thread_status.read_next_summary("t-1", values=_values(COLD)) == {"reason": "cold", "at": 1_300_000, "now": 1_250_000}


@pytest.mark.asyncio
async def test_a_slow_read_leaves_the_closing_frame_without_a_hint(monkeypatch):
    import asyncio

    async def hangs(thread_id, checkpoint_id=None):
        await asyncio.sleep(30)

    monkeypatch.setattr(thread_status, "NEXT_SUMMARY_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(thread_status, "get_checkpoint_tuple", hangs)

    assert await asyncio.wait_for(thread_status.read_next_summary("t-1", latest={}), 2) is thread_status.UNREAD


@pytest.mark.asyncio
async def test_it_reads_the_checkpoint_the_next_turn_starts_from(monkeypatch):
    """A fork stopped before its first step leaves its abandoned branch the
    newest checkpoint, and the next turn starts from the fork's base."""
    from unittest.mock import AsyncMock

    def saved(values: dict):
        return SimpleNamespace(checkpoint={"channel_values": values})

    abandoned = saved(_values(THRESHOLD))
    checkpoints = {None: abandoned, "base": saved(_values(COLD))}

    async def get_checkpoint_tuple(thread_id, checkpoint_id=None):
        return checkpoints[checkpoint_id]

    monkeypatch.setattr(thread_status, "get_checkpoint_tuple", get_checkpoint_tuple)
    monkeypatch.setattr(thread_status, "turn_base_checkpoint_id", AsyncMock(return_value="base"))
    cold = {"reason": "cold", "at": 1_300_000, "now": 1_250_000}

    assert await thread_status.read_next_summary("t-1", latest={}) == cold


@pytest.mark.parametrize(("status", "told"), [("completed", True), ("interrupted", False)])
@pytest.mark.asyncio
async def test_the_status_reads_the_checkpoint_after_the_run_it_shows_over(status, told):
    """A run ending between the status's own checkpoint read and its ledger
    read commits its answer first, so the summary reads the checkpoint again.
    An interrupted run resumes past its turn start, so nothing is told."""
    import contextlib
    from unittest.mock import AsyncMock, patch

    from src.server.database.runs import lifecycle as tl_db
    from tests.unit.server.handlers.chat.redis_fakes import FakeCache
    from tests.unit.server.services.test_thread_status_report_back import _patches

    def saved(values: dict):
        return SimpleNamespace(checkpoint={"channel_values": values, "pending_sends": []}, config={})

    # Before the answer, the context was under the cold threshold; the answer took it over.
    reads = iter([saved(_values(COLD - 1)), saved(_values(COLD))])
    latest = {"conversation_response_id": "r1", "status": status, "created_at": None}
    with contextlib.ExitStack() as stack:
        for p in _patches(FakeCache()):
            stack.enter_context(p)
        stack.enter_context(patch.object(thread_status, "get_checkpoint_tuple", AsyncMock(side_effect=lambda *a: next(reads))))
        stack.enter_context(patch.object(tl_db, "get_latest_attempt", AsyncMock(return_value=latest)))
        resp = await thread_status.read_thread_runtime_status("t-1")

    assert resp["next_summary"] == ({"reason": "cold", "at": 1_300_000, "now": 1_250_000} if told else None)



def _claude_code_values(at: float = 1_100.0) -> dict:
    from ptc_agent.agent.claude_code.compaction import RULES_KEY

    return {RULES_KEY: {"threshold": THRESHOLD, "floor_tokens": 5_000, "cold_tokens": COLD, "at": at}}


def _claude_session(tokens: int) -> dict:
    from datetime import UTC, datetime

    usage = {"input_tokens": 10, "cache_read_input_tokens": tokens - 10, "cache_creation_input_tokens": 0, "output_tokens": 0,
             "cache_creation": {"ephemeral_5m_input_tokens": 10}}
    pin = {"system_prompt": "p", "tools": [], "source_digest": release.source_digest(), "prompt_version": release.prompt_version()}
    stamp = datetime.fromtimestamp(1_000.0, UTC).isoformat().replace("+00:00", "Z")
    return {"session_id": "s", "pinned": pin, "entries": [{"type": "assistant", "timestamp": stamp, "message": {"usage": usage}}]}


@pytest.mark.asyncio
async def test_a_claude_code_thread_is_told_from_its_session_and_the_rules_its_last_turn_ran_under(monkeypatch):
    """Its turn start compacts by its own rules (``claude_code.compaction``),
    which read Claude's session rather than the main agent's last answer."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(thread_status, "_claude_session", AsyncMock(return_value=_claude_session(COLD)))
    assert await thread_status.read_next_summary("t-1", latest={}, values=_claude_code_values()) == {
        "reason": "cold", "at": 1_300_000, "now": 1_250_000}
    monkeypatch.setattr(thread_status, "_claude_session", AsyncMock(return_value=_claude_session(COLD - 1)))
    assert await thread_status.read_next_summary("t-1", latest={}, values=_claude_code_values()) is None


@pytest.mark.asyncio
async def test_a_thread_another_model_answered_since_is_told_by_the_main_agents_rules(monkeypatch):
    from unittest.mock import AsyncMock

    monkeypatch.setattr(thread_status, "_claude_session", AsyncMock(return_value=_claude_session(COLD)))
    values = {**_values(COLD, cold=0), **_claude_code_values(at=900.0)}
    assert await thread_status.read_next_summary("t-1", latest={}, values=values) is None


@pytest.mark.asyncio
async def test_a_claude_code_threads_session_is_replayed_from_its_checkpoints(monkeypatch):
    """A raw checkpoint holds none of it: the channel keeps only its writes."""
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import START, StateGraph

    from ptc_agent.agent.claude_code.runtime import ClaudeCodeState

    saver = InMemorySaver()
    graph = StateGraph(ClaudeCodeState).add_node("model", lambda state: {}).add_edge(START, "model").compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "t-cc"}}
    for uuid in ("a", "b"):
        await graph.aupdate_state(config, {"claude_session": [{"session_id": "s", "entries": [{"uuid": uuid}]}]}, as_node="model")
    monkeypatch.setattr(thread_status, "get_checkpointer", lambda: saver)
    assert "claude_session" not in thread_status.extract_state_values(await saver.aget_tuple(config))
    assert [e["uuid"] for e in (await thread_status._claude_session("t-cc", None))["entries"]] == ["a", "b"]

