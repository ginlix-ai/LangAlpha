"""SteeringMiddleware delivery: peek, partition by run stamp, LREM consumed.

Review F3 (v4 2.4c): the middleware must never destructively pop payloads it
does not deliver. A payload stamped for a FOREIGN run stays in the queue
untouched (the end-of-run drain returns it to the user); only delivered (or
garbage) payloads are removed, each by exact-value LREM — so a crash between
read and remove can only re-deliver, never lose, an accepted user message.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.ptc_agent.agent.middleware.steering import SteeringMiddleware
from tests.unit.server.handlers.chat.redis_fakes import FakeCache

_KEY = "workflow:steering:t-1"


def _config(run_id: str | None = "run-A") -> dict:
    cfg: dict = {"configurable": {"thread_id": "t-1"}}
    if run_id is not None:
        cfg["metadata"] = {"run_id": run_id}
    return cfg


def _payload(content: str, run_id: str | None) -> str:
    data: dict = {"content": content}
    if run_id is not None:
        data["run_id"] = run_id
    return json.dumps(data)


async def _run(cache: FakeCache, config: dict):
    runtime = MagicMock()
    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        return await SteeringMiddleware().abefore_model(
            {}, runtime, config=config
        )


@pytest.mark.asyncio
async def test_delivers_own_and_unstamped_but_leaves_foreign_queued():
    cache = FakeCache()
    cache.client.lists[_KEY] = [
        _payload("for me", "run-A"),
        _payload("legacy unstamped", None),
        _payload("for the dead run", "run-B"),
    ]

    result = await _run(cache, _config("run-A"))

    assert result is not None
    (msg,) = result["messages"]
    assert "for me" in msg.content
    assert "legacy unstamped" in msg.content
    assert "for the dead run" not in msg.content
    # The foreign payload is still queued — untouched, not popped-and-repushed.
    assert cache.client.lists[_KEY] == [_payload("for the dead run", "run-B")]


@pytest.mark.asyncio
async def test_all_foreign_queue_is_left_intact():
    cache = FakeCache()
    queued = [_payload("x", "run-B"), _payload("y", "run-C")]
    cache.client.lists[_KEY] = list(queued)

    result = await _run(cache, _config("run-A"))

    assert result is None
    assert cache.client.lists[_KEY] == queued


@pytest.mark.asyncio
async def test_unstamped_run_consumes_everything():
    """No own-run identity in config (legacy caller): all payloads deliver,
    matching pre-stamp behavior."""
    cache = FakeCache()
    cache.client.lists[_KEY] = [
        _payload("a", "run-B"),
        _payload("b", None),
    ]

    result = await _run(cache, _config(run_id=None))

    assert result is not None
    assert cache.client.lists.get(_KEY, []) == []


@pytest.mark.asyncio
async def test_garbage_payload_is_removed_but_not_delivered():
    cache = FakeCache()
    cache.client.lists[_KEY] = ["not json {", _payload("real", "run-A")]

    result = await _run(cache, _config("run-A"))

    assert result is not None
    (msg,) = result["messages"]
    assert "real" in msg.content
    assert cache.client.lists.get(_KEY, []) == []


@pytest.mark.asyncio
async def test_duplicate_own_payloads_all_consumed():
    """LREM count=1 per read instance: two identical payloads read = two
    removed, none stranded."""
    cache = FakeCache()
    dup = _payload("again", "run-A")
    cache.client.lists[_KEY] = [dup, dup]

    result = await _run(cache, _config("run-A"))

    assert result is not None
    assert cache.client.lists.get(_KEY, []) == []
