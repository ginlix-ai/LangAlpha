"""Tests for the flash report-back notification path.

Pins the durability contract introduced to fix the unreliable flash->PTC
report-back wake:

- ``_flash_report_back`` publishes the wake on a successful POST but no longer
  clears ``ptc_origin`` / ``flash_watch`` — the watch is cleared only when the
  report-back flash run completes and persists its summary.
- The POST is bounded-retried (network error / 5xx / 409 retry; other 4xx give
  up immediately). On exhausted failure it neither wakes nor clears.
- ``clear_flash_report_back`` is the shared cleanup helper.
- The flash completion hook clears only when ``report_back_ptc_thread_id`` is set.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.handlers.chat import flash_workflow, ptc_workflow


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


def _make_cache(origin, *, scard=0):
    """Build a MagicMock cache client with async ops wired for the report-back path."""
    cache = MagicMock()
    cache.enabled = True
    cache.get = AsyncMock(return_value=origin)
    cache.set = AsyncMock(return_value=True)
    cache.delete = AsyncMock(return_value=True)

    client = MagicMock()
    client.publish = AsyncMock()
    client.srem = AsyncMock()
    client.scard = AsyncMock(return_value=scard)
    client.delete = AsyncMock(return_value=1)
    cache.client = client
    return cache


def _origin(**overrides):
    base = {
        "origin": "flash",
        "report_back": True,
        "flash_thread_id": "flash-1",
        "flash_workspace_id": "fws-1",
        "ptc_thread_id": "ptc-1",
        "user_id": "u-1",
    }
    base.update(overrides)
    return base


class _FakeResp:
    def __init__(self, status: int, body: str = "", json_body: dict | None = None):
        self.status = status
        self._body = body
        self._json = {} if json_body is None else json_body

    async def text(self) -> str:
        return self._body

    async def json(self) -> dict:
        return self._json

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeReqCtx:
    """Async CM returned by ``session.post(...)`` — raises or yields a response."""

    def __init__(self, outcome):
        self._outcome = outcome

    async def __aenter__(self):
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    """Async CM that records ``post`` calls and replays queued outcomes."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.post_calls: list[dict] = []

    def post(self, url, **kwargs):
        self.post_calls.append({"url": url, **kwargs})
        return _FakeReqCtx(self._outcomes.pop(0))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _run_report_back(cache, session, ptc_thread_id="ptc-1", workspace_id="ws-1"):
    """Invoke ``_flash_report_back`` with mocked aiohttp + cache + zero backoff."""
    return patch.multiple(
        "src.server.handlers.chat.ptc_workflow",
        _REPORT_BACK_BACKOFFS=(0, 0),  # instant retries
    ), patch("aiohttp.ClientSession", MagicMock(return_value=session)), patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    )


# ---------------------------------------------------------------------------
# _flash_report_back — success publishes, does not clear
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_2xx_publishes_wake_with_run_id_records_pointer_and_does_not_clear():
    cache = _make_cache(_origin())
    session = _FakeSession(
        [_FakeResp(200, json_body={"status": "dispatched", "run_id": "rb-1"})]
    )

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    # Wake published exactly once on the flash thread channel, carrying the run_id
    # so an in-session client attaches to that exact run.
    cache.client.publish.assert_awaited_once()
    channel, payload = cache.client.publish.await_args.args
    assert channel == "thread:wake:flash-1"
    wake = json.loads(payload)
    assert wake["thread_id"] == "flash-1"
    assert wake["run_id"] == "rb-1"

    # Durable run pointer recorded (for the reload path) with the watch TTL.
    cache.set.assert_awaited_once()
    set_args = cache.set.await_args
    assert set_args.args[0] == "flash_rb_run:flash-1"
    assert set_args.args[1] == {"run_id": "rb-1"}
    assert set_args.kwargs["ttl"] == ptc_workflow._FLASH_RB_RUN_TTL

    # Durable watch state survives — NOT cleared here.
    cache.delete.assert_not_called()
    cache.client.srem.assert_not_called()
    cache.client.delete.assert_not_called()

    # POST body carries the carry field + system query_type.
    assert len(session.post_calls) == 1
    body = session.post_calls[0]["json"]
    assert body["report_back_ptc_thread_id"] == "ptc-1"
    assert body["query_type"] == "system"
    assert body["agent_mode"] == "flash"


@pytest.mark.asyncio
async def test_no_origin_returns_early():
    cache = _make_cache(None)
    session = _FakeSession([])

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert session.post_calls == []
    cache.client.publish.assert_not_called()
    cache.delete.assert_not_called()


@pytest.mark.asyncio
async def test_report_back_disabled_returns_early():
    cache = _make_cache(_origin(report_back=False))
    session = _FakeSession([])

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert session.post_calls == []
    cache.client.publish.assert_not_called()


# ---------------------------------------------------------------------------
# _flash_report_back — retry behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retries_on_5xx_then_succeeds():
    cache = _make_cache(_origin())
    session = _FakeSession([_FakeResp(503), _FakeResp(200)])

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert len(session.post_calls) == 2  # retried once, then dispatched
    cache.client.publish.assert_awaited_once()
    cache.delete.assert_not_called()


@pytest.mark.asyncio
async def test_retries_on_network_error_then_succeeds():
    cache = _make_cache(_origin())
    session = _FakeSession([ConnectionError("network down"), _FakeResp(200)])

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert len(session.post_calls) == 2
    cache.client.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_exhausts_on_409_5xx_network_no_wake_no_clear():
    # 409 (busy) -> 500 -> network error: all retryable, three attempts, exhaust.
    cache = _make_cache(_origin())
    session = _FakeSession(
        [_FakeResp(409), _FakeResp(500), ConnectionError("boom")]
    )

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert len(session.post_calls) == 3  # all three attempts used
    # Exhausted: neither wakes nor clears -> keys survive for reload recovery.
    cache.client.publish.assert_not_called()
    cache.delete.assert_not_called()
    cache.client.srem.assert_not_called()


@pytest.mark.asyncio
async def test_gives_up_immediately_on_other_4xx():
    cache = _make_cache(_origin())
    session = _FakeSession([_FakeResp(400), _FakeResp(200)])

    cm_backoff, cm_session, cm_cache = _run_report_back(cache, session)
    with cm_backoff, cm_session, cm_cache:
        await ptc_workflow._flash_report_back("ptc-1", "ws-1")

    assert len(session.post_calls) == 1  # no retry after a non-retryable 4xx
    cache.client.publish.assert_not_called()
    cache.delete.assert_not_called()
    cache.client.srem.assert_not_called()


# ---------------------------------------------------------------------------
# clear_flash_report_back — shared helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_deletes_origin_srems_and_drops_empty_set():
    cache = _make_cache(None, scard=0)

    await ptc_workflow.clear_flash_report_back(cache, "ptc-1", "flash-1")

    cache.client.srem.assert_awaited_once_with("flash_watch:flash-1", "ptc-1")
    cache.client.scard.assert_awaited_once_with("flash_watch:flash-1")
    # SET emptied -> deleted, along with the report-back run pointer.
    cache.client.delete.assert_awaited_once_with("flash_watch:flash-1")
    deleted = [c.args[0] for c in cache.delete.await_args_list]
    assert "ptc_origin:ptc-1" in deleted
    assert "flash_rb_run:flash-1" in deleted


@pytest.mark.asyncio
async def test_clear_keeps_set_when_other_members_remain():
    cache = _make_cache(None, scard=2)

    await ptc_workflow.clear_flash_report_back(cache, "ptc-1", "flash-1")

    cache.delete.assert_awaited_once_with("ptc_origin:ptc-1")
    cache.client.srem.assert_awaited_once_with("flash_watch:flash-1", "ptc-1")
    # SET still has members -> NOT deleted.
    cache.client.delete.assert_not_called()


@pytest.mark.asyncio
async def test_clear_without_flash_thread_id_only_deletes_origin():
    cache = _make_cache(None)

    await ptc_workflow.clear_flash_report_back(cache, "ptc-1", None)

    cache.delete.assert_awaited_once_with("ptc_origin:ptc-1")
    cache.client.srem.assert_not_called()


# ---------------------------------------------------------------------------
# Flash completion hook -> clear gated on report_back_ptc_thread_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_completion_clears_when_report_back_id_set():
    cache = _make_cache(None)
    request = SimpleNamespace(report_back_ptc_thread_id="ptc-1")

    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ), patch(
        "src.server.handlers.chat.ptc_workflow.clear_flash_report_back",
        new=AsyncMock(),
    ) as mock_clear:
        await flash_workflow._maybe_clear_report_back(request, "flash-1")

    mock_clear.assert_awaited_once_with(cache, "ptc-1", "flash-1")


@pytest.mark.asyncio
async def test_completion_skips_clear_when_report_back_id_none():
    request = SimpleNamespace(report_back_ptc_thread_id=None)

    with patch(
        "src.server.handlers.chat.ptc_workflow.clear_flash_report_back",
        new=AsyncMock(),
    ) as mock_clear, patch(
        "src.utils.cache.redis_cache.get_cache_client"
    ) as mock_get_cache:
        await flash_workflow._maybe_clear_report_back(request, "flash-1")

    mock_clear.assert_not_called()
    # Short-circuits before touching the cache at all.
    mock_get_cache.assert_not_called()
