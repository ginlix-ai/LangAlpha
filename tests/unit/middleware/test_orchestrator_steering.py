"""Tests for orchestrator steering re-invocation.

Verifies that BackgroundSubagentOrchestrator checks for pending steering
messages before returning, and re-invokes the agent when steering is found.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.background_subagent.orchestrator import (
    BackgroundSubagentOrchestrator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_middleware(*, has_pending: bool = False) -> MagicMock:
    """Create a mock BackgroundSubagentMiddleware."""
    mw = MagicMock()
    mw.registry = MagicMock()
    mw.registry.has_pending_tasks.return_value = has_pending
    mw.registry.pending_count = 1 if has_pending else 0
    mw.timeout = 60.0
    mw.registry._tasks = {}
    return mw


def _make_agent(*, stream_events: list | None = None) -> MagicMock:
    """Create a mock agent with ainvoke, astream, and aupdate_state."""
    agent = MagicMock()
    agent.ainvoke = AsyncMock(return_value={"messages": []})
    agent.aupdate_state = AsyncMock()

    async def _astream(*args, **kwargs):
        for ev in (stream_events or []):
            yield ev

    agent.astream = _astream
    return agent


def _config(thread_id: str = "test-thread") -> dict:
    return {"configurable": {"thread_id": thread_id}}


# ---------------------------------------------------------------------------
# _has_pending_steering
# ---------------------------------------------------------------------------


class TestHasPendingSteering:
    """Tests for _has_pending_steering helper."""

    @pytest.mark.asyncio
    async def test_returns_true_when_steering_queued(self):
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(return_value=True)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._has_pending_steering(_config()) is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_steering(self):
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(return_value=False)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._has_pending_steering(_config()) is False

    @pytest.mark.asyncio
    async def test_returns_false_when_checker_is_none(self):
        """Redis unavailable → build_message_checker returns None."""
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=None),
        ):
            assert await orch._has_pending_steering(_config()) is False

    @pytest.mark.asyncio
    async def test_returns_false_on_redis_exception(self):
        """Redis glitch in checker → swallowed, returns False."""
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(side_effect=ConnectionError("redis down"))
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._has_pending_steering(_config()) is False


# ---------------------------------------------------------------------------
# _reinvoke_for_steering
# ---------------------------------------------------------------------------


class TestReinvokeForSteering:
    """Tests for _reinvoke_for_steering helper."""

    @pytest.mark.asyncio
    async def test_returns_true_and_updates_state_when_steering_pending(self):
        agent = _make_agent()
        orch = BackgroundSubagentOrchestrator(agent, _make_middleware())
        cfg = _config()

        checker = AsyncMock(return_value=True)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            result = await orch._reinvoke_for_steering(cfg, iteration=1)

        assert result is True
        agent.aupdate_state.assert_awaited_once()
        call_args = agent.aupdate_state.call_args
        msg = call_args[0][1]["messages"][0]
        assert isinstance(msg, HumanMessage)
        assert msg.name == "orchestrator"
        assert call_args[1]["as_node"] == "__start__"

    @pytest.mark.asyncio
    async def test_returns_false_when_no_steering(self):
        agent = _make_agent()
        orch = BackgroundSubagentOrchestrator(agent, _make_middleware())

        checker = AsyncMock(return_value=False)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            result = await orch._reinvoke_for_steering(_config(), iteration=1)

        assert result is False
        agent.aupdate_state.assert_not_awaited()


# ---------------------------------------------------------------------------
# astream — Gap B (auto_wait=False with pending tasks + steering)
# ---------------------------------------------------------------------------


class TestAstreamGapB:
    """auto_wait=False, pending tasks, steering in Redis → re-invoke."""

    @pytest.mark.asyncio
    async def test_reinvokes_agent_when_steering_pending(self):
        agent = _make_agent(stream_events=["ev1"])
        mw = _make_middleware(has_pending=True)
        orch = BackgroundSubagentOrchestrator(
            agent, mw, auto_wait=False, max_iterations=2
        )
        cfg = _config()

        call_count = 0

        async def _checker():
            nonlocal call_count
            call_count += 1
            # First call: steering is pending → re-invoke
            # Second call: no steering → return
            return call_count == 1

        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=_checker),
        ):
            events = []
            async for ev in orch.astream({"messages": []}, cfg):
                events.append(ev)

        # Agent streamed twice (initial + re-invocation)
        assert events.count("ev1") == 2
        agent.aupdate_state.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_immediately_when_no_steering(self):
        agent = _make_agent(stream_events=["ev1"])
        mw = _make_middleware(has_pending=True)
        orch = BackgroundSubagentOrchestrator(
            agent, mw, auto_wait=False, max_iterations=3
        )

        checker = AsyncMock(return_value=False)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            events = []
            async for ev in orch.astream({"messages": []}, _config()):
                events.append(ev)

        # Streamed only once (no re-invocation)
        assert events == ["ev1"]
        agent.aupdate_state.assert_not_awaited()


# ---------------------------------------------------------------------------
# astream — Gap C (auto_wait=True, wait interrupted, no notification)
# ---------------------------------------------------------------------------


class TestAstreamGapC:
    """auto_wait=True, wait interrupted by steering, no tasks completed."""

    @pytest.mark.asyncio
    async def test_reinvokes_after_interrupted_wait(self):
        agent = _make_agent(stream_events=["ev1"])
        mw = _make_middleware(has_pending=True)
        # Make wait_for_all a no-op (simulates interrupted wait)
        mw.registry.wait_for_all = AsyncMock()
        orch = BackgroundSubagentOrchestrator(
            agent, mw, auto_wait=True, max_iterations=2
        )
        cfg = _config()

        call_count = 0

        async def _checker():
            nonlocal call_count
            call_count += 1
            return call_count == 1

        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=_checker),
        ):
            events = []
            async for ev in orch.astream({"messages": []}, cfg):
                events.append(ev)

        assert events.count("ev1") == 2
        agent.aupdate_state.assert_awaited_once()


# ---------------------------------------------------------------------------
# ainvoke — Gap D
# ---------------------------------------------------------------------------


class TestAinvokeGapD:
    """ainvoke returns without re-invoking when steering is pending."""

    @pytest.mark.asyncio
    async def test_reinvokes_when_steering_pending(self):
        agent = _make_agent()
        mw = _make_middleware(has_pending=False)
        orch = BackgroundSubagentOrchestrator(agent, mw, max_iterations=2)
        cfg = _config()

        call_count = 0

        async def _checker():
            nonlocal call_count
            call_count += 1
            return call_count == 1

        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=_checker),
        ):
            await orch.ainvoke({"messages": []}, cfg)

        # ainvoke called twice (initial + re-invocation)
        assert agent.ainvoke.await_count == 2
        agent.aupdate_state.assert_awaited_once()


# ---------------------------------------------------------------------------
# max_iterations guard
# ---------------------------------------------------------------------------


class TestMaxIterationsGuard:
    """Steering re-invocation respects max_iterations limit."""

    @pytest.mark.asyncio
    async def test_does_not_loop_forever(self):
        agent = _make_agent(stream_events=["ev1"])
        mw = _make_middleware(has_pending=True)
        orch = BackgroundSubagentOrchestrator(
            agent, mw, auto_wait=False, max_iterations=2
        )

        # Always report steering pending → would loop forever without guard
        checker = AsyncMock(return_value=True)
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            events = []
            async for ev in orch.astream({"messages": []}, _config()):
                events.append(ev)

        # Should stop at max_iterations=2
        assert events.count("ev1") == 2


# ---------------------------------------------------------------------------
# build_message_checker — own-run stamp filter (v4 2.4c)
# ---------------------------------------------------------------------------


class TestBuildMessageCheckerStampFilter:
    """The checker counts only payloads THIS run would consume:
    SteeringMiddleware never delivers a foreign-stamped payload here, so
    counting one would wake task waits and re-invoke the agent for a
    message that cannot arrive."""

    def _cache(self, raws: list) -> MagicMock:
        cache = MagicMock()
        cache.enabled = True
        cache.client = MagicMock()
        cache.client.lrange = AsyncMock(return_value=raws)
        return cache

    async def _check(self, raws: list, own_run_id: str | None) -> bool:
        from ptc_agent.agent.middleware.background_subagent.utils import (
            build_message_checker,
        )

        with patch(
            "src.utils.cache.redis_cache.get_cache_client",
            return_value=self._cache(raws),
        ):
            checker = await build_message_checker("t-1", own_run_id=own_run_id)
        return await checker()

    @pytest.mark.asyncio
    async def test_own_stamped_counts(self):
        raw = '{"content": "hi", "run_id": "r-own"}'
        assert await self._check([raw], "r-own") is True

    @pytest.mark.asyncio
    async def test_unstamped_counts(self):
        assert await self._check(['{"content": "hi"}'], "r-own") is True

    @pytest.mark.asyncio
    async def test_foreign_stamped_does_not_count(self):
        raw = '{"content": "hi", "run_id": "r-dead"}'
        assert await self._check([raw], "r-own") is False

    @pytest.mark.asyncio
    async def test_foreign_plus_own_counts(self):
        raws = [
            '{"content": "a", "run_id": "r-dead"}',
            b'{"content": "b", "run_id": "r-own"}',
        ]
        assert await self._check(raws, "r-own") is True

    @pytest.mark.asyncio
    async def test_no_identity_counts_everything(self):
        """Without a run identity (legacy caller) nothing is filtered."""
        raw = '{"content": "hi", "run_id": "r-dead"}'
        assert await self._check([raw], None) is True

    @pytest.mark.asyncio
    async def test_unparseable_does_not_count(self):
        assert await self._check(["not-json{"], "r-own") is False

    @pytest.mark.asyncio
    async def test_empty_queue_is_false(self):
        assert await self._check([], "r-own") is False
