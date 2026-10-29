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
# _pending_steer_senders
# ---------------------------------------------------------------------------


class TestPendingSteerSenders:
    """Tests for _pending_steer_senders helper."""

    @pytest.mark.asyncio
    async def test_names_the_sender_when_steering_queued(self):
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(return_value=["user"])
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._pending_steer_senders(_config()) == ["user"]

    @pytest.mark.asyncio
    async def test_returns_nobody_when_no_steering(self):
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(return_value=[])
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._pending_steer_senders(_config()) == []

    @pytest.mark.asyncio
    async def test_returns_nobody_when_checker_is_none(self):
        """Redis unavailable → build_message_checker returns None."""
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=None),
        ):
            assert await orch._pending_steer_senders(_config()) == []

    @pytest.mark.asyncio
    async def test_returns_nobody_on_redis_exception(self):
        """Redis glitch in checker → swallowed, returns no senders."""
        orch = BackgroundSubagentOrchestrator(
            _make_agent(), _make_middleware()
        )
        checker = AsyncMock(side_effect=ConnectionError("redis down"))
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            assert await orch._pending_steer_senders(_config()) == []


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

        checker = AsyncMock(return_value=["user"])
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
        assert msg.content == "User sent additional instructions."
        assert call_args[1]["as_node"] == "__start__"

    @pytest.mark.asyncio
    async def test_trigger_names_the_chief_of_staff_when_it_sent_the_steer(self):
        agent = _make_agent()
        orch = BackgroundSubagentOrchestrator(agent, _make_middleware())

        checker = AsyncMock(return_value=["chief_of_staff"])
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            result = await orch._reinvoke_for_steering(_config(), iteration=1)

        assert result is True
        msg = agent.aupdate_state.call_args[0][1]["messages"][0]
        assert msg.content == "Chief of Staff sent additional instructions."

    @pytest.mark.asyncio
    async def test_returns_false_when_no_steering(self):
        agent = _make_agent()
        orch = BackgroundSubagentOrchestrator(agent, _make_middleware())

        checker = AsyncMock(return_value=[])
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            result = await orch._reinvoke_for_steering(_config(), iteration=1)

        assert result is False
        agent.aupdate_state.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_last_pass_leaves_no_trigger(self):
        """With no re-run left, a pending steer becomes the next turn, so no
        trigger may be left in the checkpoint ahead of it."""
        agent = _make_agent()
        orch = BackgroundSubagentOrchestrator(agent, _make_middleware())

        checker = AsyncMock(return_value=["user"])
        with patch(
            "ptc_agent.agent.middleware.background_subagent.utils.build_message_checker",
            new=AsyncMock(return_value=checker),
        ):
            result = await orch._reinvoke_for_steering(
                _config(), iteration=orch.max_iterations
            )

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
            return ["user"] if call_count == 1 else []

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

        checker = AsyncMock(return_value=[])
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
            return ["user"] if call_count == 1 else []

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
            return ["user"] if call_count == 1 else []

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
        checker = AsyncMock(return_value=["user"])
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
# build_message_checker: asks the outbox what this run would take in
# ---------------------------------------------------------------------------


class TestBuildMessageChecker:
    """The checker counts only steers THIS run would take in (the outbox's
    takeable predicate): counting another would wake task waits and
    re-invoke the agent for a message that cannot arrive."""

    _PENDING = "src.server.database.runs.outbox.pending_steer_senders"

    @pytest.mark.asyncio
    async def test_asks_for_this_runs_steers(self):
        from ptc_agent.agent.middleware.background_subagent.utils import (
            build_message_checker,
        )

        pending = AsyncMock(return_value=["user"])
        with patch(self._PENDING, new=pending):
            checker = await build_message_checker("t-1", own_run_id="r-own")
            assert await checker() == ["user"]
        pending.assert_awaited_once_with("t-1", "r-own")

    @pytest.mark.asyncio
    async def test_a_failed_read_counts_as_nothing_waiting(self):
        from ptc_agent.agent.middleware.background_subagent.utils import (
            build_message_checker,
        )

        with patch(self._PENDING, new=AsyncMock(side_effect=RuntimeError("db"))):
            checker = await build_message_checker("t-1", own_run_id="r-own")
            assert await checker() == []

    @pytest.mark.asyncio
    async def test_no_thread_no_checker(self):
        from ptc_agent.agent.middleware.background_subagent.utils import (
            build_message_checker,
        )

        assert await build_message_checker(None) is None
