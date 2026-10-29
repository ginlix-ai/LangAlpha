"""A firing runs in the scheduler's context, never in whoever dispatched it.

A trigger from inside an agent turn dispatches with that turn's context,
which carries its LangChain runnable config. A firing that inherited it
streamed its own events into the chat that triggered it. The firing must see
the context the scheduler started in, and nothing its caller set since.
"""

from __future__ import annotations

import asyncio
import contextvars
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.automation_scheduler import AutomationScheduler

_MOD = "src.server.services.automation_scheduler"

_TURN = contextvars.ContextVar("turn", default=None)
_SERVER = contextvars.ContextVar("server", default=None)


async def _fire(scheduler: AutomationScheduler) -> dict[str, object]:
    seen: dict[str, object] = {}

    async def record(automation, execution_id):
        seen.update(turn=_TURN.get(), server=_SERVER.get())

    with patch.object(scheduler, "_run_execution", record):
        _TURN.set("the triggering turn's config")
        scheduler.dispatch({"automation_id": "auto-fake-1"}, "exec-fake-1", name="manual_exec_test")
        await asyncio.gather(*scheduler._running_tasks)
    return seen


class TestFiringContext:
    def teardown_method(self):
        AutomationScheduler._instance = None

    @pytest.mark.asyncio
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_a_firing_sees_the_context_the_scheduler_started_in(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        scheduler = AutomationScheduler()
        _SERVER.set("server")
        with patch.object(AutomationScheduler, "_poll_loop", AsyncMock()):
            await scheduler.start()
        try:
            _SERVER.set("overwritten by the caller")
            seen = await _fire(scheduler)
        finally:
            await scheduler.shutdown()

        assert seen == {"turn": None, "server": "server"}

    @pytest.mark.asyncio
    @patch(f"{_MOD}.AutomationExecutor")
    async def test_a_scheduler_that_never_started_still_drops_the_callers_context(self, mock_executor_cls):
        mock_executor_cls.get_instance.return_value = MagicMock()
        scheduler = AutomationScheduler()

        seen = await _fire(scheduler)

        assert seen["turn"] is None
