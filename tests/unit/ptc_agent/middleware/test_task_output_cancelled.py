"""TaskOutput miss-path contract: durable archive first, cancelled last.

A missing registry entry does not mean the result is gone — it may be
evicted, wiped by a stop, lost to a restart, or held by another worker while
the subagent's answer sits in its ``task:{id}`` checkpoint namespace. The
tool must recover from the archive first, and only report "cancelled by a
user stop" (never "not found") when the archive has nothing either.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from ptc_agent.agent.middleware.background_subagent.tools import (
    _delivered_result_text,
    create_task_output_tool,
)


def _middleware(resolved: str | None) -> MagicMock:
    middleware = MagicMock()
    registry = MagicMock()
    registry.get_by_task_id = AsyncMock(return_value=None)
    registry.resolve_result_text = AsyncMock(return_value=resolved)
    middleware.registry = registry
    return middleware


@pytest.mark.asyncio
async def test_missing_task_recovers_from_durable_archive():
    tool = create_task_output_tool(_middleware("Paris is the capital."))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "Paris is the capital." in result
    assert "recovered from the durable archive" in result
    assert "cancelled" not in result.lower()


@pytest.mark.asyncio
async def test_missing_task_with_empty_archive_reports_cancelled():
    tool = create_task_output_tool(_middleware(None))

    result = await tool.coroutine(task_id="k7Xm2p")

    assert "cancelled by a user stop" in result.lower()
    assert "not found" not in result.lower()


# ---------------------------------------------------------------------------
# Delivery derivation: durable checkpoint answer first, in-memory fallback
# ---------------------------------------------------------------------------


def _completed_task(result) -> SimpleNamespace:
    return SimpleNamespace(task_id="k7Xm2p", result=result)


class TestDeliveredResultText:
    @pytest.mark.asyncio
    async def test_durable_answer_wins_for_successful_results(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value="archived answer")
        task = _completed_task({"success": True, "result": "in-memory answer"})
        assert await _delivered_result_text(registry, task) == "archived answer"

    @pytest.mark.asyncio
    async def test_falls_back_to_memory_when_archive_empty(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value=None)
        task = _completed_task({"success": True, "result": "in-memory answer"})
        assert await _delivered_result_text(registry, task) == "in-memory answer"

    @pytest.mark.asyncio
    async def test_failures_never_consult_the_archive(self):
        registry = MagicMock()
        registry.resolve_result_text = AsyncMock(return_value="stale partial")
        task = _completed_task({"success": False, "error": "boom"})
        text = await _delivered_result_text(registry, task)
        assert "boom" in text
        registry.resolve_result_text.assert_not_awaited()


class TestRegistryResolveResultText:
    @pytest.mark.asyncio
    async def test_no_resolver_means_none(self):
        registry = BackgroundTaskRegistry(thread_id="")
        assert await registry.resolve_result_text("k7Xm2p") is None

    @pytest.mark.asyncio
    async def test_resolver_errors_degrade_to_none(self):
        registry = BackgroundTaskRegistry(thread_id="t1")
        registry.result_resolver = AsyncMock(side_effect=RuntimeError("db down"))
        assert await registry.resolve_result_text("k7Xm2p") is None
