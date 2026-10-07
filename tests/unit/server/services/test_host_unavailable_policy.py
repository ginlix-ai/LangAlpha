"""When a computer stops waiting for its sandbox's recovering host.

The clock is the computer row's (stamped by the first refused start, kept by
the rest, cleared when the sandbox comes up), so the policy only compares what
the row answers with the configured threshold. A row that names another
sandbox answers nothing, and a threshold of 0 means no policy at all.
"""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from src.server.services.computer_manager import ComputerManager

_STAMP = "src.server.services.computer_manager._providers.stamp_computer_host_unavailable"
_CLEAR = "src.server.services.computer_manager._providers.clear_computer_host_unavailable"
_SINCE = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)


def _core_config(provider: str = "daytona", minutes: int = 60) -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(
            provider=provider,
            daytona=DaytonaConfig(
                api_key="test-key", host_unavailable_rebuild_after_minutes=minutes
            ),
        ),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _policy(core_config: CoreConfig):
    return ComputerManager.__new__(ComputerManager)._host_unavailable_policy(
        "comp-1", core_config
    )


@pytest.mark.asyncio
async def test_inside_the_threshold_the_computer_keeps_waiting(caplog):
    policy = _policy(_core_config(minutes=60))

    clock = (_SINCE, timedelta(minutes=59))
    with patch(_STAMP, AsyncMock(return_value=clock)) as stamp:
        with caplog.at_level(logging.WARNING):
            assert await policy.rebuild_now("sb-old") is None

    stamp.assert_awaited_once_with("comp-1", "sb-old")
    assert not caplog.records


@pytest.mark.asyncio
async def test_past_the_threshold_it_rebuilds_and_says_so_once(caplog):
    policy = _policy(_core_config(minutes=60))

    with patch(_STAMP, AsyncMock(return_value=(_SINCE, timedelta(minutes=61)))):
        with caplog.at_level(logging.WARNING):
            # The outage it answers with is what the rebuild's bind is fenced on.
            assert await policy.rebuild_now("sb-old") == _SINCE

    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert record.computer_id == "comp-1"
    assert record.sandbox_id == "sb-old"
    assert record.unavailable_seconds == 61 * 60
    assert "keeping the old sandbox" in record.getMessage()


@pytest.mark.asyncio
async def test_a_failure_of_a_sandbox_the_row_no_longer_names_counts_for_nothing():
    policy = _policy(_core_config(minutes=60))

    with patch(_STAMP, AsyncMock(return_value=None)):
        assert await policy.rebuild_now("sb-replaced") is None


def _opened_on(sandbox_id):
    return ComputerManager.__new__(ComputerManager)._host_unavailable_policy(
        "comp-1", _core_config(minutes=60), sandbox_id
    )


@pytest.mark.asyncio
async def test_a_sandbox_that_comes_up_stops_the_clock():
    policy = _opened_on("sb-old")

    with patch(_CLEAR, AsyncMock(return_value="sb-old")) as clear:
        assert await policy.reconnected("sb-old") is True

    clear.assert_awaited_once_with("comp-1", "sb-old")


@pytest.mark.asyncio
async def test_the_sandbox_it_was_opened_on_is_replaced_once_the_row_names_another():
    with patch(_CLEAR, AsyncMock(return_value="sb-new")):
        assert await _opened_on("sb-old").reconnected("sb-old") is False


@pytest.mark.asyncio
async def test_a_sandbox_still_being_built_is_not_called_replaced():
    """Its setup reconnects on a transient while the row still names the
    sandbox it replaces, or none at all."""
    with patch(_CLEAR, AsyncMock(return_value="sb-old")):
        assert await _opened_on("sb-old").reconnected("sb-new") is None
    with patch(_CLEAR, AsyncMock(return_value=None)):
        assert await _opened_on(None).reconnected("sb-first") is None


@pytest.mark.asyncio
async def test_a_built_sandbox_the_row_confirmed_is_replaced_later():
    policy = _opened_on("sb-old")
    with patch(_CLEAR, AsyncMock(return_value="sb-new")):
        assert await policy.reconnected("sb-new") is True
    with patch(_CLEAR, AsyncMock(return_value="sb-newer")):
        assert await policy.reconnected("sb-new") is False


@pytest.mark.asyncio
async def test_a_row_naming_no_sandbox_replaces_nothing():
    with patch(_CLEAR, AsyncMock(return_value=None)):
        assert await _opened_on("sb-old").reconnected("sb-old") is None


def test_zero_disables_the_rebuild():
    assert _policy(_core_config(minutes=0)) is None


def test_a_backend_without_the_setting_never_rebuilds():
    assert _policy(_core_config(provider="docker")) is None
