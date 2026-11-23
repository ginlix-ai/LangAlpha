"""The desktop app's computer goes with its server."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.server.services import desktop_app

ROWS = [
    {"computer_id": "here", "kind": "local", "status": "running"},
    {"computer_id": "there", "kind": "ssh", "status": "running"},
    {"computer_id": "box", "kind": "docker", "status": "running"},
]


@pytest.fixture
def manager(monkeypatch):
    m = MagicMock()
    m.stop_computer = AsyncMock()
    monkeypatch.setattr(
        "src.server.services.computer_manager.ComputerManager.get_instance", lambda: m
    )
    monkeypatch.setattr(
        "src.server.database.computer.get_computers_by_status", AsyncMock(return_value=ROWS)
    )
    return m


@pytest.mark.asyncio
async def test_outside_the_desktop_app_nothing_is_stopped(manager, monkeypatch):
    monkeypatch.delenv("LANGALPHA_DESKTOP_APP", raising=False)
    await desktop_app.stop_this_computer()
    manager.stop_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_this_machines_computer_stops_and_without_a_backup(manager, monkeypatch):
    monkeypatch.setenv("LANGALPHA_DESKTOP_APP", "1")
    await desktop_app.stop_this_computer()
    manager.stop_computer.assert_awaited_once_with("here", back_up=False)


@pytest.mark.asyncio
async def test_a_stop_that_fails_does_not_fail_the_shutdown(manager, monkeypatch):
    monkeypatch.setenv("LANGALPHA_DESKTOP_APP", "1")
    manager.stop_computer.side_effect = RuntimeError("Cannot stop a computer that still has work running")
    await desktop_app.stop_this_computer()
