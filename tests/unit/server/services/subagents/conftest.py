"""The in-memory user the subagent service's tests read and write."""

from __future__ import annotations

import pytest

from tests.unit.server.services.subagents._fakes import World, install


@pytest.fixture
def world(monkeypatch) -> World:
    return install(monkeypatch)
