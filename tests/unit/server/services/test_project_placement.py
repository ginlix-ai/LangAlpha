"""Which former folders still name a project, and how they reach the turn.

A rename moves a workspace's folder and records the name it left. An old
spelling keeps naming this workspace until a sibling takes that name, and from
then on it is the sibling's.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.server.database.workspace_binding import get_project_binding
from src.server.services.workspace_layout import placement_from_binding

ROOT = "/home/workspace"
WS = "7f1c2a4e-9b3d-4c5e-8f60-1a2b3c4d5e6f"


def _binding(**overrides):
    row = {
        "computer_id": "c-1",
        "dir_name": "New Name",
        "status": "running",
        "layout_origin": None,
        "previous_dir_names": ("Old Name", "Older"),
        "sibling_dir_names": ("Beta",),
    }
    row.update(overrides)
    return row


def test_previous_names_ride_the_placement():
    placement = placement_from_binding(WS, _binding(), root=ROOT)
    assert placement.previous_dir_names == ("Old Name", "Older")
    assert placement.layout.workspace == f"{ROOT}/New Name"


def test_a_name_a_sibling_holds_now_is_the_siblings():
    placement = placement_from_binding(
        WS, _binding(previous_dir_names=("Beta", "BETA", "Old Name")), root=ROOT
    )
    assert placement.previous_dir_names == ("Old Name",)


def test_the_current_folder_is_not_a_previous_one():
    """A case-only rename leaves a real old folder whose name folds like today's."""
    placement = placement_from_binding(
        WS, _binding(previous_dir_names=("New Name", "new name", "")), root=ROOT
    )
    assert placement.previous_dir_names == ("new name",)


def test_a_row_without_previous_names_has_none():
    row = _binding()
    del row["previous_dir_names"]
    assert placement_from_binding(WS, row, root=ROOT).previous_dir_names == ()


@pytest.mark.asyncio
async def test_the_binding_read_returns_the_previous_names():
    executed: list[str] = []
    record = {**_binding(), "previous_dir_names": ["Old Name"], "sibling_dir_names": []}

    class _Cursor:
        async def execute(self, sql, params):
            executed.append(sql)

        async def fetchone(self):
            return record

    class _Conn:
        @asynccontextmanager
        async def cursor(self, row_factory=None):
            yield _Cursor()

    binding = await get_project_binding(WS, conn=_Conn())
    assert "w.previous_dir_names" in executed[0]
    assert binding["previous_dir_names"] == ("Old Name",)


@pytest.mark.asyncio
async def test_the_turn_project_carries_the_previous_names(monkeypatch):
    from src.server.services import workspace_layout

    placement = placement_from_binding(WS, _binding(), root=ROOT)
    monkeypatch.setattr(
        workspace_layout,
        "resolve_project_placement",
        AsyncMock(return_value=placement),
    )
    session = SimpleNamespace(sandbox=SimpleNamespace(working_dir=ROOT))
    project = await workspace_layout.resolve_project_context(session, WS)
    assert project.previous_dir_names == ("Old Name", "Older")
    assert project.sibling_dir_names == ("Beta",)
