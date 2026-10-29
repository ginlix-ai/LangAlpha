"""Telling Home by its id, however a caller spells or types it."""

from __future__ import annotations

import uuid

import pytest

from src.server.database.home_workspace import (
    get_home_workspace_id,
    is_home_id,
    is_home_row,
    is_unbound,
)

USER = "user-home-1"
HOME = get_home_workspace_id(USER)


@pytest.mark.parametrize(
    "workspace_id, expected",
    [
        (HOME, True),
        (HOME.upper(), True),
        (uuid.UUID(HOME), True),
        (get_home_workspace_id("someone-else"), False),
        (str(uuid.uuid4()), False),
        (None, False),
        ("", False),
    ],
    ids=["home", "upper-case", "uuid", "another-users-home", "a-workspace", "none", "empty"],
)
def test_is_home_id(workspace_id, expected):
    assert is_home_id(USER, workspace_id) is expected


def test_no_user_has_no_home():
    assert is_home_id(None, HOME) is False


@pytest.mark.parametrize(
    "row, home_row, unbound",
    [
        ({"workspace_id": HOME, "user_id": USER, "status": "unbound"}, True, True),
        ({"workspace_id": uuid.UUID(HOME), "user_id": USER, "status": "running"}, True, False),
        ({"workspace_id": str(uuid.uuid4()), "user_id": USER, "status": "unbound"}, True, True),
        ({"workspace_id": str(uuid.uuid4()), "user_id": USER, "status": "running"}, False, False),
        (None, False, False),
    ],
    ids=["home-unbound", "home-bound", "former-home-row", "a-workspace", "no-row"],
)
def test_a_home_row_is_home_by_id_or_unbound_by_status(row, home_row, unbound):
    assert is_home_row(row) is home_row
    assert is_unbound(row) is unbound
