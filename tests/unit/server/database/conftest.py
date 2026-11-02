"""Shared fixtures for the database unit tests."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest


@pytest.fixture
def artifacts_db(mock_connection, monkeypatch):
    """Every ``database/artifacts`` module reads through ``mock_connection``."""
    from src.server.database.artifacts import (
        comments,
        delivery,
        records,
        routing,
        versions,
    )

    @asynccontextmanager
    async def _get_db_connection(conn=None):
        yield mock_connection

    for module in (records, routing, versions, comments, delivery):
        monkeypatch.setattr(module, "get_db_connection", _get_db_connection)
    return mock_connection
