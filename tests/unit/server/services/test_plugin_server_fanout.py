"""The provenance a plugin's created row carries.

``plugin_server_key`` is what a later plugin update diffs the row against, so
it has to name the mcp.json entry that actually installed it. Two entry keys
can normalize to one MCP name, and only one of them lands, so the row's name
cannot recover which -- the entry itself has to travel with the write.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from src.server.models.plugin import InstallReport
from src.server.services.plugins import server_fanout
from src.server.services.plugins.mcp import McpEntryPlan

pytestmark = pytest.mark.asyncio


class _Conn:
    @asynccontextmanager
    async def transaction(self):
        yield


@pytest.fixture
def created(monkeypatch):
    """Patch the fan-out's writes away, returning the create_catalog_server calls."""

    @asynccontextmanager
    async def connection():
        yield _Conn()

    monkeypatch.setattr(
        "src.server.services.mcp_import.get_db_connection", connection
    )
    for name, value in (
        ("list_catalog_servers", []),
        ("get_user_secret_names", []),
        ("create_user_secret", None),
    ):
        monkeypatch.setattr(
            server_fanout, name, AsyncMock(return_value=value), raising=True
        )
    monkeypatch.setattr(
        server_fanout, "schedule_catalog_discovery", lambda *a, **k: None,
        raising=True,
    )
    calls = AsyncMock(return_value={"enabled": True})
    monkeypatch.setattr(server_fanout, "create_catalog_server", calls, raising=True)
    return calls


async def test_the_row_is_keyed_by_the_entry_that_landed(created):
    # ``foo-bar`` normalizes to ``foo_bar`` and then fails validation on a
    # plain-http url; ``foo_bar`` is the entry that gets to create the row, so
    # its key is the one an update must reconcile against.
    plans = [
        McpEntryPlan(
            key="foo-bar", name="foo_bar", renamed=True, transport="http",
            config={
                "name": "foo_bar",
                "transport": "http",
                "url": "http://example.com/mcp",
            },
        ),
        McpEntryPlan(
            key="foo_bar", name="foo_bar", renamed=False, transport="stdio",
            config={"name": "foo_bar", "transport": "stdio", "command": "npx"},
        ),
    ]
    report = InstallReport()

    await server_fanout.fan_out_servers("u1", "plug", plans, report)

    assert [(c.key, c.status) for c in report.components] == [
        ("foo-bar", "invalid"), ("foo_bar", "created")
    ]
    assert created.await_count == 1
    assert created.await_args.kwargs["plugin_server_key"] == "foo_bar"
