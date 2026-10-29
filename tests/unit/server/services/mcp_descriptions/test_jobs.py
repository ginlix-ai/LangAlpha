"""Discovery queues a description for the row and grant as they stand, only
from a list the server fetched from the vendor when the description is shared,
and never one nobody would read."""

from __future__ import annotations

from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace

import pytest

from src.config.models import McpDescriptionsConfig
from src.server.services.brokerages import brokerage_by_name
from src.server.services.mcp_descriptions import jobs
from src.server.services.mcp_descriptions.keys import (
    Scope,
    described_tools,
    dial_identity,
    server_digest,
)

ROBINHOOD = brokerage_by_name("robinhood").url
MINE = "https://mcp.example.com/mcp"
TOOLS = [
    {"name": "get_accounts", "description": "accounts", "input_schema": {}},
    {"name": "place_equity_order", "description": "order", "input_schema": {}},
]


class FakeDB:
    def __init__(self):
        self.rows = {
            "rh": {"url": ROBINHOOD, "description": "", "connection": {
                "server_url": ROBINHOOD, "granted_capabilities": []}},
            "mine": {"url": MINE, "description": "", "connection": None},
        }
        self.current: dict[str, str] = {}
        self.queued: list[dict] = []
        self.bumped: list[str] = []

    def grant(self, granted):
        self.rows["rh"]["connection"]["granted_capabilities"] = granted

    async def row_for(self, user_id, server_name, *, conn=None):
        return self.rows.get(server_name)

    async def enqueue_job(self, **job):
        current = self.current.get(job["scope_key"])
        if current == job["server_digest"]:
            return False, False
        self.queued.append(job)
        return True, current is not None

    async def rows_at_host(self, host, *, conn=None):
        return [{"user_id": "u1", **self.rows["rh"], "name": "rh"}]

    async def bump_workspaces(self, user_ids, *, conn=None):
        self.bumped += sorted(user_ids)


@pytest.fixture
def db(monkeypatch):
    fake = FakeDB()

    @asynccontextmanager
    async def connection(conn=None):
        yield SimpleNamespace(transaction=nullcontext)

    monkeypatch.setattr(jobs, "db", fake)
    monkeypatch.setattr(jobs, "get_db_connection", connection)
    monkeypatch.setattr(jobs, "get_mcp_descriptions_config", McpDescriptionsConfig)
    return fake


async def _discover(server="rh", from_vendor=True):
    return await jobs.enqueue_if_new(
        user_id="u1", server_name=server, tools=TOOLS, from_vendor=from_vendor
    )


@pytest.mark.asyncio
async def test_a_reconnect_that_changes_the_grant_queues_that_grants_description(db):
    db.grant(["account", "trading"])
    held = Scope.at_brokerage("robinhood", ["account", "trading"])
    db.current[held.key] = server_digest(described_tools(held, TOOLS))
    assert not await _discover()

    db.grant(["account"])
    assert await _discover()
    assert [(j["scope_key"], j["user_id"]) for j in db.queued] == [
        (Scope.at_brokerage("robinhood", ["account"]).key, None)
    ]


@pytest.mark.asyncio
async def test_a_list_the_sandbox_reported_never_writes_a_shared_description(db):
    assert not await _discover("rh", from_vendor=False)
    assert await _discover("mine", from_vendor=False)
    assert [j["scope_key"] for j in db.queued] == [Scope.of("u1", "mine", MINE, None, dial_identity("mine", db.rows["mine"])).key]


@pytest.mark.asyncio
async def test_a_server_the_user_described_still_drops_a_stale_description(db):
    db.rows["mine"]["description"] = "My notes server."
    db.current[Scope.of("u1", "mine", MINE, None, dial_identity("mine", db.rows["mine"])).key] = "older tools"

    assert await _discover("mine", from_vendor=False)
    assert db.bumped == ["u1"]


@pytest.mark.asyncio
async def test_a_changed_surface_drops_the_old_description_and_tells_its_readers(db):
    scope = Scope.at_brokerage("robinhood", [])
    db.current[scope.key] = "older tools"

    assert await _discover()
    assert db.bumped == ["u1"]


@pytest.mark.asyncio
async def test_a_server_that_lists_no_tools_is_queued_so_its_old_description_goes(db):
    assert await jobs.enqueue_if_new(user_id="u1", server_name="rh", tools=[], from_vendor=True)
