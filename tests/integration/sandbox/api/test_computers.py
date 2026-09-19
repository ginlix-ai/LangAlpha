"""Integration canary for the computers router.

Proves the router is mounted in this suite's app and that each lifecycle path
answers, so a later negative assertion here cannot pass on a 404 from a route
that was never registered.
"""

from __future__ import annotations

import pytest

from .conftest import TEST_COMPUTER_ID

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE = f"/api/v1/computers/{TEST_COMPUTER_ID}"


class TestComputersRouterIsMounted:
    async def test_list_and_get_answer(self, computers_client):
        client, _manager = computers_client

        listing = await client.get("/api/v1/computers")
        assert listing.status_code == 200
        assert listing.json()["total"] == 1

        detail = await client.get(BASE)
        assert detail.status_code == 200
        assert detail.json()["computer_id"] == TEST_COMPUTER_ID

    async def test_session_reports_the_live_sandbox(self, computers_client):
        client, manager = computers_client

        resp = await client.get(f"{BASE}/session")
        assert resp.status_code == 200
        assert resp.json()["status"] == "running"
        manager.get_session_for_computer.assert_awaited_once_with(TEST_COMPUTER_ID)

    @pytest.mark.parametrize(
        "path,payload,method_name",
        [
            ("stop", None, "stop_computer"),
            ("spec", {"tier": "standard"}, "set_computer_spec"),
            ("always-on", {"enabled": False}, "set_computer_always_on"),
        ],
    )
    async def test_each_lifecycle_path_is_routed(
        self, computers_client, path, payload, method_name
    ):
        client, manager = computers_client

        resp = await client.post(f"{BASE}/{path}", json=payload)
        assert resp.status_code == 200, resp.text
        assert getattr(manager, method_name).await_count == 1
