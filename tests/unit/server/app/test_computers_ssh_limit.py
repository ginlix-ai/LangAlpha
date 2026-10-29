"""The SSH computer limit refuses the steps before a create as well.

The pairing command puts the key on a machine and the probe dials one, both
for a computer the create would refuse anyway.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.core import SSHConfig
from src.server.app import computers_ssh
from tests.conftest import create_test_app

_HOST = {"host": "203.0.113.10", "port": 22, "username": "dev"}


@pytest.fixture(autouse=True)
def ssh_offered():
    with patch.object(
        computers_ssh,
        "_ssh_settings",
        AsyncMock(return_value=SSHConfig(max_computers_per_user=3)),
    ):
        yield


@pytest_asyncio.fixture
async def client():
    app = create_test_app(computers_ssh.router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "body", "owner", "step"),
    [
        ("pairings", None, computers_ssh.ssh_pairing, "create_pairing"),
        ("probe", _HOST, computers_ssh, "probe"),
    ],
    ids=["pairing", "probe"],
)
async def test_a_user_at_the_limit_is_refused_before_any_machine_is_touched(
    client, route, body, owner, step
):
    touched = AsyncMock()
    with (
        patch(
            "src.server.services.ssh_computers.count_live_computers_of_kind",
            AsyncMock(return_value=3),
        ),
        patch.object(owner, step, touched),
    ):
        answer = await client.post(f"/api/v1/computers/ssh/{route}", json=body)

    assert answer.status_code == 409
    assert answer.json()["detail"] == {
        "code": "ssh_computer_limit",
        "message": "You can have up to 3 SSH computers. Remove one to add another.",
        "limit": 3,
    }
    touched.assert_not_awaited()


_COMPUTER_ID = "11111111-1111-4111-8111-111111111111"


@pytest.mark.asyncio
async def test_pairing_a_computers_machine_again_is_not_another_computer(
    client, sample_computer_dict
):
    """At the limit too: a reconnect adds nothing, and the pairing carries the
    computer for its ready probe to suggest that computer's folder."""
    computer = sample_computer_dict(computer_id=_COMPUTER_ID, kind="ssh", status="stopped")
    minted = AsyncMock(
        return_value={
            "token": "t",
            "command": "c",
            "script_url": "u",
            "expires_at": "2026-10-25T17:00:00+00:00",
        }
    )
    counted = AsyncMock(return_value=3)
    with (
        patch.object(computers_ssh, "get_computer", AsyncMock(return_value=computer)),
        patch(
            "src.server.services.ssh_computers.count_live_computers_of_kind", counted
        ),
        patch.object(computers_ssh.ssh_pairing, "create_pairing", minted),
    ):
        answer = await client.post(
            "/api/v1/computers/ssh/pairings", json={"computer_id": _COMPUTER_ID}
        )

    assert answer.status_code == 201, answer.text
    counted.assert_not_awaited()
    assert minted.await_args.kwargs["computer_id"] == _COMPUTER_ID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "status"),
    [({"user_id": "someone-else"}, 404), ({"kind": "daytona"}, 400)],
    ids=["another-users", "not-ssh"],
)
async def test_a_pairing_is_bound_only_to_the_callers_own_ssh_computer(
    client, sample_computer_dict, overrides, status
):
    computer = sample_computer_dict(**{"computer_id": _COMPUTER_ID, "kind": "ssh", **overrides})
    minted = AsyncMock()
    with (
        patch.object(computers_ssh, "get_computer", AsyncMock(return_value=computer)),
        patch.object(computers_ssh.ssh_pairing, "create_pairing", minted),
    ):
        answer = await client.post(
            "/api/v1/computers/ssh/pairings", json={"computer_id": _COMPUTER_ID}
        )

    assert answer.status_code == status
    minted.assert_not_awaited()


@pytest.mark.asyncio
async def test_reaching_a_computers_machine_again_by_hand_is_not_another_computer(
    client, sample_computer_dict
):
    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

    computer = sample_computer_dict(computer_id=_COMPUTER_ID, kind="ssh", status="stopped")
    counted = AsyncMock(return_value=3)
    dialed = AsyncMock(side_effect=SSHConnectError("unreachable", "No answer."))
    with (
        patch.object(computers_ssh, "get_computer", AsyncMock(return_value=computer)),
        patch("src.server.services.ssh_computers.count_live_computers_of_kind", counted),
        patch.object(computers_ssh, "spend_dials", AsyncMock()),
        patch.object(computers_ssh, "client_key", AsyncMock(return_value="key")),
        patch.object(computers_ssh, "probe", dialed),
    ):
        answer = await client.post(
            "/api/v1/computers/ssh/probe", json={**_HOST, "computer_id": _COMPUTER_ID}
        )

    assert answer.status_code == 422
    counted.assert_not_awaited()
    dialed.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_probe_names_only_the_callers_own_ssh_computer(client, sample_computer_dict):
    computer = sample_computer_dict(
        computer_id=_COMPUTER_ID, kind="ssh", user_id="someone-else"
    )
    dialed = AsyncMock()
    with (
        patch.object(computers_ssh, "get_computer", AsyncMock(return_value=computer)),
        patch.object(computers_ssh, "probe", dialed),
    ):
        answer = await client.post(
            "/api/v1/computers/ssh/probe", json={**_HOST, "computer_id": _COMPUTER_ID}
        )

    assert answer.status_code == 404
    dialed.assert_not_awaited()
