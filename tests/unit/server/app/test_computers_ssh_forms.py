"""The pairing script's forms, read under a cap before they are parsed.

The routes are public, and a form parser holds a field whole however long it
runs, so the body is refused while it is read rather than cut after parsing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.config.core import SSHConfig
from src.server.app import computers_ssh
from src.server.services.ssh_dial_budget import SSHDialBudgetExceeded
from tests.conftest import create_test_app

PAIRING = "/api/v1/computers/ssh/pairings/abc"


@pytest.fixture(autouse=True)
def ssh_offered():
    with patch.object(
        computers_ssh, "_ssh_settings", AsyncMock(return_value=SSHConfig())
    ):
        yield


@pytest.fixture
def pairing():
    with (
        patch.object(
            computers_ssh.ssh_pairing, "report", AsyncMock(return_value=(200, "ok"))
        ) as report,
        patch.object(
            computers_ssh.ssh_pairing,
            "issue_tunnel",
            AsyncMock(return_value=(200, "tunnel_id=t")),
        ) as issue_tunnel,
    ):
        yield {"report": report, "tunnel": issue_tunnel}


@pytest_asyncio.fixture
async def client():
    app = create_test_app(computers_ssh.router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_the_scripts_form_reaches_the_pairing_as_sent(client, pairing):
    answer = await client.post(
        f"{PAIRING}/report", data={"user": "dev", "hostname": "café box", "failed": ""}
    )

    assert (answer.status_code, answer.text) == (200, "ok\n")
    form = pairing["report"].await_args.args[1]
    assert (form.user, form.hostname, form.failed) == ("dev", "café box", "")


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["report", "tunnel"])
@pytest.mark.parametrize("chunked", [False, True], ids=["sized", "chunked"])
async def test_a_form_past_the_cap_is_refused_unread(client, pairing, route, chunked):
    body = b"hostname=" + b"A" * computers_ssh._MAX_SCRIPT_BODY

    async def streamed():
        for i in range(0, len(body), 65536):
            yield body[i : i + 65536]

    answer = await client.post(
        f"{PAIRING}/{route}",
        content=streamed() if chunked else body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    assert answer.status_code == 413
    assert answer.text == "What the machine sent was too large to read.\n"
    pairing[route].assert_not_awaited()


_BUDGET_SPENT = SSHDialBudgetExceeded(1800)


@pytest.mark.asyncio
async def test_a_probe_past_the_budget_is_a_429_with_its_code_and_dials_nothing(client):
    dialed = AsyncMock()
    with (
        patch.object(computers_ssh, "check_ssh_capacity", AsyncMock()),
        patch.object(computers_ssh, "spend_dials", AsyncMock(side_effect=_BUDGET_SPENT)),
        patch.object(computers_ssh, "client_key", AsyncMock(return_value="k")),
        patch.object(computers_ssh, "probe", dialed),
    ):
        response = await client.post(
            "/api/v1/computers/ssh/probe",
            json={"host": "203.0.113.10", "port": 22, "username": "dev"},
        )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "1800"
    assert response.json()["detail"] == {
        "code": "rate_limited",
        "message": str(_BUDGET_SPENT),
        "retry_after": 1800,
    }
    dialed.assert_not_called()
