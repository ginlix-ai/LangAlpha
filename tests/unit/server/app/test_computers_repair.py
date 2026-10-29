"""The reinstall and reconnect routes, and the failed start they repair.

A repair answers 202 only once its start is claimed, as a lazy start does, so
a client re-reading the row finds it starting; and every refusal is a detail
with the ``code`` the client words its copy from.
"""

from __future__ import annotations

import asyncio
from unittest.mock import ANY, AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.services.computer_errors import ComputerRepairRefused
from tests.conftest import create_test_app

USER = "test-user-123"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
FOREIGN = "3cd2aabcfce04ecf83f69a2e2952866c"
OLD_TUNNEL = "5b0f3c1e-8d2a-4c6b-9e7f-1a2b3c4d5e6f"
NEW_TUNNEL = "6c1f4d2f-9e3b-4d7c-8f80-2b3c4d5e6f70"
KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"


@pytest.fixture(autouse=True)
def sandbox_config():
    from ptc_agent.config.core import SandboxConfig

    with patch("src.server.app.computers._sandbox_config", return_value=SandboxConfig()):
        yield


@pytest_asyncio.fixture
async def client():
    from src.server.app.computers import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def row(sample_computer_dict):
    return sample_computer_dict(
        computer_id=COMPUTER_ID,
        kind="ssh",
        status="stopped",
        provider_config={"host": "box", "tunnel": OLD_TUNNEL},
    )


@pytest.fixture
def manager(mock_computer_manager, row):
    """A manager whose start claims the row, as the real one does."""

    async def start(_id, *, on_start_claimed):
        row["status"] = "starting"
        on_start_claimed()

    mock_computer_manager.start_computer.side_effect = start
    mock_computer_manager.reinstall_computer = AsyncMock(return_value=row)
    mock_computer_manager.reconnect_computer = AsyncMock()
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=row)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        yield mock_computer_manager


@pytest.mark.asyncio
async def test_a_reinstall_answers_once_its_start_is_claimed(client, manager):
    resp = await client.post(
        f"/api/v1/computers/{COMPUTER_ID}/reinstall", json={"folder": "take_back"}
    )
    await asyncio.sleep(0)

    assert resp.status_code == 202
    assert resp.json()["status"] == "starting"
    manager.reinstall_computer.assert_awaited_once_with(
        COMPUTER_ID, folder="take_back", root_dir=None, settings=ANY, take_over=None
    )
    manager.start_computer.assert_awaited_once_with(COMPUTER_ID, on_start_claimed=ANY)


@pytest.mark.asyncio
async def test_a_reinstall_to_a_new_folder_passes_the_take_over(client, manager):
    resp = await client.post(
        f"/api/v1/computers/{COMPUTER_ID}/reinstall",
        json={"folder": "new", "root_dir": "~/langalpha-2", "take_over": FOREIGN},
    )
    await asyncio.sleep(0)

    assert resp.status_code == 202
    assert manager.reinstall_computer.await_args.kwargs["root_dir"] == "~/langalpha-2"
    assert manager.reinstall_computer.await_args.kwargs["take_over"] == FOREIGN


@pytest.mark.asyncio
async def test_a_new_folder_must_be_named(client, manager):
    resp = await client.post(
        f"/api/v1/computers/{COMPUTER_ID}/reinstall", json={"folder": "new"}
    )

    assert resp.status_code == 422
    manager.reinstall_computer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "status", "detail"),
    [
        (
            ComputerRepairRefused("not_ssh", "Only ssh.", status=400),
            400,
            {"code": "not_ssh", "message": "Only ssh."},
        ),
        (
            ComputerRepairRefused("not_stopped", "Stop it."),
            409,
            {"code": "not_stopped", "message": "Stop it."},
        ),
        (
            ComputerRepairRefused("folder_marked", "Taken.", owner=FOREIGN),
            409,
            {"code": "folder_marked", "message": "Taken.", "owner": FOREIGN},
        ),
        (
            ComputerRepairRefused("root_dir", "Holds files.", status=422),
            422,
            {"code": "root_dir", "message": "Holds files."},
        ),
        (
            SSHConnectError("auth_failed", "box refused the key."),
            422,
            {"code": "auth_failed", "message": "box refused the key."},
        ),
        (
            SSHConnectError("timeout", "box did not answer."),
            503,
            {"code": "timeout", "message": SSHConnectError("timeout", "box did not answer.").for_owner},
        ),
    ],
)
async def test_a_refused_repair_starts_nothing(client, manager, raised, status, detail):
    manager.reinstall_computer.side_effect = raised

    resp = await client.post(
        f"/api/v1/computers/{COMPUTER_ID}/reinstall", json={"folder": "take_back"}
    )

    assert resp.status_code == status
    assert resp.json()["detail"] == detail
    manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reconnect_lets_the_tunnel_it_left_go(client, manager, row):
    after = {**row, "provider_config": {"host": "box", "tunnel": NEW_TUNNEL}}
    manager.reconnect_computer.return_value = (after, dict(row))
    release = AsyncMock(return_value=True)

    with patch("src.server.app.computers.release_tunnel", release):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/reconnect",
            json={
                "ssh": {
                    "host": "box",
                    "port": 22,
                    "username": "dev",
                    "host_key": f"{KEY} root@box",
                    "tunnel_id": NEW_TUNNEL,
                    "take_over": FOREIGN,
                }
            },
        )
        for _ in range(5):
            await asyncio.sleep(0)

    assert resp.status_code == 202
    assert resp.json()["status"] == "starting"
    machine = manager.reconnect_computer.await_args.args[1]
    assert (machine.host_key, machine.tunnel_id, machine.take_over) == (
        KEY,
        NEW_TUNNEL,
        FOREIGN,
    )
    release.assert_awaited_once()
    assert release.await_args.args[1]["provider_config"]["tunnel"] == OLD_TUNNEL


@pytest.mark.asyncio
async def test_a_reconnect_keeping_its_tunnel_releases_nothing(client, manager, row):
    manager.reconnect_computer.return_value = (dict(row), dict(row))
    release = AsyncMock()

    with patch("src.server.app.computers.release_tunnel", release):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/reconnect",
            json={
                "ssh": {
                    "host": "box",
                    "port": 22,
                    "username": "dev",
                    "host_key": KEY,
                    "tunnel_id": OLD_TUNNEL,
                }
            },
        )
        await asyncio.sleep(0)

    assert resp.status_code == 202
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_start_is_on_the_row_before_its_frame_goes_out(row):
    """The client re-reads the list on the error frame, and the frame carries
    the failure too, for a client that patches its copy."""
    from src.server.app import computers

    events = []
    stored = {
        "code": "folder_marked",
        "message": "Taken.",
        "owner": FOREIGN,
        "remedy": "reinstall",
        "at": "2026-10-25T16:33:00+00:00",
    }
    starting = {**row, "status": "starting"}
    settled = {**row, "status": "stopped", "start_failure": stored}

    async def record(computer_id, exc, *, kind):
        events.append(("record", kind))

    async def publish(computer_id, status, *, extra):
        events.append(("publish", status, extra))

    with (
        patch.object(computers, "get_computer", AsyncMock(side_effect=[starting, settled])),
        patch.object(computers, "update_computer_status", AsyncMock(return_value=settled)),
        patch.object(computers, "record_start_failure", record),
        patch.object(computers, "publish_computer_status_change", publish),
    ):
        await computers._report_start_failure(COMPUTER_ID, RuntimeError("boom"))

    assert events[0] == ("record", "ssh")
    kind, status, extra = events[1]
    assert (kind, status) == ("publish", "error")
    assert extra["start_failure"]["code"] == "folder_marked"
    assert extra["start_failure"]["owner"] == FOREIGN
    assert extra["error"]


@pytest.mark.asyncio
async def test_the_error_frame_carries_the_recorded_failure(client, sample_computer_dict):
    from contextlib import asynccontextmanager

    from tests.unit.server.app.test_computers import _collect_sse_events

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    failure = {"code": "not_connected", "message": "m", "owner": None, "remedy": "reconnect"}

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return (
                    "message",
                    {
                        "computer_id": computer_id,
                        "status": "error",
                        "error": "Sandbox is not reachable: m",
                        "start_failure": failure,
                    },
                )
            return ("timeout", None)

        yield wait

    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch("src.server.app.computers.subscribe_to_computer_status", new=fake_subscribe),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    payloads = [e[1] for e in events if e[0] == "status"]
    assert payloads[-1] == {
        "computer_id": COMPUTER_ID,
        "status": "error",
        "error": "Sandbox is not reachable: m",
        "start_failure": failure,
    }
