"""Tests for the Computers API router (src/server/app/computers.py).

Two surfaces in one file, because they are one implementation: the
``/api/v1/computers`` routes, and the ``/api/v1/workspaces/{id}/...`` lifecycle
aliases that resolve workspace to computer and call the same action functions.
The alias arms are what guarantee the sandbox settings panel keeps working
while both paths are live.
"""

import contextlib
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.services.computer_errors import ProviderNotOffered, UnknownResourceTier
from tests.conftest import create_test_app

USER = "test-user-123"
OTHER_USER = "someone-else"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"


@pytest.fixture(autouse=True)
def workspace_alias_quota_gate_off():
    with (
        patch(
            "src.server.dependencies.usage_limits.platform_gating_active",
            return_value=False,
        ),
        patch(
            "src.server.app.computers.platform_gating_active", return_value=False
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def sandbox_config():
    """The deployment's sandbox settings, which the manager mocks here lack.

    Daytona by default with nothing else offered; a test that offers a kind
    sets ``offered_providers`` on what this yields.
    """
    from ptc_agent.config.core import SandboxConfig

    config = SandboxConfig()
    with patch("src.server.app.computers._sandbox_config", return_value=config):
        yield config


@pytest.fixture(autouse=True)
def capacity_lock():
    """Every create and rename holds the user's capacity lock; a test that
    asks about it patches its own over this one."""
    held = []

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        held.append(user_id)
        yield

    with patch("src.server.app.computers.computer_capacity_lock", _lock):
        yield held


@pytest.fixture(autouse=True)
def names_free():
    """None of the caller's other computers holds the name an ssh create asks for."""
    with patch(
        "src.server.app.computers.name_taken", AsyncMock(return_value=None)
    ) as taken:
        yield taken


@pytest_asyncio.fixture
async def client():
    from src.server.app.computers import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest_asyncio.fixture
async def ws_client():
    """A client on the workspaces router, for the alias arms."""
    from src.server.app.workspaces import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


@pytest.fixture
def computer(sample_computer_dict):
    return sample_computer_dict(computer_id=COMPUTER_ID)


def _ws(**overrides):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    data = {
        "workspace_id": WORKSPACE_ID,
        "user_id": USER,
        "name": "Test Workspace",
        "description": None,
        "sandbox_id": "sandbox-abc",
        "computer_id": COMPUTER_ID,
        "status": "running",
        "is_pinned": False,
        "sort_order": 0,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": None,
        "stopped_at": None,
        "config": None,
        "resource_tier": "standard",
        "is_always_on": False,
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Create: the one route that allocates a machine, so the one that is metered
# ---------------------------------------------------------------------------


@pytest.fixture
def entitled():
    """The tier gate dials the platform; these tests are about the route."""
    with patch(
        "src.server.app.computers.assert_spec_allowed", AsyncMock(return_value=None)
    ) as gate:
        yield gate


@pytest.fixture
def prepares():
    """A create hands the row to a background start; the start path has its own tests.

    Yields the stand-in, which answers with the status the claim leaves.
    """
    with patch(
        "src.server.app.computers._schedule_claimed_start",
        AsyncMock(return_value="starting"),
    ) as schedule:
        yield schedule


@pytest.mark.asyncio
async def test_create_computer_returns_the_new_machine(
    client, computer, entitled, prepares
):
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        return_value={**computer, "status": "creating", "provider_ref": None}
    )
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post(
            "/api/v1/computers",
            json={"name": "Research box", "resource_tier": "performance"},
        )

    assert resp.status_code == 201
    body = resp.json()
    assert body["computer_id"] == COMPUTER_ID
    # The sandbox is being built in the background, so the row answers as
    # starting with no sandbox yet; the client follows its events.
    assert body["status"] == "starting"
    assert body["provider_ref"] is None
    kwargs = manager.create_computer_for_user.call_args.kwargs
    assert kwargs["name"] == "Research box"
    assert kwargs["resource_tier"] == "performance"
    assert kwargs["prepare"] is True
    prepares.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_the_first_computer_is_primary_and_the_second_is_not(
    client, computer, entitled, prepares
):
    """Without this the first machine a user names is never their primary, and
    their first project then mints a second "My computer" beside it. The route
    only asks; the insert's NOT EXISTS is what decides, so asking is race-safe.
    """
    primaries: set[str] = set()

    async def _create(
        user_id,
        *,
        kind=None,
        name=None,
        resource_tier=None,
        is_primary=False,
        prepare=False,
        files_backup=True,
    ):
        # Mirrors the insert's `%s AND NOT EXISTS (... is_primary ...)`, which
        # test_computer_db locks as SQL.
        won = bool(is_primary) and user_id not in primaries
        if won:
            primaries.add(user_id)
        return {**computer, "is_primary": won}

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(side_effect=_create)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        first = await client.post("/api/v1/computers", json={"name": "Research box"})
        second = await client.post("/api/v1/computers", json={"name": "Second box"})

    assert first.json()["is_primary"] is True
    assert second.json()["is_primary"] is False
    assert manager.create_computer_for_user.call_args.kwargs["is_primary"] is True


@pytest.mark.asyncio
async def test_create_computer_defaults_to_the_standard_tier(
    client, computer, entitled, prepares
):
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(return_value=computer)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={})

    assert resp.status_code == 201
    kwargs = manager.create_computer_for_user.call_args.kwargs
    assert kwargs["resource_tier"] == "standard"
    assert kwargs["name"] is None


OTHER_COMPUTER_ID = "33333333-3333-4333-8333-333333333333"


@pytest.mark.asyncio
async def test_a_name_another_computer_has_is_refused_by_the_insert(
    client, entitled, prepares, names_free
):
    """A name says which of the user's computers it means. Nothing is dialed
    ahead of a hosted computer's insert, so the insert alone refuses it; the
    holder is named for the client to word."""
    from src.server.database.computer_names import ComputerNameTaken

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        side_effect=ComputerNameTaken("Research box", OTHER_COMPUTER_ID)
    )
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={"name": "  research BOX "})

    assert resp.status_code == 409
    assert resp.json()["detail"] == {
        "code": "name_taken",
        "message": 'You already have a computer named "Research box".',
        "name": "Research box",
        "computer_id": OTHER_COMPUTER_ID,
    }
    assert manager.create_computer_for_user.call_args.kwargs["name"] == "research BOX"
    names_free.assert_not_awaited()
    prepares.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_blank_name_is_no_name(client, computer, entitled, prepares, names_free):
    """Left to the default, which is numbered rather than refused."""
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(return_value=computer)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={"name": "   "})

    assert resp.status_code == 201
    assert manager.create_computer_for_user.call_args.kwargs["name"] is None
    names_free.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_rename_onto_another_computers_name_is_409(client, computer):
    from src.server.database.computer_names import ComputerNameTaken

    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch(
            "src.server.app.computers.db_rename_computer",
            AsyncMock(side_effect=ComputerNameTaken("Lab", OTHER_COMPUTER_ID)),
        ),
    ):
        resp = await client.patch(
            f"/api/v1/computers/{COMPUTER_ID}", json={"name": " lab "}
        )

    assert resp.status_code == 409
    assert resp.json()["detail"] == {
        "code": "name_taken",
        "message": 'You already have a computer named "Lab".',
        "name": "Lab",
        "computer_id": OTHER_COMPUTER_ID,
    }


@pytest.mark.asyncio
async def test_a_computer_renamed_to_its_own_name_in_another_case_is_renamed(
    client, computer
):
    """Its own name is never taken from it, so nothing ahead of the update
    asks whether the name is free; the index excludes the row itself."""
    renamed = {**computer, "name": "MY COMPUTER"}
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch(
            "src.server.app.computers.db_rename_computer",
            AsyncMock(return_value=renamed),
        ) as rename,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.patch(
            f"/api/v1/computers/{COMPUTER_ID}", json={"name": "MY COMPUTER"}
        )

    assert resp.status_code == 200
    assert resp.json()["name"] == "MY COMPUTER"
    rename.assert_awaited_once_with(COMPUTER_ID, "MY COMPUTER", user_id=USER)


@pytest.mark.asyncio
async def test_create_serializes_the_tier_check_with_the_insert(
    client, computer
):
    events = []

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def quota_lock(user_id):
        events.append(("lock", user_id))
        yield
        events.append(("unlock", user_id))

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        side_effect=lambda *_args, **_kwargs: (
            events.append(("insert", USER)) or computer
        )
    )
    gate = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append(("gate", USER)))
    held = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append(("held", USER)))
    limit = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append(("limit", USER))
    )
    schedule = AsyncMock(
        side_effect=lambda *_args, **_kwargs: events.append(("prepare", USER))
        or "starting"
    )
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.assert_spec_allowed", gate),
        patch("src.server.app.computers.enforce_computer_count", held),
        patch("src.server.app.computers.enforce_computer_limit", limit),
        patch("src.server.app.computers._schedule_claimed_start", schedule),
        patch("src.server.app.computers.platform_gating_active", return_value=True),
        patch("src.server.app.computers.computer_capacity_lock", quota_lock),
    ):
        response = await client.post(
            "/api/v1/computers", json={"resource_tier": "performance"}
        )

    assert response.status_code == 201
    # The new row holds a running slot from its insert, so the limit is read
    # under the same lock; the start claims that slot after the lock is gone.
    # The held count goes first, so a user at the cap is not sent to step
    # down a size only to be refused for the cap.
    assert events == [
        ("lock", USER),
        ("held", USER),
        ("gate", USER),
        ("limit", USER),
        ("insert", USER),
        ("unlock", USER),
        ("prepare", USER),
    ]


@pytest.mark.asyncio
async def test_a_capped_user_is_refused_before_the_row_is_written(
    client, entitled, prepares
):
    """Preparing takes a running slot, so the platform's refusal reaches the
    form with its own message, and no row is left behind to clean up."""
    from fastapi import HTTPException

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    capped = AsyncMock(
        side_effect=HTTPException(
            status_code=429, detail={"message": "Limit reached", "remaining": 0}
        )
    )
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.enforce_computer_limit", capped),
    ):
        resp = await client.post("/api/v1/computers", json={})

    assert resp.status_code == 429
    assert resp.json()["detail"]["message"] == "Limit reached"
    manager.create_computer_for_user.assert_not_awaited()
    prepares.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_user_holding_their_cap_is_refused_before_the_row_is_written(
    client, entitled, prepares
):
    """A stopped computer still counts toward the plan's cap, so stopping one
    does not make room for another."""
    from fastapi import HTTPException

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    held = AsyncMock(
        side_effect=HTTPException(
            status_code=429, detail={"message": "Limit reached", "remaining": 0}
        )
    )
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.enforce_computer_count", held),
        patch("src.server.app.computers.enforce_computer_limit", AsyncMock()),
    ):
        resp = await client.post("/api/v1/computers", json={})

    assert resp.status_code == 429
    held.assert_awaited_once()
    manager.create_computer_for_user.assert_not_awaited()
    prepares.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unentitled_tier_is_refused_before_the_row_is_written(client):
    """The cap and the tier are separate entitlements, so clearing one does not
    buy the other; refusing after the insert would leave a machine to clean up.
    """
    from fastapi import HTTPException

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(
            "src.server.app.computers.assert_spec_allowed",
            AsyncMock(side_effect=HTTPException(403, detail="Requires scope")),
        ),
    ):
        resp = await client.post("/api/v1/computers", json={"resource_tier": "max"})

    assert resp.status_code == 403
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unknown_tier_is_rejected_by_the_model(client):
    resp = await client.post("/api/v1/computers", json={"resource_tier": "huge"})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Provider choice: a computer is created on one of the offered kinds, for good
# ---------------------------------------------------------------------------


def _creating_manager(computer):
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        return_value={**computer, "status": "stopped", "provider_ref": None}
    )
    return manager


@pytest.mark.asyncio
async def test_a_create_without_a_kind_lands_on_the_deployment_default(
    client, computer, entitled, prepares
):
    manager = _creating_manager(computer)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={})

    assert resp.status_code == 201
    assert manager.create_computer_for_user.call_args.kwargs["kind"] == "daytona"


@pytest.mark.asyncio
async def test_an_offered_kind_reaches_the_manager(
    client, computer, entitled, sandbox_config, prepares
):
    sandbox_config.offered_providers = ("e2b",)
    manager = _creating_manager({**computer, "kind": "e2b"})
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={"kind": "e2b"})

    assert resp.status_code == 201
    assert resp.json()["kind"] == "e2b"
    assert manager.create_computer_for_user.call_args.kwargs["kind"] == "e2b"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal",
    [
        ProviderNotOffered("Provider not offered"),
        UnknownResourceTier("Unknown resource tier"),
    ],
)
async def test_a_create_the_manager_refuses_is_the_clients_error(
    client, computer, entitled, sandbox_config, refusal
):
    """Both are ValueErrors, which this router otherwise answers as a
    computer that is not there."""
    sandbox_config.offered_providers = ("e2b",)
    manager = _creating_manager(computer)
    manager.create_computer_for_user.side_effect = refusal
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post(
            "/api/v1/computers", json={"kind": "e2b", "resource_tier": "max"}
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == str(refusal)


@pytest.mark.asyncio
async def test_an_unentitled_kind_is_refused_before_the_row_is_written(
    client, computer, entitled, sandbox_config
):
    from fastapi import HTTPException

    sandbox_config.offered_providers = ("e2b",)
    manager = _creating_manager(computer)
    gate = AsyncMock(
        side_effect=HTTPException(403, detail="Requires scope: workspace:provider:e2b")
    )
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.assert_provider_allowed", gate),
    ):
        resp = await client.post("/api/v1/computers", json={"kind": "e2b"})

    assert resp.status_code == 403
    gate.assert_awaited_once_with(USER, "e2b", default_kind="daytona")
    entitled.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_unknown_kind_is_rejected_by_the_model(client):
    resp = await client.post("/api/v1/computers", json={"kind": "memory"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_providers_lists_only_the_default_when_nothing_is_offered(client):
    with patch("src.server.app.computers.get_computer") as get_computer:
        resp = await client.get("/api/v1/computers/providers")

    assert resp.status_code == 200
    assert resp.json()["providers"] == [
        {
            "kind": "daytona",
            "is_default": True,
            "allowed": True,
            "tiers": ANY,
            "default_name": "Cloud computer",
        }
    ]
    # Not read as a computer id by GET /{computer_id}.
    get_computer.assert_not_called()


@pytest.mark.asyncio
async def test_providers_lists_the_default_first_with_what_the_caller_may_pick(
    client, sandbox_config
):
    sandbox_config.provider = "e2b"
    sandbox_config.offered_providers = ("daytona", "docker")
    allowances = AsyncMock(
        return_value={"e2b": True, "daytona": False, "docker": None}
    )
    with patch("src.server.app.computers.get_provider_allowances", allowances):
        resp = await client.get("/api/v1/computers/providers")

    assert resp.status_code == 200
    assert [
        {k: v for k, v in p.items() if k != "tiers"} for p in resp.json()["providers"]
    ] == [
        {"kind": "e2b", "is_default": True, "allowed": True, "default_name": "Cloud computer"},
        {"kind": "daytona", "is_default": False, "allowed": False, "default_name": "Cloud computer"},
        {"kind": "docker", "is_default": False, "allowed": None, "default_name": "Local computer"},
    ]
    allowances.assert_awaited_once_with(
        USER, ("e2b", "daytona", "docker"), default_kind="e2b"
    )


@pytest.mark.asyncio
async def test_providers_reads_an_ungated_offer_as_unknown_in_oss_mode(
    client, sandbox_config
):
    sandbox_config.offered_providers = ("e2b",)
    resp = await client.get("/api/v1/computers/providers")

    assert resp.json()["providers"][1] == {
        "kind": "e2b",
        "is_default": False,
        "allowed": None,
        "tiers": ANY,
        "default_name": "Cloud computer",
    }


@pytest.mark.asyncio
async def test_providers_list_each_ones_sizes_from_its_config(client, sandbox_config):
    """The pickers render specs from here, so Extra Large shows only on E2B."""
    sandbox_config.offered_providers = ("e2b", "ssh")
    resp = await client.get("/api/v1/computers/providers")

    tiers = {p["kind"]: p["tiers"] for p in resp.json()["providers"]}
    assert tiers["daytona"] == [
        {"tier": "standard", "cpu": 1, "memory_gib": 2, "disk_gib": 5},
        {"tier": "performance", "cpu": 2, "memory_gib": 4, "disk_gib": 10},
        {"tier": "max", "cpu": 2, "memory_gib": 8, "disk_gib": 10},
    ]
    assert tiers["e2b"][-1] == {
        "tier": "xlarge", "cpu": 4, "memory_gib": 8, "disk_gib": 20
    }
    assert [t["tier"] for t in tiers["e2b"]] == [
        "standard", "performance", "max", "xlarge"
    ]
    # Its owner sizes the machine.
    assert tiers["ssh"] == []


@pytest.mark.asyncio
async def test_extra_large_on_daytona_is_refused_before_the_plan_gate(client):
    """A plan without xlarge would otherwise be offered an upgrade for a size
    Daytona never builds."""
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    spec_gate = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.assert_spec_allowed", spec_gate),
    ):
        resp = await client.post(
            "/api/v1/computers", json={"kind": "daytona", "resource_tier": "xlarge"}
        )

    assert resp.status_code == 400
    assert "does not offer the 'xlarge' spec" in resp.json()["detail"]
    spec_gate.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_extra_large_on_e2b_is_gated_then_created(
    client, sandbox_config, entitled, prepares, sample_computer_dict
):
    sandbox_config.offered_providers = ("e2b",)
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(
        return_value=sample_computer_dict(
            computer_id=COMPUTER_ID, kind="e2b", resource_tier="xlarge"
        )
    )
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch("src.server.app.computers.assert_provider_allowed", AsyncMock()),
        patch("src.server.app.computers.enforce_computer_count", AsyncMock()),
        patch("src.server.app.computers.enforce_computer_limit", AsyncMock()),
    ):
        resp = await client.post(
            "/api/v1/computers", json={"kind": "e2b", "resource_tier": "xlarge"}
        )

    assert resp.status_code == 201, resp.text
    assert resp.json()["resource_tier"] == "xlarge"
    entitled.assert_awaited_once_with(USER, "xlarge")
    assert manager.create_computer_for_user.await_args.kwargs["resource_tier"] == "xlarge"


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_providers_say_whether_an_ssh_machine_may_connect_out(client, enabled):
    with patch("src.server.app.computers.tunnels_enabled", return_value=enabled):
        resp = await client.get("/api/v1/computers/providers")

    assert resp.json()["tunnels"] is enabled


_HOST_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
)
_SSH_CREATE = {
    "kind": "ssh",
    "ssh": {
        "host": "203.0.113.10",
        "port": 22,
        "username": "dev",
        "host_key": _HOST_KEY,
        "root_dir": "/home/dev/langalpha",
    },
}
_SSH = "src.server.services.ssh_computers"


@pytest.fixture(autouse=True)
def ssh_computers_held():
    """How many live ssh computers the caller has, which the cap counts."""
    with patch(
        f"{_SSH}.count_live_computers_of_kind", AsyncMock(return_value=0)
    ) as count:
        yield count


def _found(**overrides):
    """What a probe of the machine in ``_SSH_CREATE`` finds."""
    from ptc_agent.core.sandbox.providers.ssh import SSHProbe

    fields = {
        "host_key": _HOST_KEY,
        "fingerprint": "SHA256:x",
        "os": "Linux",
        "arch": "x86_64",
        "distro": "ubuntu",
        "home": "/home/dev",
        "shell": "/bin/bash",
        "is_root": False,
        "has_sudo": False,
        "disk_free_bytes": None,
        "supported": True,
    }
    return SSHProbe(**{**fields, **overrides})


@pytest.mark.asyncio
async def test_an_ssh_create_where_ssh_is_not_offered_dials_nothing(
    client, computer, entitled
):
    """The probe connects to the address in the request, which a server that
    does not offer the kind has no business doing for anyone."""
    manager = _creating_manager(computer)
    dial = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.probe", dial),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
    ):
        resp = await client.post("/api/v1/computers", json=_SSH_CREATE)

    assert resp.status_code == 400
    dial.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_ssh_create_under_a_name_another_computer_has_dials_nothing(
    client, entitled, sandbox_config, names_free
):
    """Refused ahead of the probe, so a typed name never costs a dial to a
    machine, and in the shape the insert's own refusal answers."""
    from src.server.database.computer_names import ComputerNameTaken

    sandbox_config.offered_providers = ("ssh",)
    names_free.return_value = ComputerNameTaken("Research box", OTHER_COMPUTER_ID)
    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock()
    dial = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.probe", dial),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
    ):
        resp = await client.post(
            "/api/v1/computers", json={**_SSH_CREATE, "name": "  research BOX "}
        )

    assert resp.status_code == 409
    assert resp.json()["detail"] == {
        "code": "name_taken",
        "message": 'You already have a computer named "Research box".',
        "name": "Research box",
        "computer_id": OTHER_COMPUTER_ID,
    }
    names_free.assert_awaited_once_with(USER, "research BOX")
    dial.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_taken_while_the_probe_ran_is_refused_before_any_take_over(
    client, computer, entitled, sandbox_config, names_free, capacity_lock
):
    """A take-over stops the folder's old computer for good, so the name is
    asked again under the lock every rename holds, before the take-over runs."""
    from src.server.database.computer_names import ComputerNameTaken

    sandbox_config.offered_providers = ("ssh",)
    names_free.side_effect = [None, ComputerNameTaken("Research box", OTHER_COMPUTER_ID)]
    manager = _creating_manager({**computer, "kind": "ssh"})
    recheck = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", AsyncMock(return_value=_found())),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch(f"{_SSH}.SSHCreatePlan.recheck", recheck),
    ):
        resp = await client.post(
            "/api/v1/computers", json={**_SSH_CREATE, "name": "Research box"}
        )

    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "name_taken"
    assert names_free.await_count == 2
    assert capacity_lock == [USER]
    recheck.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_rename_holds_the_lock_a_create_holds_to_its_insert(
    client, computer, capacity_lock
):
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch(
            "src.server.app.computers.db_rename_computer",
            AsyncMock(return_value={**computer, "name": "Lab"}),
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.patch(f"/api/v1/computers/{COMPUTER_ID}", json={"name": "Lab"})

    assert resp.status_code == 200, resp.text
    assert capacity_lock == [USER]


@pytest.mark.asyncio
async def test_a_folder_taken_while_the_probe_ran_is_refused_under_the_lock(
    client, computer, entitled, sandbox_config, prepares
):
    """The probe is too slow to hold the lock, so the folder is checked again
    under it: two creates for one folder both pass the first check."""
    import contextlib

    sandbox_config.offered_providers = ("ssh",)
    manager = _creating_manager({**computer, "kind": "ssh"})
    sibling = {
        "kind": "ssh",
        "name": "first",
        "root_dir": "/home/dev/langalpha",
        "provider_config": {"host": "203.0.113.10", "port": 22, "host_key": _HOST_KEY},
    }
    held = []
    live = iter([[], [sibling]])

    async def _computers(user_id, *, include_deleted=False):
        return [] if include_deleted else next(live)

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        held.append(user_id)
        yield

    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", AsyncMock(return_value=_found())),
        patch(f"{_SSH}.get_computers_for_user", _computers),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
    ):
        resp = await client.post("/api/v1/computers", json=_SSH_CREATE)

    assert resp.status_code == 409
    assert '"first"' in resp.json()["detail"]
    # Taken with platform gating off too.
    assert held == [USER]
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_ssh_create_without_a_folder_works_in_langalpha_in_the_home(
    client, computer, entitled, sandbox_config, prepares
):
    """Never the deployment's root: that sits beside the homes and needs sudo."""
    import contextlib

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        yield

    sandbox_config.offered_providers = ("ssh",)
    manager = _creating_manager({**computer, "kind": "ssh"})
    probe = AsyncMock(return_value=_found())
    body = {**_SSH_CREATE, "ssh": {**_SSH_CREATE["ssh"], "root_dir": None}}
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", probe),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
        patch("src.server.app.computers.enforce_computer_limit", AsyncMock()),
    ):
        resp = await client.post("/api/v1/computers", json=body)

    assert resp.status_code == 201, resp.text
    # The folder is checked on the machine before the row exists.
    assert probe.await_args.kwargs["root_dir"] == "/home/dev/langalpha"
    kwargs = manager.create_computer_for_user.await_args.kwargs
    assert kwargs["root_dir"] == "/home/dev/langalpha"


@pytest.mark.asyncio
@pytest.mark.parametrize(("helper", "os"), [(True, "Darwin"), (False, "Linux")])
async def test_an_ssh_computer_keeps_the_helper_and_system_its_probe_found(
    client, computer, entitled, sandbox_config, prepares, helper, os
):
    """Only the probe can say so: the request never carries them."""
    import contextlib

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        yield

    sandbox_config.offered_providers = ("ssh",)
    manager = _creating_manager({**computer, "kind": "ssh"})
    body = {**_SSH_CREATE, "ssh": {**_SSH_CREATE["ssh"], "helper": not helper}}
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", AsyncMock(return_value=_found(helper=helper, os=os))),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
        patch("src.server.app.computers.enforce_computer_limit", AsyncMock()),
    ):
        resp = await client.post("/api/v1/computers", json=body)

    assert resp.status_code == 201, resp.text
    kwargs = manager.create_computer_for_user.await_args.kwargs
    assert kwargs["provider_config"]["helper"] is helper
    assert kwargs["provider_config"]["os"] == os


@pytest.mark.asyncio
async def test_an_ssh_computer_is_created_outside_the_plan_caps_and_may_skip_backups(
    client, computer, sandbox_config, prepares
):
    """Its owner brings and pays for the machine, so neither the computer cap
    nor the tier gate counts it, and only such a machine may start with its
    file backups off."""
    import contextlib

    from fastapi import HTTPException

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        yield

    sandbox_config.offered_providers = ("ssh",)
    manager = _creating_manager({**computer, "kind": "ssh", "files_backup": False})
    capped = AsyncMock(side_effect=HTTPException(429, detail="Limit reached"))
    tier = AsyncMock(side_effect=HTTPException(403, detail="Requires scope"))
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", AsyncMock(return_value=_found())),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
        patch("src.server.app.computers.enforce_computer_limit", capped),
        patch("src.server.app.computers.assert_spec_allowed", tier),
    ):
        resp = await client.post(
            "/api/v1/computers", json={**_SSH_CREATE, "files_backup": False}
        )

    assert resp.status_code == 201, resp.text
    assert resp.json()["files_backup"] is False
    assert manager.create_computer_for_user.await_args.kwargs["files_backup"] is False
    capped.assert_not_awaited()
    tier.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_ssh_create_at_the_limit_is_refused_before_dialing(
    client, computer, entitled, sandbox_config, ssh_computers_held
):
    sandbox_config.offered_providers = ("ssh",)
    sandbox_config.ssh.max_computers_per_user = 2
    ssh_computers_held.return_value = 2
    manager = _creating_manager({**computer, "kind": "ssh"})
    dial = AsyncMock()
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.probe", dial),
    ):
        resp = await client.post("/api/v1/computers", json=_SSH_CREATE)

    assert resp.status_code == 409
    assert resp.json()["detail"] == {
        "code": "ssh_computer_limit",
        "message": "You can have up to 2 SSH computers. Remove one to add another.",
        "limit": 2,
    }
    ssh_computers_held.assert_awaited_once_with(USER, "ssh")
    dial.assert_not_awaited()
    manager.create_computer_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_two_creates_for_the_last_ssh_computer_add_one(
    client, computer, entitled, sandbox_config, prepares, ssh_computers_held
):
    """Both pass the count made before their probes; the count again under
    the lock, which the insert holds too, refuses the second."""
    import asyncio
    import contextlib

    sandbox_config.offered_providers = ("ssh",)
    sandbox_config.ssh.max_computers_per_user = 3
    held = [{"kind": "ssh"}, {"kind": "ssh"}]
    ssh_computers_held.side_effect = lambda *_args: len(held)
    lock = asyncio.Lock()

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        async with lock:
            yield

    async def _insert(*_args, **kwargs):
        await asyncio.sleep(0)
        held.append({"kind": "ssh"})
        return {**computer, "kind": "ssh", "status": "stopped", "provider_ref": None}

    async def _probe(*_args, **_kwargs):
        # Both creates are past the first count before either inserts.
        await asyncio.sleep(0.01)
        return _found()

    manager = MagicMock()
    manager.create_computer_for_user = AsyncMock(side_effect=_insert)
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", _probe),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
    ):
        answers = await asyncio.gather(
            client.post("/api/v1/computers", json=_SSH_CREATE),
            client.post(
                "/api/v1/computers",
                json={
                    **_SSH_CREATE,
                    "ssh": {**_SSH_CREATE["ssh"], "root_dir": "/home/dev/other"},
                },
            ),
        )

    assert sorted(a.status_code for a in answers) == [201, 409]
    refused = next(a for a in answers if a.status_code == 409)
    assert refused.json()["detail"]["code"] == "ssh_computer_limit"
    assert manager.create_computer_for_user.await_count == 1
    assert len(held) == 3


@pytest.mark.asyncio
async def test_no_limit_counts_nothing(
    client, computer, entitled, sandbox_config, prepares, ssh_computers_held
):
    import contextlib

    @contextlib.asynccontextmanager
    async def _lock(user_id):
        yield

    sandbox_config.offered_providers = ("ssh",)
    sandbox_config.ssh.max_computers_per_user = 0
    manager = _creating_manager({**computer, "kind": "ssh"})
    with (
        patch("src.server.app.computers._computer_manager", return_value=manager),
        patch(f"{_SSH}.has_client_key", AsyncMock(return_value=True)),
        patch(f"{_SSH}.client_key", AsyncMock(return_value="private key")),
        patch(f"{_SSH}.probe", AsyncMock(return_value=_found())),
        patch(f"{_SSH}.get_computers_for_user", AsyncMock(return_value=[])),
        patch("src.server.app.computers.computer_capacity_lock", _lock),
    ):
        resp = await client.post("/api/v1/computers", json=_SSH_CREATE)

    assert resp.status_code == 201, resp.text
    ssh_computers_held.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_computer_the_server_rebuilds_is_never_created_without_backups(
    client, computer, entitled, prepares
):
    """Its folders are laid again from the backup whenever it is rebuilt.
    Refused before the row is written, so no such machine exists to go
    without one."""
    manager = _creating_manager(computer)
    with patch("src.server.app.computers._computer_manager", return_value=manager):
        resp = await client.post("/api/v1/computers", json={"files_backup": False})

    assert resp.status_code == 422
    assert resp.json()["detail"] == (
        "File backup can only be turned off on a computer you brought yourself."
    )
    manager.create_computer_for_user.assert_not_awaited()
    prepares.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["computer", "workspace"])
@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize(
    ("code", "status"), [("auth_failed", 422), ("unreachable", 503)]
)
async def test_an_ssh_failure_answers_alike_however_it_was_wrapped(
    route, wrapped, code, status
):
    """A reconnect wraps what it cannot place as transient, and a foreground
    start must not answer 503 where a background one reports the refusal,
    nor a workspace's start other than its computer's."""
    from fastapi import HTTPException

    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
    from ptc_agent.core.sandbox.runtime import SandboxTransientError
    from src.server.app.computers import _computer_action_errors
    from src.server.app.workspaces import _workspace_action_errors

    errors = (
        _computer_action_errors("start", COMPUTER_ID)
        if route == "computer"
        else _workspace_action_errors("start", WORKSPACE_ID)
    )
    cause = SSHConnectError(code, "dev@203.0.113.10 did not accept our key.")

    with pytest.raises(HTTPException) as caught:
        async with errors:
            if not wrapped:
                raise cause
            try:
                raise cause
            except SSHConnectError as e:
                raise SandboxTransientError(f"Could not reach sandbox x: {e}") from e

    assert caught.value.status_code == status


# ---------------------------------------------------------------------------
# Reads: list, get, ownership
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_computers_returns_the_callers_own(client, computer):
    with (
        patch(
            "src.server.app.computers.get_computers_for_user",
            AsyncMock(return_value=[computer]),
        ) as get_all,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 3}),
        ) as counted,
    ):
        resp = await client.get("/api/v1/computers")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["computers"][0]["computer_id"] == COMPUTER_ID
    assert body["computers"][0]["provider_ref"] == "sandbox-abc"
    # Stopping a machine takes every project on it down together, so the card
    # says how many before asking. One query for the whole list.
    assert body["computers"][0]["workspace_count"] == 3
    counted.assert_awaited_once_with([COMPUTER_ID])
    get_all.assert_awaited_once_with(USER)


@pytest.mark.asyncio
async def test_a_machine_with_no_live_project_counts_zero(client, computer):
    """A machine absent from the count is zero, not missing."""
    with (
        patch(
            "src.server.app.computers.get_computers_for_user",
            AsyncMock(return_value=[computer]),
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.get("/api/v1/computers")

    assert resp.status_code == 200
    assert resp.json()["computers"][0]["workspace_count"] == 0


@pytest.mark.asyncio
async def test_get_computer_returns_the_row(client, computer):
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 2}),
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "daytona"
    assert body["root_dir"] == "/home/workspace"
    assert body["is_primary"] is True
    assert body["workspace_count"] == 2


@pytest.mark.asyncio
async def test_get_computer_not_found(client):
    with patch("src.server.app.computers.get_computer", AsyncMock(return_value=None)):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_computer_forbidden_for_another_user(client, sample_computer_dict):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with patch(
        "src.server.app.computers.get_computer", AsyncMock(return_value=foreign)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_provider_ref_is_withheld_from_a_non_owner(sample_computer_dict):
    """The vendor id addresses a real billed machine, so it never leaves the row
    for a caller who does not own it. Asserted on the serializer because every
    route 403s first: this is the guard for a future shared-computer reader."""
    from src.server.app.computers import computer_to_response

    row = sample_computer_dict(provider_ref="sandbox-secret")
    assert computer_to_response(row, owner=True).provider_ref == "sandbox-secret"
    assert computer_to_response(row, owner=False).provider_ref is None


@pytest.mark.asyncio
async def test_an_unobserved_layout_is_null_not_version_zero(sample_computer_dict):
    """046 backfills the column at 0 and only an asset sync replaces it, so a
    reader that renders 0 as a version is reporting a layout nothing observed."""
    from src.server.app.computers import computer_to_response

    assert computer_to_response(sample_computer_dict()).layout_version == 3
    assert (
        computer_to_response(sample_computer_dict(layout_version=0)).layout_version
        is None
    )


# ---------------------------------------------------------------------------
# Session info
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_reports_the_row_without_resolving_when_not_running(
    client, sample_computer_dict, mock_computer_manager
):
    """A GET must never provision a machine, so a stopped computer is answered
    from Postgres alone."""
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    assert resp.json() == {
        "computer_id": COMPUTER_ID,
        "status": "stopped",
        "provider_ref": "sandbox-abc",
        "ready": False,
        "session_provider_ref": None,
    }
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_reports_what_this_worker_holds(
    client, computer, mock_computer_manager
):
    session = MagicMock()
    session.sandbox.sandbox_id = "sandbox-live"
    session.sandbox.runtime = MagicMock()
    mock_computer_manager.cached_session_for_computer = MagicMock(return_value=session)

    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is True
    assert body["session_provider_ref"] == "sandbox-live"
    mock_computer_manager.cached_session_for_computer.assert_called_once_with(computer)
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_degrades_when_this_worker_has_no_cached_session(
    client, computer, mock_computer_manager
):
    mock_computer_manager.cached_session_for_computer = MagicMock(return_value=None)
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/session")

    assert resp.status_code == 200
    assert resp.json()["ready"] is False
    assert resp.json()["status"] == "running"
    mock_computer_manager.get_session_for_computer.assert_not_awaited()


# ---------------------------------------------------------------------------
# Status events (SSE)
# ---------------------------------------------------------------------------


def _parse_sse(raw: str) -> list[tuple[str, str]]:
    """(event, data) pairs from one SSE chunk; comments and blanks ignored."""
    name, data = "", ""
    for line in raw.split("\n"):
        if line.startswith("event:"):
            name = line[6:].strip()
        elif line.startswith("data:"):
            data = line[5:].strip()
    return [(name, data)] if name else []


async def _collect_sse_events(client, url, *, want_events: int, timeout: float = 2.0):
    """Open an SSE stream and collect up to `want_events` events, then close."""
    import asyncio
    import json as _json

    events: list[tuple[str, dict]] = []
    async with client.stream("GET", url) as resp:
        assert resp.status_code == 200
        buffer = ""

        async def _read_loop():
            nonlocal buffer
            async for chunk in resp.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    raw, _, buffer = buffer.partition("\n\n")
                    for name, data in _parse_sse(raw + "\n\n"):
                        if name == "status" and data:
                            events.append((name, _json.loads(data)))
                        elif name == "timeout":
                            events.append((name, {}))
                    if len(events) >= want_events:
                        return

        try:
            await asyncio.wait_for(_read_loop(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
    return events


@pytest.mark.asyncio
async def test_computer_events_streams_the_transition(client, sample_computer_dict):
    """The frozen frame: {computer_id, status}. The subject is the machine, so a
    client watching one no longer has to pick a project to watch it through."""
    from contextlib import asynccontextmanager

    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = {**starting, "status": "running"}
    rows = iter([starting, starting, running])
    subscribed: list[str] = []

    async def fake_get(computer_id):
        return next(rows, running)

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        subscribed.append(computer_id)
        sent = False

        async def wait(timeout):
            nonlocal sent
            if not sent:
                sent = True
                return ("message", {"computer_id": computer_id, "status": "running"})
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer", new=AsyncMock(side_effect=fake_get)
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    assert subscribed == [COMPUTER_ID]
    payloads = [e[1] for e in events if e[0] == "status"]
    assert payloads == [
        {"computer_id": COMPUTER_ID, "status": "starting"},
        {"computer_id": COMPUTER_ID, "status": "running"},
    ]


@pytest.mark.asyncio
async def test_computer_events_forwards_a_failed_start_with_its_reason(
    client, sample_computer_dict
):
    """The one hint the stream does not confirm against the row. A failed start
    hands its claim back, so the row reads 'stopped' again and a confirm finds
    nothing to report; the reason riding on the publish is both what marks the
    message as a failure report and the only place that reason exists."""
    from contextlib import asynccontextmanager

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")

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
                        "error": "Unknown sandbox provider: 'nope'",
                    },
                )
            return ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    payloads = [e[1] for e in events if e[0] == "status"]
    assert payloads == [
        {"computer_id": COMPUTER_ID, "status": "stopped"},
        {
            "computer_id": COMPUTER_ID,
            "status": "error",
            "error": "Unknown sandbox provider: 'nope'",
        },
    ]


@pytest.mark.asyncio
async def test_computer_events_forwards_the_archived_refinement(
    client, sample_computer_dict
):
    """A slow-restore hint is emitted without a DB re-read and carries the
    payload's own status, the same contract the workspace stream has."""
    from contextlib import asynccontextmanager

    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = {**starting, "status": "running"}
    # initial read, post-subscribe read; the archived refinement does NOT
    # re-read; the running transition re-reads.
    rows = iter([starting, starting, running])

    @asynccontextmanager
    async def fake_subscribe(computer_id):
        msgs = iter(
            [
                {
                    "computer_id": computer_id,
                    "status": "starting",
                    "sandbox_state": "archived",
                },
                {"computer_id": computer_id, "status": "running"},
            ]
        )

        async def wait(timeout):
            payload = next(msgs, None)
            return ("message", payload) if payload else ("timeout", None)

        yield wait

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(side_effect=lambda _cid: next(rows, running)),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=fake_subscribe,
        ),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=3
        )

    assert events[1][1] == {
        "computer_id": COMPUTER_ID,
        "status": "starting",
        "sandbox_state": "archived",
    }
    assert events[2][1] == {"computer_id": COMPUTER_ID, "status": "running"}


@pytest.mark.asyncio
async def test_computer_events_closes_on_a_terminal_initial_status(
    client, sample_computer_dict
):
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=running)
    ):
        events = await _collect_sse_events(
            client,
            f"/api/v1/computers/{COMPUTER_ID}/events",
            want_events=1,
            timeout=1.0,
        )

    assert events == [("status", {"computer_id": COMPUTER_ID, "status": "running"})]


@pytest.mark.asyncio
async def test_computer_events_polls_when_pubsub_is_unavailable(
    client, sample_computer_dict
):
    """Redis down degrades to a DB poll, never to a dead stream: the route must
    still report the transition."""
    from contextlib import asynccontextmanager

    import src.server.app.status_stream as status_stream

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    running = {**stopped, "status": "running"}
    rows = iter([stopped, running])

    @asynccontextmanager
    async def no_subscription(computer_id):
        yield None

    with (
        patch(
            "src.server.app.computers.get_computer",
            new=AsyncMock(side_effect=lambda _cid: next(rows, running)),
        ),
        patch(
            "src.server.app.computers.subscribe_to_computer_status",
            new=no_subscription,
        ),
        patch.object(status_stream, "EVENTS_KEEPALIVE_S", 0.01),
    ):
        events = await _collect_sse_events(
            client, f"/api/v1/computers/{COMPUTER_ID}/events", want_events=2
        )

    assert [e[1]["status"] for e in events if e[0] == "status"] == [
        "stopped",
        "running",
    ]


@pytest.mark.asyncio
async def test_computer_events_forbidden_for_another_user(client, sample_computer_dict):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=foreign)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/events")

    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_computer_events_not_found(client):
    with patch(
        "src.server.app.computers.get_computer", new=AsyncMock(return_value=None)
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/events")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_from_stopped(client, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_start_is_a_no_op_when_already_running(
    client, computer, mock_computer_manager
):
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_blocking_start_joins_an_existing_start(
    client, sample_computer_dict, mock_computer_manager
):
    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    running = sample_computer_dict(computer_id=COMPUTER_ID, status="running")
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=starting),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_start_rejects_a_state_it_cannot_start_from(
    client, sample_computer_dict, mock_computer_manager
):
    stopping = sample_computer_dict(computer_id=COMPUTER_ID, status="stopping")
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=stopping)
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert resp.status_code == 400
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_start_repairs_an_abandoned_stopping_computer(
    client, sample_computer_dict, mock_computer_manager
):
    stopping = sample_computer_dict(computer_id=COMPUTER_ID, status="stopping")
    stopped = {**stopping, "status": "stopped"}
    running = {**stopping, "status": "running"}
    mock_computer_manager.reconcile_stopping_computer.return_value = stopped
    mock_computer_manager.start_computer.return_value = running
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(side_effect=[stopping, stopped]),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        response = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start")

    assert response.status_code == 200
    mock_computer_manager.reconcile_stopping_computer.assert_awaited_once_with(
        COMPUTER_ID
    )
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_lazy_start_returns_202_and_schedules(
    client, sample_computer_dict, mock_computer_manager
):
    import asyncio

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    async def start(_id, *, on_start_claimed):
        stopped['status'] = 'starting'
        on_start_claimed()
    mock_computer_manager.start_computer.side_effect = start
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        assert resp.status_code == 202
        assert resp.json()["status"] == "starting"
        # The background task holds a strong ref until it finishes; let it run.
        await asyncio.sleep(0)

    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID, on_start_claimed=ANY)


@pytest.mark.asyncio
async def test_deduplicated_lazy_waiters_acknowledge_before_provisioning_finishes(
    client, sample_computer_dict, mock_computer_manager
):
    import asyncio

    from src.server.app import computers

    row = sample_computer_dict(computer_id=COMPUTER_ID, status="creating")
    before_claim = asyncio.Event()
    release_claim = asyncio.Event()
    release_provision = asyncio.Event()
    scheduled_twice = asyncio.Event()
    schedules = 0
    schedule = computers.schedule_computer_start

    def tracked_schedule(*args, **kwargs):
        nonlocal schedules
        schedules += 1
        if schedules == 2:
            scheduled_twice.set()
        return schedule(*args, **kwargs)

    async def start(_id, *, on_start_claimed):
        before_claim.set()
        await release_claim.wait()
        row["status"] = "starting"
        on_start_claimed()
        await release_provision.wait()

    mock_computer_manager.start_computer.side_effect = start
    with (
        patch.object(computers, "get_computer", AsyncMock(return_value=row)),
        patch.object(computers, "_computer_manager", return_value=mock_computer_manager),
        patch.object(computers, "schedule_computer_start", tracked_schedule),
    ):
        requests = []
        try:
            requests.append(asyncio.create_task(client.post(
                f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true"
            )))
            await asyncio.wait_for(before_claim.wait(), 1)
            requests.append(asyncio.create_task(client.post(
                f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true"
            )))
            await asyncio.wait_for(scheduled_twice.wait(), 1)
            release_claim.set()
            responses = await asyncio.wait_for(asyncio.gather(*requests), 1)
            assert [r.status_code for r in responses] == [202, 202]
            assert all(r.json()["status"] == "starting" for r in responses)
            assert not release_provision.is_set()
            mock_computer_manager.start_computer.assert_awaited_once()
        finally:
            release_claim.set()
            release_provision.set()
            await asyncio.gather(*requests, return_exceptions=True)
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_stop(client, computer, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.stop_computer.return_value = stopped
    with (
        patch(
            "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
        ) as read,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_computer_manager.stop_computer.assert_awaited_once_with(COMPUTER_ID)
    # One read for ownership; the settled row comes from the transition itself.
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_archive(client, sample_computer_dict, mock_computer_manager):
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.archive_computer.return_value = stopped
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/archive")

    assert resp.status_code == 200
    mock_computer_manager.archive_computer.assert_awaited_once_with(COMPUTER_ID)


@pytest.mark.asyncio
async def test_stop_forbidden_for_another_user(
    client, sample_computer_dict, mock_computer_manager
):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=foreign)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 403
    mock_computer_manager.stop_computer.assert_not_awaited()


# ---------------------------------------------------------------------------
# Remove: an empty computer other than the primary, sandbox and all
# ---------------------------------------------------------------------------


def _removing(row, manager):
    return (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=row)),
        patch("src.server.app.computers._computer_manager", return_value=manager),
    )


@pytest.mark.asyncio
async def test_remove_of_a_hosted_computer_has_no_key_to_report(
    client, computer, mock_computer_manager
):
    leave = AsyncMock()
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager, patch("src.server.app.computers.leave_machine", leave):
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 200
    assert resp.json() == {"key": None, "files": None, "workspaces_deleted": []}
    # Naming none, the computer must be empty: nothing on it is deleted.
    mock_computer_manager.remove_computer_with_workspaces.assert_awaited_once_with(
        COMPUTER_ID, [], delete_files=False
    )
    leave.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_naming_workspaces_deletes_those_first(
    client, computer, mock_computer_manager
):
    # One named but moved off meanwhile is skipped, and the client must not
    # forget it: the answer names only the ones that went.
    mock_computer_manager.remove_computer_with_workspaces.return_value = ["ws-a"]
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.request(
            "DELETE",
            f"/api/v1/computers/{COMPUTER_ID}",
            json={"delete_workspaces": ["ws-a", "ws-b"]},
        )

    assert resp.status_code == 200
    assert resp.json()["workspaces_deleted"] == ["ws-a"]
    mock_computer_manager.remove_computer_with_workspaces.assert_awaited_once_with(
        COMPUTER_ID, ["ws-a", "ws-b"], delete_files=False
    )


@pytest.mark.asyncio
async def test_remove_joins_the_workspaces_an_older_client_names_in_the_query(
    client, computer, mock_computer_manager
):
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.request(
            "DELETE",
            f"/api/v1/computers/{COMPUTER_ID}",
            params=[("delete_workspaces", "ws-a"), ("delete_workspaces", "ws-b")],
            json={"delete_workspaces": ["ws-b", "ws-c"]},
        )

    assert resp.status_code == 200
    mock_computer_manager.remove_computer_with_workspaces.assert_awaited_once_with(
        COMPUTER_ID, ["ws-a", "ws-b", "ws-c"], delete_files=False
    )


def _ssh_row(computer, *, tunnel=""):
    return {
        **computer,
        "kind": "ssh",
        "provider_config": {
            "host": "203.0.113.10",
            "port": 22,
            "username": "dev",
            "host_key": _HOST_KEY,
            "key_owner": USER,
            "tunnel": tunnel,
        },
    }


@pytest.mark.asyncio
async def test_remove_asked_to_delete_files_passes_it_on_and_reports_them(
    client, computer, mock_computer_manager
):
    from src.server.models.computer import ComputerFilesRemoval
    from src.server.services.ssh_removal import MachineLeft

    files = ComputerFilesRemoval(outcome="deleted", runtime_kept=True)
    leave = AsyncMock(return_value=MachineLeft(key=None, tunnel_released=False, files=files))
    get_row, get_manager = _removing(_ssh_row(computer), mock_computer_manager)
    with get_row, get_manager, patch("src.server.app.computers.leave_machine", leave):
        resp = await client.delete(
            f"/api/v1/computers/{COMPUTER_ID}",
            params=[("delete_workspaces", "ws-a"), ("delete_files", "true")],
        )

    assert resp.status_code == 200
    assert resp.json()["files"] == files.model_dump()
    mock_computer_manager.remove_computer_with_workspaces.assert_awaited_once_with(
        COMPUTER_ID, ["ws-a"], delete_files=True
    )
    assert leave.await_args.kwargs["delete_files"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "remove_key"), [("", True), ("?remove_key=false", False)]
)
async def test_remove_of_an_ssh_computer_reports_its_key_after_the_delete(
    client, computer, mock_computer_manager, query, remove_key
):
    from src.server.models.computer import ComputerKeyRemoval
    from src.server.services.ssh_removal import MachineLeft

    events = []
    mock_computer_manager.remove_computer_with_workspaces.side_effect = (
        lambda *_, **__: events.append("delete") or []
    )
    key = ComputerKeyRemoval(
        outcome="removed", fingerprint="SHA256:abc/def+", comment="langalpha"
    )

    async def _leave(user_id, row, *, settings, remove_key, delete_files):
        events.append(("leave", user_id, row["computer_id"], remove_key, delete_files))
        return MachineLeft(key=key if remove_key else None, tunnel_released=False)

    row = _ssh_row(computer)
    get_row, get_manager = _removing(row, mock_computer_manager)
    with get_row, get_manager, patch("src.server.app.computers.leave_machine", _leave):
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}{query}")

    assert resp.status_code == 200
    assert resp.json()["key"] == (key.model_dump() if remove_key else None)
    assert events == ["delete", ("leave", USER, COMPUTER_ID, remove_key, False)]


@pytest.mark.asyncio
@pytest.mark.parametrize("released", [True, False])
async def test_a_tunnel_not_released_on_the_deletes_connection_is_released_later(
    client, computer, mock_computer_manager, released
):
    from src.server.services.ssh_removal import MachineLeft

    schedule = MagicMock()
    leave = AsyncMock(return_value=MachineLeft(key=None, tunnel_released=released))
    row = _ssh_row(computer, tunnel="t-1")
    get_row, get_manager = _removing(row, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch("src.server.app.computers.leave_machine", leave),
        patch("src.server.app.computers.schedule_start", schedule),
    ):
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 200
    assert schedule.called is not released
    if not released:
        assert schedule.call_args.args[0] == f"tunnel-release:{COMPUTER_ID}"


@pytest.mark.asyncio
async def test_remove_of_an_unknown_computer_is_404(client, mock_computer_manager):
    get_row, get_manager = _removing(None, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 404
    mock_computer_manager.remove_computer_with_workspaces.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_is_forbidden_for_another_user(
    client, sample_computer_dict, mock_computer_manager
):
    foreign = sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)
    get_row, get_manager = _removing(foreign, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 403
    mock_computer_manager.remove_computer_with_workspaces.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["primary", "has_workspaces", "busy"])
async def test_a_refused_remove_is_409_with_the_code_the_client_words(
    client, computer, mock_computer_manager, code
):
    from src.server.services.computer_errors import ComputerRemovalRefused

    refusal = ComputerRemovalRefused(code)
    mock_computer_manager.remove_computer_with_workspaces.side_effect = refusal
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 409
    assert resp.json()["detail"] == {"code": code, "message": str(refusal)}
    assert str(refusal)


@pytest.mark.asyncio
async def test_a_remove_refused_partway_names_the_workspaces_already_deleted(
    client, computer, mock_computer_manager
):
    from src.server.services.computer_errors import ComputerRemovalRefused

    mock_computer_manager.remove_computer_with_workspaces.side_effect = (
        ComputerRemovalRefused("has_workspaces", deleted=["ws-a"])
    )
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 409
    assert resp.json()["detail"]["workspaces_deleted"] == ["ws-a"]


@pytest.mark.asyncio
async def test_a_computer_retired_under_the_remove_is_404(
    client, computer, mock_computer_manager
):
    """Another remove, or the last workspace leaving, retired it first."""
    mock_computer_manager.remove_computer_with_workspaces.side_effect = ValueError(
        f"Computer {COMPUTER_ID} not found"
    )
    get_row, get_manager = _removing(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.delete(f"/api/v1/computers/{COMPUTER_ID}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_a_missing_runtime_is_503_not_a_silent_success(
    client, sample_computer_dict
):
    """The read routes stay up without the computer runtime; an action must not
    claim a transition it could not make."""
    from src.server.app.computers import ComputerRuntimeUnavailable

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch(
            "src.server.app.computers._computer_manager",
            side_effect=ComputerRuntimeUnavailable("no module"),
        ),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/stop")

    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_storage_reuses_a_breakdown_younger_than_a_minute(
    client, computer, mock_computer_manager
):
    """The panel asks on every open; a du over the whole root on each one is
    what the reuse window exists to avoid, so the route has to ask for it."""
    from src.server.services.computer_disk import STORAGE_BREAKDOWN_MAX_AGE_SECONDS

    mock_computer_manager.measures_disk = MagicMock(return_value=True)
    mock_computer_manager.refresh_computer_disk = AsyncMock(
        return_value=(computer, {"alpha": 300})
    )
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch("src.server.app.computers._computer_manager", return_value=mock_computer_manager),
        patch(
            "src.server.app.computers.get_live_workspace_folders_for_computer",
            AsyncMock(return_value=[]),
        ),
    ):
        resp = await client.get(f"/api/v1/computers/{COMPUTER_ID}/storage")

    assert resp.status_code == 200
    assert resp.json()["live"] is True
    assert mock_computer_manager.refresh_computer_disk.await_args.kwargs == {
        "breakdown": True,
        "breakdown_max_age_s": STORAGE_BREAKDOWN_MAX_AGE_SECONDS,
    }


# ---------------------------------------------------------------------------
# Spec tier and always-on
# ---------------------------------------------------------------------------


def _spec_claim(computer, **change):
    from datetime import datetime, timezone

    return {
        **computer,
        "spec_change": {
            "target_tier": "performance",
            "from_tier": "standard",
            "state": "in_progress",
            "error": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "claim_id": "claim-1",
            **change,
        },
        "spec_change_stale": False,
    }


def _spec_client_patches(computer, manager):
    return (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=computer),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=manager,
        ),
    )


@pytest.mark.asyncio
async def test_set_spec_accepts_then_runs_the_claim_in_the_background(
    client, computer, mock_computer_manager
):
    """A recreate outlives an edge-proxied request, so the route answers 202 and
    hands the claim to a background run whose outcome is read back from the row."""
    import inspect

    from src.server.services.computer_manager import ComputerManager

    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(
        return_value=_spec_claim(computer)
    )
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 2}),
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )
        await _drain_start_tasks()

    assert resp.status_code == 202
    body = resp.json()
    assert body["spec_change"]["state"] == "in_progress"
    assert body["spec_change"]["claim_id"] == "claim-1"
    assert body["workspace_count"] == 2
    mock_computer_manager.claim_computer_spec.assert_awaited_once_with(
        COMPUTER_ID, "performance"
    )
    mock_computer_manager.run_accepted_spec_change.assert_awaited_once_with(
        COMPUTER_ID, "performance", user_id=USER, claim_id="claim-1"
    )
    # The manager is a bare mock: hold every call the route made to the real
    # signatures, so a renamed method or keyword fails here, not in production.
    for name in (
        "precheck_computer_spec",
        "claim_computer_spec",
        "run_accepted_spec_change",
    ):
        call = getattr(mock_computer_manager, name).await_args
        inspect.signature(getattr(ComputerManager, name)).bind(
            None, *call.args, **call.kwargs
        )


def _assert_refusal(resp, code):
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["code"] == code
    assert isinstance(detail["message"], str) and detail["message"]
    assert detail["files"] == []


@pytest.mark.asyncio
async def test_set_spec_to_a_size_the_provider_lacks_is_a_400_before_the_gate(
    client, computer, mock_computer_manager
):
    """Extra Large on a Daytona computer: refused as a bad request, never
    offered as an upgrade."""
    mock_computer_manager.precheck_computer_spec = AsyncMock(
        side_effect=UnknownResourceTier(
            "This computer's provider does not offer the 'xlarge' spec"
        )
    )
    mock_computer_manager.claim_computer_spec = AsyncMock()
    gate = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch("src.server.app.computers.platform_gating_active", return_value=True),
        patch("src.server.app.computers.assert_spec_allowed", gate),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "xlarge"}
        )

    assert resp.status_code == 400
    assert "xlarge" in resp.json()["detail"]
    gate.assert_not_awaited()
    mock_computer_manager.claim_computer_spec.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_spec_refuses_a_second_change_while_one_runs(
    client, computer, mock_computer_manager
):
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(return_value=None)
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, "spec_in_progress")
    mock_computer_manager.run_accepted_spec_change.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_repeat_of_a_running_change_is_refused_not_read_as_done(
    client, computer, mock_computer_manager
):
    """A running change persists its target tier early, so the same-tier
    shortcut would answer a repeat 200 as if the change had finished."""
    row = _spec_claim({**computer, "resource_tier": "performance"})
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=True)
    get_row, get_manager = _spec_client_patches(row, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, "spec_in_progress")


@pytest.mark.asyncio
async def test_a_stale_change_does_not_block_a_new_one(
    client, computer, mock_computer_manager
):
    """The database says the old claim is stale, so the route reads it as
    interrupted and lets the claim take the row over."""
    row = _spec_claim(computer)
    row["spec_change_stale"] = True
    mock_computer_manager.precheck_computer_spec = AsyncMock(return_value=False)
    mock_computer_manager.claim_computer_spec = AsyncMock(
        return_value=_spec_claim(computer, claim_id="claim-2")
    )
    mock_computer_manager.run_accepted_spec_change = AsyncMock()
    get_row, get_manager = _spec_client_patches(row, mock_computer_manager)
    with (
        get_row,
        get_manager,
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    assert resp.status_code == 202
    assert resp.json()["spec_change"]["claim_id"] == "claim-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "code"),
    [("turn", "turn_active"), ("busy", "busy")],
)
async def test_set_spec_refuses_before_accepting_in_the_outcome_shape(
    client, computer, mock_computer_manager, raised, code
):
    from src.server.services.computer_errors import (
        ComputerBusyError,
        MachineBusyError,
    )

    exc = {
        "turn": MachineBusyError("turn running"),
        "busy": ComputerBusyError("mid-operation"),
    }[raised]
    mock_computer_manager.precheck_computer_spec = AsyncMock(side_effect=exc)
    mock_computer_manager.claim_computer_spec = AsyncMock()
    get_row, get_manager = _spec_client_patches(computer, mock_computer_manager)
    with get_row, get_manager:
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "performance"}
        )

    _assert_refusal(resp, code)
    mock_computer_manager.claim_computer_spec.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_incomplete_backup_names_its_files_on_the_spec_route_only():
    from fastapi import HTTPException

    from src.server.app.computers import _computer_action_errors
    from src.server.services.persistence.sync_result import (
        BackupIncomplete,
        UnsavedFile,
    )

    exc = BackupIncomplete("ws-1 unsaved", [UnsavedFile("a.bin", "too_large", 9)])
    with pytest.raises(HTTPException) as spec:
        async with _computer_action_errors("set spec for", "c1", spec=True):
            raise exc
    assert spec.value.status_code == 409
    assert spec.value.detail["code"] == "backup_incomplete"
    assert spec.value.detail["files"] == [
        {"path": "a.bin", "reason": "too_large", "size": 9}
    ]
    assert "ws-1" not in spec.value.detail["message"]

    with pytest.raises(HTTPException) as other:
        async with _computer_action_errors("stop", "c1"):
            raise exc
    assert other.value.status_code == 400
    assert other.value.detail == exc.user_message


@pytest.mark.asyncio
async def test_set_spec_rejects_an_unknown_tier(client, computer):
    with patch(
        "src.server.app.computers.get_computer", AsyncMock(return_value=computer)
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/spec", json={"tier": "enormous"}
        )

    assert resp.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_status", ["stopped", "creating"])
async def test_enabling_always_on_on_a_stopped_computer_starts_it(
    client, sample_computer_dict, mock_computer_manager, initial_status
):
    import asyncio

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status=initial_status)
    enabled = sample_computer_dict(
        computer_id=COMPUTER_ID, status=initial_status, is_always_on=True
    )
    events = []

    async def set_always_on(*args, **kwargs):
        events.append(("write", args[0]))
        return enabled
    mock_computer_manager.set_computer_always_on.side_effect = set_always_on
    async def start(_id, *, on_start_claimed):
        stopped['status'] = 'starting'
        on_start_claimed()
    mock_computer_manager.start_computer.side_effect = start
    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(return_value=stopped),
        ),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": True}
        )
        await asyncio.sleep(0)

    assert resp.status_code == 200
    # Always-on means up now, so the response reports the start it just began.
    assert resp.json()["status"] == "starting"
    assert resp.json()["is_always_on"] is True
    assert events == [("write", COMPUTER_ID)]
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, True, user_id=USER
    )
    mock_computer_manager.start_computer.assert_awaited_once_with(COMPUTER_ID, on_start_claimed=ANY)


@pytest.mark.asyncio
async def test_disabling_always_on_is_never_gated_and_starts_nothing(
    client, sample_computer_dict, mock_computer_manager
):
    on = sample_computer_dict(
        computer_id=COMPUTER_ID, status="stopped", is_always_on=True
    )
    off = sample_computer_dict(
        computer_id=COMPUTER_ID, status="stopped", is_always_on=False
    )
    mock_computer_manager.set_computer_always_on.return_value = off
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=on)),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": False}
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, False, user_id=USER
    )
    mock_computer_manager.start_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_re_enabling_an_always_on_computer_skips_the_gate(
    client, sample_computer_dict, mock_computer_manager
):
    """An idempotent retry at the quota limit must not 429: no new slot is
    consumed by re-asserting a flag that is already set."""
    on = sample_computer_dict(
        computer_id=COMPUTER_ID, status="running", is_always_on=True
    )
    mock_computer_manager.set_computer_always_on.return_value = on
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=on)),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={}),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/always-on", json={"enabled": True}
        )

    assert resp.status_code == 200
    mock_computer_manager.set_computer_always_on.assert_awaited_once_with(
        COMPUTER_ID, True, user_id=USER
    )


# ---------------------------------------------------------------------------
# File backup: the owner of a machine they brought may turn it off
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "enabled"), [("ssh", False), ("daytona", True)], ids=["ssh-off", "hosted-on"]
)
async def test_the_file_backup_switch_answers_with_the_machine(
    client, sample_computer_dict, kind, enabled
):
    """Nothing on the machine changes, so the write is the whole answer."""
    before = sample_computer_dict(computer_id=COMPUTER_ID, kind=kind)
    write = AsyncMock(return_value={**before, "files_backup": enabled})
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=before)),
        patch("src.server.app.computers.set_computer_files_backup", write),
        patch(
            "src.server.app.computers.count_live_workspaces_by_computer",
            AsyncMock(return_value={COMPUTER_ID: 2}),
        ),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/files-backup", json={"enabled": enabled}
        )

    assert resp.status_code == 200
    assert resp.json()["files_backup"] is enabled
    assert resp.json()["workspace_count"] == 2
    write.assert_awaited_once_with(COMPUTER_ID, enabled)


@pytest.mark.asyncio
async def test_file_backup_stays_on_for_a_machine_the_server_rebuilds(client, computer):
    """A rebuild lays its folders again from the backup, which off would leave
    stale."""
    write = AsyncMock()
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch("src.server.app.computers.set_computer_files_backup", write),
    ):
        resp = await client.post(
            f"/api/v1/computers/{COMPUTER_ID}/files-backup", json={"enabled": False}
        )

    assert resp.status_code == 409
    assert resp.json()["detail"] == (
        "File backup can only be turned off on a computer you brought yourself."
    )
    write.assert_not_awaited()


# ---------------------------------------------------------------------------
# Aliases: /api/v1/workspaces/{id}/... resolves to the computer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action,method_name",
    [
        ("archive", "archive_workspace"),
    ],
)
async def test_the_workspace_alias_runs_one_transition(
    ws_client, mock_computer_manager, action, method_name
):
    """The sandbox settings panel posts /workspaces/{id}/{action} with the action
    interpolated, so these paths must keep answering while both surfaces are
    live. The project-addressed manager resolves the machine and runs the one
    transition, so the route must not reach the computer surface as well."""
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws(status="running")),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        manager = AsyncMock()
        getattr(manager, method_name).return_value = _ws(status="stopped")
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(f"/api/v1/workspaces/{WORKSPACE_ID}/{action}")

    assert resp.status_code == 200
    getattr(manager, method_name).assert_awaited_once_with(WORKSPACE_ID)
    mock_computer_manager.stop_computer.assert_not_awaited()
    mock_computer_manager.archive_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_spec_alias_answers_with_the_workspace(ws_client):
    upgraded = _ws(resource_tier="performance")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws()),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        manager = AsyncMock()
        manager.set_workspace_spec.return_value = upgraded
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/spec", json={"tier": "performance"}
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["resource_tier"] == "performance"
    # WorkspaceResponse keeps its whole shape and gains the binding.
    assert body["workspace_id"] == WORKSPACE_ID
    assert body["computer_id"] == COMPUTER_ID
    assert body["sandbox_id"] == "sandbox-abc"
    manager.set_workspace_spec.assert_awaited_once_with(
        WORKSPACE_ID, "performance", user_id=USER
    )


@pytest.mark.asyncio
async def test_the_spec_alias_delegates_the_quota_decision_to_the_manager(ws_client):
    events = []

    manager = AsyncMock()
    manager.set_workspace_spec.side_effect = lambda *_args, **_kwargs: (
        events.append(("write", WORKSPACE_ID)) or _ws(resource_tier="performance")
    )
    with (
        patch("src.server.app.workspaces.db_get_workspace", AsyncMock(return_value=_ws())),
        patch("src.server.app.workspaces.WorkspaceManager") as manager_cls,
    ):
        manager_cls.get_instance.return_value = manager
        response = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/spec",
            json={"tier": "performance"},
        )

    assert response.status_code == 200
    assert events == [("write", WORKSPACE_ID)]


@pytest.mark.asyncio
async def test_the_always_on_alias_answers_with_the_workspace(ws_client):
    enabled = _ws(is_always_on=True, status="running")
    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws()),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
    ):
        manager = AsyncMock()
        manager.set_workspace_always_on.return_value = enabled
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/always-on", json={"enabled": True}
        )

    assert resp.status_code == 200
    assert resp.json()["is_always_on"] is True
    manager.set_workspace_always_on.assert_awaited_once_with(
        WORKSPACE_ID, True, user_id=USER
    )


@pytest.mark.asyncio
async def test_the_always_on_alias_delegates_the_quota_decision_to_the_manager(ws_client):
    events = []

    manager = AsyncMock()
    manager.set_workspace_always_on.side_effect = lambda *_args, **_kwargs: (
        events.append(("write", WORKSPACE_ID))
        or _ws(is_always_on=True, status="running")
    )
    with (
        patch("src.server.app.workspaces.db_get_workspace", AsyncMock(return_value=_ws())),
        patch("src.server.app.workspaces.WorkspaceManager") as manager_cls,
    ):
        manager_cls.get_instance.return_value = manager
        response = await ws_client.post(
            f"/api/v1/workspaces/{WORKSPACE_ID}/always-on",
            json={"enabled": True},
        )

    assert response.status_code == 200
    assert events == [("write", WORKSPACE_ID)]


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?lazy=true"])
async def test_the_start_alias_addresses_this_project_not_the_machine(
    ws_client, mock_computer_manager, query
):
    """Start is addressed at the project for the same reason every alias is.

    Starting the machine alone attaches whichever live sibling it finds, which
    prepares that sibling's folder and tool overlay and leaves this project
    with neither on a machine its own row now calls running.
    """
    import asyncio

    with (
        patch(
            "src.server.app.workspaces.db_get_workspace",
            AsyncMock(return_value=_ws(status="stopped")),
        ),
        patch("src.server.app.workspaces.WorkspaceManager") as MockWM,
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
    ):
        manager = AsyncMock()
        MockWM.get_instance.return_value = manager
        resp = await ws_client.post(f"/api/v1/workspaces/{WORKSPACE_ID}/start{query}")
        await asyncio.sleep(0)

    assert resp.status_code == (202 if query else 200)
    mock_computer_manager.start_computer.assert_not_awaited()
    manager.get_session_for_workspace.assert_awaited_once_with(
        WORKSPACE_ID, user_id=ANY
    )


# ---------------------------------------------------------------------------
# A background start that fails
# ---------------------------------------------------------------------------


async def _drain_start_tasks():
    """Await whatever schedule_computer_start left running."""
    import asyncio

    from src.server.services import background_starts

    await asyncio.gather(
        *list(background_starts._start_tasks.values()), return_exceptions=True
    )


@pytest.mark.asyncio
async def test_a_failed_lazy_start_settles_the_row_and_publishes_the_reason(
    client, sample_computer_dict, mock_computer_manager
):
    """The 202 is a promise that something will be published. A start that
    raises is the one transition the row cannot describe on its own, so the
    failure goes out on the machine's channel with its reason, and the claim is
    handed back first so a client re-reading on 'error' sees 'stopped'."""
    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    starting = sample_computer_dict(computer_id=COMPUTER_ID, status="starting")
    mock_computer_manager.start_computer = AsyncMock(
        side_effect=ValueError("provider https://user:synthetic-password@private.invalid/sandbox/private-id")
    )
    publish = AsyncMock()
    status_write = AsyncMock()

    with (
        patch(
            "src.server.app.computers.get_computer",
            AsyncMock(side_effect=[stopped, starting]),
        ),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
        patch("src.server.app.computers.update_computer_status", status_write),
        patch("src.server.app.computers.publish_computer_status_change", publish),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "Sandbox is not reachable: please retry in a moment"
        await _drain_start_tasks()

    status_write.assert_awaited_once_with(COMPUTER_ID, "stopped", expected="starting")
    publish.assert_awaited_once_with(
        COMPUTER_ID,
        "error",
        extra={"error": "Sandbox is not reachable: please retry in a moment"},
    )


@pytest.mark.asyncio
async def test_a_claim_already_handed_back_is_not_written_again(
    sample_computer_dict, mock_computer_manager
):
    """The claim's own revert usually wins the race, and a row that has moved
    on belongs to whoever moved it."""
    from src.server.app import computers

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.start_computer = AsyncMock(side_effect=RuntimeError("boom"))
    publish = AsyncMock()
    status_write = AsyncMock()

    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch.object(
            computers, "_computer_manager", return_value=mock_computer_manager
        ),
        patch.object(computers, "update_computer_status", status_write),
        patch.object(computers, "publish_computer_status_change", publish),
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await _drain_start_tasks()

    status_write.assert_not_awaited()
    publish.assert_awaited_once_with(COMPUTER_ID, "error", extra={"error": "Sandbox is not reachable: please retry in a moment"})


@pytest.mark.asyncio
async def test_a_drained_start_publishes_nothing(mock_computer_manager):
    """Cancellation is the start being taken away, not failing: the machine is
    coming down with the worker, so its subscribers are told nothing."""
    import asyncio

    from src.server.app import computers
    from src.server.services import background_starts

    started = asyncio.Event()

    async def _never_finishes(_computer_id):
        started.set()
        await asyncio.sleep(3600)

    mock_computer_manager.start_computer = AsyncMock(side_effect=_never_finishes)
    publish = AsyncMock()

    with (
        patch.object(
            computers, "_computer_manager", return_value=mock_computer_manager
        ),
        patch.object(computers, "publish_computer_status_change", publish),
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        await background_starts.drain_start_tasks()

    publish.assert_not_awaited()


# ---------------------------------------------------------------------------
# Shutdown drain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_cancels_in_flight_starts(mock_computer_manager):
    """A start cancelled mid-bringup reverts its own claim; torn down abruptly
    it can leave the row wedged in 'starting'."""
    import asyncio

    from src.server.app import computers
    from src.server.services import background_starts

    started = asyncio.Event()

    async def _never_finishes(_computer_id):
        started.set()
        await asyncio.sleep(3600)

    mock_computer_manager.start_computer = AsyncMock(side_effect=_never_finishes)
    with patch.object(
        computers, "_computer_manager", return_value=mock_computer_manager
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        assert f"computer:{COMPUTER_ID}" in background_starts._start_tasks
        await background_starts.drain_start_tasks()

    assert background_starts._start_tasks == {}


@pytest.mark.asyncio
async def test_a_second_lazy_start_joins_the_one_in_flight(mock_computer_manager):
    """Two 202s for the same machine must not bring it up twice. The second
    schedule finds the first task under the same key and leaves it alone; only
    once it has settled does a new schedule start the machine again."""
    import asyncio

    from src.server.app import computers
    from src.server.services import background_starts

    started = asyncio.Event()
    release = asyncio.Event()

    async def _held(_computer_id):
        started.set()
        await release.wait()

    mock_computer_manager.start_computer = AsyncMock(side_effect=_held)
    with patch.object(
        computers, "_computer_manager", return_value=mock_computer_manager
    ):
        computers.schedule_computer_start(COMPUTER_ID)
        await started.wait()
        computers.schedule_computer_start(COMPUTER_ID)
        await asyncio.sleep(0)
        assert mock_computer_manager.start_computer.await_count == 1

        release.set()
        await _drain_start_tasks()
        assert f"computer:{COMPUTER_ID}" not in background_starts._start_tasks

        computers.schedule_computer_start(COMPUTER_ID)
        await _drain_start_tasks()

    assert mock_computer_manager.start_computer.await_count == 2


@pytest.mark.asyncio
async def test_lazy_start_preserves_capacity_denial(client, sample_computer_dict, mock_computer_manager):
    from fastapi import HTTPException

    stopped = sample_computer_dict(computer_id=COMPUTER_ID, status="stopped")
    mock_computer_manager.start_computer = AsyncMock(side_effect=HTTPException(429, "capacity exhausted"))
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=stopped)),
        patch("src.server.app.computers._computer_manager", return_value=mock_computer_manager),
        patch("src.server.app.computers.publish_computer_status_change", AsyncMock()),
        patch("src.server.app.computers.update_computer_status", AsyncMock()) as update,
    ):
        response = await client.post(f"/api/v1/computers/{COMPUTER_ID}/start?lazy=true")
        await _drain_start_tasks()
    assert response.status_code == 429
    assert response.json()["detail"] == "capacity exhausted"
    update.assert_not_awaited()


# ---------------------------------------------------------------------------
# Make primary: the flip lands now, or Home moves first and the flip with it
# ---------------------------------------------------------------------------


def _home_move(**overrides):
    from datetime import datetime, timezone

    from src.server.database.home_workspace import get_home_workspace_id

    row = {
        "move_id": "44444444-4444-4444-8444-444444444444",
        "workspace_id": get_home_workspace_id(USER),
        "source_computer_id": "55555555-5555-4555-8555-555555555555",
        "target_computer_id": COMPUTER_ID,
        "make_primary": True,
        "phase": "preparing",
        "error": None,
        "stale": False,
        "created_at": datetime.now(timezone.utc),
        "committed_at": None,
        "finished_at": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def make_primary_patches(computer, mock_computer_manager):
    """The owner's computer, the flip, the Home claim and the background runner."""
    from types import SimpleNamespace

    mock_computer_manager.run_accepted_move = AsyncMock()
    with (
        patch("src.server.app.computers.get_computer", AsyncMock(return_value=computer)),
        patch(
            "src.server.app.computers._computer_manager",
            return_value=mock_computer_manager,
        ),
        patch(
            "src.server.app.computers.set_primary_computer", AsyncMock(return_value=True)
        ) as flip,
        patch(
            "src.server.app.computers.claim_workspace_move",
            AsyncMock(return_value=_home_move()),
        ) as claim,
        patch("src.server.app.computers.schedule_start", MagicMock()) as schedule,
    ):
        yield SimpleNamespace(
            flip=flip, claim=claim, schedule=schedule, manager=mock_computer_manager
        )


@pytest.mark.asyncio
async def test_make_primary_flips_now_when_home_need_not_move(
    client, make_primary_patches
):
    resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/make-primary")

    assert resp.status_code == 200
    assert resp.json() == {"computer_id": COMPUTER_ID, "is_primary": True, "move": None}
    make_primary_patches.flip.assert_awaited_once_with(USER, COMPUTER_ID)
    make_primary_patches.claim.assert_not_awaited()
    make_primary_patches.schedule.assert_not_called()


@pytest.mark.asyncio
async def test_make_primary_moves_home_first_and_answers_202_with_the_move(
    client, make_primary_patches
):
    """Home lives on the primary, so a flip that would leave it behind becomes
    Home's move, which commits the flip; the client reads both from the move."""
    from src.server.database.home_workspace import get_home_workspace_id

    make_primary_patches.flip.return_value = False
    move = make_primary_patches.claim.return_value

    resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/make-primary")

    assert resp.status_code == 202
    body = resp.json()
    assert body["computer_id"] == COMPUTER_ID
    assert body["is_primary"] is False
    assert body["move"]["move_id"] == move["move_id"]
    assert body["move"]["workspace_id"] == get_home_workspace_id(USER)
    assert body["move"]["target_computer_id"] == COMPUTER_ID
    assert body["move"]["make_primary"] is True
    assert body["move"]["phase"] == "preparing"
    make_primary_patches.claim.assert_awaited_once_with(
        user_id=USER,
        workspace_id=get_home_workspace_id(USER),
        target_computer_id=COMPUTER_ID,
        make_primary=True,
        from_backup=False,
    )
    make_primary_patches.schedule.assert_called_once()
    key, start = make_primary_patches.schedule.call_args.args
    assert key == f"workspace-move:{move['move_id']}"
    await start()
    make_primary_patches.manager.run_accepted_move.assert_awaited_once_with(move)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("step", "code", "status"),
    [
        ("flip", "move_in_progress", 409),
        ("flip", "target_unavailable", 409),
        ("flip", "not_found", 404),
        ("claim", "not_movable", 409),
        ("claim", "turn_active", 409),
        ("claim", "move_in_progress", 409),
    ],
)
async def test_make_primary_refusals_answer_in_the_move_outcome_shape(
    client, make_primary_patches, step, code, status
):
    from src.server.database.workspace_move import MoveRefused

    make_primary_patches.flip.return_value = False
    getattr(make_primary_patches, step).side_effect = MoveRefused(code)

    resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/make-primary")

    assert resp.status_code == status
    detail = resp.json()["detail"]
    assert detail["code"] == code
    assert isinstance(detail["message"], str) and detail["message"]
    assert detail["files"] == []
    make_primary_patches.schedule.assert_not_called()


@pytest.mark.asyncio
async def test_make_primary_is_refused_on_another_users_computer(
    client, make_primary_patches, sample_computer_dict
):
    with patch(
        "src.server.app.computers.get_computer",
        AsyncMock(return_value=sample_computer_dict(computer_id=COMPUTER_ID, user_id=OTHER_USER)),
    ):
        resp = await client.post(f"/api/v1/computers/{COMPUTER_ID}/make-primary")

    assert resp.status_code == 403
    make_primary_patches.flip.assert_not_awaited()
    make_primary_patches.claim.assert_not_awaited()
