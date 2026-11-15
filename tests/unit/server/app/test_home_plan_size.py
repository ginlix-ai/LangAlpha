"""Home's size beside the plan's (``GET /api/v1/computers/home/plan-size``):
whose computer it reads, the size the plan gives it, whether Home still takes
it at its first build, and whether its owner may take it now."""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

USER = "test-user-123"
COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
_ROUTES = "src.server.app.computers"
_SUCCEEDED = {
    "target_tier": "standard",
    "from_tier": "performance",
    "state": "succeeded",
    "started_at": "2026-01-01T00:00:00Z",
}


@pytest_asyncio.fixture
async def client():
    from src.server.app.computers import router

    app = create_test_app(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.fixture
def home(sample_computer_dict):
    def _make(**overrides):
        return sample_computer_dict(
            **{"computer_id": COMPUTER_ID, "kind": "daytona", "resource_tier": "standard", **overrides}
        )

    return _make


@contextlib.contextmanager
def _routes(manager, *, home=None, user=None, gating=True):
    with (
        patch(f"{_ROUTES}.platform_gating_active", return_value=gating),
        patch(f"{_ROUTES}._computer_manager", return_value=manager),
        patch(f"{_ROUTES}.get_home_computer", AsyncMock(return_value=home)),
        patch(f"{_ROUTES}.get_user", AsyncMock(return_value=user or {})),
    ):
        yield


def _manager(mock_computer_manager, plan_tier, *, provider="daytona"):
    mock_computer_manager.plan_tier = AsyncMock(return_value=plan_tier)
    mock_computer_manager.largest_tier = MagicMock(return_value="max")
    mock_computer_manager.config.sandbox.provider = provider
    return mock_computer_manager


async def _read(client, manager, **routes):
    with _routes(manager, **routes):
        resp = await client.get("/api/v1/computers/home/plan-size")
    assert resp.status_code == 200
    return resp.json()


@pytest.mark.asyncio
async def test_a_user_with_no_computer_reads_what_a_new_home_would_take(client, mock_computer_manager):
    manager = _manager(mock_computer_manager, "performance")
    assert await _read(client, manager) == {
        "computer_id": None,
        "resource_tier": None,
        "plan_tier": "performance",
        "largest_tier": "max",
        "follows_plan": True,
        "can_resize": False,
    }
    manager.plan_tier.assert_awaited_once_with(USER, None)
    manager.largest_tier.assert_called_once_with(None)


@pytest.mark.asyncio
async def test_a_user_working_on_their_own_machine_gets_no_cloud_home(client, mock_computer_manager):
    manager = _manager(mock_computer_manager, "performance")
    body = await _read(client, manager, user={"computer_choice": "own_machine"})
    assert (body["follows_plan"], body["can_resize"]) == (False, False)
    manager.plan_tier.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_home_not_built_yet_takes_the_plans_size_at_its_first_build(
    client, mock_computer_manager, home
):
    manager = _manager(mock_computer_manager, "performance")
    row = home(provider_ref=None)
    body = await _read(client, manager, home=row)
    assert body == {
        "computer_id": COMPUTER_ID,
        "resource_tier": "standard",
        "plan_tier": "performance",
        "largest_tier": "max",
        "follows_plan": True,
        "can_resize": False,
    }
    manager.plan_tier.assert_awaited_once_with(USER, row)


@pytest.mark.asyncio
async def test_a_built_home_no_longer_follows_the_plan_but_may_be_resized(
    client, mock_computer_manager, home
):
    """Built, Home keeps its size; a bigger one the plan grants is its owner's
    to take, through the resize the app opens on the primary."""
    manager = _manager(mock_computer_manager, "performance")
    body = await _read(client, manager, home=home())
    assert (body["plan_tier"], body["follows_plan"], body["can_resize"]) == (
        "performance",
        False,
        True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["owner-sized-mark", "owner-changed-it", "not-primary", "plan-no-bigger", "unread"]
)
async def test_only_a_built_cloud_primary_its_owner_did_not_size_may_be_resized(
    client, mock_computer_manager, home, case
):
    planned = {"plan-no-bigger": "standard", "unread": None}.get(case, "performance")
    manager = _manager(mock_computer_manager, planned)
    overrides = {
        "owner-sized-mark": {"config": {"sized_by_owner": True}},
        "owner-changed-it": {"spec_change": _SUCCEEDED},
        "not-primary": {"is_primary": False},
    }.get(case, {})
    body = await _read(client, manager, home=home(**overrides))
    assert body["can_resize"] is False
    assert body["follows_plan"] is False
    assert body["plan_tier"] == (planned or "standard")


@pytest.mark.asyncio
async def test_an_owner_sized_home_not_built_yet_does_not_follow_the_plan(
    client, mock_computer_manager, home
):
    manager = _manager(mock_computer_manager, "performance")
    body = await _read(
        client, manager, home=home(provider_ref=None, config={"sized_by_owner": True})
    )
    assert (body["follows_plan"], body["can_resize"]) == (False, False)


@pytest.mark.asyncio
async def test_without_a_platform_no_plan_is_read(client, mock_computer_manager, home):
    manager = _manager(mock_computer_manager, "performance")
    body = await _read(client, manager, home=home(provider_ref=None), gating=False)
    assert (body["plan_tier"], body["follows_plan"], body["can_resize"]) == (
        "standard",
        False,
        False,
    )
    manager.plan_tier.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_home_on_its_owners_hardware_is_never_resized_here(
    client, mock_computer_manager, home
):
    """Home on the user's own machine, a cloud stand-in or not: the resize
    the app opens is for a cloud primary."""
    manager = _manager(mock_computer_manager, "performance")
    body = await _read(client, manager, home=home(kind="ssh"))
    assert (body["follows_plan"], body["can_resize"]) == (False, False)
    manager.plan_tier.assert_not_awaited()
    manager.largest_tier.assert_called_once_with(None)
