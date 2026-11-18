"""The account context: the door, strict parsing, and unknown vs no org."""

from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.config.models import PolicyLayer
from src.server.services.account_contract import NO_ORG, Group, Org, OrgConnector
from src.server.services.account import account_for

LIMITS = "src.server.dependencies.usage_limits"
ACCOUNT = "src.server.services.account"

ACME = {
    "access_tier": 2,
    "plan_display_name": "Pro",
    "org": {"id": "org-1", "name": "Acme"},
    "groups": [{"id": "g-1", "level": "manage"}, {"id": "g-2", "level": "read"}],
    "features": {"teams": True, "market_watch": False, "not_in_catalog": True},
    "policy": {"trading": False},
    "connectors": [
        {
            "id": "c-1",
            "name": "crm",
            "url": "https://mcp.example.com/crm",
            "auth": "shared",
            "revision": "r1",
            "description": "The sales CRM",
        },
    ],
}
CONNECTOR = ACME["connectors"][0]


def _platform(*, teams=True, gating=True, cache=None, client=None):
    stack = ExitStack()
    stack.enter_context(patch(f"{ACCOUNT}.is_feature_enabled_system", lambda key: teams))
    stack.enter_context(patch("src.server.services.membership.is_feature_enabled_system", lambda key: teams))
    stack.enter_context(patch(f"{LIMITS}.HOST_MODE", "platform" if gating else "oss"))
    stack.enter_context(patch("src.server.services.platform_client.HOST_MODE", "platform" if gating else "oss"))
    stack.enter_context(patch(f"{LIMITS}.AUTH_SERVICE_URL", "http://localhost:8003"))
    stack.enter_context(patch("src.server.services.platform_client.AUTH_SERVICE_URL", "http://localhost:8003"))
    stack.enter_context(patch("src.server.services.platform_client.get_http_client", return_value=client or AsyncMock()))
    stack.enter_context(
        patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache or AsyncMock())
    )
    stack.enter_context(patch("os.getenv", return_value="token"))
    return stack


def _cache(cached=None, *, last_known=None, paused=None):
    cache = AsyncMock()
    cache.mget = AsyncMock(return_value=[cached, last_known, paused, None, None])
    return cache


@pytest.mark.asyncio
@pytest.mark.parametrize("teams,gating", [(False, True), (True, False)])
async def test_a_shut_door_reads_nothing(teams, gating):
    """With the deployment flag off the product makes no extra calls at all."""
    cache, client = MagicMock(), AsyncMock()
    with _platform(teams=teams, gating=gating, cache=cache, client=client):
        assert await account_for("user-1") is NO_ORG
    assert not cache.method_calls
    client.post.assert_not_called()


@pytest.mark.asyncio
async def test_an_org_that_allows_teams_opens_the_door():
    with _platform(cache=_cache(ACME)):
        account = await account_for("user-1")
    assert account.org == Org(id="org-1", name="Acme")
    assert account.groups == (Group("g-1", "manage"), Group("g-2", "read"))
    assert account.features["market_watch"] is False
    assert account.features["not_in_catalog"] is True
    assert account.policy == PolicyLayer(trading=False)
    assert account.connectors == (
        OrgConnector(
            id="c-1", name="crm", url="https://mcp.example.com/crm",
            auth="shared", revision="r1", description="The sales CRM",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry",
    [
        {"access_tier": 2, "plan_display_name": "Pro"},  # also an older worker's entry
        {**ACME, "features": {"market_watch": False}},
        {**ACME, "features": {"teams": False}},
        # Only ``org`` names the organization; ``organization`` is not read.
        {
            **{k: v for k, v in ACME.items() if k != "org"},
            "organization": {"id": "org-1", "name": "Acme"},
        },
    ],
)
async def test_no_org_or_an_org_without_teams_is_no_org(entry):
    with _platform(cache=_cache(entry)):
        assert await account_for("user-1") is NO_ORG


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        {"org": {"id": 7, "name": "Acme"}},
        {"org": "org-1"},
        {"groups": [{"id": "g-1", "level": "owner"}]},
        {"groups": {"id": "g-1", "level": "read"}},
        {"features": {"teams": "yes"}},
        {"policy": ["x"]},
        {"policy": {"a_rule_from_a_later_version": True}},
        {"policy": {"trading": "no"}},
        {"connectors": {"c-1": CONNECTOR}},
        {"connectors": [{**CONNECTOR, "a_field_from_a_later_version": 1}]},
        {"connectors": [{**CONNECTOR, "url": "http://mcp.example.com/crm"}]},
        {"connectors": [{**CONNECTOR, "url": "https://"}]},
        {"connectors": [{**CONNECTOR, "auth": "anyone"}]},
        {"connectors": [{**CONNECTOR, "id": "c:1"}]},
        {"connectors": [{**CONNECTOR, "name": "not-a-module"}]},
        {"connectors": [{**CONNECTOR, "name": "class"}]},
        {"connectors": [{**CONNECTOR, "revision": ""}]},
        {"connectors": [{k: v for k, v in CONNECTOR.items() if k != "revision"}]},
        {"connectors": [{**CONNECTOR, "description": "x" * 513}]},
        {"connectors": [CONNECTOR, {**CONNECTOR, "name": "other"}]},
        {"connectors": [CONNECTOR, {**CONNECTOR, "id": "c-2"}]},
        {"connectors": [{**CONNECTOR, "id": f"c-{i}", "name": f"s{i}"} for i in range(51)]},
    ],
)
async def test_a_malformed_block_is_unknown_not_no_org(bad, caplog):
    with _platform(cache=_cache({**ACME, **bad})):
        assert await account_for("user-1") is None
    assert "unreadable account" in caplog.text


@pytest.mark.asyncio
async def test_an_unreachable_service_with_nothing_on_record_is_unknown():
    client = AsyncMock()
    client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    with _platform(cache=_cache(None), client=client):
        assert await account_for("user-1") is None


@pytest.mark.asyncio
async def test_the_last_known_org_stands_in_during_an_outage():
    client = AsyncMock()
    client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
    with _platform(cache=_cache(None, last_known=ACME), client=client):
        account = await account_for("user-1")
    assert account.org == Org(id="org-1", name="Acme")


@pytest.mark.asyncio
async def test_the_org_blocks_ride_the_one_membership_entry():
    client = AsyncMock()
    client.post = AsyncMock(
        return_value=httpx.Response(
            200,
            json={
                "valid": True, **ACME, "scopes": [],
                "organization": {"id": "x", "name": "Other"},
            },
        )
    )
    cache = _cache(None)
    with _platform(cache=cache, client=client):
        account = await account_for("user-1")
    assert account.org.name == "Acme"
    client.post.assert_awaited_once()
    (_, fresh, _), (_, known, _) = cache.set_many_fenced.await_args.args[0]
    assert fresh == known == ACME



def test_an_org_connector_is_stored_apart_from_any_own_server_name():
    from src.server.database.org_keys import org_connector_id, org_key
    from src.server.models.mcp_names import NAME_RE

    assert org_key("c-1") == "org:c-1"
    assert not NAME_RE.match(org_key("c-1"))
    assert org_connector_id(org_key("c-1")) == "c-1"
    assert org_connector_id("wiki") is None


def test_a_record_from_before_a_notice_drops_its_connectors():
    """The connectors ride the fence the other organization blocks ride."""
    from src.server.services.membership import _standing_in

    standing = _standing_in({**ACME, "membership_epoch": 1}, 2)
    assert "connectors" not in standing
    assert standing["account_unknown"] is True


def test_an_answer_with_no_teams_block_is_in_no_team():
    """An auth service on contract 3 sends no ``teams``: in none, never unknown."""
    from src.server.services.account_contract import parse_account

    account = parse_account({"org": {"id": "o", "name": "Acme"}})
    assert account.teams == () and account.team_ids == frozenset()


def test_the_teams_block_names_each_team_and_role():
    from src.server.services.account_contract import Team, parse_account

    account = parse_account({
        "org": {"id": "o", "name": "Acme"},
        "teams": [{"id": "t1", "name": "Semis", "role": "admin"}, {"id": "t2", "name": "Macro", "role": "member"}],
    })
    assert account.teams == (Team("t1", "Semis", "admin"), Team("t2", "Macro", "member"))
    assert account.team_ids == frozenset({"t1", "t2"})


@pytest.mark.parametrize(
    "teams",
    [
        {"id": "t1"},
        [{"id": "t1", "name": "Semis", "role": "owner"}],
        [{"id": "", "name": "Semis", "role": "member"}],
        [{"id": "t1", "name": "Semis", "role": "member", "extra": 1}],
        [{"id": "t1", "name": "Semis", "role": "member"}, {"id": "t1", "name": "Again", "role": "admin"}],
    ],
    ids=["not-a-list", "unknown-role", "empty-id", "unknown-key", "duplicate-id"],
)
def test_a_teams_block_this_build_cannot_read_makes_the_account_unknown(teams):
    """A team decides who reaches a shared workspace, so no guess either way."""
    from src.server.services.account_contract import MalformedAccount, parse_account

    with pytest.raises(MalformedAccount):
        parse_account({"org": {"id": "o", "name": "Acme"}, "teams": teams})
