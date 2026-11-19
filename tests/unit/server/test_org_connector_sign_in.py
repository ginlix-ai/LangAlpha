"""A member's own sign-in to an organization connector: started only for one
the organization lists for members to sign in to, stored apart from any
server of the member's own, and refused at the callback once the connector is
withdrawn, switched or repointed."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from src.server.database.org_keys import org_key
from src.server.services.account_contract import OrgConnector
from src.server.services.mcp_oauth.redirects import CallbackError

ORG_CONNECT = "src.server.services.mcp_oauth.org_connect"
OC = "src.server.services.org_connectors"

WIKI = OrgConnector(
    id="c-wiki", name="wiki", url="https://wiki.example.test/mcp", auth="member",
    revision="r1",
)
CRM = OrgConnector(
    id="c-crm", name="crm", url="https://crm.example.test/mcp", auth="shared",
    revision="r1",
)


def _listed(value):
    return patch(f"{OC}.org_connectors_for", new=AsyncMock(return_value=value))


@pytest.mark.asyncio
async def test_a_sign_in_starts_under_the_connectors_own_key_and_url():
    from src.config.models import PolicyLayer
    from src.server.services.mcp_oauth.org_connect import start_org_connect
    from src.server.services.policy import merge

    no_own = merge(None, PolicyLayer.model_validate({"user_connectors": False}))
    start = AsyncMock(return_value="started")
    with (
        _listed((WIKI,)),
        patch(f"{ORG_CONNECT}.start_sign_in", start),
        # The organization's connector, not one of the member's own.
        patch("src.server.services.account.policy_for", AsyncMock(return_value=no_own)),
    ):
        assert await start_org_connect("u", "c-wiki") == "started"
    assert start.await_args.args == ("u", org_key("c-wiki"), WIKI.url)
    assert start.await_args.kwargs["granted_capabilities"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("listed", [(), (CRM,)], ids=["not-listed", "shared"])
async def test_a_sign_in_is_refused_for_one_members_do_not_sign_in_to(listed):
    from src.server.services.mcp_oauth.connect import McpServerNotFound
    from src.server.services.mcp_oauth.org_connect import start_org_connect

    start = AsyncMock()
    with _listed(listed), patch(f"{ORG_CONNECT}.start_sign_in", start), pytest.raises(McpServerNotFound):
        await start_org_connect("u", "c-crm" if listed else "c-wiki")
    start.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_start_route_answers_503_while_the_account_is_unknown():
    from src.server.app import mcp_oauth as route

    request = SimpleNamespace(headers={})
    with _listed(None), pytest.raises(HTTPException) as unknown:
        await route.org_connector_oauth_start("c-wiki", "u", request, SimpleNamespace(), None)
    assert unknown.value.status_code == 503


@pytest.mark.asyncio
async def test_signing_out_revokes_the_members_own_connection_only():
    from src.server.app import mcp_oauth as route

    disconnect = AsyncMock(return_value=True)
    with patch.object(route, "disconnect_server", disconnect):
        assert await route.org_connector_oauth_disconnect("c-wiki", "u") == {"ok": True}
    disconnect.assert_awaited_once_with("u", "org:c-wiki")


def _record(server_url=WIKI.url):
    return SimpleNamespace(user_id="u", server_name=org_key(WIKI.id), server_url=server_url)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("listed", "reason"),
    [
        ((), CallbackError.SERVER_CHANGED),
        ((OrgConnector(**{**WIKI.__dict__, "auth": "shared"}),), CallbackError.SERVER_CHANGED),
        ((OrgConnector(**{**WIKI.__dict__, "url": "https://moved.example.test/mcp"}),),
         CallbackError.SERVER_CHANGED),
        (None, CallbackError.INTERNAL),
    ],
    ids=["removed", "now-shared", "repointed", "unknown"],
)
async def test_the_finish_refuses_once_the_connector_is_no_longer_offered(listed, reason):
    from src.server.services.mcp_oauth.org_connect import OrgConnectorFinish
    from src.server.services.mcp_oauth.finish import CallbackFailed

    finish = OrgConnectorFinish(_record(), WIKI.id)
    for step in (finish.before_exchange, lambda: finish.after_exchange("bearer")):
        with _listed(listed), pytest.raises(CallbackFailed) as refused:
            await step()
        assert refused.value.reason is reason


@pytest.mark.asyncio
async def test_the_finish_lets_a_listed_connector_through_and_switches_nothing_on():
    from src.server.services.mcp_oauth.org_connect import OrgConnectorFinish

    switch_on = AsyncMock()
    finish = OrgConnectorFinish(_record(), WIKI.id)
    with _listed((WIKI,)), patch("src.server.services.mcp_saves.switch_on", switch_on):
        await finish.before_exchange()
        await finish.after_exchange("bearer")
        await finish.after_connect()
    switch_on.assert_not_awaited()


def test_an_org_sign_in_never_meets_a_server_of_the_members_own():
    from src.server.models.mcp_names import NAME_RE
    from src.server.database.org_keys import org_connector_id

    assert org_connector_id(org_key("c-wiki")) == "c-wiki"
    # A server of the member's own named like the connector is a reconnect.
    assert org_connector_id("wiki") is None
    # And no name of the member's own could take the connector's key.
    assert not NAME_RE.match(org_key("c-wiki"))


@pytest.mark.asyncio
async def test_the_callback_picks_the_organization_finish():
    from src.server.services.mcp_oauth import connect, org_connect

    record = SimpleNamespace(
        user_id="u", server_name=org_key("c-wiki"), server_url=WIKI.url,
        return_to=None, web_origin=None, pending=False, browser_nonce="",
        granted_capabilities=None,
    )
    seen = []

    class _Spy(org_connect.OrgConnectorFinish):
        async def before_exchange(self):
            seen.append(type(self))
            raise connect.CallbackFailed(CallbackError.SERVER_CHANGED)

    with (
        patch.object(connect, "_claim_state", AsyncMock(return_value=record)),
        patch.object(org_connect, "OrgConnectorFinish", _Spy),
        patch.object(connect, "_is_superseded", AsyncMock(return_value=False)),
        patch.object(connect, "validate_authorization_response_iss", lambda *a: None),
        patch.object(connect.OAuthMetadata, "model_validate", lambda *_: None),
    ):
        record.as_metadata = {}
        target = await connect.complete_callback(state="s", code="c")
    assert seen == [_Spy]
    assert "server_changed" in target
