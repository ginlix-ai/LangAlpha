"""A member's sign-in to an organization connector its members sign in to:
the start, the callback's checks around the exchange, and the discovery
after it. The connection is the member's own, stored under ``org_key(id)``,
so it never meets a server of theirs of the same name."""

from __future__ import annotations

from src.server.database.org_keys import org_key
from src.server.services import org_connectors
from src.server.services.account_contract import OrgConnector
from src.server.services.mcp_config import same_consented_url
from src.server.services.mcp_oauth.connect import (
    ConnectState,
    McpServerNotFound,
    OrgAccountUnknown,
    ReconnectFinish,
    StartedConnect,
    start_sign_in,
)
from src.server.services.mcp_oauth.finish import CallbackFailed
from src.server.services.mcp_oauth.org_discovery import discover_org_connector
from src.server.services.mcp_oauth.redirects import CallbackError


async def _member_connector(user_id: str, connector_id: str) -> OrgConnector | None:
    """The organization connector a member signs in to, as the account lists
    it now, or None; an unknown account raises."""
    connectors = await org_connectors.org_connectors_for(user_id)
    if connectors is None:
        raise OrgAccountUnknown("this account's organization could not be checked")
    connector = org_connectors.connector_with_id(connectors, connector_id)
    if connector is None or connector.auth != "member":
        return None
    return connector


async def start_org_connect(
    user_id: str,
    connector_id: str,
    *,
    return_to: str | None = None,
    web_origin: str | None = None,
    loopback_redirect: str | None = None,
) -> StartedConnect:
    """Phase 1, given for the url the organization lists now. The
    ``user_connectors`` rule does not apply: the connector is the
    organization's."""
    connector = await _member_connector(user_id, connector_id)
    if connector is None:
        raise McpServerNotFound("Connector not found")
    return await start_sign_in(
        user_id,
        org_key(connector.id),
        connector.url,
        return_to=return_to,
        web_origin=web_origin,
        loopback_redirect=loopback_redirect,
        granted_capabilities=None,
    )


class OrgConnectorFinish(ReconnectFinish):
    """The callback of a member's sign-in.

    The account is read again on both sides of the exchange, so a connector
    withdrawn, switched to a shared credential or repointed while the member
    was at the consent screen stores nothing; the freshly exchanged token is
    dropped and simply expires.
    """

    def __init__(self, record: ConnectState, connector_id: str) -> None:
        super().__init__(record)
        self.connector_id = connector_id

    async def _still_offered(self) -> None:
        try:
            connector = await _member_connector(self.record.user_id, self.connector_id)
        except OrgAccountUnknown:
            raise CallbackFailed(
                CallbackError.INTERNAL,
                "your organization could not be checked; try again",
            ) from None
        if connector is None or not same_consented_url(
            connector.url, self.record.server_url
        ):
            raise CallbackFailed(CallbackError.SERVER_CHANGED)

    async def before_exchange(self) -> None:
        await self._still_offered()

    async def after_exchange(self, bearer: str) -> None:
        await self._still_offered()

    async def after_connect(self) -> None:
        return None


async def discover_signed_in(user_id: str, connector_id: str) -> None:
    """Discover the tools of a connector the member just signed in to."""
    connector = await _member_connector(user_id, connector_id)
    if connector is not None:
        await discover_org_connector(user_id, connector)
