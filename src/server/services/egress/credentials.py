"""The vendor credential one grant puts on the wire, resolved per request.

A grant stores a reference, never a value, so this is where the kinds part
company: ``oauth_mcp`` mints a bearer from the connection's token store,
``header_mcp`` expands the catalog row's own headers against the user's vault,
the tier the row's probe verdict was earned against, and ``platform_mcp``
sends the key the host holds for the user, which no sandbox file carries.
All are read fresh on every call, which is what makes a rotated key and a
revoked connection take effect on the next request rather than at the next
sync. A header value also rides the sandbox's vault file for every workspace
the user runs, so for that kind resolving per request buys rotation and
revocation, not confinement.

Resolution is also the second half of revocation, for every kind. The relay
authorizes against the grant row, and a catalog row that has been deleted,
switched off, taken out of delivery by its plugin, or (for headers) repointed,
like a built-in the operator dropped or the user switched off, has no
credential to resolve -- so the call is refused here, before anything is
dialled, however long the grant row itself takes to be retired.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.server.database.egress_grants import GRANT_KIND_HEADER_MCP
from src.server.database.mcp_oauth import (
    ConnectionStatus,
    get_connection,
    get_connection_by_id,
)
from src.server.database.mcp_servers import get_catalog_server
from src.server.database.user_vault_secrets import get_user_secrets_decrypted
from src.server.services.egress import AccountUnavailable, RelayError, RelayRejection
from src.server.services.egress.platform_accounts import (
    PLATFORM_ACCOUNTS,
    PlatformServer,
)
from src.server.services.mcp_oauth.lifecycle import (
    AccessToken,
    TokenUnavailable,
    ensure_fresh_access_token,
)
from src.server.services.vendor_terms import TERMS_REQUIRED
from src.server.utils.egress_guard import RESERVED_HEADERS

if TYPE_CHECKING:
    from ptc_agent.config.core import MCPServerConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VendorCredential:
    """The headers this call authenticates with, and the bundle they came from.

    ``token`` is set only where there is a rotating one behind the headers: it
    is what the vendor's 401 is re-examined against, and its absence is what
    says a 401 is the vendor's final answer rather than a stale-token race.
    """

    headers: Mapping[str, str] = field(default_factory=dict)
    token: AccessToken | None = None


async def resolve_vendor_credential(
    grant: Mapping[str, Any], platform: PlatformServer | None = None
) -> VendorCredential:
    """Ready the credential for one authorized grant, or refuse the request.

    ``platform`` is the built-in a platform grant names, already resolved by
    ``platform_server``.
    """
    if platform is not None:
        return await _platform_credential(grant["user_id"], platform)
    if grant.get("kind") == GRANT_KIND_HEADER_MCP:
        return await _header_credential(grant)
    return await _oauth_credential(grant)


def _unconfigured(server_name: str, why: str) -> RelayRejection:
    """One refusal for every way a row stops being able to authenticate.

    The reason is logged rather than returned: which of them it was is a fact
    about the user's own catalog, and the caller's repair is the same in every
    case.
    """
    logger.warning("[egress_relay] no credential for server %r: %s", server_name, why)
    return RelayRejection(
        401, RelayError.NEEDS_REAUTH, "this server's credentials are not configured"
    )


def _require_deliverable(name: str, row: Mapping[str, Any] | None) -> Mapping[str, Any]:
    """The row gate both kinds share: a server the user has taken out of
    delivery has no credential to resolve, however it authenticates.

    A plugin's disable leaves its rows' own flag alone, and the plugin's is what
    takes them out of delivery, so it has to take them off the relay too.
    """
    if row is None or not row.get("enabled"):
        raise _unconfigured(name, "the row is gone or switched off")
    if row.get("plugin_enabled") is False:
        raise _unconfigured(name, "its plugin is switched off")
    return row


async def _oauth_credential(grant: Mapping[str, Any]) -> VendorCredential:
    connection_id = grant["connection_id"]
    # A connection is bound to the catalog row of its own name, so the row
    # governs this kind too: a bearer mints just as happily for a server the
    # user has switched off. Read before the mint, so a refusal never spends a
    # refresh round trip at the vendor; a connection gone entirely is left to
    # the lifecycle below, which carries the reason for it.
    connection = await get_connection_by_id(connection_id)
    if connection is not None:
        _require_deliverable(
            connection.server_name,
            await get_catalog_server(connection.user_id, connection.server_name),
        )
    try:
        token = await ensure_fresh_access_token(connection_id)
    except TokenUnavailable as e:
        if e.reason == "refresh_in_progress":
            raise RelayRejection(503, RelayError.REFRESH_IN_PROGRESS)
        raise RelayRejection(401, RelayError.NEEDS_REAUTH, e.reason)
    return VendorCredential(headers={"authorization": token.header()}, token=token)


async def _header_credential(grant: Mapping[str, Any]) -> VendorCredential:
    from src.server.services.mcp_config import same_consented_url
    from src.server.services.mcp_probe import (
        RejectedHeaderValue,
        resolve_header_refs,
        vault_ref_names,
    )

    name = grant.get("server_name") or ""
    user_id = grant["user_id"]
    row = _require_deliverable(name, await get_catalog_server(user_id, name))
    # ``http`` exactly, the same predicate that earns the grant: the relay
    # dials streamable HTTP, so a row edited onto legacy ``sse`` is one the
    # next sync will retire and one no call should be resolved for meanwhile.
    if row.get("transport") != "http":
        raise _unconfigured(name, "the row is no longer streamable HTTP")
    # A connection that has not been revoked still claims the row, whatever
    # its state: the user was told these headers are not sent while the server
    # is OAuth-connected, and an expired token must not quietly change that.
    connection = await get_connection(user_id, name)
    if connection is not None and connection.status is not ConnectionStatus.REVOKED:
        raise _unconfigured(name, "an OAuth connection claims this row")
    # The address the grant was issued for is the address these headers belong
    # to. An edit that repoints the row wins the next sync, and until then this
    # is what keeps the key for one host from being sent to another.
    if not same_consented_url(row.get("url"), grant.get("destination_url")):
        raise _unconfigured(name, "the row now points somewhere else")
    refs = vault_ref_names(row.get("headers"))
    # The vault, which is what host-side discovery resolved to earn this row's
    # verdict.
    try:
        headers, missing = resolve_header_refs(
            row.get("headers"),
            await get_user_secrets_decrypted(user_id, refs) if refs else {},
        )
    except RejectedHeaderValue as e:
        raise _unconfigured(
            name, f"header {e.header!r} resolves to a value HTTP cannot frame"
        ) from e
    if missing:
        # Sending the literal ``${vault:NAME}`` would come back as a rejected
        # key, which reads as a wrong credential rather than an absent one.
        raise _unconfigured(name, "missing vault secret(s): " + ", ".join(missing))
    # Lowercased for the reason ``_vendor_headers`` normalizes: a row spelling
    # a header the allowlist also passes would otherwise put it on the wire
    # twice. The row supplies authentication and nothing else: a name in
    # ``RESERVED_HEADERS`` would route the vendor on one the policy never saw,
    # overwrite what the MCP client negotiated, or corrupt the framing the
    # relay builds. That set is shared with the host probe and the sandbox
    # runtime so one row reads the same on all three.
    return VendorCredential(
        headers={
            k.lower(): v for k, v in headers.items() if k.lower() not in RESERVED_HEADERS
        }
    )


def _builtin_server(name: str) -> MCPServerConfig | None:
    """The operator's config for built-in ``name``, if it still declares one."""
    from src.server.app import setup

    config = setup.agent_config
    if config is None:
        return None
    return next((s for s in config.mcp.servers if s.name == name and s.enabled), None)


async def platform_server(grant: Mapping[str, Any]) -> PlatformServer:
    """The built-in a platform grant names, as the operator's config declares
    it now, or the refusal.

    Read per request rather than trusted from the grant row, for the reason
    the other kinds re-read their row: a deploy that drops the server, its
    credential or its address, and a user who switches it off for the
    account, have to stop the relay spending the key before the next sync
    retires the grant.
    """
    from src.server.services.mcp_config import account_disabled_builtins

    name = grant.get("server_name") or ""
    server = _builtin_server(name)
    account = PLATFORM_ACCOUNTS.get(server.auth or "") if server is not None else None
    if server is None or account is None:
        raise _unconfigured(name, "the built-in is gone or holds no per-user credential")
    if server.url != grant.get("destination_url"):
        raise _unconfigured(name, "the built-in now points somewhere else")
    if name in await account_disabled_builtins(grant["user_id"]):
        raise _unconfigured(name, "the user has switched it off")
    return PlatformServer(config=server, account=account)


async def _platform_credential(user_id: str, platform: PlatformServer) -> VendorCredential:
    try:
        key = await platform.account.key_for(user_id)
    except AccountUnavailable as e:
        logger.warning(
            "[egress_relay] no %s key for user %s: %s", platform.config.name, user_id, e.code
        )
        if e.code == TERMS_REQUIRED:
            raise RelayRejection(
                403,
                RelayError.TERMS_REQUIRED,
                f"the user has not accepted {platform.account.name}'s Terms of Service",
            ) from e
        raise RelayRejection(
            503 if e.retryable else 403,
            RelayError.CREDENTIAL_UNAVAILABLE,
            "this service's account could not be set up for this user",
            retry_after=30 if e.retryable else None,
        ) from e
    return VendorCredential(headers={"authorization": f"Bearer {key}"})
