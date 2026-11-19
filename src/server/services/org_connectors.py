"""The MCP servers an organization sets up for its members.

The organization defines them and the auth service sends the definitions
with the membership (``services.account``), so they are read through
``account_for`` like everything else the organization decides: with the
``org_connectors`` flag off in the deployment nothing is read at all, and an
account that cannot be read gives None, which every caller refuses on.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re

from src.config.features import is_feature_enabled_system
from src.server.services.account_contract import OrgConnector
from src.server.services.account import account_for
from src.server.services.brokerages import brokerage_for_url
from src.server.services import features, platform_client

logger = logging.getLogger(__name__)

FEATURE = "org_connectors"


def org_connectors_enabled() -> bool:
    """Whether the deployment can serve organization connectors at all; the
    one check that keeps every reader free when it cannot."""
    return is_feature_enabled_system(FEATURE)


async def org_connectors_for(user_id: str) -> tuple[OrgConnector, ...] | None:
    """The organization connectors the user may use, or None when their
    account is unknown, which no caller may read as none.

    A connector at a known brokerage is left out: a live order is placed only
    through a connection the member made and consented to themselves.
    """
    if not org_connectors_enabled():
        return ()
    account = await account_for(user_id)
    if account is None:
        return None
    if not account.connectors:
        return ()
    if not await features.feature_enabled_for_account(user_id, FEATURE, account):
        return ()
    kept = []
    for connector in account.connectors:
        if brokerage_for_url(connector.url) is not None:
            logger.warning(
                "[org_connectors] connector id=%s for user_id=%s is a brokerage "
                "and is left out",
                connector.id, user_id,
            )
            continue
        kept.append(connector)
    return tuple(kept)


def org_digest(connectors: tuple[OrgConnector, ...]) -> str:
    """A fingerprint of what a workspace's tools depend on in the
    organization's list, "" when it lists none.

    Compared at every acquire, so a warm computer resolves again when the
    list changes even if no change notice reached this worker.
    """
    if not connectors:
        return ""
    payload = sorted(
        (c.id, c.name, c.url, c.auth, c.revision) for c in connectors
    )
    return hashlib.sha256(json.dumps(payload).encode()).hexdigest()


async def current_org_digest(user_id: str | None) -> str | None:
    """:func:`org_digest` of the user's connectors now, None when unknown."""
    if not user_id or not org_connectors_enabled():
        return ""
    connectors = await org_connectors_for(user_id)
    return None if connectors is None else org_digest(connectors)


def resolvable_connectors(
    connectors: tuple[OrgConnector, ...], *, reserved: set[str], user_id: str
) -> tuple[OrgConnector, ...]:
    """The connectors a workspace can actually get, given the built-in names
    the deployment reserves.

    The resolver and the member's listing both read through this, so a
    connector one of them leaves out is never shown by the other as usable.
    """
    kept = []
    for connector in connectors:
        if connector.name in reserved:
            logger.warning(
                "[org_connectors] connector %r (id=%s) for user_id=%s is left "
                "out: its name collides with a built-in",
                connector.name, connector.id, user_id,
            )
            continue
        kept.append(connector)
    return tuple(kept)


def connector_named(
    connectors: tuple[OrgConnector, ...], name: str
) -> OrgConnector | None:
    return next((c for c in connectors if c.name == name), None)


def connector_with_id(
    connectors: tuple[OrgConnector, ...], connector_id: str
) -> OrgConnector | None:
    return next((c for c in connectors if c.id == connector_id), None)


# The auth service holds a shared connector's credential and hands it out per
# call, so it is never stored here: not in memory, Redis, Postgres or a log.
CREDENTIAL_PATH = "/api/auth/connectors/credential"
CREDENTIAL_CONNECT_TIMEOUT_S = 2.0
CREDENTIAL_TIMEOUT_S = 3.0
CREDENTIAL_TOKEN_ENV = "ORG_CONNECTOR_CREDENTIAL_TOKEN"
_missing_token_logged = False

# An RFC 9110 field name; anything else could not be framed as a header.
_HEADER_NAME_RE = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")
# Printable ASCII only: httpx encodes header values as ASCII, and the error it
# raises on anything else quotes the character it could not encode.
_HEADER_VALUE_RE = re.compile(r"[\x20-\x7e]*")


class CredentialRefused(Exception):
    """The auth service will not give this member the connector's credential."""


class CredentialUnavailable(Exception):
    """No usable answer from the auth service: down, slow, or unreadable."""


# Visible ASCII, no whitespace: anything else cannot be sent as a header value.
_TOKEN_RE = re.compile(r"[\x21-\x7e]+")


def _credential_token() -> str | None:
    """The configured token, or None when it is unset or unusable, said once
    per process: every shared connector call would otherwise log the same
    cause. The value is never logged."""
    global _missing_token_logged
    token = os.getenv(CREDENTIAL_TOKEN_ENV, "").strip()
    if _TOKEN_RE.fullmatch(token):
        return token
    if not _missing_token_logged:
        _missing_token_logged = True
        logger.warning(
            "[org_connectors] %s %s: organization connectors whose credential "
            "is shared cannot be used",
            CREDENTIAL_TOKEN_ENV,
            "has an unusable value (it must be visible ASCII with no whitespace)"
            if token
            else "is not set",
        )
    return None


def _framable(name: object, value: object) -> bool:
    return (
        isinstance(name, str)
        and _HEADER_NAME_RE.fullmatch(name) is not None
        and isinstance(value, str)
        and _HEADER_VALUE_RE.fullmatch(value) is not None
    )


async def shared_credential(
    user_id: str, connector: OrgConnector, *, method: str | None, tool: str | None
) -> dict[str, str]:
    """The headers a shared connector's call authenticates with, asked of the
    auth service for this one call.

    Never retried and never cached, so a revoked credential or a removed
    member is refused at the next call. Raises :class:`CredentialRefused` on
    403 or 404 and :class:`CredentialUnavailable` on anything else that is not
    a well-formed answer; neither carries, and nothing here logs, a value from
    the answer.

    It authenticates with a token of its own, never the service token: the
    token that can obtain an organization's vendor credential is held by this
    one caller only, so it can be rotated and scoped apart from the token
    services use to act for a user. Without it nothing is asked.
    """
    import asyncio

    import httpx

    from src.config.settings import AUTH_SERVICE_URL
    from src.server.utils.egress_guard import RESERVED_HEADERS
    from src.server.utils.http_client import get_http_client

    if not platform_client.platform_gating_active():
        raise CredentialUnavailable("no auth service is configured")
    token = _credential_token()
    if token is None:
        raise CredentialUnavailable(f"{CREDENTIAL_TOKEN_ENV} is not set or unusable")
    body = {
        "connector_id": connector.id,
        "revision": connector.revision,
        "url": connector.url,
        "method": method,
        "tool": tool,
    }
    client = await get_http_client()
    try:
        resp = await asyncio.wait_for(
            client.post(
                f"{AUTH_SERVICE_URL.rstrip('/')}{CREDENTIAL_PATH}",
                json=body,
                headers={"X-User-Id": user_id, "X-Connector-Credential-Token": token},
                timeout=httpx.Timeout(
                    CREDENTIAL_TIMEOUT_S, connect=CREDENTIAL_CONNECT_TIMEOUT_S
                ),
            ),
            CREDENTIAL_TIMEOUT_S,
        )
    except Exception as e:
        # Anything, so the relay always gets its unreachable answer; named by
        # type only, since an httpx error can quote the request it failed on.
        logger.warning(
            "[org_connectors] credential request for connector id=%s user_id=%s "
            "failed: %s",
            connector.id, user_id, type(e).__name__,
        )
        raise CredentialUnavailable(type(e).__name__) from None
    if resp.status_code in (403, 404):
        logger.info(
            "[org_connectors] credential for connector id=%s refused for user_id=%s "
            "(%d)",
            connector.id, user_id, resp.status_code,
        )
        raise CredentialRefused(str(resp.status_code))
    if resp.status_code != 200:
        logger.warning(
            "[org_connectors] credential request for connector id=%s user_id=%s "
            "answered %d",
            connector.id, user_id, resp.status_code,
        )
        raise CredentialUnavailable(f"HTTP {resp.status_code}")
    try:
        answer = resp.json()
    except ValueError:
        answer = None
    headers = answer.get("headers") if isinstance(answer, dict) else None
    if not isinstance(headers, dict) or not all(
        _framable(k, v) for k, v in headers.items()
    ):
        logger.warning(
            "[org_connectors] unreadable credential answer for connector id=%s "
            "user_id=%s",
            connector.id, user_id,
        )
        raise CredentialUnavailable("unreadable answer")
    # Authentication and nothing else: a reserved name would route the vendor
    # on a header the policy never saw or break the framing the relay builds.
    return {
        k.lower(): v for k, v in headers.items() if k.lower() not in RESERVED_HEADERS
    }
