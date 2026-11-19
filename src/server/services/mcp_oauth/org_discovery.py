"""Host-side tool discovery for the organization's connectors.

An organization connector is never probed from a sandbox, which holds no
credential for it: its tools are listed from this process, with the shared
credential the auth service hands out for the probe or the member's own
bearer, and cached per member in ``user_mcp_tool_schemas`` under
``org_key(id)``. Nothing a probe sent is stored: the error text is scrubbed of
every value on the wire, and so is the tool list a server answers with.

A connector has no catalog row to carry a kick clock, so probes are throttled
by a Redis key per member, connector and fingerprint, which every worker
shares.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace

from src.server.database.mcp_oauth import get_connection
from src.server.database.pool import get_db_connection
from src.server.database.org_keys import org_key
from src.server.services.account_contract import OrgConnector
from src.server.services.mcp_oauth.discovery import land_verdict, verdict_write
from src.server.services.mcp_oauth.lifecycle import (
    TokenUnavailable,
    ensure_fresh_access_token,
)
from src.server.services.mcp_probe import (
    DISCOVERY_TIMEOUT_S,
    ProbeOutcome,
    sent_secrets,
    bounded_probe,
    probe_result,
)

logger = logging.getLogger(__name__)

#: How long one member's probe of one connector configuration holds off the
#: next, from any worker.
ORG_PROBE_THROTTLE_S = 120

_REDACTED = "[redacted]"

# Strong refs only: asyncio keeps weak references to tasks. Never read as
# state; the Redis key is what every worker agrees on.
_tasks: set[asyncio.Task] = set()


def org_probe_key(user_id: str, connector_id: str, fingerprint: str) -> str:
    return f"org_connectors:probe:{user_id}:{connector_id}:{fingerprint}"


def _scrubbed(outcome: ProbeOutcome, url: str, headers: dict[str, str]) -> ProbeOutcome:
    """The outcome with every value the probe sent blanked from what it keeps.

    The error is already scrubbed by the probe; the tools and the server's
    identity are what a server could echo a credential into, and they are
    stored and shown to the member, who never holds a shared one.
    """
    secrets = sent_secrets(url, headers, headers.values())
    if not secrets:
        return outcome

    def scrub(value):
        text = json.dumps(value, default=str)
        escaped = [json.dumps(s)[1:-1] for s in secrets]
        if not any(s in text for s in escaped):
            return value
        for secret in escaped:
            text = text.replace(secret, _REDACTED)
        try:
            return json.loads(text)
        except ValueError:
            # A value that cannot be blanked in place is not kept at all.
            return type(value)() if isinstance(value, (list, dict)) else None

    return replace(
        outcome,
        tools=scrub(outcome.tools),
        identity=scrub(outcome.identity),
        skipped=[tuple(item) for item in scrub([list(s) for s in outcome.skipped])],
    )


async def _member_connection(user_id: str, connector: OrgConnector):
    """The member's own sign-in to ``connector`` that a probe can spend, or None."""
    from src.server.services.mcp_config import servable_member_connection

    connection = await get_connection(user_id, org_key(connector.id))
    return connection if servable_member_connection(connector, connection) else None


async def needs_sign_in(user_id: str, connector: OrgConnector) -> bool:
    """Whether ``connector`` is one members sign in to and this one has no
    sign-in a probe could use."""
    return connector.auth == "member" and (
        await _member_connection(user_id, connector) is None
    )


async def _credential_headers(
    user_id: str, connector: OrgConnector
) -> tuple[dict[str, str] | None, str | None, ProbeOutcome | None]:
    """The headers to probe with and the connection behind them, or the
    outcome that stands for a probe that could not be sent. All None: a
    member who has not signed in, so there is nothing to discover yet."""
    from src.server.services.org_connectors import (
        CredentialRefused,
        CredentialUnavailable,
        shared_credential,
    )

    if connector.auth == "shared":
        try:
            headers = await shared_credential(
                user_id, connector, method="tools/list", tool=None
            )
        except CredentialRefused:
            return None, None, ProbeOutcome(
                ok=False, auth="credential", sent_credential=True,
                error="your organization does not give this account the connector's credential",
            )
        except CredentialUnavailable:
            return None, None, ProbeOutcome(
                ok=False, auth="unknown",
                error="the organization's credential for this connector could not be obtained",
            )
        return headers, None, None

    connection = await _member_connection(user_id, connector)
    if connection is None:
        return None, None, None
    try:
        token = await ensure_fresh_access_token(connection.connection_id)
    except TokenUnavailable as e:
        return None, connection.connection_id, ProbeOutcome(
            ok=False, auth="oauth", error=f"token unavailable: {e.reason}"
        )
    return {"Authorization": token.header()}, connection.connection_id, None


async def discover_org_connector(user_id: str, connector: OrgConnector) -> dict | None:
    """Discover one organization connector's tools for one member and cache
    them; the cache row, or None when there was nothing to discover or the
    connector changed while the probe was out.

    A write that makes the snapshot usable, or moves its tools, bumps the
    member's workspaces so a warm computer builds them in.
    """
    from src.server.services.mcp_config import org_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint
    from src.server.services.org_connectors import org_connectors_for

    fingerprint = mcp_discovery_fingerprint(org_server_config(connector))
    headers, connection_id, outcome = await _credential_headers(user_id, connector)
    if headers is None and outcome is None:
        return None
    if outcome is None:
        outcome = _scrubbed(
            await bounded_probe(
                connector.url,
                headers,
                timeout_s=DISCOVERY_TIMEOUT_S,
                background=True,
                credential=True,
                vault_values=list(headers.values()),
            ),
            connector.url,
            headers,
        )

    # The definition this probe dialled has to be the one still listed, or
    # its snapshot would stand for a connector the member no longer has.
    current = await org_connectors_for(user_id)
    if current is None or connector not in current:
        logger.info(
            "[org_discovery] dropped a discovery of connector id=%s for user_id=%s: "
            "it changed or left the organization's list",
            connector.id, user_id,
        )
        return None

    async with get_db_connection() as conn, conn.transaction():
        write = await land_verdict(
            conn, user_id=user_id, server_name=org_key(connector.id),
            fingerprint=fingerprint, connection_id=connection_id,
            # An organization connector's description comes with its definition.
            describe=False, **verdict_write(probe_result(outcome), outcome),
        )
    if not outcome.ok:
        logger.warning(
            "[org_discovery] discovery of connector id=%s for user_id=%s failed: %s",
            connector.id, user_id, outcome.error,
        )
    return write.row


async def claim_probe(user_id: str, connector: OrgConnector) -> bool:
    """Whether this worker may probe now; one probe per window across workers.

    Redis unavailable lets the probe go: the window only spares the vendor and
    the auth service repeated dials, and a member left without tools would be
    the worse failure.
    """
    from src.server.services.mcp_config import org_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint
    from src.utils.cache.redis_cache import get_cache_client

    key = org_probe_key(
        user_id, connector.id, mcp_discovery_fingerprint(org_server_config(connector))
    )
    claimed = await get_cache_client().acquire_lock(
        key, "1", ORG_PROBE_THROTTLE_S * 1000
    )
    return claimed is not False


async def _org_discovery_pass(user_id: str, connector: OrgConnector) -> None:
    from src.server.services.mcp_oauth.connect import resync_live_sandboxes

    try:
        if not await claim_probe(user_id, connector):
            return
        row = await discover_org_connector(user_id, connector)
    except Exception:
        logger.warning(
            "[org_discovery] discovery of connector id=%s for user_id=%s failed",
            connector.id, user_id, exc_info=True,
        )
        return
    if row is not None and row.get("status") == "ok":
        await resync_live_sandboxes(user_id)


def schedule_org_discovery(user_id: str, connectors: list[OrgConnector]) -> None:
    """Discover the given connectors in the background, each at most once per
    throttle window across every worker."""
    for connector in connectors:
        task = asyncio.create_task(
            _org_discovery_pass(user_id, connector),
            name=f"org-discover-{connector.id}",
        )
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)
