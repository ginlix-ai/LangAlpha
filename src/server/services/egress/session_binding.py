"""Egress-relay binding for one workspace session: grants, JWT, credential file.

The sandbox's only relay credential is a short-lived JWT plus a server→grant
map, written to a single file (`upload_egress_relay_credentials`). This module
owns the lifecycle of that file across the resolve path (`sync_egress_relay`)
and the warm fast path (`maybe_remint_egress_jwt`). The file is the ONLY place
a grant id reaches the sandbox — resolved server configs carry none, so a
retired grant cannot survive in a second channel.

Multi-worker contract: the `sandbox_egress_grants` table is the truth about
which grants exist — `EgressBinding` on the session is execution context only
(what THIS process last pushed), so a worker that never bound anything still
converges removals by reading the table. The grant replacement itself is
whole-set, so it is fenced in the DB layer by a workspace advisory lock plus a
`mcp_config_version` CAS; a worker whose resolve was superseded is told so and
pushes nothing. No cross-worker lock guards the credential-file push: the
upload writes atomically (temp + same-dir rename in
`upload_egress_relay_credentials`), so two workers pushing the same workspace
concurrently can only ever leave one complete file — last rename wins, both
relay JWTs are valid, and the grant map converges on the next push.
"""

from __future__ import annotations

import logging
from enum import StrEnum
from typing import TYPE_CHECKING

from src.config.env import EGRESS_RELAY_SECRET
from src.server.database.egress_grants import sync_oauth_grants

if TYPE_CHECKING:
    from ptc_agent.core.session import Session
    from src.server.services.mcp_config import ResolvedMCP

logger = logging.getLogger(__name__)


class RelayBind(StrEnum):
    """What a bind settled on. Three outcomes because the caller owes each a
    different response, and collapsing two of them is what let a superseded
    resolve republish the tools its user had just declined."""

    APPLIED = "applied"
    #: A credential push was needed and the sandbox refused it. Nothing else
    #: retries the file, so the caller must withhold the stamp.
    REFUSED = "refused"
    #: A newer config version owns the grant set. This resolve's view of the
    #: world is stale, so nothing derived from it may be published.
    SUPERSEDED = "superseded"


async def sync_egress_relay(
    workspace_id: str,
    user_id: str | None,
    session: "Session",
    resolved: "ResolvedMCP",
) -> RelayBind:
    """Converge grants + relay JWT + sandbox credential file to ``resolved``.

    One grant per OAuth-connected server in the resolved set; grants the
    workspace no longer resolves are retired in the same transaction (they are
    an authorization overhang otherwise — the sandbox may still hold their ids
    and a live JWT). Removal of the last OAuth server also deletes the
    credential file, decided from the table so it converges on any worker. A
    no-op when ``resolved`` is already superseded by a newer config version.

    ``REFUSED`` only when a credential push was NEEDED and the sandbox refused
    it — the one outcome the caller must not stamp as applied, since nothing
    else retries a refused file. Settled non-push outcomes (relay disabled,
    unowned resolve) are ``APPLIED``: re-running them would produce the same
    decision, so a retry buys nothing.

    ``SUPERSEDED`` is separate from both. It reads as settled from here, since
    a newer sync owns the grants and re-running changes nothing — but the
    caller has a whole composite derived from the same stale resolve, and
    publishing that into the sandbox undoes what the newer resolve just wrote.
    """
    oauth_servers = [s for s in resolved.servers if s.oauth_connection_id]
    if oauth_servers and not EGRESS_RELAY_SECRET:
        logger.warning(
            "[EGRESS] OAuth-connected MCP servers %s present but "
            "EGRESS_RELAY_SECRET is unset — they stay unbound",
            [s.name for s in oauth_servers],
        )
        return RelayBind.APPLIED
    # The replacement below is whole-set, so a resolve with no owner is never
    # authoritative: OAuth connections only resolve for an authenticated user,
    # and an unowned resolve is indistinguishable from one that resolved empty
    # because the owner was unknown — which would retire every live grant.
    if not user_id:
        return RelayBind.APPLIED

    synced = await sync_oauth_grants(
        user_id=user_id or "",
        workspace_id=workspace_id,
        connection_ids=[s.oauth_connection_id for s in oauth_servers],
        config_version=resolved.version,
    )
    # Superseded config: a newer sync owns the grant set, so returning here is
    # what keeps a stale grant map out of the credential file. The near-miss in
    # the other direction is benign — resolve reads the version before the rows,
    # so a resolver can carry v1 with slightly newer rows — because the CAS only
    # rejects genuinely stale replacements, and this worker's stamped v1 forces
    # a re-resolve on its next acquire.
    if synced is None:
        return RelayBind.SUPERSEDED

    grants: dict[str, str] = {}
    for srv in oauth_servers:
        grant_id = synced.grants.get(srv.oauth_connection_id)
        if grant_id is None:
            # The connection vanished between resolve and here (disconnect
            # race): leave this one server unbound, keep binding the rest.
            logger.warning(
                "[EGRESS] connection %s gone for server %s — left unbound",
                srv.oauth_connection_id, srv.name,
            )
            continue
        grants[srv.name] = grant_id

    if grants or synced.retired or session.egress_binding is not None:
        pushed = await _push_credentials(workspace_id, session, user_id or "", grants)
        return RelayBind.APPLIED if pushed else RelayBind.REFUSED
    return RelayBind.APPLIED


async def maybe_remint_egress_jwt(workspace_id: str, session: "Session") -> None:
    """Re-push credentials when the relay JWT nears expiry.

    Runs on the warm-cooldown path (which skips the resolve entirely), so a
    long-lived session keeps a valid JWT without re-resolving config. Pure
    in-memory compare unless the token is actually near expiry.
    """
    binding = session.egress_binding
    if binding is None or not EGRESS_RELAY_SECRET:
        return
    from src.server.services.egress.relay_jwt import needs_remint

    if not needs_remint(binding.jwt_exp):
        return
    try:
        # A refused push already logged its own warning; jwt_exp stays put, so
        # the remint retries on the next warm acquire.
        if await _push_credentials(
            workspace_id, session, binding.user_id, dict(binding.grants)
        ):
            logger.info("[EGRESS] relay JWT reminted for workspace %s", workspace_id)
    except Exception as e:
        logger.warning(
            "[EGRESS] relay JWT remint failed for %s: %s", workspace_id, e
        )


async def _push_credentials(
    workspace_id: str,
    session: "Session",
    user_id: str,
    grants: dict[str, str],
) -> bool:
    """(Re)write the sandbox credential file; ``grants == {}`` deletes it.

    The binding records what the sandbox is known to hold, so it advances only
    on a publication the sandbox confirmed — a refused upload returns False so
    the caller keeps a retry signal. No cross-worker lock is needed: the
    upload replaces the file atomically, so a concurrent push can at worst
    overwrite this one's file with an equally-valid credential — never tear it.
    """
    from ptc_agent.core.session import EgressBinding
    from src.server.services.egress.reachability import (
        effective_relay_base_url,
        relay_reachability_warning,
    )
    from src.server.services.egress.relay_jwt import mint_relay_jwt

    sandbox = session.sandbox
    if sandbox is None:
        return True

    payload, binding = None, None
    if grants:
        provider = session.config.sandbox.provider
        relay_base = effective_relay_base_url(provider)
        warning = relay_reachability_warning(provider, relay_base)
        if warning:
            logger.warning("[EGRESS] %s", warning)
        minted = mint_relay_jwt(
            EGRESS_RELAY_SECRET,
            user_id=user_id,
            workspace_id=workspace_id,
            sandbox_id=sandbox.sandbox_id or "",
        )
        payload = {
            "relay_base_url": relay_base.rstrip("/"),
            "token": minted.token,
            "grants": grants,
        }
        binding = EgressBinding(
            grants=grants, jwt_exp=minted.expires_at, user_id=user_id
        )

    if not await sandbox.upload_egress_relay_credentials(payload):
        logger.warning(
            "[EGRESS] credential push failed for workspace %s — binding unchanged",
            workspace_id,
        )
        return False
    session.egress_binding = binding
    return True
