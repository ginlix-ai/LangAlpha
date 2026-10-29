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
converges removals by reading the table. The grant replacement is whole-set for
this project's share of its machine, so it is fenced in the DB layer by the
owner's advisory lock plus a `mcp_config_version` CAS; the lock is the user's
because every project on a machine is that one user's, so two replacements
that could collide are two of theirs. A worker whose resolve was superseded is
told so and pushes nothing. The owner's database advisory lock stays held from
grant replacement through the whole-machine read and credential-file push.
Atomic replacement prevents a torn file; the lock prevents an older complete
map from landing after a newer one.
"""

from __future__ import annotations

import json
import logging
import shlex
from enum import StrEnum
from typing import TYPE_CHECKING

from src.config.env import EGRESS_RELAY_SECRET
from src.server.database.egress_grants import (
    active_relay_grants_for_computer,
    sync_egress_grants,
    user_egress_state_lock,
)
from src.server.services.egress.grant_scope import grant_refs

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
    computer_id: str,
    user_id: str | None,
    session: "Session",
    resolved: "ResolvedMCP",
) -> RelayBind:
    """Converge grants + relay JWT + sandbox credential file to ``resolved``.

    One grant per server that earns one (``grant_scope`` decides which, and of
    which kind); grants no live project on the machine resolves any more are
    retired in the same transaction (they are an authorization overhang
    otherwise; the sandbox may still hold their ids and a live JWT). What a
    sibling project still resolves is spared, because the machine's grant set
    is the union of its projects' and this workspace only speaks for its own
    share. Removal of the last of them also deletes the credential file,
    decided from the table so it
    converges on any worker. A no-op when ``resolved`` is already superseded by
    a newer config version.

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
    # The replacement below is whole-set, so a resolve with no owner is never
    # authoritative: a grant only resolves for an authenticated user, and an
    # unowned resolve is indistinguishable from one that resolved empty
    # because the owner was unknown — which would retire every live grant.
    if not user_id:
        return RelayBind.APPLIED

    refs = await grant_refs(resolved, user_id=user_id)
    if refs and not EGRESS_RELAY_SECRET:
        logger.warning(
            "[EGRESS] relay-bound MCP servers %s present but "
            "EGRESS_RELAY_SECRET is unset, they stay unbound",
            [r.server_name for r in refs],
        )
        return RelayBind.APPLIED

    async with user_egress_state_lock(user_id) as conn:
        synced = await sync_egress_grants(
            user_id=user_id,
            workspace_id=workspace_id,
            refs=refs,
            config_version=resolved.version,
            conn=conn,
        )
        # Superseded config: a newer sync owns the grant set, so returning here
        # is what keeps a stale grant map out of the credential file.
        if synced is None:
            return RelayBind.SUPERSEDED

        for ref in refs:
            if synced.grants.get(ref.key) is None:
                logger.warning(
                    "[EGRESS] %s %s gone for server %s, left unbound",
                    ref.kind,
                    ref.subject,
                    ref.server_name,
                )

        # Read and publish the machine's union while the same lock is held. A
        # later writer cannot commit its rows and an earlier writer cannot
        # overwrite its file between these two operations.
        grants = await active_relay_grants_for_computer(
            computer_id, user_id=user_id, conn=conn
        )

        if grants or synced.retired or session.egress_binding is not None:
            pushed = await _push_credentials(
                workspace_id, computer_id, session, user_id, grants
            )
            return RelayBind.APPLIED if pushed else RelayBind.REFUSED
        return RelayBind.APPLIED


async def refresh_computer_grant_map(
    runtime, *, root: str, computer_id: str, user_id: str
) -> None:
    """Converge a surviving machine after deletion without starting a session.

    Keep its current computer JWT; the authoritative DB revocation already
    denies retired IDs. Serialize the surviving map's read and publication
    with ordinary session binds so a stale best-effort refresh cannot win.
    """
    from ptc_agent.core.paths import SandboxLayout

    async with user_egress_state_lock(user_id) as conn:
        grants = await active_relay_grants_for_computer(
            computer_id, user_id=user_id, conn=conn
        )
        path = SandboxLayout.for_root(root).egress_relay
        script = """import json,os,sys,tempfile
path,grants=sys.argv[1],json.loads(sys.argv[2])
if not os.path.exists(path):
    sys.exit(0)
if not grants:
    os.unlink(path)
    sys.exit(0)
with open(path) as source:
    payload=json.load(source)
payload['grants']=grants
fd,tmp=tempfile.mkstemp(dir=os.path.dirname(path))
try:
    with os.fdopen(fd,'w') as dest:
        json.dump(payload,dest)
    os.replace(tmp,path)
finally:
    if os.path.exists(tmp):
        os.unlink(tmp)
"""
        result = await runtime.exec(
            f"python3 -I -c {shlex.quote(script)} {shlex.quote(path)} "
            f"{shlex.quote(json.dumps(grants))}"
        )
        if result.exit_code:
            raise RuntimeError("Could not refresh computer egress grant map")


async def maybe_remint_egress_jwt(
    workspace_id: str, computer_id: str, session: "Session"
) -> None:
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
        async with user_egress_state_lock(binding.user_id) as conn:
            grants = await active_relay_grants_for_computer(
                computer_id, user_id=binding.user_id, conn=conn
            )
            if await _push_credentials(
                workspace_id, computer_id, session, binding.user_id, grants
            ):
                logger.info("[EGRESS] relay JWT reminted for workspace %s", workspace_id)
    except Exception as e:
        logger.warning("[EGRESS] relay JWT remint failed for %s: %s", workspace_id, e)


async def _push_credentials(
    workspace_id: str,
    computer_id: str,
    session: "Session",
    user_id: str,
    grants: dict[str, str],
) -> bool:
    """(Re)write the sandbox credential file; ``grants == {}`` deletes it.

    The binding records what the sandbox is known to hold, so it advances only
    on a publication the sandbox confirmed — a refused upload returns False so
    the caller keeps a retry signal. Callers serialize the read and publication
    under the owner's egress lock; this helper only performs the side effect.
    """
    from ptc_agent.core.session import EgressBinding
    from src.server.services.egress.reachability import (
        effective_relay_base_url,
        relay_reachability_warning,
    )
    from src.server.services.egress.relay_jwt import identity_claim, mint_relay_jwt

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
        # Both identity claims are omitted when absent, and the sandbox here may
        # not be provisioned yet: this used to mint ``sandbox_id=""``, which the
        # validator refused, so the credential could not be used. Neither claim
        # is authorized against, so an absent one costs audit detail and nothing
        # else. The machine comes from the caller's binding rather than the
        # session, whose own label is whichever project built it.
        minted = mint_relay_jwt(
            EGRESS_RELAY_SECRET,
            user_id=user_id,
            workspace_id=workspace_id,
            sandbox_id=identity_claim(sandbox.sandbox_id),
            computer_id=identity_claim(computer_id),
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
