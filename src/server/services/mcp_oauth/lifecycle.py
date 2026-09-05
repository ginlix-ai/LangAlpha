"""Token lifecycle: refresh single-flight, disconnect, status transitions.

The hot path takes NO lock while the access token has >10 minutes left. When
due, ``pg_try_advisory_lock`` (never blocking) elects one refresher across all
workers; losers use the still-valid old token immediately, or briefly poll the
row near expiry. The commit is a ``token_generation`` CAS so a stale winner
can never clobber a newer bundle — and so is every failure status, because a
reconnect can land its own fresh bundle while a refresh is still in flight.

A refresh failure that cannot be placed before the request left the wire is NOT
retryable — the refresh token may already be consumed server-side. The
connection flips to ``refresh_ambiguous`` (old access token keeps serving until
expiry, UI warns); a definitive ``invalid_grant`` flips to ``needs_reauth``
(blocks calls).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

import anyio
import httpx2

from src.server.database.mcp_oauth import (
    SERVABLE,
    BearerBundle,
    ConnectionStatus,
    ConnectionSummary,
    RefreshBundle,
    Secrets,
    commit_refresh,
    get_connection_by_id,
    mark_needs_reauth,
    mark_status,
    mark_status_if_generation,
)
from src.server.database.egress_grants import revoke_grants_for_connection
from src.server.services.mcp_config import same_consented_url
from src.server.services.writer_guard import advisory_key
from src.server.services.mcp_oauth.tokens import (
    TokenExchangeError,
    TokenFailure,
    exchange_token,
    registered_client,
)

logger = logging.getLogger(__name__)

# No lock while more than this much validity remains.
REFRESH_MARGIN_SECONDS = 600
# A loser may keep using the old token down to this floor.
OLD_TOKEN_FLOOR_SECONDS = 60
# Near-expiry losers poll the row for the winner's commit up to this long.
LOSER_POLL_SECONDS = 2.0
REFRESH_TIMEOUT = httpx2.Timeout(10.0, connect=5.0)


class TokenUnavailable(Exception):
    """No usable access token: carries a machine-readable reason."""

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        super().__init__(detail or reason)


@dataclass(frozen=True, slots=True)
class AccessToken:
    """A usable vendor bearer, tagged with the bundle generation it came from.

    ``generation`` is what makes a rotation observable: every write that
    replaces the access token increments it, so a holder can tell "the bundle
    moved under me" from "the vendor is rejecting the current token".
    """

    access_token: str
    token_type: str
    generation: int

    def header(self) -> str:
        return f"{self.token_type} {self.access_token}"


def _expiry_seconds(row: ConnectionSummary) -> float | None:
    expires_at = row.expires_at
    if expires_at is None:
        return None  # non-expiring token
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return (expires_at - datetime.now(timezone.utc)).total_seconds()


def _usable(row: ConnectionSummary, *, floor: float = 0.0) -> bool:
    """Both halves of "may this row's bearer be served": status and validity.

    Expiry alone is not enough — a disconnect leaves the access token unexpired,
    so a fallback that only checked the clock would hand out a bearer for a
    grant the user has surrendered.
    """
    if row.status not in SERVABLE:
        return False
    remaining = _expiry_seconds(row)
    return remaining is None or remaining > floor


async def ensure_fresh_access_token(connection_id: str) -> AccessToken:
    """Return a usable :class:`AccessToken` for a live connection.

    Raises :class:`TokenUnavailable` with reason ``needs_reauth`` /
    ``revoked`` / ``refresh_in_progress`` / ``expired``.
    """
    # Bearer-only: this runs on every relayed tool call, and the refresh token
    # and client secret are needed only if we actually end up refreshing —
    # which re-reads the full bundle under the lock anyway.
    row = await get_connection_by_id(connection_id, secrets=Secrets.BEARER)
    if row is None:
        raise TokenUnavailable("unknown_connection")
    if row.status not in SERVABLE:
        # The reason IS the status: revoked and needs_reauth are the only ways
        # out of the servable set, and both are the caller's answer verbatim.
        raise TokenUnavailable(str(row.status))

    remaining = _expiry_seconds(row)
    if remaining is None or remaining > REFRESH_MARGIN_SECONDS:
        return _token_view(row)
    if not row.has_refresh_token or row.status == ConnectionStatus.REFRESH_AMBIGUOUS:
        # Nothing to refresh with, or nothing we may retry (an ambiguous refresh
        # may already have consumed the token): ride the access token to expiry,
        # then re-auth.
        if remaining > 0:
            return _token_view(row)
        newer = await _mark_refresh_outcome(
            connection_id, row, ConnectionStatus.NEEDS_REAUTH
        )
        if newer is not None:
            return newer
        raise TokenUnavailable("needs_reauth", "access token expired, cannot refresh")

    return await _refresh_single_flight(connection_id, row)


async def current_access_token(connection_id: str) -> AccessToken | None:
    """The stored bearer as-is — no refresh, no freshness gate.

    For a caller that already sent a token and got a 401: the question is
    whether the stored bundle has since moved, which is about the row, not
    about freshness. Servability is still required — a disconnect that lands
    between the 401 and the caller's retry must win, or the retry sends one
    post-revocation request with a rotated, still-vendor-valid bearer.
    """
    row = await get_connection_by_id(connection_id, secrets=Secrets.BEARER)
    if row is None or not row.access_token:
        return None
    if row.status not in SERVABLE:
        return None
    return _token_view(row)


async def mark_connection_needs_reauth(
    connection_id: str, *, seen_token_generation: int
) -> bool:
    """Record that the vendor rejected the bundle at ``seen_token_generation``.

    A no-op unless that generation is still the current one and the connection
    is still ``connected`` — a 401 against a bundle that has already been
    replaced is stale news, and the terminal states are not ours to overwrite.
    """
    flipped = await mark_needs_reauth(
        connection_id, expected_generation=seen_token_generation
    )
    if flipped:
        logger.warning(
            "[mcp_oauth] vendor rejected a current token; connection %s "
            "flipped to needs_reauth",
            connection_id,
        )
    return flipped


def _token_view(row: BearerBundle) -> AccessToken:
    return AccessToken(
        access_token=row.access_token,
        token_type=row.token_type or "Bearer",
        generation=row.token_generation,
    )


async def _mark_refresh_outcome(
    connection_id: str,
    row: BearerBundle,
    status: ConnectionStatus,
    *,
    conn=None,
) -> AccessToken | None:
    """Record a refresh failure against the exact bundle it describes.

    Returns a token to serve instead when the write was refused because the
    generation moved: a reconnect landed while we were failing, so its bundle
    must neither inherit this outcome nor be reported as unusable.
    """
    applied = await mark_status_if_generation(
        connection_id, status, expected_generation=row.token_generation, conn=conn
    )
    if applied:
        return None
    current = await get_connection_by_id(
        connection_id, secrets=Secrets.BEARER, conn=conn
    )
    if current is not None and _usable(current):
        return _token_view(current)
    return None


async def _refresh_single_flight(
    connection_id: str, row: BearerBundle
) -> AccessToken:
    """Try-lock refresh: one winner per cluster; losers never block on it."""
    from src.server.database.pool import get_db_connection

    key = advisory_key("mcp_oauth_refresh", connection_id)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
            won = (await cur.fetchone())[0]
        if not won:
            return await _wait_for_winner(connection_id, row)
        try:
            # Re-read under the lock on the HELD connection — the previous
            # winner may already have committed a fresh bundle. Full bundle:
            # this is the one path that spends the refresh token and client
            # secret. Reusing `conn` keeps the whole refresh on one pool slot
            # instead of nesting a second acquire while this one is held.
            current = await get_connection_by_id(
                connection_id, secrets=Secrets.FULL, conn=conn
            )
            if current is None:
                raise TokenUnavailable("unknown_connection")
            if current.status not in SERVABLE:
                # The entry gate ran before the lock: a disconnect (or a 401
                # report) may have landed in between, and refreshing now would
                # spend the refresh token against a surrendered grant.
                raise TokenUnavailable(str(current.status))
            fresh_remaining = _expiry_seconds(current)
            if fresh_remaining is None or fresh_remaining > REFRESH_MARGIN_SECONDS:
                return _token_view(current)
            if (
                not current.refresh_token
                or current.status == ConnectionStatus.REFRESH_AMBIGUOUS
            ):
                # The entry gate's other decision, re-made under the lock: a
                # previous winner may have flipped this bundle to ambiguous (or
                # a reconnect nulled its refresh token) between the entry read
                # and this acquire. Ambiguous is servable but never retryable —
                # the refresh token may already be consumed, and replaying it
                # can revoke the whole grant.
                if fresh_remaining > 0:
                    return _token_view(current)
                newer = await _mark_refresh_outcome(
                    connection_id, current, ConnectionStatus.NEEDS_REAUTH, conn=conn
                )
                if newer is not None:
                    return newer
                raise TokenUnavailable(
                    "needs_reauth", "access token expired, cannot refresh"
                )
            return await _do_refresh(connection_id, current, conn=conn)
        finally:
            # The advisory lock is session-scoped to THIS connection and the
            # pool does not reset it on return (no DISCARD configured), so an
            # unshielded unlock skipped by a re-delivered CancelledError would
            # strand the cluster-wide election on this pooled connection —
            # every later refresher then loses the try-lock until the
            # connection is recycled. Shield so the unlock always runs.
            with anyio.CancelScope(shield=True):
                async with conn.cursor() as cur:
                    await cur.execute("SELECT pg_advisory_unlock(%s)", (key,))


async def _wait_for_winner(connection_id: str, row: BearerBundle) -> AccessToken:
    """Loser path: old token if comfortably valid, else briefly poll the row."""
    if _usable(row, floor=OLD_TOKEN_FLOOR_SECONDS):
        return _token_view(row)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + LOSER_POLL_SECONDS
    generation = row.token_generation
    while loop.time() < deadline:
        await asyncio.sleep(0.25)
        current = await get_connection_by_id(connection_id, secrets=Secrets.BEARER)
        if current is None:
            raise TokenUnavailable("unknown_connection")
        if current.token_generation > generation and _usable(current):
            return _token_view(current)
    if _usable(row):
        return _token_view(row)
    raise TokenUnavailable("refresh_in_progress")


async def _do_refresh(
    connection_id: str, row: RefreshBundle, *, conn=None
) -> AccessToken:
    """Winner path: one refresh POST, generation-CAS commit.

    ``conn`` is the pool connection already holding this refresh's advisory
    lock; every DB write here runs on it so the refresh never occupies a second
    pool slot.
    """
    token_endpoint = row.as_metadata.get("token_endpoint")
    if not token_endpoint:
        newer = await _mark_refresh_outcome(
            connection_id, row, ConnectionStatus.NEEDS_REAUTH, conn=conn
        )
        if newer is not None:
            return newer
        raise TokenUnavailable("needs_reauth", "no token endpoint on record")

    client_info = registered_client(row.client_info, row.client_secret)
    grant: dict[str, str] = {
        "grant_type": "refresh_token",
        "refresh_token": row.refresh_token,
        "client_id": client_info.client_id,
    }
    resource = (row.resource_metadata or {}).get("resource")
    if resource:
        grant["resource"] = str(resource)

    try:
        token = await exchange_token(
            str(token_endpoint),
            grant,
            client_info=client_info,
            timeout=REFRESH_TIMEOUT,
        )
    except TokenExchangeError as e:
        if e.kind in (TokenFailure.AMBIGUOUS, TokenFailure.BLOCKED):
            # Ambiguous: the AS may have rotated the refresh token already.
            # Retrying could burn the one-time token — flip to ambiguous and
            # keep serving the old access token until it expires.
            logger.warning("[mcp_oauth] ambiguous refresh for %s: %s", connection_id, e)
            newer = await _mark_refresh_outcome(
                connection_id, row, ConnectionStatus.REFRESH_AMBIGUOUS, conn=conn
            )
            if newer is not None:
                return newer
            if _usable(row):
                return _token_view(row)
            raise TokenUnavailable("needs_reauth", "ambiguous refresh, token expired")
        if e.kind is TokenFailure.REJECTED:
            logger.warning("[mcp_oauth] refresh rejected for %s (%s)", connection_id, e)
            newer = await _mark_refresh_outcome(
                connection_id, row, ConnectionStatus.NEEDS_REAUTH, conn=conn
            )
            if newer is not None:
                return newer
            raise TokenUnavailable("needs_reauth", "refresh token rejected")
        # Never left the wire — a transport failure, or an endpoint the pin
        # refused to dial — or a 5xx the AS itself emitted about this request:
        # nothing was consumed, so keep the status and ride the old token,
        # retrying later.
        logger.warning("[mcp_oauth] refresh failed for %s: %s", connection_id, e)
        if _usable(row):
            return _token_view(row)
        raise TokenUnavailable("expired", "refresh failing, token expired")

    committed = await commit_refresh(
        connection_id,
        expected_generation=row.token_generation,
        access_token=token.access_token,
        # None keeps the stored one; "" would otherwise overwrite a working
        # refresh token with an encrypted empty string.
        refresh_token=token.refresh_token,
        expires_at=token.expires_at,
        scope=token.scope,
        conn=conn,
    )
    if not committed:
        # Lost the CAS. The generation cannot have moved under the lock, so the
        # commit's other guard is the live one: the status left the servable set
        # mid-refresh, i.e. the user disconnected. Answer with what the row now
        # says rather than a transient-sounding reason.
        current = await get_connection_by_id(
            connection_id, secrets=Secrets.BEARER, conn=conn
        )
        if current is None:
            raise TokenUnavailable("unknown_connection")
        if current.status not in SERVABLE:
            raise TokenUnavailable(str(current.status))
        if _usable(current):
            return _token_view(current)
        raise TokenUnavailable("refresh_in_progress")
    logger.info(
        "[mcp_oauth] refreshed connection %s (rotated_refresh=%s)",
        connection_id, token.refresh_token is not None,
    )
    # The CAS above committed exactly one increment over the generation we read.
    return AccessToken(
        access_token=token.access_token,
        token_type=token.token_type,
        generation=row.token_generation + 1,
    )


async def disconnect_server(user_id: str, server_name: str) -> bool:
    """Disconnect: revoke the connection + its grants, drop schemas, fan out.

    Revocation is instant — the relay checks grant/connection status per
    request, so no sandbox convergence is needed. Vendor-side revocation lives
    in the vendor's own connected-apps page; we only drop our copy.
    """
    from src.server.database.mcp_oauth import get_connection
    from src.server.database.mcp_tool_schemas import (
        delete_user_and_workspace_tool_schemas_and_bump,
    )
    from src.server.database.pool import get_db_connection

    row = await get_connection(user_id, server_name)
    if row is None:
        return False
    # One transaction: a partial disconnect disagrees with itself — grants
    # revoked while the row still reads connected leaves the refresh sweeper
    # renewing a credential the user gave up. The pool is autocommit, so the
    # explicit transaction() — not merely sharing a connection — is the bind.
    async with get_db_connection() as conn:
        async with conn.transaction():
            await mark_status(
                row.connection_id, ConnectionStatus.REVOKED, conn=conn
            )
            await revoke_grants_for_connection(row.connection_id, conn=conn)
            # Both snapshot tiers, plus the fan-out bump. The per-workspace
            # snapshot's fingerprint is OAuth-blind, so a surviving workspace
            # row keeps publishing the connected tool set while the resolved
            # config no longer carries a connection — the sandbox would dial
            # the vendor directly, with no relay in front of it.
            await delete_user_and_workspace_tool_schemas_and_bump(
                user_id, [server_name], conn=conn
            )
    logger.info(
        "[mcp_oauth] disconnected user=%s server=%s connection=%s",
        user_id, server_name, row.connection_id,
    )
    return True


@asynccontextmanager
async def oauth_fence(user_id: str, names: Sequence[str]):
    """Disconnect ``names`` on both sides of a catalog-row drop.

    Deleting a catalog row on its own orphans the server's OAuth connection:
    there is no catalog FK, so the refresh sweeper keeps renewing the token
    forever and a same-name recreate silently reuses it. Disconnecting only
    beforehand is not enough either — the drop is a separate transaction, so a
    callback landing in the gap leaves a live connection behind a row that no
    longer exists. Closing the fence on exit is what makes the second pass
    impossible to forget. Idempotent throughout: no connection is a no-op, and
    revoked-on-revoked is an accepted write.
    """
    for name in names:
        await disconnect_server(user_id, name)
    try:
        yield
    finally:
        # Also on the failure path: a drop that half happened is exactly when
        # an orphaned live token can be left behind.
        for name in names:
            await disconnect_server(user_id, name)


async def revoke_live_grants(user_id: str, names: Sequence[str]) -> None:
    """Cut egress for servers that just went inert, without disconnecting them.

    Taking a server out of delivery has to bite now, not at next acquire: an
    idle sandbox holds its grant_id and a relay JWT for hours, and the relay
    checks the grant and the connection but never the catalog row. Weaker than
    ``oauth_fence`` on purpose — the connection survives, so re-enabling
    self-heals when the next acquire's grant sync re-activates via its upsert
    arm. Safe against a concurrent re-mint: the caller's version bump fails the
    sync's CAS.
    """
    from src.server.database.egress_grants import revoke_grants_for_connection
    from src.server.database.mcp_oauth import get_connection

    for name in names:
        try:
            connection = await get_connection(user_id, name)
            if connection is not None:
                await revoke_grants_for_connection(connection.connection_id)
        except Exception:
            # One unreachable row must not strand the rest. Whatever made these
            # servers inert is already committed, so aborting the loop cuts the
            # servers before the failure and leaves the ones after it live.
            logger.exception("failed to revoke live grants for %s", name)


async def revoke_if_consent_moved(
    user_id: str, server_name: str, *, transport: str, url: str | None
) -> bool:
    """Revoke the OAuth connection when an edit moves it off its consented endpoint.

    Every write that can redefine a catalog row goes through here, so the token
    issued for the old host never carries to the new one; a transport away from
    a remote scheme invalidates consent outright (no relay path exists).
    """
    from src.server.database.mcp_oauth import get_connection

    connection = await get_connection(user_id, server_name)
    if connection is None or connection.status == ConnectionStatus.REVOKED:
        return False
    if transport in ("http", "sse") and same_consented_url(connection.server_url, url):
        return False
    await disconnect_server(user_id, server_name)
    return True
