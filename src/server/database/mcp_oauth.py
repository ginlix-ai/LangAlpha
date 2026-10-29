"""
Database layer for user MCP OAuth connections.

The token bundle is pgcrypto-encrypted; the refresh token never leaves this
table in any API response or sandbox artifact. token_generation increments on
every successful refresh — commit_refresh is compare-and-swap on it so two
workers can never both commit a refresh for the same generation (rotation
would otherwise destroy the surviving refresh token).

Statuses: connected | needs_reauth | refresh_ambiguous | revoked.
refresh_ambiguous means a refresh timed out ambiguously: the refresh token
may already be consumed server-side, so it must never be retried — the old
access token stays in use until expiry, then the connection needs re-auth.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.encryption import get_encryption_key as _get_encryption_key
from src.server.database.pool import get_db_connection

logger = logging.getLogger(__name__)


class ConnectionStatus(StrEnum):
    """The stored ``status`` values, verbatim — this is the wire/DB vocabulary."""

    CONNECTED = "connected"
    NEEDS_REAUTH = "needs_reauth"
    REFRESH_AMBIGUOUS = "refresh_ambiguous"
    REVOKED = "revoked"


class Secrets(StrEnum):
    """How much of the encrypted bundle a read decrypts.

    Each ``pgp_sym_decrypt`` re-runs OpenPGP S2K key derivation, so the column
    count — not the row count — dominates this read's cost. The relayed-call
    path needs only the bearer; the refresh token and client secret are read
    exclusively by the refresh winner and by DCR re-registration, both rare.
    """

    NONE = "none"
    BEARER = "bearer"
    FULL = "full"


# Statuses a token may still be served for. refresh_ambiguous is in: its
# refresh token must never be retried, but the old access token stays valid
# until expiry.
SERVABLE: frozenset[ConnectionStatus] = frozenset(
    {ConnectionStatus.CONNECTED, ConnectionStatus.REFRESH_AMBIGUOUS}
)
# Deterministic list form for `= ANY(%s)`; StrEnum members adapt as plain text.
SERVABLE_PARAM = sorted(s.value for s in SERVABLE)


@dataclass(frozen=True, slots=True)
class ConnectionSummary:
    """One connection, decrypting nothing — what :class:`Secrets.NONE` reads.

    ``has_refresh_token`` rides on every mode so "can this refresh?" is
    answerable without paying to decrypt the token it would use.
    """

    connection_id: str
    user_id: str
    server_name: str
    server_url: str
    status: ConnectionStatus
    token_type: str | None
    scope: str | None
    # The capability groups this connection was granted, None for a server we
    # curate none for. Beside ``scope`` because it answers the same question the
    # other way round: scope is what the vendor allows, this is what we do.
    granted_capabilities: list[str] | None
    expires_at: datetime | None
    token_generation: int
    client_info: dict[str, Any]
    as_metadata: dict[str, Any]
    resource_metadata: dict[str, Any] | None
    has_refresh_token: bool
    last_refresh_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class BearerBundle(ConnectionSummary):
    """Adds the vendor bearer — the relayed-call path's read."""

    access_token: str


@dataclass(frozen=True, slots=True)
class RefreshBundle(BearerBundle):
    """Adds the credentials only a refresh (or DCR reuse) may spend."""

    refresh_token: str | None
    client_secret: str | None


def _row_summary(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "connection_id": str(r["connection_id"]),
        "user_id": r["user_id"],
        "server_name": r["server_name"],
        "server_url": r["server_url"],
        "status": r["status"],
        "scope": r["scope"],
        "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
        "token_generation": r["token_generation"],
        "last_refresh_at": r["last_refresh_at"].isoformat() if r["last_refresh_at"] else None,
        # resolve_mcp_config reads consent from this dict, so dropping the key
        # here would not raise — it would quietly serve no brokerage tools.
        "granted_capabilities": r.get("granted_capabilities"),
        "created_at": r["created_at"].isoformat(),
        "updated_at": r["updated_at"].isoformat(),
    }


async def upsert_connection(
    user_id: str,
    server_name: str,
    *,
    server_url: str,
    access_token: str,
    refresh_token: str | None,
    token_type: str = "Bearer",
    scope: str | None = None,
    expires_at: datetime | None = None,
    client_info: dict[str, Any] | None = None,
    client_secret: str | None = None,
    as_metadata: dict[str, Any] | None = None,
    resource_metadata: dict[str, Any] | None = None,
    granted_capabilities: list[str] | None = None,
    conn=None,
) -> str:
    """Store a freshly exchanged bundle (connect or re-auth). Returns connection_id.

    Re-auth on an existing row bumps token_generation like a refresh would —
    any caller pinned to the old generation sees rotation.

    ``conn`` joins the caller's transaction. The callback passes one so the
    consent on this row and the policy on the grants it governs commit
    together: written apart, a worker that dies between them leaves the row
    recording a narrowing that no grant enforces, and nothing later reconciles
    it because the version bump had not happened either.
    """
    enc_key = _get_encryption_key()
    async with get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO user_mcp_oauth_connections
                    (user_id, server_name, server_url,
                     access_token, refresh_token, token_type, scope, expires_at,
                     token_generation, client_info, client_secret,
                     as_metadata, resource_metadata, granted_capabilities,
                     status, created_at, updated_at)
                VALUES (%s, %s, %s,
                        pgp_sym_encrypt(%s, %s),
                        CASE WHEN %s::text IS NULL THEN NULL ELSE pgp_sym_encrypt(%s, %s) END,
                        %s, %s, %s,
                        0, %s,
                        CASE WHEN %s::text IS NULL THEN NULL ELSE pgp_sym_encrypt(%s, %s) END,
                        %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (user_id, server_name) DO UPDATE SET
                    server_url = EXCLUDED.server_url,
                    access_token = EXCLUDED.access_token,
                    -- Keep the stored refresh token when the re-auth exchange
                    -- returned none (many AS omit it if the prior grant is
                    -- still valid) — but only while the row still describes the
                    -- same grant. Rows here are never deleted, so a catalog
                    -- entry deleted and recreated against a different provider
                    -- lands on this same row: retaining unconditionally would
                    -- hand provider A's refresh token to provider B's token
                    -- endpoint on the next refresh. Same-client is part of
                    -- same-grant: refresh tokens are bound to the client that
                    -- obtained them, so after a re-registration the old
                    -- client's token would only earn an invalid_grant. Nulling
                    -- instead only costs a re-auth at expiry. A row parked in
                    -- refresh_ambiguous never retains: its stored token may
                    -- already be consumed at the AS, and this write resets the
                    -- row to connected — retaining would hand the next refresh
                    -- a replay that trips the AS's reuse detection.
                    refresh_token = CASE
                        WHEN EXCLUDED.refresh_token IS NOT NULL
                            THEN EXCLUDED.refresh_token
                        WHEN user_mcp_oauth_connections.status
                             <> 'refresh_ambiguous'
                         AND user_mcp_oauth_connections.as_metadata->>'issuer'
                             IS NOT DISTINCT FROM EXCLUDED.as_metadata->>'issuer'
                         AND user_mcp_oauth_connections.server_url
                             = EXCLUDED.server_url
                         AND user_mcp_oauth_connections.client_info->>'client_id'
                             IS NOT DISTINCT FROM EXCLUDED.client_info->>'client_id'
                            THEN user_mcp_oauth_connections.refresh_token
                        ELSE NULL END,
                    token_type = EXCLUDED.token_type,
                    scope = EXCLUDED.scope,
                    expires_at = EXCLUDED.expires_at,
                    token_generation = user_mcp_oauth_connections.token_generation + 1,
                    client_info = EXCLUDED.client_info,
                    client_secret = EXCLUDED.client_secret,
                    as_metadata = EXCLUDED.as_metadata,
                    resource_metadata = EXCLUDED.resource_metadata,
                    -- Kept when the exchange carried none, so a re-auth that
                    -- does not re-ask keeps what the user already chose. A
                    -- plain assignment would null it, which reads downstream as
                    -- consent to nothing and silently empties the connection.
                    granted_capabilities = COALESCE(
                        EXCLUDED.granted_capabilities,
                        user_mcp_oauth_connections.granted_capabilities
                    ),
                    -- Deliberately rewrites a terminal status: a legitimate
                    -- reconnect lands on this same row and must go
                    -- revoked→connected, which is why a freshly consented
                    -- bundle is written here rather than through mark_status
                    -- and its terminal guard. The residual interleaving —
                    -- a revoke between the callback's catalog re-read and this
                    -- write, ~1ms — is accepted: the resurrected row cannot
                    -- serve (its grants stay revoked and URL binding fails at
                    -- resolution), it is only visible to the refresh sweeper.
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING connection_id
                """,
                (
                    user_id, server_name, server_url,
                    access_token, enc_key,
                    refresh_token, refresh_token, enc_key,
                    token_type, scope, expires_at,
                    Json(client_info) if client_info is not None else None,
                    client_secret, client_secret, enc_key,
                    Json(as_metadata) if as_metadata is not None else None,
                    Json(resource_metadata) if resource_metadata is not None else None,
                    Json(granted_capabilities)
                    if granted_capabilities is not None
                    else None,
                    ConnectionStatus.CONNECTED.value,
                ),
            )
            row = await cur.fetchone()
            logger.info(
                f"[mcp_oauth_db] upsert_connection user_id={user_id} server={server_name}"
            )
            return str(row["connection_id"])


async def get_connection(
    user_id: str, server_name: str, *, secrets: Secrets = Secrets.NONE
) -> ConnectionSummary | None:
    """Fetch one connection; ``secrets`` selects how much bundle to decrypt."""
    return await _fetch_one(
        "user_id = %s AND server_name = %s", (user_id, server_name), secrets=secrets
    )


async def get_connection_by_id(
    connection_id: str, *, secrets: Secrets = Secrets.NONE, conn=None
) -> ConnectionSummary | None:
    return await _fetch_one(
        "connection_id = %s", (connection_id,), secrets=secrets, conn=conn
    )


# Decrypted columns per mode, in SELECT order, and the record each mode hands
# back — the concrete class IS the mode, so a reader can only reach a secret it
# actually paid to decrypt.
_SECRET_COLUMNS: dict[Secrets, tuple[str, ...]] = {
    Secrets.NONE: (),
    Secrets.BEARER: ("access_token",),
    Secrets.FULL: ("access_token", "refresh_token", "client_secret"),
}
_RECORD: dict[Secrets, type[ConnectionSummary]] = {
    Secrets.NONE: ConnectionSummary,
    Secrets.BEARER: BearerBundle,
    Secrets.FULL: RefreshBundle,
}


def _to_record(r: dict[str, Any], secrets: Secrets) -> ConnectionSummary:
    """Normalize one raw row into its mode's record — the only place that runs.

    Native types throughout: the lifecycle does expiry math on ``expires_at``,
    and the ISO-string form is ``list_connections``' concern alone.
    """
    fields: dict[str, Any] = {
        "connection_id": str(r["connection_id"]),
        "user_id": r["user_id"],
        "server_name": r["server_name"],
        "server_url": r["server_url"],
        "status": ConnectionStatus(r["status"]),
        "token_type": r["token_type"],
        "scope": r["scope"],
        "granted_capabilities": r["granted_capabilities"],
        "expires_at": r["expires_at"],
        "token_generation": r["token_generation"],
        "client_info": r["client_info"] or {},
        "as_metadata": r["as_metadata"] or {},
        "resource_metadata": r["resource_metadata"],
        "has_refresh_token": bool(r["has_refresh_token"]),
        "last_refresh_at": r["last_refresh_at"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }
    for column in _SECRET_COLUMNS[secrets]:
        fields[column] = r[f"{column}_plain"]
    return _RECORD[secrets](**fields)


async def _fetch_one(
    where: str, params: tuple, *, secrets: Secrets, conn=None
) -> ConnectionSummary | None:
    columns = _SECRET_COLUMNS[secrets]
    secret_cols = "".join(
        f",\n                       pgp_sym_decrypt({c}, %s) AS {c}_plain"
        for c in columns
    )
    enc_key = _get_encryption_key()
    query_params = tuple(enc_key for _ in columns) + params
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT connection_id, user_id, server_name, server_url, status,
                       token_type, scope, granted_capabilities, expires_at,
                       token_generation,
                       client_info, as_metadata, resource_metadata,
                       last_refresh_at, created_at, updated_at,
                       (refresh_token IS NOT NULL) AS has_refresh_token{secret_cols}
                FROM user_mcp_oauth_connections
                WHERE {where}
                """,
                query_params,
            )
            row = await cur.fetchone()
            return _to_record(row, secrets) if row else None


async def list_connections(user_id: str) -> list[dict[str, Any]]:
    """Status view for the UI. Never decrypts."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT connection_id, user_id, server_name, server_url, status,
                       scope, expires_at, token_generation, last_refresh_at,
                       granted_capabilities, created_at, updated_at
                FROM user_mcp_oauth_connections
                WHERE user_id = %s
                ORDER BY server_name
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
            return [_row_summary(r) for r in rows]


async def commit_refresh(
    connection_id: str,
    *,
    expected_generation: int,
    access_token: str,
    refresh_token: str | None,
    expires_at: datetime | None,
    scope: str | None = None,
    conn=None,
) -> bool:
    """Atomically commit a refresh iff the generation hasn't moved.

    refresh_token=None keeps the stored one (the AS didn't rotate it).
    Returns False when another worker already committed a newer generation —
    the caller must discard its result and re-read.
    """
    enc_key = _get_encryption_key()
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_mcp_oauth_connections SET
                    access_token = pgp_sym_encrypt(%s, %s),
                    refresh_token = CASE WHEN %s::text IS NULL
                        THEN refresh_token
                        ELSE pgp_sym_encrypt(%s, %s) END,
                    expires_at = %s,
                    scope = COALESCE(%s, scope),
                    token_generation = token_generation + 1,
                    status = %s,
                    last_refresh_at = NOW(),
                    updated_at = NOW()
                WHERE connection_id = %s
                  AND token_generation = %s
                  AND status = ANY(%s)
                """,
                (
                    access_token, enc_key,
                    refresh_token, refresh_token, enc_key,
                    expires_at, scope,
                    ConnectionStatus.CONNECTED.value,
                    connection_id, expected_generation, SERVABLE_PARAM,
                ),
            )
            committed = cur.rowcount == 1
            if committed:
                logger.info(
                    f"[mcp_oauth_db] commit_refresh connection_id={connection_id} "
                    f"generation={expected_generation + 1}"
                )
            return committed


async def mark_status(
    connection_id: str, status: ConnectionStatus | str, *, conn=None
) -> bool:
    """Transition durable status. Tokens are left in place: refresh_ambiguous
    keeps serving the old access token until expiry, and needs_reauth keeps
    metadata for the reconnect flow.

    ``revoked`` is terminal: a refresh already in flight when the user
    disconnects would otherwise land its outcome on the surrendered row and put
    it back in the servable set. Writing ``revoked`` onto ``revoked`` still
    succeeds, so disconnect stays idempotent.
    """
    status = ConnectionStatus(status)  # rejects anything outside the vocabulary
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_mcp_oauth_connections
                SET status = %s, updated_at = NOW()
                WHERE connection_id = %s
                  AND (status <> 'revoked' OR %s::text = 'revoked')
                """,
                (status.value, connection_id, status.value),
            )
            if cur.rowcount == 1:
                logger.info(
                    f"[mcp_oauth_db] mark_status connection_id={connection_id} status={status}"
                )
                return True
            logger.info(
                f"[mcp_oauth_db] mark_status connection_id={connection_id} "
                f"status={status} not applied"
            )
            return False


async def mark_status_if_generation(
    connection_id: str,
    status: ConnectionStatus | str,
    *,
    expected_generation: int,
    conn=None,
) -> bool:
    """:func:`mark_status`, fenced on the bundle the caller's outcome describes.

    A refresh outcome is about one bundle, and ``upsert_connection`` takes no
    lock: a reconnect completing mid-refresh writes a fresh bundle and bumps the
    generation, which the stale outcome would otherwise flip straight back out
    of ``connected``. The status guard stays :func:`mark_status`'s "not revoked"
    rather than :func:`mark_needs_reauth`'s stricter "still connected" — these
    callers hold the row under the refresh lock and may legitimately re-mark a
    ``refresh_ambiguous`` one.
    """
    status = ConnectionStatus(status)  # rejects anything outside the vocabulary
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_mcp_oauth_connections
                SET status = %s, updated_at = NOW()
                WHERE connection_id = %s
                  AND token_generation = %s
                  AND (status <> 'revoked' OR %s::text = 'revoked')
                """,
                (status.value, connection_id, expected_generation, status.value),
            )
            applied = cur.rowcount == 1
            logger.info(
                f"[mcp_oauth_db] mark_status_if_generation "
                f"connection_id={connection_id} status={status} "
                f"generation={expected_generation} applied={applied}"
            )
            return applied


async def mark_needs_reauth(connection_id: str, *, expected_generation: int) -> bool:
    """CAS a still-connected connection into needs_reauth. Returns whether it moved.

    Both guards are load-bearing for the caller that observed a vendor 401: a
    bundle that rotated since that observation says nothing about the token now
    stored, and refresh_ambiguous/revoked are terminal states this must not
    overwrite. Doing it in one statement leaves no read-then-write window.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_mcp_oauth_connections
                SET status = %s, updated_at = NOW()
                WHERE connection_id = %s
                  AND token_generation = %s
                  AND status = %s
                """,
                (
                    ConnectionStatus.NEEDS_REAUTH.value,
                    connection_id,
                    expected_generation,
                    ConnectionStatus.CONNECTED.value,
                ),
            )
            return cur.rowcount == 1


async def list_due_refresh(margin_seconds: int, limit: int = 25) -> list[dict[str, Any]]:
    """Sweeper scan: connected rows whose access token expires within the margin."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT connection_id, user_id, server_name, token_generation, expires_at
                FROM user_mcp_oauth_connections
                WHERE status = %s
                  AND refresh_token IS NOT NULL
                  AND expires_at IS NOT NULL
                  AND expires_at < NOW() + make_interval(secs => %s)
                ORDER BY expires_at
                LIMIT %s
                """,
                (ConnectionStatus.CONNECTED.value, margin_seconds, limit),
            )
            rows = await cur.fetchall()
            return [
                {
                    "connection_id": str(r["connection_id"]),
                    "user_id": r["user_id"],
                    "server_name": r["server_name"],
                    "token_generation": r["token_generation"],
                    "expires_at": r["expires_at"],
                }
                for r in rows
            ]
