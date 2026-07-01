"""Database CRUD for Robinhood Agentic Trading MCP OAuth state (encrypted at rest).

One row per (user_id, workspace_id). Holds the dynamically-registered client,
the discovered authorization/token endpoints, the transient PKCE state while a
connect is pending, and the encrypted access/refresh tokens once connected.

Same ``pgp_sym_encrypt``/``pgp_sym_decrypt`` pattern as ``oauth_tokens.py``.
Secret columns always store an ENCRYPTED STRING (never SQL NULL) — absent
secrets are encrypted empty strings, so decrypt always yields a plain ``str``
and we never hit pgcrypto's NULL-input edge cases.
"""

import logging
from datetime import datetime
from typing import Any, Optional

from psycopg.rows import dict_row

from src.server.database.conversation import get_db_connection
from src.server.database.encryption import get_encryption_key

logger = logging.getLogger(__name__)


async def upsert_pending(
    *,
    user_id: str,
    workspace_id: str,
    resource: str,
    authorization_endpoint: str,
    token_endpoint: str,
    registration_endpoint: Optional[str],
    client_id: str,
    client_secret: Optional[str],
    redirect_uri: str,
    scopes: str,
    state: str,
    code_verifier: str,
) -> None:
    """Create or reset a pending connect row (post-initiate, pre-callback).

    Resets any prior row for this (user, workspace) back to ``pending`` and
    clears stored tokens — a fresh authorize flow supersedes the old grant.
    """
    enc = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO robinhood_oauth (
                    user_id, workspace_id, status, resource,
                    authorization_endpoint, token_endpoint, registration_endpoint,
                    client_id, client_secret, redirect_uri, scopes,
                    state, code_verifier, access_token, refresh_token, expires_at,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, 'pending', %s,
                    %s, %s, %s,
                    %s, pgp_sym_encrypt(%s, %s), %s, %s,
                    %s, pgp_sym_encrypt(%s, %s),
                    pgp_sym_encrypt('', %s), pgp_sym_encrypt('', %s), NULL,
                    NOW(), NOW()
                )
                ON CONFLICT (user_id, workspace_id) DO UPDATE SET
                    status                  = 'pending',
                    resource                = EXCLUDED.resource,
                    authorization_endpoint  = EXCLUDED.authorization_endpoint,
                    token_endpoint          = EXCLUDED.token_endpoint,
                    registration_endpoint   = EXCLUDED.registration_endpoint,
                    client_id               = EXCLUDED.client_id,
                    client_secret           = EXCLUDED.client_secret,
                    redirect_uri            = EXCLUDED.redirect_uri,
                    scopes                  = EXCLUDED.scopes,
                    state                   = EXCLUDED.state,
                    code_verifier           = EXCLUDED.code_verifier,
                    access_token            = EXCLUDED.access_token,
                    refresh_token           = EXCLUDED.refresh_token,
                    expires_at              = NULL,
                    updated_at              = NOW()
                """,
                (
                    user_id, workspace_id, resource,
                    authorization_endpoint, token_endpoint, registration_endpoint,
                    client_id, (client_secret or ""), enc, redirect_uri, scopes,
                    state, code_verifier, enc,
                    enc, enc,
                ),
            )
    logger.debug(
        "[robinhood_oauth] pending upsert user_id=%s workspace_id=%s",
        user_id, workspace_id,
    )


async def set_tokens(
    *,
    user_id: str,
    workspace_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: Optional[datetime],
) -> None:
    """Store freshly-exchanged or refreshed tokens and mark the row connected.

    Clears the transient PKCE state once the code has been redeemed.
    """
    enc = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE robinhood_oauth SET
                    status        = 'connected',
                    access_token  = pgp_sym_encrypt(%s, %s),
                    refresh_token = pgp_sym_encrypt(%s, %s),
                    expires_at    = %s,
                    state         = NULL,
                    code_verifier = pgp_sym_encrypt('', %s),
                    updated_at    = NOW()
                WHERE user_id = %s AND workspace_id = %s
                """,
                (
                    access_token, enc,
                    refresh_token, enc,
                    expires_at, enc,
                    user_id, workspace_id,
                ),
            )
    logger.debug(
        "[robinhood_oauth] tokens set user_id=%s workspace_id=%s",
        user_id, workspace_id,
    )


def _to_str(v: Any) -> str:
    """pgp_sym_decrypt yields text but psycopg3 may return bytes/memoryview."""
    if v is None:
        return ""
    return v.decode() if isinstance(v, (bytes, bytearray, memoryview)) else str(v)


async def get(user_id: str, workspace_id: str) -> Optional[dict[str, Any]]:
    """Return the full decrypted row, or None. Empty-string secrets = absent."""
    enc = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    user_id, workspace_id, status, resource,
                    authorization_endpoint, token_endpoint, registration_endpoint,
                    client_id, redirect_uri, scopes, state, expires_at,
                    pgp_sym_decrypt(client_secret, %s) AS client_secret,
                    pgp_sym_decrypt(code_verifier, %s) AS code_verifier,
                    pgp_sym_decrypt(access_token, %s)  AS access_token,
                    pgp_sym_decrypt(refresh_token, %s) AS refresh_token
                FROM robinhood_oauth
                WHERE user_id = %s AND workspace_id = %s
                """,
                (enc, enc, enc, enc, user_id, workspace_id),
            )
            row = await cur.fetchone()
            if not row:
                return None
            for key in (
                "client_secret", "code_verifier", "access_token", "refresh_token",
            ):
                row[key] = _to_str(row[key])
            return row


async def get_pending_by_state(state: str) -> Optional[dict[str, Any]]:
    """Look up a pending connect row by its (high-entropy, single-use) state.

    The OAuth callback is a top-level browser redirect with no auth header, so
    the unguessable ``state`` is what binds it back to the initiating session.
    Returns the decrypted row (client_secret / code_verifier as strings) or None.
    """
    enc = get_encryption_key()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    user_id, workspace_id, status, resource,
                    token_endpoint, client_id, redirect_uri, scopes,
                    pgp_sym_decrypt(client_secret, %s) AS client_secret,
                    pgp_sym_decrypt(code_verifier, %s) AS code_verifier
                FROM robinhood_oauth
                WHERE state = %s AND status = 'pending'
                """,
                (enc, enc, state),
            )
            row = await cur.fetchone()
            if not row:
                return None
            row["client_secret"] = _to_str(row["client_secret"])
            row["code_verifier"] = _to_str(row["code_verifier"])
            return row


async def get_status(user_id: str, workspace_id: str) -> dict[str, Any]:
    """Quick status check (no decryption): {connected, status, expires_at, scopes}."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT status, expires_at, scopes
                FROM robinhood_oauth
                WHERE user_id = %s AND workspace_id = %s
                """,
                (user_id, workspace_id),
            )
            row = await cur.fetchone()
            if not row:
                return {"connected": False, "status": None, "expires_at": None}
            return {
                "connected": row["status"] == "connected",
                "status": row["status"],
                "expires_at": row["expires_at"],
                "scopes": row["scopes"] or "",
            }


async def delete(user_id: str, workspace_id: str) -> bool:
    """Remove the row. Returns True if one was deleted."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM robinhood_oauth WHERE user_id = %s AND workspace_id = %s",
                (user_id, workspace_id),
            )
            deleted = cur.rowcount > 0
    logger.info(
        "[robinhood_oauth] delete user_id=%s workspace_id=%s deleted=%s",
        user_id, workspace_id, deleted,
    )
    return deleted
