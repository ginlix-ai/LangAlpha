"""
Robinhood OAuth service — PKCE Authorization Code Flow via MCP OAuth discovery.

Discovery endpoint: GET https://agent.robinhood.com/.well-known/oauth-authorization-server
Registration:       POST https://agent.robinhood.com/oauth/trading/register  (RFC 7591, no secret)
Authorize:          https://robinhood.com/oauth
Token:              https://api.robinhood.com/oauth2/token/

Public client (token_endpoint_auth_method=none) — PKCE is the sole proof-of-possession.
The registration endpoint is idempotent on redirect_uri, so the same redirect_uri always
yields the same client_id. We store client_id in the account_id column so refresh works.
"""

import asyncio
import base64
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx

from src.server.database.oauth_tokens import (
    get_oauth_tokens,
    invalidate_oauth_active_cache,
    upsert_oauth_tokens,
)

logger = logging.getLogger(__name__)

ROBINHOOD_PROVIDER     = "robinhood"
ROBINHOOD_REGISTER_URL = "https://agent.robinhood.com/oauth/trading/register"
ROBINHOOD_AUTHORIZE_URL = "https://robinhood.com/oauth"
ROBINHOOD_TOKEN_URL    = "https://api.robinhood.com/oauth2/token/"
ROBINHOOD_SCOPE        = "internal"


# ── Registration ──────────────────────────────────────────────────────────────

async def register_client(redirect_uri: str) -> str:
    """Dynamically register a client and return client_id.

    The endpoint is idempotent on redirect_uri — same URI always returns the
    same client_id, so calling this on every initiate is safe.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            ROBINHOOD_REGISTER_URL,
            json={
                "redirect_uris": [redirect_uri],
                "token_endpoint_auth_method": "none",
            },
        )
        resp.raise_for_status()
        return resp.json()["client_id"]


# ── PKCE + authorize URL ──────────────────────────────────────────────────────

def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


async def generate_authorize_url(redirect_uri: str) -> tuple[str, str, str, str]:
    """Register client, build PKCE authorize URL.

    Returns:
        (authorize_url, client_id, verifier, state)
        — verifier and state must be stored server-side for callback validation.
    """
    client_id = await register_client(redirect_uri)
    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(16)

    params = {
        "response_type":         "code",
        "client_id":             client_id,
        "redirect_uri":          redirect_uri,
        "scope":                 ROBINHOOD_SCOPE,
        "state":                 state,
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
    }
    url = f"{ROBINHOOD_AUTHORIZE_URL}?{urlencode(params)}"
    return url, client_id, verifier, state


# ── Token exchange ────────────────────────────────────────────────────────────

async def exchange_code(
    code: str,
    verifier: str,
    client_id: str,
    redirect_uri: str,
) -> dict:
    """Exchange authorization code for tokens.

    Returns: {access_token, refresh_token, expires_in}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            ROBINHOOD_TOKEN_URL,
            data={
                "grant_type":    "authorization_code",
                "code":          code,
                "client_id":     client_id,
                "redirect_uri":  redirect_uri,
                "code_verifier": verifier,
            },
        )
        if resp.status_code >= 400:
            logger.error(
                f"[robinhood_oauth] Token exchange failed: "
                f"status={resp.status_code} body={resp.text}"
            )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token":  data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_in":    data.get("expires_in", 86400),
        }


async def _refresh_tokens(refresh_token: str, client_id: str) -> dict:
    """Use refresh token to get new tokens.

    Returns: {access_token, refresh_token, expires_in}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            ROBINHOOD_TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
                "client_id":     client_id,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token":  data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_in":    data.get("expires_in", 86400),
        }


# ── Token getter with refresh lock ────────────────────────────────────────────

async def get_valid_token(user_id: str) -> dict | None:
    """Return a valid Robinhood access token, refreshing if near expiry.

    Uses a Redis SETNX lock to prevent concurrent refresh races.
    Returns None if the user hasn't connected their Robinhood account.
    Returns {"access_token": str} on success.
    """
    tokens = await get_oauth_tokens(user_id, ROBINHOOD_PROVIDER)
    if not tokens or not tokens.get("access_token"):
        return None

    now = datetime.now(timezone.utc)
    expires_at = tokens.get("expires_at")

    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at > now + timedelta(minutes=5):
            return {"access_token": tokens["access_token"]}
    else:
        # No expiry stored — assume valid
        return {"access_token": tokens["access_token"]}

    # Token is expiring — attempt refresh
    client_id = tokens.get("account_id", "")
    if not client_id:
        logger.warning(
            f"[robinhood_oauth] Token near expiry for user_id={user_id} "
            "but no client_id stored; returning existing token."
        )
        return {"access_token": tokens["access_token"]}

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    lock_key = f"oauth:refresh:{user_id}:{ROBINHOOD_PROVIDER}"

    if cache.enabled and cache.client:
        acquired = await cache.client.set(lock_key, "1", nx=True, ex=35)
        if not acquired:
            await asyncio.sleep(1)
            tokens = await get_oauth_tokens(user_id, ROBINHOOD_PROVIDER)
            if tokens:
                return {"access_token": tokens["access_token"]}
            return None

    try:
        new = await _refresh_tokens(tokens["refresh_token"], client_id)
        new_expires = now + timedelta(seconds=new.get("expires_in", 86400))

        await upsert_oauth_tokens(
            user_id=user_id,
            provider=ROBINHOOD_PROVIDER,
            access_token=new["access_token"],
            refresh_token=new["refresh_token"],
            account_id=client_id,
            email=tokens.get("email"),
            plan_type=tokens.get("plan_type"),
            expires_at=new_expires,
        )

        try:
            await invalidate_oauth_active_cache(user_id)
        except Exception:
            pass

        logger.debug(f"[robinhood_oauth] Refreshed tokens for user_id={user_id}")
        return {"access_token": new["access_token"]}
    except Exception as e:
        logger.error(f"[robinhood_oauth] Token refresh failed for user_id={user_id}: {e}")
        return None
    finally:
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(lock_key)
            except Exception:
                pass
