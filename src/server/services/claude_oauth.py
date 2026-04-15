"""
Claude OAuth service — PKCE authorization code flow, token exchange, refresh with Redis lock.

Handles the OAuth 2.0 PKCE flow for connecting Claude models via users'
existing Anthropic subscriptions. Unlike Codex (device code flow), Anthropic
uses a hosted callback page where the user copies a code#state string.

Protocol details (discovered from @mariozechner/pi-ai 0.58.0):
- Authorize URL: https://claude.ai/oauth/authorize
- Token URL: https://platform.claude.com/v1/oauth/token
- Redirect URI: https://platform.claude.com/oauth/code/callback (hosted)
- PKCE: S256, state = verifier
- Token exchange: Content-Type: application/json (not form-urlencoded)
- Extra authorize param: code=true
"""

import asyncio
import base64
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

import httpx

from src.server.database.oauth_tokens import (
    get_oauth_tokens,
    invalidate_oauth_active_cache,
    upsert_oauth_tokens,
)

logger = logging.getLogger(__name__)

# --- Constants ---

CLAUDE_PROVIDER = "claude-oauth"
CLAUDE_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
CLAUDE_AUTHORIZE_URL = "https://claude.ai/oauth/authorize"
CLAUDE_TOKEN_URL = "https://platform.claude.com/v1/oauth/token"
CLAUDE_REDIRECT_URI = "https://platform.claude.com/oauth/code/callback"
CLAUDE_SCOPES = "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"


# --- PKCE ---

def generate_pkce_pair() -> tuple[str, str]:
    """Generate a PKCE verifier and S256 challenge.

    Returns:
        (verifier, challenge)
    """
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def build_authorize_url(verifier: str, challenge: str) -> str:
    """Build the Anthropic OAuth authorize URL.

    Anthropic's convention: state = verifier.
    """
    from urllib.parse import urlencode

    params = {
        "code": "true",
        "client_id": CLAUDE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": CLAUDE_REDIRECT_URI,
        "scope": CLAUDE_SCOPES,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": verifier,
    }
    return f"{CLAUDE_AUTHORIZE_URL}?{urlencode(params)}"


def generate_authorize_url() -> tuple[str, str]:
    """Generate PKCE pair and build authorize URL.

    Returns:
        (authorize_url, verifier) — verifier must be stored server-side.
    """
    verifier, challenge = generate_pkce_pair()
    url = build_authorize_url(verifier, challenge)
    return url, verifier


# --- Callback input parsing ---

def parse_callback_input(raw: str) -> tuple[str, str]:
    """Parse various callback input formats into (code, state).

    Supported formats:
    - Full URL: https://platform.claude.com/oauth/code/callback?code=X&state=Y
    - code#state
    - Just the code (state derived from context)

    Returns:
        (code, state)

    Raises:
        ValueError: If input cannot be parsed.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty callback input")

    # Try as full URL with query params
    if raw.startswith("http"):
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]
        if code and state:
            return code, state
        # Try fragment
        frag_qs = parse_qs(parsed.fragment)
        code = code or frag_qs.get("code", [None])[0]
        state = state or frag_qs.get("state", [None])[0]
        if code and state:
            return code, state

    # Try code#state format
    if "#" in raw:
        parts = raw.split("#", 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0].strip(), parts[1].strip()

    # Try code=X&state=Y query string format
    if "code=" in raw and "state=" in raw:
        qs = parse_qs(raw)
        code = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]
        if code and state:
            return code, state

    raise ValueError(
        "Could not parse callback input. "
        "Expected full URL, code#state, or code=X&state=Y format."
    )


# --- Token exchange ---

async def exchange_code(
    code: str,
    state: str,
    code_verifier: str,
) -> dict:
    """Exchange authorization code for tokens.

    Anthropic uses JSON body (not form-urlencoded).

    Returns:
        {access_token, refresh_token, expires_in}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CLAUDE_TOKEN_URL,
            json={
                "grant_type": "authorization_code",
                "client_id": CLAUDE_CLIENT_ID,
                "code": code,
                "state": state,
                "redirect_uri": CLAUDE_REDIRECT_URI,
                "code_verifier": code_verifier,
            },
        )
        if resp.status_code >= 400:
            logger.error(
                f"[claude_oauth] Token exchange failed: status={resp.status_code} body={resp.text}"
            )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_in": data.get("expires_in", 3600),
        }


async def refresh_tokens(refresh_token: str) -> dict:
    """Use refresh token to get new tokens.

    Returns:
        {access_token, refresh_token, expires_in}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CLAUDE_TOKEN_URL,
            json={
                "grant_type": "refresh_token",
                "client_id": CLAUDE_CLIENT_ID,
                "refresh_token": refresh_token,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_in": data.get("expires_in", 3600),
        }


# --- Orchestrator with refresh-lock ---

async def get_valid_token(user_id: str) -> dict | None:
    """
    Get a valid access_token, refreshing if needed.

    Uses Redis SETNX lock to prevent concurrent refresh.
    Returns None if user hasn't connected.
    """
    tokens = await get_oauth_tokens(user_id, CLAUDE_PROVIDER)
    if not tokens:
        return None

    now = datetime.now(timezone.utc)
    expires_at = tokens["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    # Token still valid (with 5-min buffer)
    if expires_at > now + timedelta(minutes=5):
        return {"access_token": tokens["access_token"]}

    # Need to refresh — acquire Redis lock
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    lock_key = f"oauth:refresh:{user_id}:{CLAUDE_PROVIDER}"

    if cache.enabled and cache.client:
        acquired = await cache.client.set(lock_key, "1", nx=True, ex=35)
        if not acquired:
            # Another request is refreshing — wait briefly and re-read
            await asyncio.sleep(1)
            tokens = await get_oauth_tokens(user_id, CLAUDE_PROVIDER)
            if tokens:
                return {"access_token": tokens["access_token"]}
            return None

    try:
        new = await refresh_tokens(tokens["refresh_token"])

        new_expires = now + timedelta(seconds=new.get("expires_in", 3600))

        await upsert_oauth_tokens(
            user_id=user_id,
            provider=CLAUDE_PROVIDER,
            access_token=new["access_token"],
            refresh_token=new["refresh_token"],
            account_id=tokens.get("account_id", ""),
            email=tokens.get("email"),
            plan_type=tokens.get("plan_type"),
            expires_at=new_expires,
        )

        try:
            await invalidate_oauth_active_cache(user_id)
        except Exception:
            pass

        logger.debug(f"[claude_oauth] Refreshed tokens for user_id={user_id}")
        return {"access_token": new["access_token"]}
    except Exception as e:
        logger.error(f"[claude_oauth] Token refresh failed for user_id={user_id}: {e}")
        return None
    finally:
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(lock_key)
            except Exception:
                pass
