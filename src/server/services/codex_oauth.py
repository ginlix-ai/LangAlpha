"""
Codex OAuth service — device code flow, token exchange, refresh with Redis lock.

Handles the OAuth 2.0 device code flow (RFC 8628) for connecting ChatGPT Codex
models via users' existing ChatGPT Plus/Pro/Team subscriptions.

References:
- Official Codex CLI: github.com/openai/codex (codex-rs/login/src/device_code_auth.rs)
"""

import asyncio
import base64
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx

from src.server.database.oauth_tokens import (
    get_oauth_tokens,
    invalidate_oauth_active_cache,
    upsert_oauth_tokens,
)

logger = logging.getLogger(__name__)

# --- Constants (matching official Codex CLI) ---

CODEX_PROVIDER = "codex-oauth"
CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_TOKEN_URL = "https://auth.openai.com/oauth/token"

# Device code flow endpoints (codex-rs/login/src/device_code_auth.rs)
CODEX_DEVICE_USERCODE_URL = "https://auth.openai.com/api/accounts/deviceauth/usercode"
CODEX_DEVICE_TOKEN_URL = "https://auth.openai.com/api/accounts/deviceauth/token"
CODEX_DEVICE_CALLBACK = "https://auth.openai.com/deviceauth/callback"
CODEX_DEVICE_VERIFY_URL = "https://auth.openai.com/codex/device"


# --- Device code flow ---

async def request_device_code() -> dict:
    """Request a device code from OpenAI.

    Returns:
        {device_auth_id, user_code, interval}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CODEX_DEVICE_USERCODE_URL,
            json={"client_id": CODEX_CLIENT_ID},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "device_auth_id": data["device_auth_id"],
            "user_code": data["user_code"],
            "interval": int(data.get("interval", "5")),
        }


async def poll_device_authorization(device_auth_id: str, user_code: str) -> dict | None:
    """Single poll attempt for device authorization.

    Returns:
        {authorization_code, code_verifier} on success, None if pending.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CODEX_DEVICE_TOKEN_URL,
            json={"device_auth_id": device_auth_id, "user_code": user_code},
        )
        if resp.status_code == 200:
            data = resp.json()
            return {
                "authorization_code": data["authorization_code"],
                "code_verifier": data["code_verifier"],
            }
        if resp.status_code in (403, 404):
            return None  # User hasn't approved yet
        resp.raise_for_status()  # Unexpected error


async def exchange_device_code(authorization_code: str, code_verifier: str) -> dict:
    """Exchange device authorization code for tokens.

    Returns:
        {access_token, refresh_token, id_token}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CODEX_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": CODEX_DEVICE_CALLBACK,
                "client_id": CODEX_CLIENT_ID,
                "code_verifier": code_verifier,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "id_token": data.get("id_token", ""),
        }


async def refresh_tokens(refresh_token: str) -> dict:
    """
    Use single-use refresh token to get new tokens.

    Returns:
        {access_token, refresh_token, id_token}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CODEX_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": CODEX_CLIENT_ID,
                "refresh_token": refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "id_token": data.get("id_token", ""),
        }


def parse_jwt_claims(token: str) -> dict:
    """
    Base64-decode JWT payload (no signature verification — we trust OpenAI's token endpoint).

    Extracts chatgpt_account_id with the same priority as OpenCode:
    1. Root claim: chatgpt_account_id
    2. Namespaced: https://api.openai.com/auth → chatgpt_account_id
    3. Fallback: organizations[0].id

    Returns:
        {account_id, email, plan_type, exp}
    """
    if not token:
        return {"account_id": "", "email": None, "plan_type": None, "exp": None}

    parts = token.split(".")
    if len(parts) < 2:
        return {"account_id": "", "email": None, "plan_type": None, "exp": None}

    payload_b64 = parts[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return {"account_id": "", "email": None, "plan_type": None, "exp": None}

    auth_info = payload.get("https://api.openai.com/auth", {})

    # Extract account_id (priority: root → namespaced → organizations fallback)
    account_id = (
        payload.get("chatgpt_account_id")
        or auth_info.get("chatgpt_account_id")
    )
    if not account_id:
        orgs = payload.get("organizations") or payload.get(
            "https://api.openai.com/organizations", []
        )
        if orgs and isinstance(orgs, list):
            account_id = orgs[0].get("id", "")

    if not account_id:
        account_id = auth_info.get("user_id", payload.get("sub", ""))

    plan_type = auth_info.get("chatgpt_plan_type") or auth_info.get("plan_type")

    return {
        "account_id": account_id or "",
        "email": payload.get("email"),
        "plan_type": plan_type,
        "exp": payload.get("exp"),
    }


# --- Orchestrator with refresh-lock ---

async def get_valid_token(user_id: str) -> dict | None:
    """
    Get a valid access_token + account_id, refreshing if needed.

    Uses Redis SETNX lock to prevent concurrent refresh of single-use
    refresh tokens. Returns None if user hasn't connected.
    """
    tokens = await get_oauth_tokens(user_id, CODEX_PROVIDER)
    if not tokens:
        return None

    now = datetime.now(timezone.utc)
    expires_at = tokens["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    # Token still valid (with 5-min buffer)
    if expires_at > now + timedelta(minutes=5):
        return {
            "access_token": tokens["access_token"],
            "account_id": tokens["account_id"],
        }

    # Need to refresh — acquire Redis lock
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    lock_key = f"oauth:refresh:{user_id}:{CODEX_PROVIDER}"

    if cache.enabled and cache.client:
        acquired = await cache.client.set(lock_key, "1", nx=True, ex=35)
        if not acquired:
            # Another request is refreshing — wait briefly and re-read
            await asyncio.sleep(1)
            tokens = await get_oauth_tokens(user_id, CODEX_PROVIDER)
            if tokens:
                return {
                    "access_token": tokens["access_token"],
                    "account_id": tokens["account_id"],
                }
            return None

    try:
        new = await refresh_tokens(tokens["refresh_token"])
        claims = parse_jwt_claims(new["id_token"])

        exp_ts = claims.get("exp")
        new_expires = (
            datetime.fromtimestamp(exp_ts, tz=timezone.utc)
            if exp_ts
            else now + timedelta(hours=1)
        )

        account_id = claims["account_id"] or tokens["account_id"]

        await upsert_oauth_tokens(
            user_id=user_id,
            provider=CODEX_PROVIDER,
            access_token=new["access_token"],
            refresh_token=new["refresh_token"],
            account_id=account_id,
            email=claims.get("email") or tokens.get("email"),
            plan_type=claims.get("plan_type") or tokens.get("plan_type"),
            expires_at=new_expires,
        )

        try:
            await invalidate_oauth_active_cache(user_id)
        except Exception:
            pass

        logger.debug(f"[codex_oauth] Refreshed tokens for user_id={user_id}")
        return {
            "access_token": new["access_token"],
            "account_id": account_id,
        }
    except Exception as e:
        logger.error(f"[codex_oauth] Token refresh failed for user_id={user_id}: {e}")
        return None
    finally:
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(lock_key)
            except Exception:
                pass
