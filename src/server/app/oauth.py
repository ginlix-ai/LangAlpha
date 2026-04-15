"""
OAuth Router — Connect external OAuth providers (ChatGPT Codex, Claude).

Codex — Device Code Flow (RFC 8628):
- POST   /api/v1/oauth/codex/device/initiate — Start device code flow
- POST   /api/v1/oauth/codex/device/poll     — Poll for user approval
- GET    /api/v1/oauth/codex/status           — Check connection status
- DELETE /api/v1/oauth/codex                  — Disconnect (delete tokens)

Claude — PKCE Authorization Code Flow:
- POST   /api/v1/oauth/claude/initiate        — Generate PKCE + authorize URL
- POST   /api/v1/oauth/claude/callback        — Exchange code#state for tokens
- GET    /api/v1/oauth/claude/status           — Check connection status
- DELETE /api/v1/oauth/claude                  — Disconnect (delete tokens)
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.server.utils.api import CurrentUserId
from src.server.services.codex_oauth import (
    CODEX_PROVIDER,
    CODEX_DEVICE_VERIFY_URL,
    exchange_device_code,
    parse_jwt_claims,
    poll_device_authorization,
    request_device_code,
)
from src.server.services.claude_oauth import (
    CLAUDE_PROVIDER,
    exchange_code as claude_exchange_code,
    generate_authorize_url as claude_generate_authorize_url,
    parse_callback_input as claude_parse_callback_input,
)
from src.server.database.oauth_tokens import (
    delete_oauth_tokens,
    get_oauth_status,
    invalidate_oauth_active_cache,
    upsert_oauth_tokens,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/oauth", tags=["OAuth"])


# ─── Device Code: Initiate ────────────────────────────────────────────────────

@router.post("/codex/device/initiate")
async def codex_device_initiate(user_id: CurrentUserId):
    """Start device code flow. Returns user_code + verification URL.

    The frontend should:
    1. Display the user_code prominently
    2. Open verification_url in a new tab
    3. Start polling /device/poll every `interval` seconds
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(status_code=503, detail="Cache unavailable for OAuth")

    try:
        device = await request_device_code()
    except Exception as e:
        logger.error(f"[oauth] Device code request failed: {e}")
        raise HTTPException(status_code=502, detail="Failed to request device code from OpenAI")

    # Store device_auth_id + user_code in Redis (15-min TTL matching OpenAI's expiry)
    await cache.client.set(
        f"oauth:device:{user_id}",
        json.dumps({
            "device_auth_id": device["device_auth_id"],
            "user_code": device["user_code"],
        }),
        ex=900,
    )

    logger.info(f"[oauth] Device code initiated for user_id={user_id}")
    return {
        "user_code": device["user_code"],
        "verification_url": CODEX_DEVICE_VERIFY_URL,
        "interval": device["interval"],
    }


# ─── Device Code: Poll ────────────────────────────────────────────────────────

@router.post("/codex/device/poll")
async def codex_device_poll(user_id: CurrentUserId):
    """Poll for device authorization.

    Returns:
        {pending: true} if user hasn't approved yet
        {success: true, email, plan_type, account_id} on approval
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(status_code=503, detail="Cache unavailable")

    raw = await cache.client.get(f"oauth:device:{user_id}")
    if not raw:
        raise HTTPException(status_code=400, detail="No pending device authorization. Please initiate again.")

    device = json.loads(raw)

    try:
        result = await poll_device_authorization(device["device_auth_id"], device["user_code"])
    except Exception as e:
        logger.error(f"[oauth] Device poll error for user_id={user_id}: {e}")
        raise HTTPException(status_code=502, detail="Failed to poll OpenAI")

    if result is None:
        return {"pending": True}

    # User approved — exchange authorization code for tokens
    try:
        tokens = await exchange_device_code(result["authorization_code"], result["code_verifier"])

        # Parse JWT claims
        claims = parse_jwt_claims(tokens.get("id_token", ""))
        if not claims.get("account_id"):
            at_claims = parse_jwt_claims(tokens.get("access_token", ""))
            if at_claims.get("account_id"):
                claims["account_id"] = at_claims["account_id"]

        exp_ts = claims.get("exp")
        expires_at = (
            datetime.fromtimestamp(exp_ts, tz=timezone.utc)
            if exp_ts
            else datetime.now(timezone.utc) + timedelta(hours=1)
        )

        await upsert_oauth_tokens(
            user_id=user_id,
            provider=CODEX_PROVIDER,
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            account_id=claims.get("account_id", ""),
            email=claims.get("email"),
            plan_type=claims.get("plan_type"),
            expires_at=expires_at,
        )

        try:
            await invalidate_oauth_active_cache(user_id)
        except Exception:
            pass

        # Clean up Redis
        await cache.client.delete(f"oauth:device:{user_id}")

        logger.info(
            f"[oauth] Codex connected for user_id={user_id} "
            f"email={claims.get('email')} plan={claims.get('plan_type')}"
        )
        return {
            "success": True,
            "email": claims.get("email"),
            "plan_type": claims.get("plan_type"),
            "account_id": claims.get("account_id", ""),
        }

    except Exception as e:
        logger.error(f"[oauth] Device code exchange failed for user_id={user_id}: {e}")
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {e}")


# ─── Status ──────────────────────────────────────────────────────────────────

@router.get("/codex/status")
async def codex_status(user_id: CurrentUserId):
    """Return connection status (no token decryption — fast check)."""
    status = await get_oauth_status(user_id, CODEX_PROVIDER)
    return status


# ─── Disconnect ──────────────────────────────────────────────────────────────

@router.delete("/codex")
async def codex_disconnect(user_id: CurrentUserId):
    """Delete stored OAuth tokens."""
    await delete_oauth_tokens(user_id, CODEX_PROVIDER)
    try:
        await invalidate_oauth_active_cache(user_id)
    except Exception:
        pass
    logger.info(f"[oauth] Codex disconnected for user_id={user_id}")
    return {"success": True}


# ═══════════════════════════════════════════════════════════════════════════════
# Claude OAuth — PKCE Authorization Code Flow
# ═══════════════════════════════════════════════════════════════════════════════

class ClaudeCallbackRequest(BaseModel):
    callback_input: str


# ─── Initiate ────────────────────────────────────────────────────────────────

@router.post("/claude/initiate")
async def claude_initiate(user_id: CurrentUserId):
    """Generate PKCE pair and return authorize URL.

    Frontend should open the URL in a new tab. After the user authorizes,
    they'll see a code on Anthropic's callback page which they paste back
    via POST /claude/callback.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(status_code=503, detail="Cache unavailable for OAuth")

    authorize_url, verifier = claude_generate_authorize_url()

    # Store verifier in Redis (10-min TTL)
    await cache.client.set(
        f"oauth:claude:{user_id}",
        verifier,
        ex=600,
    )

    logger.info(f"[oauth] Claude OAuth initiated for user_id={user_id}")
    return {"authorize_url": authorize_url}


# ─── Callback ────────────────────────────────────────────────────────────────

@router.post("/claude/callback")
async def claude_callback(user_id: CurrentUserId, body: ClaudeCallbackRequest):
    """Exchange callback code#state for tokens.

    Accepts various input formats:
    - Full URL: https://console.anthropic.com/oauth/code/callback?code=X&state=Y
    - code#state
    - code=X&state=Y
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise HTTPException(status_code=503, detail="Cache unavailable")

    # Retrieve stored verifier
    verifier = await cache.client.get(f"oauth:claude:{user_id}")
    if not verifier:
        raise HTTPException(
            status_code=400,
            detail="No pending Claude authorization. Please initiate again.",
        )
    if isinstance(verifier, bytes):
        verifier = verifier.decode()

    # Parse callback input
    try:
        code, state = claude_parse_callback_input(body.callback_input)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Validate state = verifier (Anthropic's PKCE convention)
    if state != verifier:
        raise HTTPException(status_code=400, detail="State mismatch — possible CSRF. Please try again.")

    # Exchange code for tokens
    try:
        tokens = await claude_exchange_code(code, state, verifier)

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=tokens.get("expires_in", 3600))

        await upsert_oauth_tokens(
            user_id=user_id,
            provider=CLAUDE_PROVIDER,
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            account_id="",  # Anthropic tokens don't include account_id in JWT
            email=None,
            plan_type=None,
            expires_at=expires_at,
        )

        try:
            await invalidate_oauth_active_cache(user_id)
        except Exception:
            pass

        # Clean up Redis
        await cache.client.delete(f"oauth:claude:{user_id}")

        logger.info(f"[oauth] Claude connected for user_id={user_id}")
        return {"success": True}

    except Exception as e:
        logger.error(f"[oauth] Claude token exchange failed for user_id={user_id}: {e}")
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {e}")


# ─── Status ──────────────────────────────────────────────────────────────────

@router.get("/claude/status")
async def claude_status(user_id: CurrentUserId):
    """Return Claude connection status (no token decryption — fast check)."""
    status = await get_oauth_status(user_id, CLAUDE_PROVIDER)
    return status


# ─── Disconnect ──────────────────────────────────────────────────────────────

@router.delete("/claude")
async def claude_disconnect(user_id: CurrentUserId):
    """Delete stored Claude OAuth tokens."""
    await delete_oauth_tokens(user_id, CLAUDE_PROVIDER)
    try:
        await invalidate_oauth_active_cache(user_id)
    except Exception:
        pass
    logger.info(f"[oauth] Claude disconnected for user_id={user_id}")
    return {"success": True}
