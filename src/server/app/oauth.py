"""
OAuth Router: connect subscription providers (ChatGPT Codex, Grok, Claude).

- POST   /api/v1/oauth/{codex,grok}/device/initiate   Start a device code flow
- POST   /api/v1/oauth/{codex,grok}/device/poll       Poll for the user's approval
- POST   /api/v1/oauth/claude/initiate                PKCE authorize URL
- POST   /api/v1/oauth/claude/callback                Exchange the pasted code#state
- GET    /api/v1/oauth/{codex,claude,grok}/status     Connection status
- DELETE /api/v1/oauth/{codex,claude,grok}            Disconnect (delete tokens)
"""

import asyncio
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.server.database.oauth_tokens import connection_change, delete_oauth_tokens, get_oauth_connection
from src.server.models.oauth import OAuthStatus
from src.server.services import claude_oauth, codex_oauth, grok_oauth
from src.server.services.oauth_health import connection_changed, store_tokens
from src.server.services.oauth_protocol import (
    SLOW_DOWN_STEP,
    DeviceFlow,
    DeviceFlowUnavailable,
    DevicePollFailed,
    DeviceUpstreamUnavailable,
    TokenSet,
    upstream_error_code,
)
from src.server.services.oauth_providers import SUBSCRIPTIONS
from src.server.utils.api import CurrentUserId
from src.utils.cache.throttle import throttle

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/oauth", tags=["OAuth"])


class OAuthRefusal(HTTPException):
    """A failure from these routes: ``detail`` the sentence, ``error`` its
    ``{code, message, retryable}``, where ``retryable`` says whether asking
    again (the same poll, the same pasted code) can succeed or the user has to
    start over. Provider text never reaches the client, only its code.

    The code rides beside ``detail`` rather than in it because pages loaded
    before it existed render ``detail`` as text; ``setup.py`` writes the body.
    """

    def __init__(self, status_code: int, code: str, message: str, **extra):
        super().__init__(status_code=status_code, detail=message)
        self.error = {"code": code, "message": message, **extra}


_fail = OAuthRefusal


def _require_cache():
    # Where a sign-in's pending state lives.
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise _fail(
            503, "redis_unavailable",
            "Connecting an account is unavailable right now. Try again shortly.",
            retryable=True,
        )
    return cache


def exchange_failure(upstream: str, e: Exception) -> OAuthRefusal:
    """The token exchange's failure, by whether trying again can help."""
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        if status != 429 and status < 500:
            return _fail(
                400, "exchange_failed", f"{upstream} did not accept the authorization. Start again.",
                retryable=False, upstream_code=upstream_error_code(e.response),
            )
    if isinstance(e, (httpx.HTTPStatusError, httpx.TransportError, TimeoutError)):
        return _fail(
            502, "upstream_unavailable", f"{upstream} could not be reached. Try again.",
            retryable=True,
        )
    return _fail(
        502, "exchange_failed", f"{upstream} sent an unexpected answer. Start again.",
        retryable=False,
    )


_SAVE_DEADLINE_S = 10


async def _save(cache, key: str, raw, user_id: str, provider: str, tokens: TokenSet) -> bool:
    """Store a new connection if the sign-in that produced it is still the
    pending one; False when a newer sign-in replaced it.

    The pending key is claimed (compare-and-delete on the value this request
    read) before the tokens are stored, so a sign-in the user restarted,
    perhaps for another account, cannot be overwritten by the approval of the
    one it replaced. The claim ends the sign-in whether or not the save lands:
    the provider already spent the authorization, so only a new one helps.
    """
    try:
        # Bounded so the caller's lease outlasts it (the poll's 60 seconds
        # plus this stay under its 75): a save stalled on the pool or the lock
        # past the lease would leave the approved flow to a second poll.
        async with asyncio.timeout(_SAVE_DEADLINE_S), connection_change(user_id, provider) as conn:
            if not await cache.claim(key, raw):
                return False
            await store_tokens(user_id, provider, tokens, conn=conn)
    except Exception:
        logger.exception(f"[oauth] Saving {provider} tokens failed for user_id={user_id}")
        raise _fail(500, "save_failed", "The account could not be saved. Start again.", retryable=False)
    await connection_changed(user_id)
    return True


def _subscription(key: str) -> str:
    provider = f"{key}-oauth"
    if provider not in SUBSCRIPTIONS:
        raise HTTPException(status_code=404, detail="Not Found")
    return provider


# ─── Device code flow ────────────────────────────────────────────────────────

_DEVICE_FLOWS = {"codex": codex_oauth.DEVICE_FLOW, "grok": grok_oauth.DEVICE_FLOW}


def _device_flow(key: str) -> DeviceFlow:
    if key not in _DEVICE_FLOWS:
        raise HTTPException(status_code=404, detail="Not Found")
    return _DEVICE_FLOWS[key]


# One poll at a time per user, across workers: two overlapping polls would
# both see the approval and exchange the single-use authorization code twice,
# or ask inside the interval, which the provider answers slow_down. The lease
# outlasts the poll's own deadline.
_POLL_LOCK_TTL_MS = 75_000
# The provider requests (device token, then the Codex code exchange) are
# bounded per phase only, so a slow trickle could hold one past any lease. The
# whole exchange gets one deadline instead, short of it.
_POLL_DEADLINE_S = 60
# Polls closer together than the interval are answered ``pending`` without
# asking the provider; a second's slack keeps a client polling on time from
# losing every other poll to jitter.
_POLL_SLACK_S = 1.0


def _poll_result(status: str, **extra) -> dict:
    # ``pending`` stays for clients that predate ``status``.
    return {"status": status, "pending": status == "pending", **extra}


@router.post("/{key}/device/initiate")
async def device_initiate(key: str, user_id: CurrentUserId):
    """Start the device code flow: ``{user_code, verification_url,
    verification_url_has_code, interval, expires_at}``, where
    ``verification_url_has_code`` says the page already carries the code. Poll
    every ``interval`` seconds until ``status`` is no longer ``pending``."""
    return await _device_initiate(_device_flow(key), user_id)


@router.post("/{key}/device/poll")
async def device_poll(key: str, user_id: CurrentUserId):
    """Poll for device authorization. ``status`` is ``pending`` (poll again
    after ``interval``), ``success`` ({success: true, email, plan_type,
    account_id}), ``denied`` or ``expired`` (``code`` says whether the code
    expired or no flow is pending); the last two mean start again. A retryable
    error (``upstream_unavailable``) means poll again."""
    return await _device_poll(_device_flow(key), user_id)


# Each start asks the provider for a code from the deployment's one address,
# so a user looping on it would get every user's sign-in throttled there.
_INITIATES_PER_MINUTE = 5
# The code request is bounded per phase only, so a slow trickle could hold the
# request open indefinitely; the whole request gets one deadline.
_INITIATE_DEADLINE_S = 30


async def _device_initiate(flow: DeviceFlow, user_id: str) -> dict:
    cache = _require_cache()
    if not await throttle(f"oauth_device_init:{flow.provider}:{user_id}", _INITIATES_PER_MINUTE, 60):
        # Worded as the provider being busy, which asking again shortly fixes.
        raise _fail(
            429, "upstream_unavailable", "Too many sign-in attempts. Try again in a minute.",
            retryable=True,
        )
    try:
        async with asyncio.timeout(_INITIATE_DEADLINE_S):
            device = await flow.request()
    except DeviceFlowUnavailable:
        raise _fail(
            502, "device_flow_unavailable", f"{flow.upstream} is not offering device sign-in right now.",
            retryable=False,
        )
    except Exception as e:
        logger.error(f"[oauth] {flow.provider} device code request failed: {type(e).__name__}")
        raise _fail(
            502, "upstream_unavailable", f"Could not get a sign-in code from {flow.upstream}. Try again.",
            retryable=True,
        )

    expires_at = device.expires_at.isoformat()
    ttl = int((device.expires_at - datetime.now(timezone.utc)).total_seconds())
    # ``flow_id`` tells this sign-in from the one it replaces, whose state may
    # otherwise match byte for byte.
    state = {
        **device.state, "expires_at": expires_at, "interval": device.interval,
        "next_poll_at": 0, "flow_id": secrets.token_hex(8),
    }
    await cache.client.set(f"{flow.redis_key}:{user_id}", json.dumps(state), ex=max(1, ttl))
    logger.info(f"[oauth] {flow.provider} device code initiated for user_id={user_id}")
    return {
        "user_code": device.user_code,
        "verification_url": device.verification_url,
        "verification_url_has_code": device.url_has_code,
        "interval": device.interval,
        "expires_at": expires_at,
    }


async def _device_poll(flow: DeviceFlow, user_id: str) -> dict:
    cache = _require_cache()
    lock_key = f"{flow.redis_key}:poll:{user_id}"
    lock_token = secrets.token_hex(16)
    acquired = await cache.acquire_lock(lock_key, lock_token, _POLL_LOCK_TTL_MS)
    if acquired is False:
        return _poll_result("pending")
    if acquired is None:
        # Redis failed to answer: without the lock two polls could both spend
        # the approval, so this one waits for the next.
        raise _fail(
            503, "redis_unavailable",
            "Connecting an account is unavailable right now. Try again shortly.",
            retryable=True,
        )
    try:
        return await _poll_once(cache, flow, user_id)
    finally:
        await cache.release_lock(lock_key, lock_token)


def _deadline(state: dict) -> datetime:
    # A sign-in an older worker started has no deadline of its own; its Redis
    # key, which expires with the code, ends it instead.
    raw = state.get("expires_at")
    if raw:
        return datetime.fromisoformat(raw)
    return datetime.now(timezone.utc) + timedelta(minutes=15)


async def _poll_once(cache, flow: DeviceFlow, user_id: str) -> dict:
    key = f"{flow.redis_key}:{user_id}"
    raw = await cache.client.get(key)
    if not raw:
        return _poll_result("expired", code="no_pending_flow")

    state = json.loads(raw)
    now = datetime.now(timezone.utc).timestamp()
    if now < state.get("next_poll_at", 0) - _POLL_SLACK_S:
        return _poll_result("pending")
    # Every write below is conditioned on ``raw``, the state this poll read:
    # starting again rewrites the key while this request may still be
    # waiting on the provider, and the new sign-in must be left alone.

    try:
        async with asyncio.timeout(_POLL_DEADLINE_S):
            result = await flow.poll(state, _deadline(state))
    except (DeviceUpstreamUnavailable, TimeoutError) as e:
        logger.warning(
            f"[oauth] {flow.provider} device poll unavailable for user_id={user_id}: {type(e).__name__} {e}"
        )
        raise _fail(
            502, "upstream_unavailable", f"{flow.upstream} could not be reached. Still waiting.",
            retryable=True,
        )
    except DevicePollFailed as e:
        logger.error(f"[oauth] {flow.provider} device poll refused for user_id={user_id}: {e}")
        await cache.claim(key, raw)
        raise _fail(
            502, "device_poll_failed", f"{flow.upstream} ended this sign-in. Start again.",
            retryable=False, upstream_code=e.code,
        )
    except Exception as e:
        # The approval came back but could not be turned into tokens.
        logger.error(
            f"[oauth] {flow.provider} device token exchange failed for user_id={user_id}: "
            f"{type(e).__name__}"
        )
        failure = exchange_failure(flow.upstream, e)
        if not failure.error["retryable"]:
            await cache.claim(key, raw)
        raise failure

    if result.status == "pending":
        interval = int(state.get("interval", 5))
        if result.code == "slow_down":
            interval += SLOW_DOWN_STEP
        state.update(interval=interval, next_poll_at=now + interval)
        await cache.swap(key, raw, json.dumps(state))
        return _poll_result("pending")
    if result.status in ("denied", "expired"):
        await cache.claim(key, raw)
        logger.info(
            f"[oauth] {flow.provider} device flow {result.status} for user_id={user_id} "
            f"(upstream code={result.code})"
        )
        code = "access_denied" if result.status == "denied" else "device_code_expired"
        return _poll_result(result.status, code=code)

    tokens = result.tokens
    if not await _save(cache, key, raw, user_id, flow.provider, tokens):
        return _poll_result("expired", code="no_pending_flow")
    logger.info(f"[oauth] {flow.provider} connected for user_id={user_id} plan={tokens.plan_type}")
    return _poll_result(
        "success", success=True,
        email=tokens.email, plan_type=tokens.plan_type, account_id=tokens.account_id,
    )


# ─── Claude: PKCE authorization code flow ────────────────────────────────────

CLAUDE_FLOW_TTL = 600


class ClaudeCallbackRequest(BaseModel):
    callback_input: str


def _claude_key(user_id: str) -> str:
    return f"oauth:claude:{user_id}"


class PendingSignIn:
    """A Claude PKCE sign-in waiting for the code Claude's page shows, kept in
    Redis under ``key``: its verifier and state, and once a paste of its code
    saved, a marker naming that state, which answers a second paste of it.

    The kept pair is the sign-in's id: starting again writes a new one, and a
    save claims (compare-and-delete) the pair it read, so the approval of a
    sign-in the user restarted cannot land.
    """

    def __init__(self, key: str) -> None:
        self.cache = _require_cache()
        self.key = key

    @staticmethod
    def expired() -> OAuthRefusal:
        return _fail(
            400, "no_pending_flow", "This sign-in has expired or was already used. Start again.",
            retryable=False,
        )

    async def start(self, authorize) -> dict:
        """Keep a new sign-in, replacing any pending one, and return its
        authorize URL. Once the pair expires a pasted code has nothing to
        match, so the page counts down to the same moment."""
        authorize_url, verifier, state = authorize()
        await self.cache.client.set(self.key, json.dumps({"verifier": verifier, "state": state}), ex=CLAUDE_FLOW_TTL)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=CLAUDE_FLOW_TTL)
        return {"authorize_url": authorize_url, "expires_at": expires_at.isoformat()}

    async def read(self) -> tuple[str, str, str]:
        """The pending pair as kept (what ``claim`` compares), its verifier and its state."""
        raw = await self.cache.client.get(self.key)
        if raw is None:
            raise PendingSignIn.expired()
        raw = raw.decode() if isinstance(raw, bytes) else raw
        try:
            flow = json.loads(raw)
            return raw, flow["verifier"], flow["state"]
        except (TypeError, ValueError, KeyError):
            # A sign-in a worker started before this format kept the bare
            # verifier, which was also its state.
            return raw, raw, raw

    @staticmethod
    def code(callback_input: str, state: str) -> str:
        """The code in a paste from Claude's callback page, once its state names this sign-in."""
        try:
            code, pasted_state = claude_oauth.parse_callback_input(callback_input)
        except ValueError:
            raise _fail(
                400, "invalid_callback_input",
                "That doesn't look like the code from Claude's page. Paste the whole code.", retryable=True,
            )
        if not secrets.compare_digest(pasted_state.encode(errors="surrogatepass"), state.encode()):
            raise _fail(
                400, "state_mismatch", "That code belongs to a different sign-in. Start again.",
                retryable=False,
            )
        return code

    async def claim(self, raw: str) -> bool:
        """End the sign-in ``read`` returned; False when a newer one replaced it."""
        return await self.cache.claim(self.key, raw)

    async def mark_saved(self, state: str, ttl: int) -> None:
        await self.cache.client.set(f"{self.key}:saved", state, ex=ttl)

    async def saved(self, callback_input: str) -> bool:
        """Whether a paste just ahead of this one saved the sign-in it is for.

        Read from the marker, which names its sign-in by its state, rather
        than from the stored connection: on a reconnect whose save failed the
        old connection still stands, and it is not this one.
        """
        try:
            _, state = claude_oauth.parse_callback_input(callback_input)
        except ValueError:
            return False
        saved = await self.cache.client.get(f"{self.key}:saved")
        return (saved.decode() if isinstance(saved, bytes) else saved) == state


@router.post("/claude/initiate")
async def claude_initiate(user_id: CurrentUserId):
    """Generate a PKCE pair and return the authorize URL.

    The frontend opens it in a new tab; once the user authorizes, Anthropic's
    page shows a code they paste back via POST /claude/callback.
    """
    started = await PendingSignIn(_claude_key(user_id)).start(claude_oauth.generate_authorize_url)
    logger.info(f"[oauth] Claude OAuth initiated for user_id={user_id}")
    return started


@router.post("/claude/callback")
async def claude_callback(user_id: CurrentUserId, body: ClaudeCallbackRequest):
    """Exchange the pasted callback for tokens.

    Accepts the full callback URL, ``code#state``, or ``code=X&state=Y``.

    One at a time per user: the code is single-use, so a second paste of it (a
    second tab, a resubmit after the first timed out at the client) would have
    the provider refuse it and report a failure for a sign-in that worked. The
    second waits instead, and answers as the first did when that one connected
    this same sign-in.
    """
    flow = PendingSignIn(_claude_key(user_id))
    cache = flow.cache
    lock_key = f"{flow.key}:callback"
    lock_token = secrets.token_hex(16)
    deadline = asyncio.get_running_loop().time() + _CALLBACK_WAIT_S
    waited = False
    while (acquired := await cache.acquire_lock(lock_key, lock_token, _CALLBACK_LOCK_TTL_MS)) is False:
        if asyncio.get_running_loop().time() > deadline:
            acquired = None
            break
        waited = True
        await asyncio.sleep(0.2)
    if acquired is None:
        raise _fail(
            503, "redis_unavailable",
            "Connecting an account is unavailable right now. Try again shortly.",
            retryable=True,
        )
    try:
        if waited and await flow.saved(body.callback_input):
            return {"success": True}
        return await _claude_exchange(flow, user_id, body)
    finally:
        await cache.release_lock(lock_key, lock_token)


# The exchange gets one deadline for the whole of it (httpx's timeout bounds
# each phase only, so a slow trickle could outlast any lease); the lease
# outlasts the deadline, and a second paste waits about as long.
_CALLBACK_DEADLINE_S = 30
_CALLBACK_LOCK_TTL_MS = 45_000
_CALLBACK_WAIT_S = 40.0


async def _claude_exchange(flow: PendingSignIn, user_id: str, body: ClaudeCallbackRequest) -> dict:
    stored, verifier, state = await flow.read()
    code = flow.code(body.callback_input, state)

    try:
        async with asyncio.timeout(_CALLBACK_DEADLINE_S):
            tokens = await claude_oauth.exchange_code(code, state, verifier)
    except Exception as e:
        logger.error(f"[oauth] Claude token exchange failed for user_id={user_id}: {type(e).__name__}")
        raise exchange_failure("Claude", e)

    if not await _save(flow.cache, flow.key, stored, user_id, claude_oauth.CLAUDE_PROVIDER, tokens):
        raise PendingSignIn.expired()
    # For a second paste of this code waiting on this one (``PendingSignIn.saved``).
    await flow.mark_saved(state, int(_CALLBACK_WAIT_S * 2))
    logger.info(f"[oauth] Claude connected for user_id={user_id}")
    return {"success": True}


# ─── Status and disconnect ───────────────────────────────────────────────────

@router.get("/{key}/status", response_model=OAuthStatus)
async def oauth_status(key: str, user_id: CurrentUserId) -> OAuthStatus:
    """The connection's status, read without decrypting its tokens."""
    return OAuthStatus.from_row(await get_oauth_connection(user_id, _subscription(key)))


def _pending_flow_key(key: str, user_id: str) -> str | None:
    if key in _DEVICE_FLOWS:
        return f"{_DEVICE_FLOWS[key].redis_key}:{user_id}"
    return _claude_key(user_id) if key == "claude" else None


@router.delete("/{key}")
async def oauth_disconnect(key: str, user_id: CurrentUserId):
    """Delete the stored tokens, and first any sign-in still pending: its save
    claims the flow before writing (``_save``), so with the flow gone an
    approval arriving after the disconnect cannot bring the connection back,
    and one already saving finishes before this deletes."""
    provider = _subscription(key)
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    async with connection_change(user_id, provider) as conn:
        flow_key = _pending_flow_key(key, user_id)
        if flow_key and cache.enabled and cache.client:
            await cache.client.delete(flow_key)
        await delete_oauth_tokens(user_id, provider, conn=conn)
    await connection_changed(user_id)
    logger.info(f"[oauth] {provider} disconnected for user_id={user_id}")
    return {"success": True}
