"""A subscription sign-in's pieces that outlive one request: the refusal its
routes answer with, the provider's code exchange read by whether asking again
can help, and a Claude sign-in waiting for its pasted code. Shared by the
OAuth routes and Claude Code's token flows (``services.claude_code``).
"""

import json
import secrets
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import HTTPException

from src.server.services import claude_oauth
from src.server.services.oauth_protocol import upstream_error_code

#: How long a Claude sign-in waits for its code.
CLAUDE_FLOW_TTL = 600


class OAuthRefusal(HTTPException):
    """A failure from a sign-in's routes: ``detail`` the sentence, ``error`` its
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


def require_cache():
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


class PendingSignIn:
    """A Claude PKCE sign-in waiting for the code Claude's page shows, kept in
    Redis under ``key``: its verifier and state, and once a paste of its code
    saved, a marker naming that state, which answers a second paste of it.

    The kept pair is the sign-in's id: starting again writes a new one, and a
    save claims (compare-and-delete) the pair it read, so the approval of a
    sign-in the user restarted cannot land.
    """

    def __init__(self, key: str) -> None:
        self.cache = require_cache()
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
