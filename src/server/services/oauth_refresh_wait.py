"""What a request does while another one holds the lock to refresh the same
OAuth token."""

import asyncio
from datetime import datetime, timedelta, timezone

from src.server.database.oauth_tokens import get_oauth_tokens

# How close to expiry a token is refreshed rather than used.
REFRESH_BUFFER = timedelta(minutes=5)
# The refresh lock lives 35 seconds; a provider answers a refresh in a few,
# so a waiter gives up well before the lock would.
REFRESH_LOCK_TTL_MS = 35_000
# The whole refresh call, inside the lease: httpx bounds each phase only, so a
# slow trickle could outlast it and let a second worker spend the same
# single-use refresh token.
REFRESH_DEADLINE_S = 25
# Saving what the refresh returned, retries included, in what the lease has left.
REFRESH_SAVE_DEADLINE_S = 8
_WAIT_SECONDS = 10.0
_POLL_SECONDS = 0.5


def token_expiry(tokens: dict) -> datetime:
    expires_at = tokens["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at


async def await_refreshed_tokens(user_id: str, provider: str) -> dict | None:
    """The stored tokens once the refresh lands; None when it has not landed
    in time and the stored token has already expired. A row the refresh
    flagged as needing sign-in comes back at once, for the caller to refuse.

    The old token is handed out only while it still works: an expired one
    comes back from the provider as a 401, which reads the same as a key the
    user revoked, and an automation is switched off for that.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _WAIT_SECONDS
    while True:
        await asyncio.sleep(_POLL_SECONDS)
        tokens = await get_oauth_tokens(user_id, provider)
        if not tokens:
            return None
        if tokens.get("needs_reauth_at"):
            return tokens
        now = datetime.now(timezone.utc)
        if token_expiry(tokens) > now + REFRESH_BUFFER:
            return tokens
        if loop.time() >= deadline:
            return tokens if token_expiry(tokens) > now else None
