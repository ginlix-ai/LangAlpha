"""The market-data token each computer's sandbox holds.

The server mints from the platform a short-lived token bound to the computer,
with no way to refresh it, and writes it into the sandbox, whose data client
only reads it. A turn's start and a background sweep each write a fresh one
once less than ``REFRESH_BELOW`` is left, and a stop or delete revokes every
token the computer was given, so one taken out of the sandbox stops working
within the hour, or as soon as the computer stops. A revocation the platform
does not confirm stays owed in Postgres, and the sweep sends it again.

Nothing here raises: without a token the market-data tools answer that they
are not configured, which costs data, never a turn or a start.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from src.config.env import AUTH_SERVICE_URL, GINLIX_DATA_URL, PUBLIC_API_BASE_URL
from src.server.database import computer_data_tokens as db

logger = logging.getLogger(__name__)

# Write a fresh token once less than this is left. It has to outlast the
# longest a sandbox goes between two writes: the sweep's interval, and a
# failed write or two.
REFRESH_BELOW = timedelta(minutes=30)

_TIMEOUT_S = 5.0
_REVOKE_ATTEMPTS = 3

# A sandbox holding no live token fails every market-data call until one
# arrives, so its mint keeps trying this long: the platform deploys in place
# and refuses connections for a few seconds. A refresh of a token with time
# left takes one try, so a healthy turn never waits on the platform.
MINT_PATIENCE = timedelta(seconds=10)
_MINT_BACKOFF_S = (0.25, 0.5, 1.0, 2.0)
# A patient mint that waited all of that found the platform down, not
# restarting, so for this long every worker's mints take one try: a start
# keeps the token up to three times, and each would wait again.
_PLATFORM_DOWN_TTL_S = 15

#: How the platform answered a revocation. ``refused`` is a 4xx a retry will
#: not change, for that request alone; ``unreachable`` would fail every other
#: revocation as well: no connection, a 5xx or 429, or the service token refused.
RevokeAnswer = Literal["revoked", "refused", "unreachable"]

#: Writes the token file into a sandbox; True only once the write is confirmed.
Write = Callable[[dict[str, Any]], Awaitable[bool]]


def platform_down_key() -> str:
    return "data_tokens:platform_down"


@dataclass(frozen=True)
class Minted:
    token: str
    #: When the platform issued it, in epoch ms by the one clock its mints and
    #: revocations share, and after any revocation standing then: a later
    #: mint carries a larger one, and the sandbox keeps the larger.
    issued_at_ms: int
    expires_at: datetime


def configured() -> bool:
    return bool(
        AUTH_SERVICE_URL
        and GINLIX_DATA_URL
        and os.getenv("INTERNAL_SERVICE_TOKEN", "")
    )


def due(row: db.DataTokenRow, sandbox_id: str | None) -> bool:
    """Whether ``sandbox_id`` is owed a token: it holds none, or one with
    less than ``REFRESH_BELOW`` left."""
    if row.expires_at is None or row.held_by is None or row.held_by != sandbox_id:
        return True
    return row.expires_at - datetime.now(UTC) < REFRESH_BELOW


def holds(row: db.DataTokenRow, sandbox_id: str | None) -> bool:
    """Whether ``sandbox_id`` holds a token with any time left."""
    return (
        row.held_by is not None
        and row.held_by == sandbox_id
        and row.expires_at is not None
        and row.expires_at > datetime.now(UTC)
    )


async def keep(
    computer_id: str, user_id: str | None, sandbox_id: str | None, write: Write
) -> None:
    """Write a fresh token into ``sandbox_id``, the computer's own sandbox,
    when it is owed one."""
    if not user_id or not sandbox_id or not configured():
        return
    try:
        row = await db.load(str(computer_id))
        if due(row, sandbox_id):
            await push(
                str(computer_id),
                user_id,
                sandbox_id,
                write,
                generation=row.generation,
                patient=not holds(row, sandbox_id),
            )
    except Exception as e:
        logger.warning(
            "Market-data token not kept for computer %s (%s)",
            computer_id,
            type(e).__name__,
        )


async def push(
    computer_id: str,
    user_id: str,
    sandbox_id: str,
    write: Write,
    *,
    generation: int,
    patient: bool = False,
) -> bool:
    """Mint a token, record it over the row read at ``generation``, and write
    it into the sandbox; True once the sandbox holds it. A ``patient`` mint
    retries for up to ``MINT_PATIENCE``."""
    minted = await _mint(user_id, computer_id, patient=patient)
    if minted is None:
        return False
    saved = await db.save(
        computer_id, user_id, sandbox_id, minted.expires_at, generation
    )
    if not saved.serving or saved.generation is None:
        # The computer is leaving service, changed hands or sandboxes, or the
        # row moved since it was read (a newer token, or a revocation). The
        # token never left this process, so it lapses unrevoked: revoking
        # ends every token the computer holds, the live one included, and a
        # stop that completes revokes on its own.
        return False
    if not await write(_payload(minted)):
        return False
    await db.mark_held(computer_id, saved.generation, sandbox_id)
    return True


async def revoke(computer_id: Any) -> None:
    """End every token the computer was given, at a stop or delete, rather
    than when the last one would have run out. Best effort: never fails the
    stop, and one the platform does not confirm stays owed for the sweep."""
    if not configured():
        return
    computer_id = str(computer_id)
    try:
        revocation = await db.revoke(computer_id)
    except Exception as e:
        logger.warning(
            "Market-data token row not revoked for computer %s (%s)",
            computer_id,
            type(e).__name__,
        )
        return
    if revocation is not None:
        await settle(revocation)


async def settle(revocation: db.Revocation) -> RevokeAnswer:
    """Send ``revocation`` to the platform and record what it confirms, then
    put a fresh token in a sandbox the computer is still running on;
    ``revoked`` once every user is confirmed. Never raises.

    The platform refuses every token issued before it handled the request,
    and a start that read the tombstone may have minted one in between: a
    revert's revoke lands after the start it let in, a retry lands whenever
    the platform is back. Nothing else would replace that token until it
    runs low, so it is replaced here, after any user's confirmation, since
    the sandbox's token may be that user's.

    Each user's tokens are revoked on their own, so one the platform refuses
    does not stop the next being sent, and only the users it has not
    confirmed stay owed.
    """
    computer_id = revocation.computer_id
    confirmed: list[str] = []
    answer: RevokeAnswer = "revoked"
    try:
        for user_id in revocation.users:
            sent = await _revoke_upstream(user_id, computer_id)
            if sent == "revoked":
                confirmed.append(user_id)
                continue
            answer = sent
            if sent == "unreachable":
                break
        if not confirmed:
            return answer
        in_service = await db.revoked(revocation, tuple(confirmed))
    except Exception as e:
        logger.warning(
            "Market-data token revocation not settled for computer %s (%s)",
            computer_id,
            type(e).__name__,
        )
        # Postgres failing here fails every other settle too.
        return "unreachable"
    if in_service:
        from src.server.services.data_token_sweep import DataTokenSweeper

        try:
            await DataTokenSweeper.get_instance().rewrite(computer_id)
        except Exception as e:
            logger.warning(
                "Market-data token not rewritten for computer %s after its "
                "revocation (%s)",
                computer_id,
                type(e).__name__,
            )
    return answer


def _payload(minted: Minted) -> dict[str, Any]:
    return {
        "access_token": minted.token,
        # What the sandbox orders tokens by: a write that stalled across a
        # newer one must not put the older, possibly revoked, token back.
        "iat_ms": minted.issued_at_ms,
        "expires_at": minted.expires_at.astimezone(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        # Sandboxes are remote, so they reach ginlix-data at its public address.
        "ginlix_data_url": PUBLIC_API_BASE_URL or GINLIX_DATA_URL.rstrip("/"),
    }


def _url(path: str = "") -> str:
    return f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/data-tokens/computer{path}"


def _expires_at(body: dict[str, Any]) -> datetime:
    raw = body.get("expires_at")
    if raw:
        try:
            parsed = datetime.fromisoformat(str(raw))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC) + timedelta(seconds=int(body.get("expires_in") or 0))


async def _mint(
    user_id: str, computer_id: str, *, patient: bool = False
) -> Minted | None:
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if patient and await cache.get(platform_down_key()):
        patient = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + (MINT_PATIENCE.total_seconds() if patient else 0.0)
    timeout = _TIMEOUT_S
    tries = 0
    while True:
        minted, outcome, retryable = await _mint_once(user_id, computer_id, timeout)
        if minted is not None:
            return minted
        delay = _MINT_BACKOFF_S[min(tries, len(_MINT_BACKOFF_S) - 1)]
        tries += 1
        if not retryable or loop.time() + delay >= deadline:
            break
        await asyncio.sleep(delay)
        # The last try ends near the deadline rather than a full timeout past it.
        timeout = max(1.0, min(_TIMEOUT_S, deadline - loop.time()))
    if patient and retryable:
        await cache.set(platform_down_key(), True, ttl=_PLATFORM_DOWN_TTL_S)
    logger.warning(
        "Market-data token not minted for computer %s (%s%s)",
        computer_id,
        outcome,
        f", {tries} tries" if tries > 1 else "",
    )
    return None


async def _mint_once(
    user_id: str, computer_id: str, timeout: float
) -> tuple[Minted | None, str, bool]:
    """One mint: the token, or why not and whether another try may succeed."""
    from src.server.dependencies.usage_limits import get_http_client, service_headers

    try:
        client = await get_http_client()
        resp = await client.post(
            _url(),
            json={"user_id": user_id, "computer_id": computer_id},
            headers=service_headers(),
            timeout=timeout,
        )
    except Exception as e:
        return None, type(e).__name__, True
    if not resp.is_success:
        status = resp.status_code
        return None, f"HTTP {status}", status >= 500 or status == 429
    try:
        body = resp.json()
        minted = Minted(body["access_token"], int(body["iat_ms"]), _expires_at(body))
    except Exception as e:
        return None, type(e).__name__, False
    return minted, "", False


async def _revoke_upstream(user_id: str, computer_id: str) -> RevokeAnswer:
    from src.server.dependencies.usage_limits import get_http_client, service_headers

    outcome = ""
    answer: RevokeAnswer = "unreachable"
    for attempt in range(_REVOKE_ATTEMPTS):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            client = await get_http_client()
            resp = await client.post(
                _url("/revoke"),
                json={"user_id": user_id, "computer_id": computer_id},
                headers=service_headers(),
                timeout=_TIMEOUT_S,
            )
        except Exception as e:
            outcome = type(e).__name__
            continue
        if resp.is_success:
            return "revoked"
        outcome = f"HTTP {resp.status_code}"
        if resp.status_code < 500 and resp.status_code != 429:
            # A refused service token refuses every revocation alike.
            if resp.status_code not in (401, 403):
                answer = "refused"
            break
    logger.warning(
        "Market-data tokens not revoked for computer %s (%s); the revocation "
        "stays owed",
        computer_id,
        outcome,
    )
    return answer
