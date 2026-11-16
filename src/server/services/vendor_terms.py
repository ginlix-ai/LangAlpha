"""A vendor's Terms of Service, put to the user before the deployment creates
the user's account at that vendor.

Nothing records the answer: the account, created when the user accepts, is
the record, so a user with one is never asked. The ask is Redis only: the
relay refuses a call that would need an account the user does not have,
marks the vendor pending for the user (``vendorterms:{user}:{vendor}``,
holding when it was first asked, gone an hour after the last ask, so an agent
retrying keeps one ask rather than starting a new one each call), and tells
the user's open apps, which ask them. Losing the mark loses only the dialog:
the agent's next call marks it again, and the feed snapshot lists what is
pending for an app that missed the event.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Final, TypeVar

from redis.exceptions import RedisError

from src.server.services.egress import AccountUnavailable
from src.server.services.thread_lifecycle_feed import publish_user_event
from src.utils.cache.redis_cache import get_cache_client

if TYPE_CHECKING:
    from src.server.services.egress.platform_accounts import PlatformAccount

logger = logging.getLogger(__name__)

EVENT_TYPE: Final = "vendor_terms"
TERMS_REQUIRED: Final = "terms_required"
PENDING_TTL: Final = timedelta(hours=1)

_DOWN: Final = (RedisError, OSError, asyncio.TimeoutError)

T = TypeVar("T")


class Unavailable(Exception):
    """Redis cannot be reached, so what is pending cannot be read."""


class Unknown(Exception):
    """No vendor by that name asks for terms."""


def terms_for(vendor: str) -> PlatformAccount | None:
    """The platform account whose terms ``vendor`` names."""
    # Imported here: the registry imports the accounts that call this gate.
    from src.server.services.egress.platform_accounts import PLATFORM_ACCOUNTS

    return next((a for a in PLATFORM_ACCOUNTS.values() if a.vendor == vendor), None)


def _vendors() -> list[str]:
    from src.server.services.egress.platform_accounts import PLATFORM_ACCOUNTS

    return [a.vendor for a in PLATFORM_ACCOUNTS.values()]


def _pending_key(user_id: str, vendor: str) -> str:
    return f"vendorterms:{{{user_id}}}:{vendor}"


def _client() -> Any:
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise Unavailable()
    return cache.client


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def announce(user_id: str, vendors: list[str]) -> None:
    """Tell the user's open apps that what is pending for ``vendors`` changed;
    best effort, since the snapshot lists what is pending."""
    await publish_user_event(user_id, {"type": EVENT_TYPE, "vendors": vendors})


async def ask(user_id: str, vendor: str, account: Callable[[], Awaitable[T | None]]) -> T:
    """Ask the user to accept ``vendor``'s terms, which creates the account,
    and refuse the call that needs it; unless ``account`` (the user's account
    there, read fresh) shows they already have.

    ``account`` is read after the mark is written: an accept creates the
    account before it clears the mark, so one that cleared it before this
    wrote it is seen here, and the mark it would have left is taken back.
    The refusal is the same whether or not the mark could be written.
    """
    key, ttl_ms = _pending_key(user_id, vendor), int(PENDING_TTL.total_seconds() * 1000)
    added = False
    try:
        async with _client().pipeline(transaction=True) as pipe:
            pipe.set(key, int(time.time() * 1000), nx=True, px=ttl_ms)
            pipe.pexpire(key, ttl_ms)
            added, _ = await pipe.execute()
    except (Unavailable, *_DOWN) as exc:
        logger.warning("vendor terms ask not recorded (user=%s %s): %s", user_id, vendor, exc)
    if (found := await account()) is not None:
        await _clear(user_id, vendor)
        # A mark this call wrote was never announced; one it found may be
        # showing, and is now gone.
        if not added:
            await announce(user_id, [vendor])
        return found
    # Only a new ask: an agent retrying would otherwise announce each call.
    if added:
        await announce(user_id, [vendor])
    raise AccountUnavailable(TERMS_REQUIRED, f"the user has not accepted {vendor}'s terms")


async def _clear(user_id: str, vendor: str) -> None:
    try:
        await _client().delete(_pending_key(user_id, vendor))
    except (Unavailable, *_DOWN) as exc:
        # Left standing, the ask lapses within the hour, and accepting it
        # again finds the account already there.
        logger.warning("vendor terms ask not cleared (user=%s %s): %s", user_id, vendor, exc)


async def pending(user_id: str) -> list[dict[str, Any]]:
    """The terms the user is being asked to accept, oldest ask first.

    Raises:
        Unavailable: Redis cannot be reached.
    """
    vendors = _vendors()
    try:
        stored = await _client().mget([_pending_key(user_id, v) for v in vendors])
    except _DOWN as exc:
        raise Unavailable() from exc
    asked = {vendor: int(_text(ms)) for vendor, ms in zip(vendors, stored) if ms is not None}
    out = []
    for vendor, ms in sorted(asked.items(), key=lambda item: item[1]):
        account = terms_for(vendor)
        out.append({
            "vendor": vendor,
            "name": account.name,
            "terms_url": account.terms_url,
            "requested_at": datetime.fromtimestamp(ms / 1000, UTC).isoformat(),
        })
    return out


async def accept(user_id: str, vendor: str) -> dict[str, Any]:
    """Create the user's account at the vendor, which they agreed to by
    accepting its terms. A failure leaves the ask pending, to answer again.

    Raises:
        Unknown: no such vendor.
        AccountUnavailable: the account could not be created.
        RelayRejection: the user may not have one yet (no credits).
    """
    account = terms_for(vendor)
    if account is None:
        raise Unknown()
    await account.provision(user_id)
    await _clear(user_id, vendor)
    await announce(user_id, [vendor])
    return {"vendor": vendor}


async def dismiss(user_id: str, vendor: str) -> None:
    """Stop asking until the agent's next call needs the terms again.

    Raises:
        Unknown: no such vendor.
    """
    if terms_for(vendor) is None:
        raise Unknown()
    await _clear(user_id, vendor)
    await announce(user_id, [vendor])
