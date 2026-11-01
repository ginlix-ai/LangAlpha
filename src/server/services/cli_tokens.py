"""The token the ``la`` CLI presents from inside a computer.

The agent's commands call the API as the computer's user through ``la``, so
the server writes each computer a token of its own: ``lac1.<computer>.<secret>``,
opaque and stored as a digest like the file mount's (see
``src.server.auth.computer_token_core``), but its own kind. It differs from
the mount's in who may read it, since agent code reads this one by design,
and in what it reaches: the API routes that declare a scope in ``SCOPES``,
and on them only what the computer itself holds
(``src.server.dependencies.principal``).

Same lifecycle as the market-data token: written on acquire, start and
reconnect, and by the sweep (``cli_token_sweep``) for a computer left
running, whenever the sandbox holds none with ``REFRESH_BELOW`` left;
revoked at stop and delete, so a copy taken out of the sandbox stops working within the hour, and at once
when the computer stops. Unlike that token it needs no platform: a
self-hosted server mints it too.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Any

from src.server.auth import computer_token_core as core
from src.server.database import computer_cli_tokens as db
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

PREFIX = "lac1"
# Write a fresh token once less than this is left. It has to outlast the
# longest a sandbox goes between two writes: the sweep's interval, and a
# failed write or two.
REFRESH_BELOW = timedelta(minutes=30)

# What a computer's token may do: every one may do the same, so the row keeps
# none. Hard deletes, approvals and orders are not scopes at all: the
# principal refuses them to a computer whatever it holds.
SCOPES: tuple[str, ...] = (
    "agent:read",
    "agent:run",
    "workspace:write",
    "computer:write",
    "data:read",
)


def _client() -> Any:
    cache = get_cache_client()
    return cache.client if cache.enabled and cache.client else None


KIND = core.Kind(
    PREFIX,
    lambda computer_id: f"clitoken:{{{computer_id}}}",
    # Per computer. The CLI makes a handful of calls per command; this binds
    # only on a loop, or a token taken out of the sandbox.
    rate_per_s=10,
    burst=60,
    ttl=timedelta(hours=1),
    client=lambda: _client(),
)

#: Writes the token file into a sandbox; True only once the write is confirmed.
Write = Callable[[dict[str, Any]], Awaitable[bool]]

#: A live token whose computer is past its request rate.
Throttled = core.Throttled


@dataclass(frozen=True)
class CliIdentity:
    computer_id: str
    user_id: str
    scopes: tuple[str, ...] = SCOPES


def is_cli_token(token: str) -> bool:
    return token.startswith(f"{PREFIX}.")


async def authenticate(token: str) -> CliIdentity:
    """Who a token acts for, counted against its computer's rate.

    ``core.Refused`` for a token that is not live, ``Throttled`` once the
    computer is past its rate.
    """
    computer_id, digest = core.parse(token, PREFIX)
    admitted = await core.admit(
        KIND,
        computer_id,
        digest,
        lambda: db.load_token(computer_id),
        lambda row: row["user_id"],
    )
    return CliIdentity(computer_id, admitted.who)


def payload(minted: core.Minted, computer_id: str, base_url: str) -> dict[str, Any]:
    return {
        "token": minted.token,
        # What the sandbox orders writes by: one that stalled across a newer
        # write must not put the older token back. Dated by the mint, not the
        # write, since an older mint's write can resume last.
        "iat_ms": int(minted.issued_at.timestamp() * 1000),
        "computer_id": computer_id,
        # The address this computer reaches the server at, which is not the
        # one a browser uses: the CLI reads it so it needs no configuration.
        "base_url": base_url.rstrip("/"),
        "expires_at": minted.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


async def keep(
    computer_id: Any,
    user_id: str | None,
    sandbox_id: str | None,
    provider: str | None,
    write: Write,
) -> bool:
    """Write a fresh token into ``sandbox_id``, the computer's own sandbox on
    ``provider``, when it is owed one; True once one is written. Never
    raises: a computer without the CLI's token still runs every turn."""
    from src.server.services.egress.reachability import effective_relay_base_url

    if not user_id or not sandbox_id or not provider:
        return False
    computer_id = str(computer_id)
    try:
        # The address the file mount dials too, from the same provider.
        base_url = effective_relay_base_url(provider)
        if not base_url:
            return False
        row = await db.current(computer_id)
        if not core.due(row, sandbox_id, REFRESH_BELOW):
            return False
        minted = await core.mint(
            KIND,
            computer_id,
            lambda digest, expires_at: db.save_token(
                computer_id, user_id, digest, expires_at, row.expires_at
            ),
        )
        if minted is None:
            # The computer is leaving service, or another mint replaced the
            # row since it was read and writes its own token.
            return False
        if not await write(payload(minted, computer_id, base_url)):
            return False
        await db.mark_held(computer_id, sandbox_id, minted.expires_at)
        return True
    except Exception as e:
        logger.warning(
            "CLI token not kept for computer %s (%s)", computer_id, type(e).__name__
        )
        return False


async def revoke(computer_id: Any) -> None:
    """End every token the computer was given, at a stop or delete. Best
    effort: never fails the stop."""
    computer_id = str(computer_id)
    try:
        await core.revoke(KIND, computer_id, lambda: db.delete_token(computer_id))
    except Exception as e:
        # The load is fenced on the computer serving, so a stopped computer's
        # row authenticates nothing once the mirror is gone, and the revoke
        # drops that whether or not the row went.
        logger.warning(
            "CLI token row not revoked for computer %s (%s)",
            computer_id,
            type(e).__name__,
        )
