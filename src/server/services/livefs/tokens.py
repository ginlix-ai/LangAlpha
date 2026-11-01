"""The token a computer's file mount authenticates with.

Opaque rather than signed: the server stores only its digest, so ending every
token a computer holds is one row delete, and a self-hosted server has no
signing secret to configure. The shape is ``lfs1.<computer_id>.<secret>``;
the computer id only says which row to compare against.

A token lives about an hour. Once the current one runs low, a turn start or a
command that runs code has the server write a fresh one in the background,
so a copy taken out of the sandbox stops working within the hour, and at
once when the computer stops.

Every file operation authenticates, so Redis mirrors the row: one hash per
computer holding each live digest (by prefix) with its expiry and who it acts
for. A request then costs one script that checks the digest and takes from
the computer's rate in the same round trip, and the database only answers
what the mirror does not hold. The row stays the truth: minting and revoking
drop the mirror, and a generation counter they bump fences out a copy read
from the row before the change, so no revoked or rotated-out token is put
back. With Redis down, authentication reads the row and the rate is not
limited.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from src.server.auth import computer_token_core as core
from src.server.database import livefs_tokens as db
from src.server.services.livefs import cache

logger = logging.getLogger(__name__)

PREFIX = "lfs1"
# Rewrite once less than this is left. It has to outlast the longest command
# between two rewrites, since a background process keeps the token it saw.
REFRESH_BELOW = timedelta(minutes=30)
# Below this a token no longer carries a command, so it is replaced before
# one runs rather than in the background.
LAPSED_BELOW = timedelta(minutes=1)

KIND = core.Kind(
    PREFIX,
    cache.tag,
    # Per computer. The daemon paces itself below this (about 20 a second,
    # in bursts of about 100), so it binds only on a client that does not:
    # an older daemon, or a token taken out of the sandbox.
    rate_per_s=40,
    burst=400,
    ttl=timedelta(hours=1),
    client=lambda: cache.client(),
)

MintedToken = core.Minted


class LivefsAuthError(Exception):
    """The presented token is not a live one (never says why)."""


class LivefsThrottled(Exception):
    """A live token whose computer is past its request rate."""

    def __init__(self, identity: LivefsIdentity, retry_after: int) -> None:
        super().__init__(identity.computer_id)
        self.identity = identity
        #: Whole seconds, for ``Retry-After``.
        self.retry_after = retry_after


@dataclass(frozen=True)
class LivefsIdentity:
    computer_id: str
    user_id: str
    root_dir: str


class NotServing(Exception):
    """The computer is leaving service or gone, so it gets no new token."""


async def mint_token(computer_id: str, user_id: str) -> MintedToken:
    minted = await core.mint(
        KIND,
        computer_id,
        lambda digest, expires_at: db.save_token(computer_id, user_id, digest, expires_at),
    )
    if minted is None:
        raise NotServing(f"computer {computer_id} is not serving, so it gets no token")
    return minted


def runs_low(expires_at: datetime | None) -> bool:
    """Whether a token expiring then is due for a rewrite; unknown is."""
    return expires_at is None or expires_at - datetime.now(UTC) < REFRESH_BELOW


def lapsed(expires_at: datetime | None) -> bool:
    """Whether a token expiring then is as good as gone; unknown is."""
    return expires_at is None or expires_at - datetime.now(UTC) < LAPSED_BELOW


async def revoke(computer_id: str) -> None:
    await core.revoke(KIND, computer_id, lambda: db.delete_token(computer_id))


def _presented(authorization: str | None) -> tuple[str, bytes]:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer":
        raise LivefsAuthError
    try:
        return core.parse(token, PREFIX)
    except core.Refused:
        raise LivefsAuthError from None


def _identity(computer_id: str, who: list[str]) -> LivefsIdentity:
    user_id, root_dir = who
    return LivefsIdentity(computer_id, user_id, root_dir)


async def authenticate(authorization: str | None) -> LivefsIdentity:
    """Who a request acts for, counted against its computer's rate.

    Raises ``LivefsAuthError`` for a token that is not live, and
    ``LivefsThrottled`` once the computer is past its rate, before anything
    of the request runs, so a retried save never lands twice.
    """
    computer_id, digest = _presented(authorization)
    try:
        admitted = await core.admit(
            KIND,
            computer_id,
            digest,
            lambda: db.load_token(computer_id),
            lambda row: [row["user_id"], row["root_dir"]],
        )
    except core.Refused:
        raise LivefsAuthError from None
    except core.Throttled as throttled:
        raise LivefsThrottled(
            _identity(throttled.computer_id, throttled.who), throttled.retry_after
        ) from None
    return _identity(admitted.computer_id, admitted.who)
