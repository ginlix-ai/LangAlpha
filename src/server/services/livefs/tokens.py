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

import asyncio
import hashlib
import hmac
import json
import logging
import math
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from src.server.database import livefs_tokens as db
from src.server.services.livefs import cache

logger = logging.getLogger(__name__)

PREFIX = "lfs1"
TOKEN_TTL = timedelta(hours=1)
# Rewrite once less than this is left. It has to outlast the longest command
# between two rewrites, since a background process keeps the token it saw.
REFRESH_BELOW = timedelta(minutes=30)
# Below this a token no longer carries a command, so it is replaced before
# one runs rather than in the background.
LAPSED_BELOW = timedelta(minutes=1)

# Per computer. The daemon paces itself below this (about 20 a second, in
# bursts of about 100), so it binds only on a client that does not: an older
# daemon, or a token taken out of the sandbox.
RATE_PER_S = 40
BURST = 400

# Outlives any read of the row that it fences, by a wide margin.
_GENERATION_TTL_S = 86400
# The mirror lapses well before the tokens it holds, so a revoke whose drop
# never reached Redis, or a change the row saw without one, still ends them
# within minutes; each lapse costs the computer one read of the row.
_MIRROR_TTL = timedelta(minutes=5)
_DIGEST_FIELD_CHARS = 32
_REVOKE_ATTEMPTS = 3
_DROP_RETRY_S = 2.0

# The token bucket, shared by both admission scripts; ``now`` is Redis's
# clock, the one every worker agrees on. Answers the milliseconds until a
# request would be admitted, 0 when this one is.
_TAKE = """
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local function take(key, rate, burst)
  local held = redis.call('HMGET', key, 't', 'ms')
  local tokens = tonumber(held[1]) or burst
  local last = tonumber(held[2]) or now
  tokens = math.min(burst, tokens + math.max(0, now - last) * rate / 1000)
  local wait = 0
  if tokens >= 1 then
    tokens = tokens - 1
  else
    wait = math.ceil((1 - tokens) * 1000 / rate)
  end
  redis.call('HSET', key, 't', tokens, 'ms', now)
  redis.call('PEXPIRE', key, math.ceil(burst * 1000 / rate) + 1000)
  return wait
end
"""

# KEYS: mirror, generation, bucket. ARGV: digest field, rate, burst.
# {1, identity, wait} for a live digest; {0, generation} for one the mirror
# does not hold, which leaves the bucket alone: an unknown token must not
# spend a computer's rate.
_ADMIT = (
    _TAKE
    + """
local entry = redis.call('HGET', KEYS[1], ARGV[1])
if entry then
  local bar = string.find(entry, '|', 1, true)
  if not bar or tonumber(string.sub(entry, 1, bar - 1)) <= now then
    entry = false
  end
end
if not entry then
  return {0, redis.call('GET', KEYS[2]) or '0'}
end
return {1, entry, take(KEYS[3], tonumber(ARGV[2]), tonumber(ARGV[3]))}
"""
)

# After the row admitted a digest the mirror lacked. KEYS as _ADMIT. ARGV:
# the generation read before the row, rate, burst, then the mirror (expiry
# in ms, field, entry, ...), written only if nothing changed the token set
# since. Answers the wait.
_ADMIT_FROM_ROW = (
    _TAKE
    + """
if #ARGV > 4 and (redis.call('GET', KEYS[2]) or '0') == ARGV[1] then
  redis.call('DEL', KEYS[1])
  for i = 5, #ARGV, 2 do
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
  end
  redis.call('PEXPIREAT', KEYS[1], ARGV[4])
end
return take(KEYS[3], tonumber(ARGV[2]), tonumber(ARGV[3]))
"""
)

# The token set changed: drop the mirror, and bump the generation so a copy
# read from the row before the change is not written back. KEYS: mirror,
# generation. ARGV: the generation's TTL.
_DROP = """
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
return 1
"""


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


@dataclass(frozen=True)
class MintedToken:
    token: str
    expires_at: datetime


def _digest(secret: str) -> bytes:
    return hashlib.sha256(secret.encode()).digest()


def _keys(computer_id: str) -> tuple[str, str, str]:
    tag = cache.tag(computer_id)
    return f"{tag}:auth", f"{tag}:gen", f"{tag}:rate"


def _field(digest: bytes) -> str:
    return bytes(digest).hex()[:_DIGEST_FIELD_CHARS]


def _mirror(
    user_id: str, root_dir: str, slots: list[tuple[bytes | None, datetime | None]]
) -> list[Any]:
    """The mirror of a row's live slots, as script arguments: when the
    mirror lapses, then each digest's field and entry. Empty when none is live."""
    now = datetime.now(UTC)
    who = json.dumps([user_id, root_dir])
    live = [(d, e) for d, e in slots if d is not None and e is not None and e > now]
    if not live:
        return []
    until = min(max(e for _, e in live), now + _MIRROR_TTL)
    args: list[Any] = [int(until.timestamp() * 1000)]
    for digest, expires_at in live:
        args += [_field(digest), f"{int(expires_at.timestamp() * 1000)}|{who}"]
    return args


async def _drop_mirror(computer_id: str) -> bool:
    client = cache.client()
    if client is None:
        return True
    try:
        await client.eval(_DROP, 2, *_keys(computer_id)[:2], _GENERATION_TTL_S)
    except Exception:
        logger.debug("livefs token mirror not dropped", exc_info=True)
        return False
    return True


async def mint_token(computer_id: str, user_id: str) -> MintedToken:
    secret = secrets.token_urlsafe(32)
    expires_at = datetime.now(UTC) + TOKEN_TTL
    if not await db.save_token(computer_id, user_id, _digest(secret), expires_at):
        raise NotServing(f"computer {computer_id} is not serving, so it gets no token")
    # The new token's first request reads the row and writes the mirror back.
    # Left standing, the mirror would admit the token this mint pushed out of
    # the row until it lapses.
    if not await _drop_mirror(computer_id):
        logger.warning("livefs token mirror not dropped for computer %s", computer_id)
    return MintedToken(f"{PREFIX}.{computer_id}.{secret}", expires_at)


def runs_low(expires_at: datetime | None) -> bool:
    """Whether a token expiring then is due for a rewrite; unknown is."""
    return expires_at is None or expires_at - datetime.now(UTC) < REFRESH_BELOW


def lapsed(expires_at: datetime | None) -> bool:
    """Whether a token expiring then is as good as gone; unknown is."""
    return expires_at is None or expires_at - datetime.now(UTC) < LAPSED_BELOW


async def revoke(computer_id: str) -> None:
    try:
        await db.delete_token(computer_id)
    finally:
        # Even when the row stays: nothing rewrites the mirror after a revoke,
        # so one left standing would serve the ended tokens until it lapses.
        await _drop_mirror_now(computer_id)


async def _drop_mirror_now(computer_id: str) -> None:
    for attempt in range(_REVOKE_ATTEMPTS):
        if await _drop_mirror(computer_id):
            return
        await asyncio.sleep(0.1 * (attempt + 1))
    logger.error("livefs token mirror not dropped for computer %s; retrying", computer_id)
    task = asyncio.create_task(_drop_later(computer_id))
    _dropping.add(task)
    task.add_done_callback(_dropping.discard)


# Drops a revoke still owes, held so a pending one is not collected.
_dropping: set[asyncio.Task[None]] = set()


async def _drop_later(computer_id: str) -> None:
    """Keep trying to drop a mirror a revoke could not, for as long as it
    could still answer: a Redis back within that time must not serve it."""
    deadline = asyncio.get_running_loop().time() + _MIRROR_TTL.total_seconds()
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(_DROP_RETRY_S)
        if await _drop_mirror(computer_id):
            logger.info("livefs token mirror dropped for computer %s", computer_id)
            return


def _live(stored: bytes | None, expires_at: datetime | None, digest: bytes) -> bool:
    if stored is None or expires_at is None or expires_at <= datetime.now(UTC):
        return False
    return hmac.compare_digest(bytes(stored), digest)


def _presented(authorization: str | None) -> tuple[str, bytes]:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer":
        raise LivefsAuthError
    prefix, _, rest = token.strip().partition(".")
    computer_id, _, secret = rest.partition(".")
    if prefix != PREFIX or not secret:
        raise LivefsAuthError
    try:
        computer_id = str(uuid.UUID(computer_id))
    except ValueError:
        raise LivefsAuthError from None
    return computer_id, _digest(secret)


def _admitted(identity: LivefsIdentity, wait_ms: Any) -> LivefsIdentity:
    wait_ms = int(wait_ms or 0)
    if wait_ms > 0:
        raise LivefsThrottled(identity, max(1, math.ceil(wait_ms / 1000)))
    return identity


async def authenticate(authorization: str | None) -> LivefsIdentity:
    """Who a request acts for, counted against its computer's rate.

    Raises ``LivefsAuthError`` for a token that is not live, and
    ``LivefsThrottled`` once the computer is past its rate, before anything
    of the request runs, so a retried save never lands twice.
    """
    computer_id, digest = _presented(authorization)
    keys = _keys(computer_id)
    client = cache.client()
    seen = "-"
    if client is not None:
        try:
            answer = await client.eval(
                _ADMIT, 3, *keys, _field(digest), RATE_PER_S, BURST
            )
        except Exception:
            logger.debug("livefs admission script failed", exc_info=True)
            client = None
        else:
            if int(answer[0]) == 1:
                user_id, root_dir = json.loads(cache.text(answer[1]).partition("|")[2])
                return _admitted(LivefsIdentity(computer_id, user_id, root_dir), answer[2])
            seen = cache.text(answer[1])
    row = await db.load_token(computer_id)
    if row is None:
        raise LivefsAuthError
    if not (
        _live(row["token_sha256"], row["expires_at"], digest)
        or _live(row["prev_token_sha256"], row["prev_expires_at"], digest)
    ):
        raise LivefsAuthError
    identity = LivefsIdentity(computer_id, row["user_id"], row["root_dir"])
    if client is None:
        return identity
    mirror = _mirror(
        row["user_id"],
        row["root_dir"],
        [
            (row["token_sha256"], row["expires_at"]),
            (row["prev_token_sha256"], row["prev_expires_at"]),
        ],
    )
    try:
        wait = await client.eval(
            _ADMIT_FROM_ROW, 3, *keys, seen, RATE_PER_S, BURST, *mirror
        )
    except Exception:
        logger.debug("livefs admission script failed", exc_info=True)
        return identity
    return _admitted(identity, wait)
