"""What every opaque token a computer holds shares: the shape, the digest,
the mint and revoke, and the Redis mirror that admits one and takes from the
computer's rate in a single round trip.

A token is ``<prefix>.<computer_id>.<secret>`` and the server keeps only the
secret's SHA-256, in a row with the current and previous digest, so a rewrite
never races a request already in flight and ending every token the computer
holds is one row delete. Each kind (the file mount's, the CLI's) has its own
table, keys and rate; this module never touches a table itself, the kind
handing it the row's save, load and delete.
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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# Outlives any read of the row that it fences, by a wide margin.
GENERATION_TTL_S = 86400
# The mirror lapses well before the tokens it holds, so a revoke whose drop
# never reached Redis, or a change the row saw without one, still ends them
# within minutes; each lapse costs the computer one read of the row.
MIRROR_TTL = timedelta(minutes=5)
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
# {1, identity, wait} for a live digest; {2} for one a standing mirror does
# not hold, refused without reading the row, since the mirror holds every
# live digest the row does (each mint and revoke drops it, and a copy read
# before either is never written back); {0, generation} with no mirror. A
# digest not admitted leaves the bucket alone: an unknown token must not
# spend a computer's rate.
ADMIT = (
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
  if redis.call('EXISTS', KEYS[1]) == 1 then
    return {2}
  end
  return {0, redis.call('GET', KEYS[2]) or '0'}
end
return {1, entry, take(KEYS[3], tonumber(ARGV[2]), tonumber(ARGV[3]))}
"""
)

# After the row admitted a digest the mirror lacked. KEYS as ADMIT. ARGV:
# the generation read before the row, rate, burst, then the mirror (expiry
# in ms, field, entry, ...), written only if nothing changed the token set
# since. Answers the wait.
ADMIT_FROM_ROW = (
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
DROP = """
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[1])
return 1
"""


class Refused(Exception):
    """The presented token is not a live one (never says why)."""


class Throttled(Exception):
    """A live token whose computer is past its request rate."""

    def __init__(self, computer_id: str, who: Any, wait_ms: int) -> None:
        super().__init__(computer_id)
        self.computer_id = computer_id
        #: What the row says the token acts for, as the kind serialized it.
        self.who = who
        #: Milliseconds until the computer's rate would admit the request.
        self.wait_ms = wait_ms

    @property
    def retry_after(self) -> int:
        """Whole seconds, for ``Retry-After``."""
        return max(1, math.ceil(self.wait_ms / 1000))


@dataclass(frozen=True)
class Admitted:
    computer_id: str
    #: What the row says the token acts for, as the kind serialized it.
    who: Any


@dataclass(frozen=True)
class Minted:
    token: str
    expires_at: datetime
    issued_at: datetime


@dataclass(frozen=True)
class Kind:
    """One kind of computer token: how it is spelled, where Redis keeps it,
    how long one lives, and how fast one computer's tokens may be used."""

    prefix: str
    #: The prefix of every Redis key one computer's tokens of this kind keep;
    #: braced, so one script may take several keys in a cluster.
    key_tag: Callable[[str], str]
    rate_per_s: float
    burst: int
    ttl: timedelta
    #: The Redis client, or None with Redis off; asked at each use, since a
    #: revoke's drop keeps retrying across an outage.
    client: Callable[[], Any]

    def keys(self, computer_id: str) -> tuple[str, str, str]:
        tag = self.key_tag(computer_id)
        return f"{tag}:auth", f"{tag}:gen", f"{tag}:rate"


class Held(Protocol):
    """What a row records of the token a sandbox was written."""

    #: When the current token runs out; None with no token.
    expires_at: datetime | None
    #: The sandbox that took it: None until a write lands, since one can fail
    #: after its mint.
    held_by: str | None


def due(row: Held, sandbox_id: str | None, refresh_below: timedelta) -> bool:
    """Whether ``sandbox_id`` is owed a token the server writes into it: it
    holds none, or one with less than ``refresh_below`` left. For every token
    a sandbox is written, the market-data one included."""
    if row.expires_at is None or row.held_by is None or row.held_by != sandbox_id:
        return True
    return row.expires_at - datetime.now(UTC) < refresh_below


def digest(secret: str) -> bytes:
    return hashlib.sha256(secret.encode()).digest()


def field(token_digest: bytes) -> str:
    return bytes(token_digest).hex()[:_DIGEST_FIELD_CHARS]


def text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def mirror(who: Any, slots: list[tuple[bytes | None, datetime | None]]) -> list[Any]:
    """The mirror of a row's live slots, as script arguments: when the
    mirror lapses, then each digest's field and entry. Empty when none is live."""
    now = datetime.now(UTC)
    serialized = json.dumps(who)
    live_slots = [(d, e) for d, e in slots if d is not None and e is not None and e > now]
    if not live_slots:
        return []
    until = min(max(e for _, e in live_slots), now + MIRROR_TTL)
    args: list[Any] = [int(until.timestamp() * 1000)]
    for token_digest, expires_at in live_slots:
        args += [field(token_digest), f"{int(expires_at.timestamp() * 1000)}|{serialized}"]
    return args


def live(stored: bytes | None, expires_at: datetime | None, token_digest: bytes) -> bool:
    if stored is None or expires_at is None or expires_at <= datetime.now(UTC):
        return False
    return hmac.compare_digest(bytes(stored), token_digest)


def parse(token: str, prefix: str) -> tuple[str, bytes]:
    """The computer a token names and its secret's digest; ``Refused`` for a
    token not of this kind."""
    head, _, rest = token.strip().partition(".")
    computer_id, _, secret = rest.partition(".")
    if head != prefix or not secret:
        raise Refused
    try:
        computer_id = str(uuid.UUID(computer_id))
    except ValueError:
        raise Refused from None
    return computer_id, digest(secret)


async def drop_mirror(kind: Kind, computer_id: str) -> bool:
    """Drop the computer's mirror and fence out stale copies; True once done
    (or with no Redis to drop)."""
    client = kind.client()
    if client is None:
        return True
    try:
        await client.eval(DROP, 2, *kind.keys(computer_id)[:2], GENERATION_TTL_S)
    except Exception:
        logger.debug("%s token mirror not dropped", kind.prefix, exc_info=True)
        return False
    return True


async def mint(
    kind: Kind,
    computer_id: str,
    save: Callable[[bytes, datetime], Awaitable[bool]],
) -> Minted | None:
    """A fresh token, once ``save`` (its digest, its expiry) makes it the
    row's current one; None when the save refuses it."""
    secret = secrets.token_urlsafe(32)
    issued_at = datetime.now(UTC)
    expires_at = issued_at + kind.ttl
    if not await save(digest(secret), expires_at):
        return None
    # The new token's first request reads the row and writes the mirror back.
    # Left standing, the mirror would admit the token this mint pushed out of
    # the row and refuse the new one (``ADMIT``) until it lapses, so a drop
    # that fails keeps being retried as a revoke's does.
    await _drop_now(kind, computer_id)
    return Minted(f"{kind.prefix}.{computer_id}.{secret}", expires_at, issued_at)


async def revoke(
    kind: Kind, computer_id: str, delete: Callable[[], Awaitable[None]]
) -> None:
    """End every token of the kind the computer holds: ``delete`` the row,
    then drop the mirror, retried until it lapses. Raises what ``delete``
    raises, once the drop is done or left retrying."""
    try:
        await delete()
    finally:
        # Even when the row stays: nothing rewrites the mirror after a revoke,
        # so one left standing would serve the ended tokens until it lapses.
        await _drop_now(kind, computer_id)


async def _drop_now(kind: Kind, computer_id: str) -> None:
    for attempt in range(_REVOKE_ATTEMPTS):
        if await drop_mirror(kind, computer_id):
            return
        await asyncio.sleep(0.1 * (attempt + 1))
    logger.error("%s token mirror not dropped for computer %s; retrying", kind.prefix, computer_id)
    task = asyncio.create_task(_drop_later(kind, computer_id))
    _dropping.add(task)
    task.add_done_callback(_dropping.discard)


# Drops a revoke still owes, held so a pending one is not collected.
_dropping: set[asyncio.Task[None]] = set()


async def _drop_later(kind: Kind, computer_id: str) -> None:
    """Keep trying to drop a mirror a revoke could not, for as long as it
    could still answer: a Redis back within that time must not serve it."""
    deadline = asyncio.get_running_loop().time() + MIRROR_TTL.total_seconds()
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(_DROP_RETRY_S)
        if await drop_mirror(kind, computer_id):
            logger.info("%s token mirror dropped for computer %s", kind.prefix, computer_id)
            return


async def admit(
    kind: Kind,
    computer_id: str,
    token_digest: bytes,
    load_row: Callable[[], Awaitable[dict[str, Any] | None]],
    who_of_row: Callable[[dict[str, Any]], Any],
) -> Admitted:
    """Admit a token by the mirror, or by the row when there is no mirror,
    and take from the computer's rate. ``Refused`` for one not live,
    ``Throttled`` once the computer is past its rate, before anything of the
    request runs.

    ``load_row`` answers the kind's row (``token_sha256``, ``expires_at``,
    ``prev_token_sha256``, ``prev_expires_at`` plus whatever ``who_of_row``
    reads), or None. With Redis off the row answers alone and nothing is rated.
    """
    client = kind.client()
    keys = kind.keys(computer_id)
    seen = "-"
    if client is not None:
        try:
            answer = await client.eval(
                ADMIT, 3, *keys, field(token_digest), kind.rate_per_s, kind.burst
            )
        except Exception:
            logger.debug("%s admission script failed", kind.prefix, exc_info=True)
            client = None
        else:
            if int(answer[0]) == 1:
                who = json.loads(text(answer[1]).partition("|")[2])
                return _rated(computer_id, who, answer[2])
            if int(answer[0]) == 2:
                raise Refused
            seen = text(answer[1])
    row = await load_row()
    if row is None:
        raise Refused
    if not (
        live(row["token_sha256"], row["expires_at"], token_digest)
        or live(row["prev_token_sha256"], row["prev_expires_at"], token_digest)
    ):
        raise Refused
    who = who_of_row(row)
    if client is None:
        return Admitted(computer_id, who)
    args = mirror(
        who,
        [
            (row["token_sha256"], row["expires_at"]),
            (row["prev_token_sha256"], row["prev_expires_at"]),
        ],
    )
    try:
        wait = await client.eval(
            ADMIT_FROM_ROW, 3, *keys, seen, kind.rate_per_s, kind.burst, *args
        )
    except Exception:
        logger.debug("%s admission script failed", kind.prefix, exc_info=True)
        return Admitted(computer_id, who)
    return _rated(computer_id, who, wait)


def _rated(computer_id: str, who: Any, wait_ms: Any) -> Admitted:
    wait_ms = int(wait_ms or 0)
    if wait_ms > 0:
        raise Throttled(computer_id, who, wait_ms)
    return Admitted(computer_id, who)
