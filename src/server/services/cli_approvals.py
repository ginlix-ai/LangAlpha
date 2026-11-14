"""The user's answer to what a computer's ``la`` asks to do.

Redis only. An approval lives at most about half an hour and is never read
after it is used or lapses, and every loss fails closed: a lost waiting
request means the next send asks again, a lost approval means the user is
asked again, and nothing ever runs without an approved record to take. With
Redis unreachable nothing is asked and nothing runs (``Unavailable``).

Every key of one user carries the same hash tag, so one script may take
several of them on a cluster:

- ``cliapproval:{user}:<approval_id>``: the request, a hash. It lives exactly
  as long as it may still be answered or used, so a hash that exists is
  live; ``expires_ms`` is that same end, kept for the app to show. Once
  declined or used it lives ``SETTLED_TTL`` more, so a send that crossed
  the answer still reads it.
- ``cliapproval:{user}:intent:<computer_id>:<digest>``: the id of the
  waiting or approved request for one exact command, so the same command
  sent again finds it. Gone once the request is declined or used.
- ``cliapproval:{user}:pending``: the ids waiting on an answer, scored by
  when each expires; the app's list and the cap read it.

Each transition is one script that checks the status it leaves, so two
workers never both create, answer or take one request. Redis also wakes the
request held for an answer, on whichever worker holds it, and tells the
user's open apps that something changed; a missed wake is caught by the
request's own re-read, and a missed feed event by the feed's snapshot, which
lists what is waiting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from redis.exceptions import RedisError

from src.config.settings import get_cli_approval_approved_ttl
from src.server.services.thread_lifecycle_feed import publish_user_event
from src.server.services.workspace_status_pubsub import subscribe_to_channel
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)

PENDING: Final = "pending"
APPROVED: Final = "approved"
REJECTED: Final = "rejected"
EXPIRED: Final = "expired"
USED: Final = "used"

#: What one step of asking (``ask``) found.
TAKEN: Final = "taken"
ABSENT: Final = "absent"
CREATED: Final = "created"
MOVED: Final = "moved"
LIMIT: Final = "limit"

EVENT_TYPE: Final = "cli_approval"

# How often a held request reads its row again with no wake: the floor on how
# late it learns of an answer when Redis drops the publish.
REREAD_S: Final = 5.0

#: How long a declined or used request stays readable after its answer.
SETTLED_TTL: Final = timedelta(minutes=10)

# The waiting set outlives its newest member by this much, so a read in that
# window still finds the requests that expired unanswered and tells the apps.
_LAPSE_GRACE_MS: Final = 10 * 60 * 1000

# -- scripts -------------------------------------------------------------------------
#
# Times are milliseconds since the epoch, passed in by the caller.

# KEYS: intent, pending, the row the intent named when read (or the new row),
#       the new row, the row the caller says it waits on (or the new row)
# ARGV: now, pending ttl, new id, id the intent named when read ('' for none),
#       id the caller waits on ('' for none), computer, digest, max pending,
#       settled ttl, how long the set outlives its newest member, action,
#       summary json ('' only to look, never to write)
# -> {'moved'} the intent changed since it was read; {'taken', id} the
#    command's approval, now used; {'rejected', id} the request the caller
#    waits on was declined; {'pending', id} the command's request still
#    waits; {'absent'} none, and none written; {'limit'} the cap is reached;
#    {'created', id} written.
ASK: Final = """
local seen = redis.call('GET', KEYS[1])
if (seen or '') ~= ARGV[4] then
  return {'moved'}
end
local status = seen and redis.call('HGET', KEYS[3], 'status')
if status == 'approved' then
  redis.call('HSET', KEYS[3], 'status', 'used')
  redis.call('PEXPIRE', KEYS[3], ARGV[9])
  redis.call('DEL', KEYS[1])
  return {'taken', seen}
end
if ARGV[5] ~= '' then
  local named = redis.call('HMGET', KEYS[5], 'status', 'computer_id', 'digest')
  if named[1] == 'rejected' and named[2] == ARGV[6] and named[3] == ARGV[7] then
    return {'rejected', ARGV[5]}
  end
end
if status == 'pending' then
  return {'pending', seen}
end
if ARGV[12] == '' then
  return {'absent'}
end
if redis.call('ZCOUNT', KEYS[2], '(' .. ARGV[1], '+inf') >= tonumber(ARGV[8]) then
  return {'limit'}
end
local ttl = tonumber(ARGV[2])
local expires = tonumber(ARGV[1]) + ttl
redis.call('HSET', KEYS[4], 'action', ARGV[11], 'computer_id', ARGV[6], 'digest', ARGV[7],
  'summary', ARGV[12], 'status', 'pending', 'created_ms', ARGV[1], 'expires_ms', expires)
redis.call('PEXPIRE', KEYS[4], ttl)
redis.call('SET', KEYS[1], ARGV[3], 'PX', ttl)
redis.call('ZADD', KEYS[2], expires, ARGV[3])
-- Only ever lengthened: a request asked with a shorter TTL must not drop an
-- earlier one from the list while it still waits.
local keep = ttl + tonumber(ARGV[10])
if redis.call('PTTL', KEYS[2]) < keep then
  redis.call('PEXPIRE', KEYS[2], keep)
end
return {'created', ARGV[3]}
"""

# KEYS: row, pending, intent
# ARGV: now, '1' to approve or '0', approved ttl, settled ttl, approval id
# -> 0 gone, -1 not waiting (answered or used), 1 answered.
DECIDE: Final = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return 0
end
if status ~= 'pending' then
  return -1
end
redis.call('ZREM', KEYS[2], ARGV[5])
local mine = redis.call('GET', KEYS[3]) == ARGV[5]
if ARGV[2] == '1' then
  local ttl = tonumber(ARGV[3])
  redis.call('HSET', KEYS[1], 'status', 'approved', 'expires_ms', tonumber(ARGV[1]) + ttl)
  redis.call('PEXPIRE', KEYS[1], ttl)
  if mine then
    redis.call('PEXPIRE', KEYS[3], ttl)
  end
else
  redis.call('HSET', KEYS[1], 'status', 'rejected')
  redis.call('PEXPIRE', KEYS[1], ARGV[4])
  if mine then
    redis.call('DEL', KEYS[3])
  end
end
return 1
"""

# KEYS: pending
# ARGV: now
# -> the ids that expired unanswered, each returned to one caller only.
LAPSE: Final = """
local gone = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #gone > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
end
return gone
"""


class Unavailable(Exception):
    """Redis cannot be reached, so nothing can be asked or taken."""


class Unknown(Exception):
    """No such approval for this user."""


class AlreadyDecided(Exception):
    """The approval is no longer waiting on an answer."""


# -- keys and rows ------------------------------------------------------------------


def _tag(user_id: str) -> str:
    return f"cliapproval:{{{user_id}}}"


def row_key(user_id: str, approval_id: str) -> str:
    return f"{_tag(user_id)}:{approval_id}"


def intent_key(user_id: str, computer_id: str, digest: str) -> str:
    return f"{_tag(user_id)}:intent:{computer_id}:{digest}"


def pending_key(user_id: str) -> str:
    return f"{_tag(user_id)}:pending"


def approval_channel(approval_id: str) -> str:
    return f"cli_approval:{approval_id}"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ms(td: timedelta) -> int:
    return int(td.total_seconds() * 1000)


def _at(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _client() -> Any:
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        raise Unavailable()
    return cache.client


_DOWN: Final = (RedisError, OSError, asyncio.TimeoutError)
_FIELDS: Final = (
    "action", "computer_id", "digest", "summary", "status", "created_ms", "expires_ms"
)


@dataclass(frozen=True)
class Approval:
    """One request as the gate and the app read it. ``summary`` is what the
    app shows (``target``, ``details``, ``requester``); ``computer_id`` and
    ``digest`` name the command it is for."""

    approval_id: str
    action: str
    status: str
    computer_id: str
    digest: str
    summary: dict[str, Any]
    created_at: datetime
    expires_at: datetime

    @classmethod
    def from_hash(cls, approval_id: str, stored: dict[Any, Any]) -> Approval | None:
        """The request from its hash; None for one that is gone, or was
        evicted part way."""
        fields = {_text(k): _text(v) for k, v in (stored or {}).items()}
        if any(name not in fields for name in _FIELDS):
            return None
        return cls(
            approval_id=approval_id,
            action=fields["action"],
            status=fields["status"],
            computer_id=fields["computer_id"],
            digest=fields["digest"],
            summary=json.loads(fields["summary"]),
            created_at=_at(int(fields["created_ms"])),
            expires_at=_at(int(fields["expires_ms"])),
        )

    def to_wire(self) -> dict[str, Any]:
        """The ``Approval`` the app reads."""
        return {
            "approval_id": self.approval_id,
            "status": self.status,
            "action": self.action,
            "target": self.summary["target"],
            "details": self.summary["details"],
            "requester": self.summary["requester"],
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }


async def get(user_id: str, approval_id: str) -> Approval | None:
    """The user's request, or None when it is not theirs, never was, or has
    lapsed.

    Raises:
        Unavailable: Redis cannot be reached.
    """
    client = _client()
    try:
        return Approval.from_hash(approval_id, await client.hgetall(row_key(user_id, approval_id)))
    except _DOWN as exc:
        raise Unavailable() from exc


async def announce(user_id: str, approval_id: str, status: str) -> None:
    """Tell the held request and the user's apps; best effort, since each
    reads the row again on its own."""
    await publish_user_event(
        user_id, {"type": EVENT_TYPE, "approval_id": approval_id, "status": status}
    )
    try:
        await _client().publish(approval_channel(approval_id), json.dumps({"status": status}))
    except (Unavailable, *_DOWN) as exc:
        logger.debug("cli approval publish failed (%s): %s", approval_id, exc)


# -- reads -------------------------------------------------------------------------


async def pending(user_id: str) -> list[Approval]:
    """The requests waiting on the user, oldest first. One that expired
    unanswered leaves the set here, and the user's apps are told, once.

    Raises:
        Unavailable: Redis cannot be reached.
    """
    client = _client()
    key = pending_key(user_id)
    try:
        lapsed = await client.eval(LAPSE, 1, key, _now_ms())
        ids = [_text(i) for i in await client.zrange(key, 0, -1)]
        async with client.pipeline(transaction=False) as pipe:
            for approval_id in ids:
                pipe.hgetall(row_key(user_id, approval_id))
            stored = await pipe.execute() if ids else []
        rows = [Approval.from_hash(i, s) for i, s in zip(ids, stored, strict=True)]
        # An id whose request is gone (evicted) would hold a place under the
        # cap until it lapsed.
        lost = [i for i, row in zip(ids, rows, strict=True) if row is None]
        if lost:
            await client.zrem(key, *lost)
    except _DOWN as exc:
        raise Unavailable() from exc
    for approval_id in lapsed or []:
        await announce(user_id, _text(approval_id), EXPIRED)
    # Scored by expiry, which is creation plus one TTL, so this is the order
    # they were asked in. One answered between the set's read and its row's
    # is no longer waiting.
    return [row for row in rows if row is not None and row.status == PENDING]


# -- transitions -------------------------------------------------------------------


@dataclass(frozen=True)
class Found:
    """What one step of asking found: ``outcome`` is one of ``TAKEN``,
    ``REJECTED``, ``PENDING``, ``CREATED``, ``ABSENT``, ``MOVED`` or
    ``LIMIT``, and ``approval_id`` the request it found or wrote."""

    outcome: str
    approval_id: str | None = None


async def ask(
    user_id: str,
    computer_id: str,
    digest: str,
    *,
    action: str,
    named: str | None,
    summary: dict[str, Any] | None,
    ttl: timedelta,
    max_pending: int,
) -> Found:
    """One step of asking for this command, in this order: take its
    approval; report the "no" to ``named``, the request the caller says it
    waits on; report the request the command still waits on; and with none,
    write one when ``summary`` is given (what the app shows), else answer
    ``ABSENT``. Two workers asking at once end with one request, which only
    one of them wrote.

    ``MOVED`` says another worker changed the command's request between this
    step's read and its write; the caller asks again.

    Raises:
        Unavailable: Redis cannot be reached.
    """
    client = _client()
    approval_id = str(uuid.uuid4())
    new_key = row_key(user_id, approval_id)
    ikey = intent_key(user_id, computer_id, digest)
    try:
        seen = await client.get(ikey)
        seen = _text(seen) if seen else ""
        answer = await client.eval(
            ASK, 5,
            ikey,
            pending_key(user_id),
            row_key(user_id, seen) if seen else new_key,
            new_key,
            row_key(user_id, named) if named else new_key,
            _now_ms(), _ms(ttl), approval_id, seen, named or "", computer_id, digest,
            max_pending, _ms(SETTLED_TTL), _LAPSE_GRACE_MS, action,
            json.dumps(summary) if summary is not None else "",
        )
    except _DOWN as exc:
        raise Unavailable() from exc
    outcome, *found = (_text(part) for part in answer)
    return Found(outcome, found[0] if found else None)


async def decide(user_id: str, approval_id: str, *, approve: bool) -> dict[str, Any]:
    """Record the user's answer and wake the request held for it. An
    approval holds for the approved TTL from the answer.

    Raises:
        Unknown: the user has no such approval.
        AlreadyDecided: it was answered or used already.
        Unavailable: Redis cannot be reached.
    """
    approval = await get(user_id, approval_id)
    if approval is None:
        raise Unknown()
    approved_ttl = _ms(get_cli_approval_approved_ttl())
    now = _now_ms()
    try:
        answered = await _client().eval(
            DECIDE, 3,
            row_key(user_id, approval_id),
            pending_key(user_id),
            intent_key(user_id, approval.computer_id, approval.digest),
            now, "1" if approve else "0", approved_ttl, _ms(SETTLED_TTL), approval_id,
        )
    except _DOWN as exc:
        raise Unavailable() from exc
    answered = int(answered)
    if answered == 0:
        raise Unknown()
    if answered == -1:
        raise AlreadyDecided()
    if approve:
        approval = replace(approval, status=APPROVED, expires_at=_at(now + approved_ttl))
    else:
        approval = replace(approval, status=REJECTED)
    await announce(user_id, approval_id, approval.status)
    return approval.to_wire()


# -- the held request ---------------------------------------------------------------


async def wait_for_answer(user_id: str, approval_id: str, deadline: float) -> Approval | None:
    """The request once it is no longer waiting, or as it stands at
    ``deadline`` (a ``time.monotonic`` instant); None when it is gone.

    Subscribes first and reads after, so an answer published between the
    two is still seen; every wake, and every ``REREAD_S`` without one, reads
    the row again rather than trusting the message.

    Raises:
        Unavailable: Redis cannot be reached.
    """
    async with subscribe_to_channel(approval_channel(approval_id)) as wait:
        while True:
            row = await get(user_id, approval_id)
            if row is None or row.status != PENDING:
                return row
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return row
            pause = min(REREAD_S, remaining)
            if wait is None:
                await asyncio.sleep(pause)
                continue
            kind, _ = await wait(pause)
            if kind == "error":
                # A broken subscription answers at once; pace the re-reads.
                wait = None
                await asyncio.sleep(min(REREAD_S, max(0.0, deadline - time.monotonic())))
