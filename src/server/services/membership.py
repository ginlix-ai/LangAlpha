"""The account's membership as the auth service last gave it, cached per user."""

import asyncio
import contextlib
import contextvars
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

from src.config.features import is_feature_enabled_system
from src.server.services import platform_client
from src.server.services.account_contract import ACCOUNT_CONTRACT, parse_account
from src.server.database.shared_workspaces import is_holder
from src.server.services.shared_workspaces.holder import refuse_holder

logger = logging.getLogger(__name__)

_PLATFORM_MEMBERSHIP_CACHE_TTL = 300  # 5 minutes

# The last membership the platform gave, kept to stand in while it cannot be
# asked. Long, because it is read only during an outage and the credit gate
# still asks about every turn: a plan that changed meanwhile is wrong for the
# length of the outage at most, and spends nothing the gate did not allow.
_PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL = 30 * 24 * 3600  # 30 days

# After a failed ask, requests for that user stop asking for this long and read
# the last-known membership instead, so a down or hanging platform is not called
# on every request. Matches the Retry-After an unknown membership answers with,
# so a client that honours it asks again once the pause is over.
_PLATFORM_MEMBERSHIP_PAUSE_TTL = platform_client.SERVICE_UNAVAILABLE_RETRY_AFTER


# The blocks of the validate answer that say which organization the account is
# in (``services.account``). They ride the membership entry, so one ask and one
# cache serve both.
MEMBERSHIP_ACCOUNT_KEYS = ("org", "groups", "features", "policy", "connectors", "teams")


def teams_door_open() -> bool:
    """Whether the account is read for anyone at all, answered without a read,
    so a caller can skip work it would only throw away."""
    return is_feature_enabled_system("teams") and platform_client.platform_gating_active()


# With the ``teams`` door open the read declares the account contract, and the
# entry and the last-known record live under keys that carry its version: two
# builds sharing one cache (a blue/green cutover) must never read an entry
# fetched for another version, whose blocks one of them cannot read, nor one
# fetched with the door shut, which has no blocks at all and would read as no
# organization. The pause (the auth service did not answer) and the epoch (a
# change was announced) mean the same to every build, so each stays one key,
# and one notice fences every version's entries. With the door shut the keys
# and the read are the ones the deployment used before organizations existed.
def _membership_contract() -> int | None:
    return ACCOUNT_CONTRACT if teams_door_open() else None


def platform_membership_cache_key(user_id: str, contract: int | None = None) -> str:
    if contract is None:
        return f"platform_membership:{user_id}"
    return f"platform_membership:v{contract}:{user_id}"


def _membership_last_known_key(user_id: str, contract: int | None = None) -> str:
    if contract is None:
        return f"platform_membership:last_known:{user_id}"
    return f"platform_membership:last_known:v{contract}:{user_id}"


def _membership_pause_key(user_id: str) -> str:
    return f"platform_membership:unreachable:{user_id}"


def _membership_epoch_key(user_id: str) -> str:
    return f"platform_membership:epoch:{user_id}"


def _membership_suspended_key(user_id: str) -> str:
    return f"platform_membership:suspended:{user_id}"


# Each change notice bumps the user's epoch. An answer is written only while the
# epoch still reads what it did before the ask, and a value stamped with any
# other epoch predates a change: the entry reads as absent, and the last-known
# record keeps only its plan. A notice and every write under the epoch renew it
# to longer than the last-known record lives, so any record stamped otherwise
# expires before the epoch does and a missing epoch (read as 0) can never match
# a record from before a notice.
_PLATFORM_MEMBERSHIP_EPOCH_TTL = _PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL + 24 * 3600
# The stamp's field in the cached value. Left out at epoch 0, so a user no
# notice was ever sent for is cached exactly as before.
_MEMBERSHIP_EPOCH_FIELD = "membership_epoch"
# Set on a last-known record standing in from before a change notice, in place
# of its organization blocks: ``services.account`` reads such an account as
# unknown until the auth service answers again.
MEMBERSHIP_ACCOUNT_UNKNOWN = "account_unknown"

# What the auth service last said of the account's suspension, kept apart from
# the entry so a change notice can carry it (``{"suspended": true}``): after
# that notice the account reads as unknown until the auth service answers,
# and an unknown account keeps its reads, which someone just removed must not.
# Marked on the value read (``MEMBERSHIP_SUSPENDED_HINT``) whenever the key is
# set, and acted on only while the account is otherwise unknown. Every answer
# from the auth service sets or removes it under the epoch fence its entry is
# written under, and a notice moves it in the same transaction as the epoch,
# so it never outlives what the auth service last said. It lives as long as
# the last-known record, whose organization it stands in for once a notice
# has made that record's organization unreadable: an outage that long keeps
# the person out, and any answer corrects it.
_PLATFORM_MEMBERSHIP_SUSPENDED_TTL = _PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL
MEMBERSHIP_SUSPENDED_HINT = "suspended_hint"


def plan_unknown(membership: Optional[dict]) -> bool:
    """No plan is on record: no answer at all, or the suspension hint alone."""
    return membership is None or (
        MEMBERSHIP_SUSPENDED_HINT in membership and "access_tier" not in membership
    )


async def access_tier(user_id: str, *, recheck: bool = False) -> Optional[int]:
    """The plan's access tier from the membership, -1 for no access; None
    when no plan is on record."""
    answer = await read_membership(user_id, recheck=recheck)
    if plan_unknown(answer):
        return None
    return int(answer.get("access_tier", -1))


async def drive_allowance(user_id: str) -> Optional[int]:
    """The Drive allowance the deployment's account service gives this person,
    in bytes, -1 for no cap; None when it gives none (no account service, or no
    answer on record, or an answer without one), so the deployment's own
    setting applies."""
    if not platform_client.platform_gating_active() or is_holder(user_id):
        return None
    answer = await read_membership(user_id)
    if plan_unknown(answer):
        return None
    return _drive_bytes(answer.get("drive_max_bytes"))


def _drive_bytes(value: object) -> Optional[int]:
    """The allowance as sent, when it reads as one; anything else is no answer."""
    if isinstance(value, bool) or not isinstance(value, int) or value < -1:
        return None
    return value


def _answer_suspended(result: dict) -> bool:
    org = result.get("org")
    return isinstance(org, dict) and org.get("suspended") is True


def _answer_says_active(membership: dict) -> bool:
    """Whether a fresh answer reads as an account that is not suspended. One
    with a block that cannot be read leaves the account unknown, so it says
    nothing of the suspension and may not clear the hint."""
    try:
        return not parse_account(membership).suspended
    except ValueError:
        return False


def _is_membership(value: object) -> bool:
    return isinstance(value, dict) and "access_tier" in value


def _stamped(membership: dict, epoch: int) -> dict:
    return {**membership, _MEMBERSHIP_EPOCH_FIELD: epoch} if epoch else membership


def _unstamped(value: dict) -> dict:
    if _MEMBERSHIP_EPOCH_FIELD not in value:
        return value
    return {k: v for k, v in value.items() if k != _MEMBERSHIP_EPOCH_FIELD}


def _epoch_of(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _standing_in(last_known: object, epoch: int) -> Optional[dict]:
    """The last-known record as it may stand in for the auth service.

    One from before a change notice keeps its plan, so a notice does not cost
    a plan read during an outage, but not the organization it describes.
    """
    if not _is_membership(last_known):
        return None
    if _epoch_of(last_known.get(_MEMBERSHIP_EPOCH_FIELD)) == epoch:
        return _unstamped(last_known)
    plan = {
        k: v
        for k, v in _unstamped(last_known).items()
        if k not in MEMBERSHIP_ACCOUNT_KEYS
    }
    return {**plan, MEMBERSHIP_ACCOUNT_UNKNOWN: True}


# The fields every build's record carries and reads the same way.
_MEMBERSHIP_PLAN_KEYS = ("access_tier", "plan_display_name", "drive_max_bytes")


async def _earlier_plan(cache, user_id: str, contract: int) -> Optional[dict]:
    """The plan from the nearest last-known record an earlier contract, or a
    build with the door shut, kept: switching ``teams`` on or moving the
    contract starts this version's record empty, and an outage then must not
    cost the user the plan a build before the switch would have stood in with.
    Read only on that outage path. The organization is not taken from it, and
    reads as unknown until the auth service answers.
    """
    keys = [_membership_last_known_key(user_id, v) for v in range(contract - 1, 0, -1)]
    keys.append(_membership_last_known_key(user_id))
    for record in await cache.mget(keys):
        if _is_membership(record):
            plan = {k: record[k] for k in _MEMBERSHIP_PLAN_KEYS if k in record}
            return {**plan, MEMBERSHIP_ACCOUNT_UNKNOWN: True}
    return None


# A request's reads of one user's membership share the first, so its gate and
# its route pay for one read between them. The share opens with the request
# (``membership_read_scope``, entered for every request the app serves) and
# closes when it ends, and even inside it an answer older than this is read
# again: a stream or a connection that lives as long as its request, and any
# task the request starts, which shares its context, never acts for long on an
# answer from before a change notice.
_SHARED_READ_MAX_AGE = 2.0  # seconds


@dataclass
class _RequestReads:
    user_id: Optional[str] = None
    value: Any = None
    read_at: Optional[float] = None
    open: bool = True


_REQUEST_READS: contextvars.ContextVar[Optional[_RequestReads]] = contextvars.ContextVar(
    "membership_request_reads", default=None
)


@contextlib.contextmanager
def membership_read_scope():
    """The lifetime of one request's shared membership read."""
    reads = _RequestReads()
    token = _REQUEST_READS.set(reads)
    try:
        yield
    finally:
        reads.open = False
        with contextlib.suppress(ValueError):
            _REQUEST_READS.reset(token)


class MembershipReadScope:
    """ASGI middleware opening :func:`membership_read_scope` for each request.

    Only with the ``teams`` door open: shut, the gate reads nothing and there
    is no second read to share, so a request costs what it did before."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket") or not teams_door_open():
            await self.app(scope, receive, send)
            return
        with membership_read_scope():
            await self.app(scope, receive, send)


def share_membership_read(user_id: str) -> None:
    """Let the rest of this request reuse its next read of ``user_id``'s
    membership. Outside a request it does nothing, and every read asks."""
    reads = _REQUEST_READS.get()
    if reads is not None and reads.open and reads.user_id != user_id:
        reads.user_id, reads.value, reads.read_at = user_id, None, None


def _shared_read(user_id: str) -> Optional[_RequestReads]:
    reads = _REQUEST_READS.get()
    if reads is None or not reads.open or reads.user_id != user_id:
        return None
    return reads


class _CacheUnreadable(Exception):
    """Redis failed the read of a caller that takes that as unknown."""


async def read_membership(
    user_id: str,
    *,
    refresh: bool = False,
    recheck: bool = False,
    unknown_on_cache_error: bool = False,
    answered_only: bool = False,
) -> Optional[dict]:
    """:func:`_read_platform_membership`, once per request where the request
    shares its read (``share_membership_read``); a ``refresh`` or ``recheck``
    asks again and is what later reads get.

    A Redis error reads as a miss, so the auth service is asked as it was
    before organizations existed. ``unknown_on_cache_error`` reads it as
    unknown instead, for a check that never asked before (whether the account
    is suspended, on every request): a Redis outage must not send each of
    those to the auth service. That unknown is not shared, so a later read in
    the request still asks. ``answered_only`` reads as unknown what the last
    answer on record would stand in for while the auth service cannot be
    asked, for a grant that must be its word now; nor is that read shared."""
    refuse_holder(user_id, "read_membership")
    shared = _shared_read(user_id)
    if (
        shared is not None
        and shared.read_at is not None
        and not (refresh or recheck)
        and time.monotonic() - shared.read_at <= _SHARED_READ_MAX_AGE
    ):
        return shared.value
    try:
        value = await _read_platform_membership(
            user_id,
            refresh=refresh,
            recheck=recheck,
            strict=unknown_on_cache_error,
            answered_only=answered_only,
        )
    except _CacheUnreadable:
        logger.warning(
            "[platform_membership] cache unreadable, membership unknown: user_id=%s",
            user_id,
        )
        return None
    if shared is not None and shared.open and not answered_only:
        shared.value, shared.read_at = value, time.monotonic()
    return value


# The asks of the auth service in flight in this process, by user, contract
# and epoch: concurrent reads that all missed the cache wait for one ask
# rather than each making its own. Execution context only: another worker
# makes its own ask, and a notice moves the epoch, so no read after it waits
# for an ask from before it.
_ASKS: dict[tuple, "asyncio.Task[Optional[dict]]"] = {}


async def _coalesced(
    key: tuple, ask: Callable[[], Awaitable[Optional[dict]]]
) -> Optional[dict]:
    loop = asyncio.get_running_loop()
    key = (id(loop), *key)
    task = _ASKS.get(key)
    if task is None or task.get_loop() is not loop:
        task = loop.create_task(ask())
        _ASKS[key] = task

        def _forget(done: asyncio.Task, key: tuple = key) -> None:
            if _ASKS.get(key) is done:
                del _ASKS[key]

        task.add_done_callback(_forget)
    # Shielded: one waiter's cancel must not cancel the ask the others wait on.
    return await asyncio.shield(task)


async def _read_platform_membership(
    user_id: str,
    *,
    refresh: bool = False,
    recheck: bool = False,
    strict: bool = False,
    answered_only: bool = False,
) -> Optional[dict]:
    """The user's platform membership: ``{"access_tier", "plan_display_name"}``
    and ``drive_max_bytes`` when the answer carried it,
    plus, with the ``teams`` door open, whichever of
    :data:`MEMBERSHIP_ACCOUNT_KEYS` the auth service sent.

    ``access_tier`` -1 is the platform's own answer that the user has no access.
    None means there is no answer: the platform could not be asked and has not
    answered for this user within the last-known TTL. An outage is not a plan,
    so no caller may read None as -1. While the platform is unreachable the last
    membership it gave stands in. ``refresh`` asks now, past the 5-minute cache
    and any pause, for a plan known to have changed. ``recheck`` asks past the
    cache only, for one that may have, and leaves the cache to the user's other
    reads meanwhile. No-op in OSS mode.
    """
    if not platform_client.platform_gating_active():
        return {"access_tier": -1, "plan_display_name": None}

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    contract = _membership_contract()
    entry_key = platform_membership_cache_key(user_id, contract)
    last_known_key = _membership_last_known_key(user_id, contract)
    pause_key = _membership_pause_key(user_id)
    epoch_key = _membership_epoch_key(user_id)
    # Only an entry that can carry organization blocks needs the fence; with
    # the door shut nothing written here is stamped, and epoch 0 matches it.
    fenced = contract is not None
    fenced_keys = [
        entry_key, last_known_key, pause_key, epoch_key,
        _membership_suspended_key(user_id),
    ]
    if fenced and strict:
        try:
            values = await cache.mget(fenced_keys, strict=True)
        except Exception as e:
            raise _CacheUnreadable from e
    else:
        values = await cache.mget(
            fenced_keys if fenced else [entry_key, last_known_key, pause_key]
        )
    cached, last_known, paused = values[:3]
    epoch = _epoch_of(values[3]) if fenced else 0
    hinted = fenced and values[4] is True
    if _is_membership(cached) and _epoch_of(cached.get(_MEMBERSHIP_EPOCH_FIELD)) != epoch:
        cached = None  # answered before a change the auth service has since sent
    # Seeds a missing record below, so whether one exists is read unstamped.
    has_last_known = _is_membership(last_known)
    last_known = _standing_in(last_known, epoch)

    async def standing_in() -> Optional[dict]:
        if answered_only:
            return None
        if last_known is not None or not fenced:
            value = last_known
        else:
            value = await _earlier_plan(cache, user_id, contract)
        if not hinted:
            return value
        # With no plan on record the hint is all there is: an account that
        # reads as unknown, with no plan for a plan reader to take.
        return {
            **(value or {MEMBERSHIP_ACCOUNT_UNKNOWN: True}),
            MEMBERSHIP_SUSPENDED_HINT: True,
        }

    if refresh:
        # Dropped, not only skipped: should this ask fail, later requests must
        # not take the plan it was asked to replace back from the cache. With
        # the door open the entry a door-shut worker sharing the cache reads
        # goes too, or it would keep serving that plan.
        if fenced:
            await cache.delete(entry_key, platform_membership_cache_key(user_id))
        else:
            await cache.delete(entry_key)
    else:
        if not recheck and _is_membership(cached):
            # An entry an older worker wrote has no last-known record beside it.
            # Not from a -1, which an older worker also caches for a failed ask.
            # Copied stamped, so a notice landing meanwhile still shows on it.
            if not has_last_known and int(cached.get("access_tier", -1)) >= 0:
                await cache.set(
                    last_known_key, cached, ttl=_PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL
                )
            value = _unstamped(cached)
            return {**value, MEMBERSHIP_SUSPENDED_HINT: True} if hinted else value
        if paused:
            # A door-shut worker writes the pause unfenced, so one it set after
            # a notice, from an ask that failed, holds a record from before the
            # notice here as unknown while the pause lasts. That refuses
            # rather than allows, and a pause this side ignored would send every
            # read to an auth service that just failed, so it is left.
            return await standing_in()

    if fenced and not (refresh or recheck):
        return await _coalesced(
            (user_id, contract, epoch),
            lambda: _ask_platform_membership(
                user_id, contract, epoch, standing_in, cache=cache, hinted=hinted
            ),
        )
    return await _ask_platform_membership(
        user_id, contract, epoch, standing_in, cache=cache, hinted=hinted
    )


async def _ask_platform_membership(
    user_id: str,
    contract: Optional[int],
    epoch: int,
    standing_in: Callable[[], Awaitable[Optional[dict]]],
    *,
    cache: Any,
    hinted: bool = False,
) -> Optional[dict]:
    """Ask the auth service and cache its answer; what stands in when it
    gives none."""
    entry_key = platform_membership_cache_key(user_id, contract)
    last_known_key = _membership_last_known_key(user_id, contract)
    pause_key = _membership_pause_key(user_id)
    epoch_key = _membership_epoch_key(user_id)
    fenced = contract is not None
    result = await platform_client.call_validate(user_id, account_contract=contract)
    if result is not None:
        membership = {
            "access_tier": int(result.get("access_tier", -1)),
            "plan_display_name": result.get("plan_display_name"),
        }
        drive_bytes = _drive_bytes(result.get("drive_max_bytes"))
        if drive_bytes is not None:
            membership["drive_max_bytes"] = drive_bytes
        if not fenced:
            await cache.set_many([
                (entry_key, membership, _PLATFORM_MEMBERSHIP_CACHE_TTL),
                (last_known_key, membership, _PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL),
            ])
            return membership
        # Kept as sent: ``services.account`` parses them where they are read,
        # so a malformed block reads as unknown rather than as no organization.
        membership.update(
            (key, result[key])
            for key in MEMBERSHIP_ACCOUNT_KEYS
            if result.get(key) is not None
        )
        stamped = _stamped(membership, epoch)
        writes = [
            (entry_key, stamped, _PLATFORM_MEMBERSHIP_CACHE_TTL),
            (last_known_key, stamped, _PLATFORM_MEMBERSHIP_LAST_KNOWN_TTL),
        ]
        suspended_key = _membership_suspended_key(user_id)
        clear = ()
        if _answer_suspended(result):
            writes.append((suspended_key, True, _PLATFORM_MEMBERSHIP_SUSPENDED_TTL))
            hinted = True
        elif _answer_says_active(membership):
            clear = (suspended_key,)
            hinted = False
        # Refused when a notice came in during the ask, the hint with the
        # entry: the notice's word on it is newer than this answer. This
        # caller still gets the answer, which is no older than the request it
        # serves.
        await cache.set_many_fenced(
            writes,
            counter=(epoch_key, epoch, _PLATFORM_MEMBERSHIP_EPOCH_TTL),
            delete=clear,
        )
        # An answer that cannot be read keeps the hint, as a cached read of
        # it would (``MEMBERSHIP_SUSPENDED_HINT``).
        return {**membership, MEMBERSHIP_SUSPENDED_HINT: True} if hinted else membership

    # The failure is recorded beside the membership, never in its place: a
    # cached -1 here is what used to send a paying user to the setup wizard.
    if not fenced:
        await cache.set(pause_key, True, ttl=_PLATFORM_MEMBERSHIP_PAUSE_TTL)
        return await standing_in()
    # Fenced too: an ask out when a notice cleared the pause must not put it
    # back, or reads would serve the record from before the change unasked.
    await cache.set_many_fenced(
        [(pause_key, True, _PLATFORM_MEMBERSHIP_PAUSE_TTL)],
        counter=(epoch_key, epoch, _PLATFORM_MEMBERSHIP_EPOCH_TTL),
    )
    return await standing_in()


async def forget_membership(
    user_id: str, *, suspended: Optional[bool] = None
) -> Optional[int]:
    """Drop the cached membership so the user's next read asks the auth service;
    the epoch the notice moved it to (:func:`membership_epoch`).

    For the auth service telling us a membership changed. The pause goes too:
    the service that just called is reachable. Nothing is asked here, so the
    notice never waits on a request back to the service that sent it. The
    last-known record stays for its plan; the epoch bump marks its
    organization as from before the change, and fences off an ask already in
    flight. The bump raises when Redis cannot take it, so the sender sees the
    notice fail and sends it again.

    It runs whatever the ``teams`` flag says, where builds before organizations
    had no notice at all: while a deployment switches ``teams`` on, workers on
    both sides of the flag share the cache and the notice has to fence both.
    The one epoch fences every version's entries; a door-shut worker reads no
    epoch, so with the door open its unversioned entry is deleted here too.

    ``suspended`` is what the notice says of the account's suspension, None
    when it says nothing; it is recorded (``MEMBERSHIP_SUSPENDED_HINT``) in
    the bump's own transaction, so of two notices the later bump's word
    stands, and an ask the bump fences off cannot overwrite it.
    """
    if not platform_client.platform_gating_active():
        return None
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    suspended_key = _membership_suspended_key(user_id)
    epoch = await cache.bump(
        _membership_epoch_key(user_id),
        _PLATFORM_MEMBERSHIP_EPOCH_TTL,
        also_set=(
            (suspended_key, True, _PLATFORM_MEMBERSHIP_SUSPENDED_TTL) if suspended else None
        ),
        also_delete=(suspended_key,) if suspended is False else (),
    )
    contract = _membership_contract()
    entries = [platform_membership_cache_key(user_id)]
    if contract is not None:
        entries.insert(0, platform_membership_cache_key(user_id, contract))
    await cache.delete(*entries)
    await cache.delete(_membership_pause_key(user_id))
    return epoch


async def membership_epoch(user_id: str) -> Optional[int]:
    """How many change notices the user's membership has had, as
    :func:`forget_membership` counts them; None when there is no
    cache to count in, or Redis cannot be read, so a caller comparing it to
    a count it holds finds a mismatch rather than reads an error as no news."""
    from src.utils.cache.redis_cache import get_cache_client

    value = await get_cache_client().get(_membership_epoch_key(user_id))
    return None if value is None else _epoch_of(value)
