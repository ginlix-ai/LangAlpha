"""Coverage for the report-back dispatch caps (per-flash + per-user).

A flash thread can fan out many background PTC analyses, but unbounded fan-out
would overload the single backend. ``report_back._reserve_slot_membership``
atomically admits a dispatch under both caps *before* the dispatch POST (rolled
back on failure), so racing calls can't both pass the check then overshoot.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.chat import report_back as T
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache


async def _reserve_err(flash, ptc, user) -> str | None:
    """Reserve a slot and return the cap-error string (None if admitted)."""
    err, _added, _watch = await T._reserve_slot_membership(flash, ptc, user)
    return err


@pytest.fixture
def cache(monkeypatch):
    c = _FakeCache()
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: c
    )
    return c


@pytest.mark.asyncio
async def test_per_flash_cap_rejects_beyond_limit(cache):
    flash, user = "flash-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_FLASH):
        assert await _reserve_err(flash, f"p{i}", user) is None
    err = await _reserve_err(flash, "p-over", user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_FLASH) in err
    # The rejected dispatch left no residue in either SET.
    assert "p-over" not in cache.client.sets[f"flash_watch:{flash}"]
    assert "p-over" not in cache.client.sets[f"flash_user_pending:{user}"]


@pytest.mark.asyncio
async def test_per_user_cap_spans_multiple_flash_threads(cache):
    user = "u-1"
    # Spread dispatches across flash threads, staying under each per-flash cap
    # (<5 each) but reaching the per-user cap of 10.
    placed = 0
    flash_idx = 0
    while placed < T.MAX_DISPATCH_PER_USER:
        flash = f"flash-{flash_idx}"
        for _ in range(min(T.MAX_DISPATCH_PER_FLASH - 1, T.MAX_DISPATCH_PER_USER - placed)):
            assert await _reserve_err(flash, f"p{placed}", user) is None
            placed += 1
        flash_idx += 1
    # 11th anywhere is rejected by the per-user cap.
    err = await _reserve_err("flash-new", "p-over", user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_USER) in err


@pytest.mark.asyncio
async def test_idempotent_redispatch_does_not_count_against_cap(cache):
    flash, user = "flash-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_FLASH):
        assert await _reserve_err(flash, f"p{i}", user) is None
    # Re-reserving an existing member (idempotent re-dispatch) is admitted even
    # though the SET is already at the cap.
    assert await _reserve_err(flash, "p0", user) is None


@pytest.mark.asyncio
async def test_release_rolls_back_both_sets(cache):
    flash, user, ptc = "flash-1", "u-1", "p0"
    cmd, added, watch_member = await T._reserve_slot_membership(flash, ptc, user)
    assert cmd is None
    assert added == {"watch": True, "user": True}
    # Membership durably in place -> report-back is wired.
    assert watch_member is True
    assert ptc in cache.client.sets[f"flash_watch:{flash}"]
    assert ptc in cache.client.sets[f"flash_user_pending:{user}"]

    await T._release_slot_membership(flash, ptc, user, added)

    assert ptc not in cache.client.sets[f"flash_watch:{flash}"]
    assert ptc not in cache.client.sets[f"flash_user_pending:{user}"]
    # A freed slot is reusable.
    assert await _reserve_err(flash, "p-new", user) is None


@pytest.mark.asyncio
async def test_precise_rollback_keeps_first_dispatch_membership(cache):
    """A second (idempotent) reserve for the same PTC adds nothing, so releasing
    it must not remove the first dispatch's membership."""
    flash, user, ptc = "flash-1", "u-1", "T"

    cmd1, added1, watch1 = await T._reserve_slot_membership(flash, ptc, user)
    assert cmd1 is None
    assert added1 == {"watch": True, "user": True}
    assert watch1 is True
    assert ptc in cache.client.sets[f"flash_watch:{flash}"]

    # Second reserve for the SAME ptc is idempotent — newly added nothing, but
    # membership is still durably in place, so it stays wired (watch_member True
    # despite added all-False).
    cmd2, added2, watch2 = await T._reserve_slot_membership(flash, ptc, user)
    assert cmd2 is None
    assert added2 == {"watch": False, "user": False}
    assert watch2 is True

    # Releasing the second reservation srems nothing it didn't add: the first
    # dispatch's membership survives.
    await T._release_slot_membership(flash, ptc, user, added2)
    assert ptc in cache.client.sets[f"flash_watch:{flash}"]
    assert ptc in cache.client.sets[f"flash_user_pending:{user}"]

    # Releasing with the owning reservation's dict frees the membership.
    await T._release_slot_membership(flash, ptc, user, added1)
    assert ptc not in cache.client.sets[f"flash_watch:{flash}"]
    assert ptc not in cache.client.sets[f"flash_user_pending:{user}"]


@pytest.mark.asyncio
async def test_reserve_is_noop_when_cache_disabled(monkeypatch):
    class _Disabled:
        enabled = False
        client = None

    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _Disabled()
    )
    # No Redis -> best-effort admit (never block the dispatch), added all-False,
    # and unwired: no flash_watch member means the completion-time gate can't
    # deliver a report-back, so watch_member is False.
    cmd, added, watch_member = await T._reserve_slot_membership("f", "p", "u")
    assert cmd is None
    assert added == {"watch": False, "user": False}
    assert watch_member is False


@pytest.mark.asyncio
async def test_reserve_fails_open_unwired_on_redis_exception(monkeypatch):
    """A Redis hiccup mid-reserve admits the dispatch (no cap error) but leaves
    no flash_watch member, so it reports unwired (watch_member False) — the
    caller must not then promise an undeliverable report-back."""
    class _Boom:
        enabled = True

        class client:  # noqa: N801 - stub namespace
            @staticmethod
            async def sismember(*_a, **_k):
                raise RuntimeError("redis down")

    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _Boom()
    )
    cmd, added, watch_member = await T._reserve_slot_membership("f", "p", "u")
    assert cmd is None
    assert added == {"watch": False, "user": False}
    assert watch_member is False


# ---------------------------------------------------------------------------
# reserve() context manager — fail-closed on an owning origin-write failure
# ---------------------------------------------------------------------------


class _OriginWriteFailCache:
    """Cache whose origin GET is empty and origin SET fails (returns False).

    Records deletes so the rollback (origin-owner cleanup) can be asserted.
    """

    enabled = True
    client = object()  # truthy

    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def get(self, key):
        return None  # no existing origin -> not cross-flash

    async def set(self, key, value, ttl=None):
        return False  # owning origin write fails -> dispatch_failed

    async def delete(self, key):
        self.deleted.append(key)
        return True


@pytest.mark.asyncio
async def test_reserve_cm_fails_closed_when_origin_write_fails():
    """reserve() owns the origin write for a freshly-added watch member; a write
    failure must fail CLOSED (slot.error == 'dispatch_failed'), and the
    non-committed exit must roll the reservation back — releasing exactly what it
    reserved and deleting the origin it owns (no half-written record stranded)."""
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    fresh_slot = (None, {"watch": True, "user": True}, True)  # cap ok, wired
    fake_cache = _OriginWriteFailCache()
    release = AsyncMock()

    with patch.object(
        T, "_reserve_slot_membership", AsyncMock(return_value=fresh_slot)
    ), patch.object(
        T, "_release_slot_membership", release
    ), patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=fake_cache
    ):
        async with T.reserve(flash, ptc, "ptc-ws-1", "flash-ws-1", user) as slot:
            # Owning origin write failed -> the dispatch must abort (never commit).
            assert slot.error == "dispatch_failed"

    # Rollback released exactly the reservation this dispatch made...
    release.assert_awaited_once_with(flash, ptc, user, {"watch": True, "user": True})
    # ...and deleted the origin it owned (fail-closed cleanup, no strand).
    assert T.ptc_origin_key(ptc) in fake_cache.deleted


# ---------------------------------------------------------------------------
# Concurrency — _dispatch_reserve_lock serializes the cap check + the add
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_reserves_cannot_overshoot_per_flash_cap(cache):
    """Two racing reserves for the last free per-flash slot can't both win.

    ``scard`` yields mid-check so, absent the lock, both reserves would read the
    same under-cap count then both add (overshoot to cap+1). ``_dispatch_reserve_lock``
    must keep the check-through-add critical section atomic: exactly one is
    admitted, and the watch SET never exceeds ``MAX_DISPATCH_PER_FLASH``."""
    flash, user = "flash-1", "u-1"
    # Fill to one below the per-flash cap (the binding cap here; per-user stays low).
    for i in range(T.MAX_DISPATCH_PER_FLASH - 1):
        assert await _reserve_err(flash, f"p{i}", user) is None
    watch_key = f"flash_watch:{flash}"

    orig_scard = cache.client.scard
    orig_sadd = cache.client.sadd
    peak = 0

    async def slow_scard(key):
        await asyncio.sleep(0)  # force the two reserves to interleave mid-check
        return await orig_scard(key)

    async def tracking_sadd(key, member):
        nonlocal peak
        result = await orig_sadd(key, member)
        if key == watch_key:
            peak = max(peak, len(cache.client.sets.get(key, set())))
        return result

    cache.client.scard = slow_scard
    cache.client.sadd = tracking_sadd

    results = await asyncio.gather(
        T._reserve_slot_membership(flash, "p-new-1", user),
        T._reserve_slot_membership(flash, "p-new-2", user),
    )

    cap_errors = [err for err, _added, _watch in results]
    admitted = [e for e in cap_errors if e is None]
    rejected = [e for e in cap_errors if e is not None]
    assert len(admitted) == 1  # exactly one winner
    assert len(rejected) == 1  # the other is capped out
    assert "on this thread" in rejected[0]  # rejected by the per-flash cap
    assert str(T.MAX_DISPATCH_PER_FLASH) in rejected[0]
    # The SET never overshot: the serialized check-then-add filled exactly one slot.
    assert peak == T.MAX_DISPATCH_PER_FLASH
    assert len(cache.client.sets[watch_key]) == T.MAX_DISPATCH_PER_FLASH


# ---------------------------------------------------------------------------
# check_dispatch_capacity — advisory pre-check used before sandbox provisioning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_precheck_mirrors_per_flash_rejection(cache):
    """The pre-check reports the same per-flash cap error reserve() enforces,
    without reserving anything."""
    flash, user = "flash-1", "u-1"
    assert await T.check_dispatch_capacity(flash, user) is None
    for i in range(T.MAX_DISPATCH_PER_FLASH):
        assert await _reserve_err(flash, f"p{i}", user) is None
    err = await T.check_dispatch_capacity(flash, user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_FLASH) in err
    # Advisory only: the probe added no members.
    assert len(cache.client.sets[f"flash_watch:{flash}"]) == T.MAX_DISPATCH_PER_FLASH


@pytest.mark.asyncio
async def test_capacity_precheck_mirrors_per_user_rejection(cache):
    user = "u-1"
    placed = 0
    flash_idx = 0
    while placed < T.MAX_DISPATCH_PER_USER:
        flash = f"flash-{flash_idx}"
        for _ in range(min(T.MAX_DISPATCH_PER_FLASH - 1, T.MAX_DISPATCH_PER_USER - placed)):
            assert await _reserve_err(flash, f"p{placed}", user) is None
            placed += 1
        flash_idx += 1
    err = await T.check_dispatch_capacity("flash-fresh", user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_USER) in err


@pytest.mark.asyncio
async def test_capacity_precheck_fails_open(cache):
    """No flash thread (report_back off) and a disabled cache both mean no cap —
    reserve() stays the authority for those paths."""
    assert await T.check_dispatch_capacity(None, "u-1") is None
    cache.enabled = False
    assert await T.check_dispatch_capacity("flash-1", "u-1") is None
