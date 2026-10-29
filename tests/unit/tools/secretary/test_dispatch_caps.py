"""Coverage for the report-back dispatch caps (per-thread + per-user).

A Chief of Staff thread can fan out many background PTC analyses, but unbounded fan-out
would overload the single backend. ``report_back.reserve()`` admits a dispatch
under both caps as ONE atomic Redis script *before* the dispatch POST (rolled
back gen-gated on failure), so racing calls can't both pass the check then
overshoot, and no concurrent teardown can observe a half-applied reservation.
"""

from __future__ import annotations

import asyncio

import pytest

from src.server.services.report_back.handoff import reserve as T
from tests.unit.server.handlers.chat.redis_fakes import FakeCache as _FakeCache


async def _reserve_err(dispatcher, ptc, user) -> str | None:
    """Reserve a slot like a successful dispatch; return the cap error."""
    async with T.reserve(dispatcher, ptc, f"ws-{ptc}", "fws-1", user) as slot:
        if slot.error is None:
            slot.commit()
        return slot.error


@pytest.fixture
def cache(monkeypatch):
    c = _FakeCache()
    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: c
    )
    return c


@pytest.mark.asyncio
async def test_per_thread_cap_rejects_beyond_limit(cache):
    dispatcher, user = "cos-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_THREAD):
        assert await _reserve_err(dispatcher, f"p{i}", user) is None
    err = await _reserve_err(dispatcher, "p-over", user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_THREAD) in err
    # The rejected dispatch left no residue in either SET.
    assert "p-over" not in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert "p-over" not in cache.client.sets[f"handoff_user_pending:{user}"]


@pytest.mark.asyncio
async def test_per_user_cap_spans_multiple_dispatcher_threads(cache):
    user = "u-1"
    # Spread dispatches across threads, staying under each per-thread cap
    # (<5 each) but reaching the per-user cap of 10.
    placed = 0
    dispatcher_idx = 0
    while placed < T.MAX_DISPATCH_PER_USER:
        dispatcher = f"cos-{dispatcher_idx}"
        for _ in range(min(T.MAX_DISPATCH_PER_THREAD - 1, T.MAX_DISPATCH_PER_USER - placed)):
            assert await _reserve_err(dispatcher, f"p{placed}", user) is None
            placed += 1
        dispatcher_idx += 1
    # 11th anywhere is rejected by the per-user cap.
    err = await _reserve_err("cos-new", "p-over", user)
    assert err is not None
    assert str(T.MAX_DISPATCH_PER_USER) in err


@pytest.mark.asyncio
async def test_idempotent_redispatch_does_not_count_against_cap(cache):
    dispatcher, user = "cos-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_THREAD):
        assert await _reserve_err(dispatcher, f"p{i}", user) is None
    # Re-reserving an existing member (idempotent re-dispatch) is admitted even
    # though the SET is already at the cap.
    assert await _reserve_err(dispatcher, "p0", user) is None


@pytest.mark.asyncio
async def test_uncommitted_reserve_rolls_back_both_sets_and_origin(cache):
    dispatcher, user, ptc = "cos-1", "u-1", "p0"
    async with T.reserve(dispatcher, ptc, "ws-1", "fws-1", user) as slot:
        assert slot.error is None
        assert slot.wired is True
        assert slot.dispatch_gen
        assert ptc in cache.client.sets[f"handoff_watch:{dispatcher}"]
        assert ptc in cache.client.sets[f"handoff_user_pending:{user}"]
        assert cache.kv[T.ptc_origin_key(ptc)]["dispatch_gen"] == slot.dispatch_gen
        # ...exit WITHOUT commit -> rollback

    assert ptc not in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert ptc not in cache.client.sets[f"handoff_user_pending:{user}"]
    assert T.ptc_origin_key(ptc) not in cache.kv
    # A freed slot is reusable.
    assert await _reserve_err(dispatcher, "p-new", user) is None


@pytest.mark.asyncio
async def test_refresh_rollback_restores_prev_origin_and_memberships(cache):
    """A continuation reserve of an existing pair stashes the previous origin;
    its uncommitted exit restores that incarnation and srems NOTHING it did
    not add — the live pair keeps its membership and its own generation."""
    dispatcher, user, ptc = "cos-1", "u-1", "T"

    assert await _reserve_err(dispatcher, ptc, user) is None  # committed incarnation
    first_gen = cache.kv[T.ptc_origin_key(ptc)]["dispatch_gen"]
    assert first_gen

    async with T.reserve(dispatcher, ptc, "ws-T", "fws-1", user) as slot:
        assert slot.error is None
        assert slot.wired is True
        assert slot._added == {"watch": False, "user": False}
        # The refresh bumped the generation in place...
        assert cache.kv[T.ptc_origin_key(ptc)]["dispatch_gen"] == slot.dispatch_gen
        assert slot.dispatch_gen != first_gen

    # ...and the uncommitted exit restored the previous incarnation wholesale.
    assert cache.kv[T.ptc_origin_key(ptc)]["dispatch_gen"] == first_gen
    assert ptc in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert ptc in cache.client.sets[f"handoff_user_pending:{user}"]


@pytest.mark.asyncio
async def test_rollback_is_noop_after_a_rival_rewrote_the_origin(cache):
    """Codex round-4 race, cross-process form: another WORKER (no shared
    pair lock) refreshes the origin while A's dispatch is in flight, then A
    fails. A's rollback must not srem the membership the rival's live
    dispatch depends on, nor touch its origin — the gen CAS makes it a
    no-op. (The in-process interleaving is now impossible: same-pair
    lifecycles serialize on the pair lock.)"""
    dispatcher, user, ptc = "cos-1", "u-1", "p-race"

    async with T.reserve(dispatcher, ptc, "ws-1", "fws-1", user) as slot_a:
        assert slot_a._added == {"watch": True, "user": True}
        # A rival worker's reserve commits mid-flight: same memberships,
        # origin rewritten under ITS generation (Redis-side state is what
        # the rollback CAS sees; the asyncio lock doesn't span processes).
        rival = dict(cache.kv[T.ptc_origin_key(ptc)])
        rival["dispatch_gen"] = "g-rival"
        cache.kv[T.ptc_origin_key(ptc)] = rival
        # A exits without commit -> its rollback runs against the rival's gen.

    assert ptc in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert ptc in cache.client.sets[f"handoff_user_pending:{user}"]
    assert cache.kv[T.ptc_origin_key(ptc)]["dispatch_gen"] == "g-rival"


@pytest.mark.asyncio
async def test_cross_dispatcher_reserve_proceeds_unwired_touching_nothing(cache):
    """A PTC whose origin belongs to a DIFFERENT Chief of Staff thread can't be wired
    twice: the reserve returns unwired without mutating any state."""
    other_origin = {
        "origin": "home",
        "report_back": True,
        "home_thread_id": "cos-OTHER",
        "user_id": "u-2",
        "dispatch_gen": "gen-other",
    }
    cache.kv[T.ptc_origin_key("p1")] = other_origin
    async with T.reserve("cos-1", "p1", "ws-1", "fws-1", "u-1") as slot:
        assert slot.error is None
        assert slot.wired is False
        assert slot.dispatch_gen is None

    assert cache.kv[T.ptc_origin_key("p1")] == other_origin
    assert "p1" not in cache.client.sets.get("handoff_watch:cos-1", set())
    assert "p1" not in cache.client.sets.get("handoff_user_pending:u-1", set())


@pytest.mark.asyncio
async def test_over_cap_reserve_reaps_orphaned_members_and_retries(cache):
    """Codex round-5 F1: a member stranded WITHOUT an origin (lost reserve
    reply — fail-closed keeps state, nothing else removes it, and every
    later reserve refreshes the shared SET's TTL) must not wedge the cap
    forever. An over-cap reserve reaps originless members once and retries."""
    dispatcher, user = "cos-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_THREAD):
        assert await _reserve_err(dispatcher, f"p{i}", user) is None
    # Strand two members: their origins vanish (TTL expiry of a lost-reply
    # reservation) but the memberships remain.
    for ptc in ("p0", "p1"):
        del cache.kv[T.ptc_origin_key(ptc)]

    assert await _reserve_err(dispatcher, "p-new", user) is None
    assert "p0" not in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert "p1" not in cache.client.sets[f"handoff_watch:{dispatcher}"]
    assert "p-new" in cache.client.sets[f"handoff_watch:{dispatcher}"]
    # Members with live origins were untouched by the reap.
    assert "p2" in cache.client.sets[f"handoff_watch:{dispatcher}"]


@pytest.mark.asyncio
async def test_same_pair_lifecycles_serialize_no_resurrection(cache):
    """Codex round-5 F2: A reserves fresh, B refreshes stashing A's origin as
    prev, A fails, B fails -> B's rollback would restore A's already-failed
    incarnation. The per-pair lock serializes whole lifecycles, so B only
    starts after A's rollback settled and the interleaving cannot exist."""
    dispatcher, user, ptc = "cos-1", "u-1", "p-race"
    a_inside = asyncio.Event()
    a_release = asyncio.Event()

    async def cycle_a():
        async with T.reserve(dispatcher, ptc, "ws-1", "fws-1", user) as slot:
            assert slot.error is None
            a_inside.set()
            await a_release.wait()
            # exit WITHOUT commit -> rollback (A failed)

    async def cycle_b():
        await a_inside.wait()
        async with T.reserve(dispatcher, ptc, "ws-1", "fws-1", user) as slot:
            # B could only enter after A's whole lifecycle (incl. rollback)
            # finished: A's state is gone, B reserves FRESH (no prev stash).
            assert slot._prev_origin_raw is None
            assert slot._added == {"watch": True, "user": True}
            # B also fails uncommitted.

    task_b = asyncio.create_task(cycle_b())
    task_a = asyncio.create_task(cycle_a())
    await a_inside.wait()
    await asyncio.sleep(0)  # let B reach the pair lock and block
    a_release.set()
    await asyncio.gather(task_a, task_b)

    # No resurrected incarnation: everything rolled back clean.
    assert T.ptc_origin_key(ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(f"handoff_watch:{dispatcher}", set())
    assert ptc not in cache.client.sets.get(f"handoff_user_pending:{user}", set())
    assert T._pair_locks == {}


@pytest.mark.asyncio
async def test_reserve_is_noop_when_cache_disabled(monkeypatch):
    class _Disabled:
        enabled = False
        client = None

    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _Disabled()
    )
    # No Redis by config -> no report-back system at all: admit the dispatch
    # unwired (the completion-time gate can't deliver a report-back).
    async with T.reserve("f", "p", "ws", "fws", "u") as slot:
        assert slot.error is None
        assert slot.wired is False
        assert slot.dispatch_gen is None


@pytest.mark.asyncio
async def test_reserve_fails_closed_on_script_failure(monkeypatch):
    """A reserve-script failure leaves UNKNOWN state: the dispatch must abort
    (slot.error == 'dispatch_failed') and the exit must NOT attempt a blind
    rollback — destroying a possibly live incarnation is worse than a
    TTL-bounded leak."""
    evals: list = []

    class _BoomClient:
        async def eval(self, *args):
            evals.append(args)
            raise RuntimeError("redis down")

    class _BoomCache:
        enabled = True
        client = _BoomClient()

    monkeypatch.setattr(
        "src.utils.cache.redis_cache.get_cache_client", lambda: _BoomCache()
    )
    async with T.reserve("f", "p", "ws", "fws", "u") as slot:
        assert slot.error == "dispatch_failed"
        assert slot.wired is False

    # Exactly one eval (the reserve attempt); no rollback script fired.
    assert len(evals) == 1


# ---------------------------------------------------------------------------
# Concurrency — the single reserve script serializes the cap check + the add
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_reserves_cannot_overshoot_per_thread_cap(cache):
    """Two racing reserves for the last free per-thread slot can't both win:
    the whole check-through-add runs as one Redis script, so rivals see each
    other's writes regardless of event-loop interleaving."""
    dispatcher, user = "cos-1", "u-1"
    for i in range(T.MAX_DISPATCH_PER_THREAD - 1):
        assert await _reserve_err(dispatcher, f"p{i}", user) is None

    results = await asyncio.gather(
        _reserve_err(dispatcher, "p-new-1", user),
        _reserve_err(dispatcher, "p-new-2", user),
    )
    admitted = [e for e in results if e is None]
    rejected = [e for e in results if e is not None]
    assert len(admitted) == 1  # exactly one winner
    assert len(rejected) == 1  # the other is capped out
    assert "on this thread" in rejected[0]  # rejected by the per-thread cap
    assert str(T.MAX_DISPATCH_PER_THREAD) in rejected[0]
    assert len(cache.client.sets[f"handoff_watch:{dispatcher}"]) == T.MAX_DISPATCH_PER_THREAD
