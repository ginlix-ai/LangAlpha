"""Flash report-back execution path (outbox-executor model, v4 2.3).

``execute_report_back`` runs one durable ``report_back`` outbox job: it POSTs
the summary turn (with a job-deterministic request_key), persists the
dispatched run id for crash-resume, and holds the job open — lease-heartbeated
— until the summary run's row reaches terminal. Per-flash ordering is enforced
by the outbox claim query's ordering-key gate (DB-level, exercised in E2E);
these tests lock the executor's half: dispatch idempotency, teardown paths,
and the hold-until-terminal contract.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.chat import notify_turn, report_back
from tests.unit.server.handlers.chat.redis_fakes import (
    FakeCache as _FakeCache,
    origin as _origin,
    seed_dispatched as _seed_dispatched,
)


def _job(
    ptc: str, job_id: str = "job-1", ordering_key: str = "flash-1", **payload_extra
) -> dict:
    return {
        "hook_outbox_id": job_id,
        "hook_type": "report_back",
        "payload": {"ptc_thread_id": ptc, **payload_extra},
        "run_id": "ptc-run-1",
        "attempts": 1,
        "ordering_key": ordering_key,
    }


def _run_row(status: str) -> dict:
    return {"conversation_response_id": "rb-run", "status": status}


class _ExecHarness:
    """Patches every collaborator of execute_report_back in one place."""

    def __init__(
        self,
        cache: _FakeCache,
        *,
        post_result=("dispatched", "rb-run"),
        run_statuses=("completed",),
        lease_ok=True,
        guard_owned=True,
    ):
        self.cache = cache
        self.post = AsyncMock(return_value=post_result)
        self.get_run = AsyncMock(side_effect=[_run_row(s) if s else None for s in run_statuses])
        self.extend_lease = AsyncMock(return_value=lease_ok)
        self.merge_payload = AsyncMock(return_value=True)
        self.requeue = AsyncMock(return_value="pending")
        self.guard_owned = guard_owned
        self.guard_entries = 0

        harness = self

        @asynccontextmanager
        async def _guard(job_id, attempts):
            harness.guard_entries += 1
            yield harness.guard_owned

        self.fenced_guard = _guard

    def patches(self):
        return [
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=self.cache),
            patch.object(report_back, "_post_report_back", self.post),
            patch("src.server.database.turn_lifecycle.get_run", self.get_run),
            patch("src.server.database.hook_outbox.extend_job_lease", self.extend_lease),
            patch("src.server.database.hook_outbox.merge_job_payload", self.merge_payload),
            patch("src.server.database.hook_outbox.requeue_job_with_key", self.requeue),
            patch("src.server.database.hook_outbox.fenced_job_guard", self.fenced_guard),
            patch.object(notify_turn, "_TERMINAL_POLL", 0.0),
        ]

    async def run(self, job: dict) -> None:
        import contextlib

        with contextlib.ExitStack() as stack:
            for p in self.patches():
                stack.enter_context(p)
            await report_back.execute_report_back(job)


# ---------------------------------------------------------------------------
# clear_flash_report_back — full per-pair teardown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_tears_down_all_per_pair_state():
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-1"}

    await report_back.clear_flash_report_back(cache, ptc, flash)

    assert f"ptc_origin:{ptc}" not in cache.kv
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
    assert ptc not in cache.client.sets.get("flash_user_pending:u-1", set())


@pytest.mark.asyncio
async def test_clear_without_flash_thread_id_only_deletes_origin():
    cache = _FakeCache()
    cache.kv["ptc_origin:ptc-1"] = _origin("ptc-1")

    await report_back.clear_flash_report_back(cache, "ptc-1", None)

    assert "ptc_origin:ptc-1" not in cache.kv


@pytest.mark.asyncio
async def test_clear_teardown_is_one_atomic_script_then_drained_record():
    """The whole gen-gated teardown is ONE Lua eval (a separate compare-then-
    pipeline would race a concurrent reserve()); the drained-run record
    follows in its own best-effort pipeline, never interleaved."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-1"}

    batches: list[list[str]] = []
    orig_pipeline = cache.client.pipeline

    def _recording_pipeline(transaction: bool = True):
        pipe = orig_pipeline(transaction=transaction)
        orig_execute = pipe.execute

        async def _execute():
            batches.append([op[0] for op in pipe._ops])
            return await orig_execute()

        pipe.execute = _execute
        return pipe

    cache.client.pipeline = _recording_pipeline

    cleared = await report_back.clear_flash_report_back(cache, ptc, flash)

    assert cleared
    assert f"ptc_origin:{ptc}" not in cache.kv
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
    # Only the drained-run record uses a pipeline; the teardown itself is Lua.
    assert batches == [["lrem", "lpush", "ltrim", "expire"]]
    assert cache.client.lists[report_back.flash_rb_done_key(flash)] == ["rb-1"]


@pytest.mark.asyncio
async def test_clear_with_stale_gen_is_skipped_entirely():
    """Incarnation fence: a clear carrying an OLD dispatch generation finds a
    re-dispatched origin and must leave origin/pointer/membership/cap intact."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-NEW"
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-NEW"}

    cleared = await report_back.clear_flash_report_back(
        cache, ptc, flash, expected_gen="g-OLD"
    )

    assert not cleared
    assert f"ptc_origin:{ptc}" in cache.kv
    assert cache.kv[report_back.flash_rb_run_key(flash, ptc)] == {"run_id": "rb-NEW"}
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert ptc in cache.client.sets["flash_user_pending:u-1"]


@pytest.mark.asyncio
async def test_clear_with_matching_or_absent_gen_proceeds():
    """Same generation clears; a legacy origin without a generation (or a
    missing origin) never blocks the teardown."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2"])
    cache.kv["ptc_origin:ptc-1"]["dispatch_gen"] = "g-1"

    assert await report_back.clear_flash_report_back(
        cache, "ptc-1", flash, expected_gen="g-1"
    )
    # Legacy origin (no gen field) + fenced clear: proceeds.
    assert await report_back.clear_flash_report_back(
        cache, "ptc-2", flash, expected_gen="g-x"
    )
    assert not cache.client.sets.get(report_back.flash_watch_key(flash))


@pytest.mark.asyncio
async def test_genless_clear_never_touches_a_generated_origin():
    """Codex round-4: a caller that cannot name the incarnation it means to
    destroy (legacy job, unresolvable crash context) must not clear a
    GENERATED origin — 'unknown' degrades to a TTL leak, never to erasure."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-LIVE"

    cleared = await report_back.clear_flash_report_back(cache, ptc, flash)

    assert not cleared
    assert f"ptc_origin:{ptc}" in cache.kv
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert ptc in cache.client.sets["flash_user_pending:u-1"]


@pytest.mark.asyncio
async def test_offchain_clear_refuses_while_a_run_pointer_is_live():
    """Codex round-19 P1: a gen-MATCHED crash clear (off the flash ordering
    chain) must not drain a pair whose summary admission another lineage
    already claimed — the pointer is that admission's consummation record,
    and only its serialized lifecycle may drain it. The refusal is atomic
    inside the teardown script and touches NOTHING (no tombstone: nothing
    was fenced by a generation, so no rollback may complete it)."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-CRASHED"
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-live"}

    outcome = await report_back.clear_flash_report_back(
        cache, ptc, flash, expected_gen="g-CRASHED", refuse_if_pointer=True
    )

    assert not outcome.cleared
    assert outcome.fencer_gen is None
    assert f"ptc_origin:{ptc}" in cache.kv
    assert cache.kv[report_back.flash_rb_run_key(flash, ptc)] == {"run_id": "rb-live"}
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert ptc in cache.client.sets["flash_user_pending:u-1"]
    assert f"ptc_rb_tombstone:{ptc}" not in cache.client.sets


@pytest.mark.asyncio
async def test_offchain_clear_proceeds_when_no_pointer_exists():
    """The refusal is pointer-scoped: a rowless crash with no admission in
    flight still tears its own pair down."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-CRASHED"

    assert await report_back.clear_flash_report_back(
        cache, ptc, flash, expected_gen="g-CRASHED", refuse_if_pointer=True
    )
    assert f"ptc_origin:{ptc}" not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


# ---------------------------------------------------------------------------
# Teardown tombstone — a fenced-out teardown must survive a later rollback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fenced_clear_tombstones_and_rollback_completes_it():
    """Codex round-9 F3 (resurrection): G1's teardown arriving while
    provisional G2 holds the origin is gen-skipped; G2's rollback must then
    HONOR that teardown — full pair clear — not restore G1, whose only
    cleanup already ran and acked. Round-10 F5: the completed clear also
    records the consumed run pointer as drained (the fenced teardown never
    got to), preserving the client's discovery path."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    _seed_dispatched(cache, flash, [ptc], user)
    cache.kv[report_back.ptc_origin_key(ptc)]["dispatch_gen"] = "g-1"
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-1"}
    tomb = report_back.ptc_teardown_tombstone_key(ptc)

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user) as slot:
            assert slot.error is None
            # G1's terminal teardown lands mid-reservation: fenced + tombstoned.
            cleared = await report_back.clear_flash_report_back(
                cache, ptc, flash, expected_gen="g-1"
            )
            assert not cleared
            assert "g-1" in cache.client.sets[tomb]
            # No commit → the CM rolls back on exit.

    assert report_back.ptc_origin_key(ptc) not in cache.kv
    assert tomb not in cache.client.sets
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
    assert ptc not in cache.client.sets.get(
        report_back.flash_user_pending_key(user), set()
    )
    assert cache.client.lists[report_back.flash_rb_done_key(flash)] == ["rb-1"]


@pytest.mark.asyncio
async def test_two_fenced_clears_both_survive_in_the_tombstone_set():
    """Codex round-10 F4: a second stale clear (older G0) fenced in the same
    window must not overwrite G1's tombstone — the rollback must still see
    that the stashed predecessor G1's teardown arrived."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    _seed_dispatched(cache, flash, [ptc], user)
    cache.kv[report_back.ptc_origin_key(ptc)]["dispatch_gen"] = "g-1"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user):
            assert not await report_back.clear_flash_report_back(
                cache, ptc, flash, expected_gen="g-1"
            )
            assert not await report_back.clear_flash_report_back(
                cache, ptc, flash, expected_gen="g-0"
            )
            # No commit → rollback must honor g-1 despite g-0 arriving later.

    assert report_back.ptc_origin_key(ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


@pytest.mark.asyncio
async def test_legacy_predecessor_fenced_clear_completes_on_rollback():
    """Codex round-10 F6: a gen-less predecessor's clear leaves the legacy
    sentinel; the rollback recognizes a gen-less stashed predecessor as dead
    on ANY fenced clear (every teardown is authorized against a legacy
    origin) instead of resurrecting it."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    _seed_dispatched(cache, flash, [ptc], user)  # legacy: no dispatch_gen

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user):
            # A legacy (gen-less) teardown lands mid-reservation: fenced.
            assert not await report_back.clear_flash_report_back(
                cache, ptc, flash
            )
            tomb = report_back.ptc_teardown_tombstone_key(ptc)
            assert "__legacy__" in cache.client.sets[tomb]

    assert report_back.ptc_origin_key(ptc) not in cache.kv
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())
    assert ptc not in cache.client.sets.get(
        report_back.flash_user_pending_key(user), set()
    )


@pytest.mark.asyncio
async def test_successful_clear_removes_stale_tombstone():
    """A completed teardown deletes any lingering tombstone so it can't
    poison a FUTURE incarnation's rollback into over-clearing."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[report_back.ptc_origin_key(ptc)]["dispatch_gen"] = "g-1"
    tomb = report_back.ptc_teardown_tombstone_key(ptc)
    cache.client.sets[tomb] = {"g-0"}

    cleared = await report_back.clear_flash_report_back(
        cache, ptc, flash, expected_gen="g-1"
    )

    assert cleared
    assert tomb not in cache.client.sets


@pytest.mark.asyncio
async def test_rollback_ignores_tombstone_for_a_different_generation():
    """Only the stashed predecessor's OWN teardown is honored: a tombstone
    naming some other generation must not stop the restore of a GENERATED
    predecessor."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    _seed_dispatched(cache, flash, [ptc], user)
    cache.kv[report_back.ptc_origin_key(ptc)]["dispatch_gen"] = "g-1"
    cache.client.sets[report_back.ptc_teardown_tombstone_key(ptc)] = {"g-OTHER"}

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user):
            pass  # no commit → rollback

    assert cache.kv[report_back.ptc_origin_key(ptc)]["dispatch_gen"] == "g-1"
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]


@pytest.mark.asyncio
async def test_claim_replaces_a_stale_incarnations_pointer():
    """Codex round-4 stranding: G1's terminal pointer survives its skipped
    gen-mismatch clear; G2's admission must REPLACE it, not adopt the dead
    run (adopting would ack G2's summary without ever running it). A retry
    of the SAME incarnation still dedups to the incumbent."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    key = report_back.flash_rb_run_key(flash, ptc)
    cache.kv[key] = {"run_id": "rb-G1", "dispatch_gen": "g-1"}
    # G2's admission implies a live pair (its reserve created the membership).
    cache.client.sets.setdefault(report_back.flash_watch_key(flash), set()).add(ptc)

    # New incarnation: stale pointer replaced, claim won.
    result = await report_back.claim_report_back_run(
        cache, flash, ptc, "rb-G2", "g-2"
    )
    assert (result.winning_run_id, result.claimed) == ("rb-G2", True)
    assert cache.kv[key]["run_id"] == "rb-G2"
    assert cache.kv[key]["dispatch_gen"] == "g-2"

    # Same-incarnation retry: incumbent honored, no second run.
    result = await report_back.claim_report_back_run(
        cache, flash, ptc, "rb-G2-retry", "g-2"
    )
    assert (result.winning_run_id, result.claimed) == ("rb-G2", False)

    # A legacy (gen-less) claimer keeps today's semantics: adopt whatever is
    # there — it cannot prove the pointer stale.
    result = await report_back.claim_report_back_run(
        cache, flash, ptc, "rb-legacy", None
    )
    assert (result.winning_run_id, result.claimed) == ("rb-G2", False)


@pytest.mark.asyncio
async def test_clear_releases_cap_slot_via_explicit_user_id_when_origin_expired():
    """Origin TTL-expired: an explicit user_id still releases the per-user cap slot."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    cache.client.sets[report_back.flash_watch_key(flash)] = {ptc}
    cache.client.sets[f"flash_user_pending:{user}"] = {ptc}
    # ptc_origin intentionally absent (expired) -> can't be read for the user id.

    await report_back.clear_flash_report_back(cache, ptc, flash, user_id=user)

    assert ptc not in cache.client.sets.get(f"flash_user_pending:{user}", set())
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


@pytest.mark.asyncio
async def test_clear_warns_when_cap_slot_user_unresolvable():
    """No explicit user_id and no origin -> warn (leak observable) but still tear down."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    cache.client.sets[report_back.flash_watch_key(flash)] = {ptc}

    with patch.object(report_back.logger, "warning") as warn:
        await report_back.clear_flash_report_back(cache, ptc, flash)

    assert warn.called
    assert ptc not in cache.client.sets.get(report_back.flash_watch_key(flash), set())


# ---------------------------------------------------------------------------
# execute_report_back — gating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_skips_non_member_and_non_report_back():
    cache = _FakeCache()
    flash = "flash-1"
    # origin present but PTC was never a watch member (cap rollback / already cleared)
    cache.kv["ptc_origin:ptc-gone"] = _origin("ptc-gone", flash)
    # origin present, report_back disabled
    cache.kv["ptc_origin:ptc-noflag"] = _origin("ptc-noflag", flash)
    cache.kv["ptc_origin:ptc-noflag"]["report_back"] = False
    cache.client.sets[report_back.flash_watch_key(flash)] = {"ptc-noflag"}

    h = _ExecHarness(cache)
    await h.run(_job("ptc-gone"))
    await h.run(_job("ptc-noflag"))

    h.post.assert_not_called()
    h.merge_payload.assert_not_called()


@pytest.mark.asyncio
async def test_duplicate_job_execution_after_clear_is_a_noop():
    """At-least-once outbox delivery: a re-executed job whose member was
    already terminal-cleared returns without POSTing a second summary."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    await report_back.clear_flash_report_back(cache, ptc, flash)

    h = _ExecHarness(cache)
    await h.run(_job(ptc))

    h.post.assert_not_called()


# ---------------------------------------------------------------------------
# execute_report_back — dispatch + hold-until-terminal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_dispatches_with_deterministic_request_key_and_holds_open():
    """The POST carries uuid5(RB_REQUEST_NS, job_id); the job then stays open
    (run polled + lease heartbeated) until the summary run's row goes terminal.
    This is the executor half of the mark_completed(N) happens-before
    mark_active(N+1) invariant — the DB half is the claim query's
    ordering-key gate."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(
        cache, run_statuses=("in_progress", "in_progress", "completed")
    )
    await h.run(_job(ptc, job_id="job-42"))

    # POSTed exactly once, with the job-deterministic request_key and the
    # fenced heartbeat (so a long defer loop can't outlive the lease).
    h.post.assert_awaited_once()
    assert h.post.await_args.kwargs["request_key"] == str(
        uuid.uuid5(report_back.RB_REQUEST_NS, "job-42")
    )
    assert callable(h.post.await_args.kwargs["heartbeat"])
    # Durable resume pointer merged onto the job payload.
    h.merge_payload.assert_awaited_once_with(
        "job-42", {"dispatched_run_id": "rb-run"}
    )
    # /status reattach pointer re-asserted while membership held, scoped to
    # this job's request identity.
    pointer = cache.kv[report_back.flash_rb_run_key(flash, ptc)]
    assert pointer["run_id"] == "rb-run"
    assert pointer["request_key"] == str(
        uuid.uuid5(report_back.RB_REQUEST_NS, "job-42")
    )
    # Wake published with the run id.
    assert any('"run_id": "rb-run"' in msg for _, msg in cache.client.published)
    # Held open until terminal: fence-extend precedes every poll, so three
    # polls (two in_progress + the terminal one) mean three heartbeats.
    assert h.get_run.await_count == 3
    assert h.extend_lease.await_count == 3


@pytest.mark.asyncio
async def test_execute_drop_clears_member_so_chain_advances():
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, post_result=("drop", None))
    await h.run(_job(ptc))

    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert f"ptc_origin:{ptc}" not in cache.kv
    h.get_run.assert_not_called()  # no run to await


@pytest.mark.asyncio
async def test_drop_refused_by_live_pointer_nacks():
    """Codex round-5 F1: the last POST's server route can outlive the client
    socket timeout and still be pre-START inside a lawful admission hold when
    the busy-wait cap expires — its live pointer claim must fence the drop's
    teardown (nack, retry adopts/takes over/finds released), never be cleared
    out from under it. The chain lease binds the executor, not that route."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    # A route mid-admission holds the pointer claim for this pair.
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {
        "run_id": "rt-run",
        "claimed_at": 12345.0,
    }

    h = _ExecHarness(cache, post_result=("drop", None))
    with pytest.raises(RuntimeError, match="live run pointer"):
        await h.run(_job(ptc))

    # Everything intact: membership, origin, and the claim itself.
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert f"ptc_origin:{ptc}" in cache.kv
    assert report_back.flash_rb_run_key(flash, ptc) in cache.kv


@pytest.mark.asyncio
async def test_execute_deleted_discards_whole_flash_thread():
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2", "ptc-3"])

    h = _ExecHarness(cache, post_result=("deleted", None))
    await h.run(_job("ptc-1"))

    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    for ptc in ["ptc-1", "ptc-2", "ptc-3"]:
        assert f"ptc_origin:{ptc}" not in cache.kv


@pytest.mark.asyncio
async def test_discard_nacks_on_transient_origin_read_failure():
    """Codex round-5 F4: a transient origin-read failure mid-discard must
    nack the job (raise) with the watch set INTACT — degrading to a gen-less
    clear would refuse a generated origin and then delete the set (its only
    reference), stranding origin/pointer/cap state forever."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-2"])
    cache.kv["ptc_origin:ptc-2"]["dispatch_gen"] = "g-live"

    real_get_strict = cache.get_strict

    async def _flaky(key):
        if key == report_back.ptc_origin_key("ptc-2"):
            raise ConnectionError("redis blip")
        return await real_get_strict(key)

    cache.get_strict = _flaky
    with pytest.raises(ConnectionError):
        await report_back._discard_flash_thread(cache, flash)

    # Watch set retained: the nacked job's retry re-reads remaining members.
    assert "ptc-2" in cache.client.sets[report_back.flash_watch_key(flash)]
    assert "ptc_origin:ptc-2" in cache.kv


@pytest.mark.asyncio
async def test_discard_nacks_when_member_clear_is_refused():
    """A member whose clear is refused (generation moved between read and
    CAS, still owned by THIS flash) must nack the whole job rather than
    fall through to deleting the watch set."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1"])
    cache.kv["ptc_origin:ptc-1"]["dispatch_gen"] = "g-1"

    real_get_strict = cache.get_strict

    async def _racing(key):
        origin = await real_get_strict(key)
        if key == report_back.ptc_origin_key("ptc-1"):
            # A rival re-dispatch bumps the generation right after our read,
            # so the observed-gen CAS below will refuse.
            cache.kv[key] = {**origin, "dispatch_gen": "g-2"}
            return origin
        return origin

    cache.get_strict = _racing
    with pytest.raises(RuntimeError, match="refused"):
        await report_back._discard_flash_thread(cache, flash)

    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key(flash)]
    assert cache.kv["ptc_origin:ptc-1"]["dispatch_gen"] == "g-2"


@pytest.mark.asyncio
async def test_discard_drops_only_our_reference_for_cross_flash_members():
    """A member whose origin moved to a different flash thread loses only
    OUR stale watch reference — its live dispatch keeps origin/pointer/cap."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1", "ptc-x"])
    cache.kv["ptc_origin:ptc-x"] = _origin("ptc-x", flash="flash-OTHER")

    await report_back._discard_flash_thread(cache, flash)

    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert "ptc_origin:ptc-1" not in cache.kv  # ours: fully cleared
    assert "ptc_origin:ptc-x" in cache.kv  # theirs: untouched
    assert "ptc-x" in cache.client.sets[report_back.flash_user_pending_key("u-1")]


@pytest.mark.asyncio
async def test_discard_spares_a_member_reserved_after_the_snapshot():
    """Codex round-6 P2: a member SADDed by a concurrent reserve after the
    discard's SMEMBERS snapshot must survive — the old unconditional final
    DEL removed it, so its completion later acked as a non-member and the
    summary was dropped."""
    cache = _FakeCache()
    flash = "flash-1"
    _seed_dispatched(cache, flash, ["ptc-1"])
    watch_key = report_back.flash_watch_key(flash)
    real_smembers = cache.client.smembers

    async def _snapshot_then_rival(key):
        snapshot = await real_smembers(key)
        if key == watch_key:
            # A concurrent reserve lands right after our snapshot (SADD +
            # origin write are one atomic script in prod).
            cache.client.sets[watch_key].add("ptc-2")
            cache.kv[report_back.ptc_origin_key("ptc-2")] = _origin("ptc-2", flash)
        return snapshot

    cache.client.smembers = _snapshot_then_rival

    await report_back._discard_flash_thread(cache, flash)

    # The snapshotted member is fully cleared; the late one survives intact.
    assert "ptc_origin:ptc-1" not in cache.kv
    assert cache.client.sets[watch_key] == {"ptc-2"}
    assert "ptc_origin:ptc-2" in cache.kv


@pytest.mark.asyncio
async def test_stale_ordering_key_requeues_onto_flash_chain():
    """A pre-deploy job claimed under its PTC-thread key must be requeued onto
    the real flash chain, not executed: N such jobs busy-wait their caps
    CONCURRENTLY against one flash admission gate and can drop summaries."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache)
    await h.run(_job(ptc, job_id="job-7", ordering_key=ptc))

    h.requeue.assert_awaited_once_with(
        "job-7", attempts=1, ordering_key=flash, max_attempts=5
    )
    h.post.assert_not_called()
    # Nothing touched: the requeued row runs later at the chain head.
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert not cache.client.published


@pytest.mark.asyncio
async def test_drop_clear_is_fenced_to_own_generation():
    """The drop-path clear carries the job's dispatch generation: when the
    pair was re-dispatched (origin holds a newer gen), the teardown skips."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-NEW"

    h = _ExecHarness(cache, post_result=("drop", None))
    await h.run(_job(ptc, dispatch_gen="g-OLD"))

    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert f"ptc_origin:{ptc}" in cache.kv


@pytest.mark.asyncio
async def test_drop_without_guard_ownership_does_no_teardown():
    """The row-lock fence lost between POST and teardown: the reclaiming
    owner decides the teardown; the loser must leave everything intact."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, post_result=("drop", None), guard_owned=False)
    await h.run(_job(ptc))

    assert h.guard_entries == 1
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert f"ptc_origin:{ptc}" in cache.kv


@pytest.mark.asyncio
async def test_reassert_never_overwrites_a_different_runs_pointer():
    """ABA guard: a stale owner's re-assert must not repoint a re-dispatched
    pair at its own (dead) run — the Lua sets only absent-or-same run_id."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    # A newer incarnation already owns the pointer.
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {"run_id": "rb-NEW"}

    h = _ExecHarness(cache, run_statuses=("completed",))
    await h.run(_job(ptc))  # POSTs and re-asserts rb-run

    assert cache.kv[report_back.flash_rb_run_key(flash, ptc)] == {
        "run_id": "rb-NEW"
    }
    # Pointer refused -> the wake is suppressed too: clients must never be
    # woken toward a run the pointer doesn't acknowledge.
    assert not cache.client.published


@pytest.mark.asyncio
async def test_reassert_replaces_a_stale_incarnations_pointer():
    """Converse of the ABA guard: when the executor DOES carry its dispatch
    generation, a pointer left behind by a PREVIOUS incarnation (its clear
    was gen-skipped) is replaced, so clients reattach to the live run."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])
    cache.kv[f"ptc_origin:{ptc}"]["dispatch_gen"] = "g-2"
    # G1's terminal pointer survived its skipped gen-mismatch clear.
    cache.kv[report_back.flash_rb_run_key(flash, ptc)] = {
        "run_id": "rb-G1", "dispatch_gen": "g-1",
    }

    h = _ExecHarness(cache, run_statuses=("completed",))
    await h.run(_job(ptc, dispatch_gen="g-2"))

    pointer = cache.kv[report_back.flash_rb_run_key(flash, ptc)]
    assert pointer["run_id"] == "rb-run"
    assert pointer["dispatch_gen"] == "g-2"
    assert pointer["request_key"] == str(
        uuid.uuid5(report_back.RB_REQUEST_NS, "job-1")
    )
    assert any('"run_id": "rb-run"' in msg for _, msg in cache.client.published)


@pytest.mark.asyncio
async def test_resume_without_guard_ownership_publishes_no_wake():
    """Stale resume: a reclaimed job's old owner waking clients toward its
    (possibly superseded) run id would race the live owner's delivery."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, guard_owned=False)
    await h.run(_job(ptc, dispatched_run_id="rb-old"))

    h.post.assert_not_called()
    assert not cache.client.published
    h.get_run.assert_not_called()  # stands down before the terminal wait


@pytest.mark.asyncio
async def test_execute_resumes_dispatched_run_id_without_reposting():
    """A reclaimed job (worker crash mid terminal-wait) resumes via the merged
    dispatched_run_id instead of POSTing — and paying for — a second summary."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, run_statuses=("completed",))
    await h.run(_job(ptc, dispatched_run_id="prior-run"))

    h.post.assert_not_called()
    h.merge_payload.assert_not_called()  # already persisted
    # Wake re-published (idempotent) with the resumed run id.
    assert any('"run_id": "prior-run"' in msg for _, msg in cache.client.published)


@pytest.mark.asyncio
async def test_reassert_skipped_when_membership_already_cleared():
    """A fast terminal during the POST must not have its deleted run pointer
    resurrected by the post-dispatch re-assert."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    async def _post_then_clear(*args, **kwargs):
        # Terminal fires during the POST: clears the run pointer AND the watch
        # membership before we return "dispatched".
        await report_back.clear_flash_report_back(cache, ptc, flash)
        return "dispatched", "rb-run"

    h = _ExecHarness(cache, run_statuses=("completed",))
    h.post = AsyncMock(side_effect=_post_then_clear)
    await h.run(_job(ptc))

    # Membership was gone at re-assert time, so the deleted pointer stays deleted.
    assert report_back.flash_rb_run_key(flash, ptc) not in cache.kv


@pytest.mark.asyncio
async def test_execute_no_run_id_raises_to_nack():
    """A 2xx whose body yielded no run id can't be awaited — acking would
    release the chain early. Raise (nack): the retry re-POSTs the SAME
    request_key and recovers the id via 409 duplicate_request adoption."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, post_result=("dispatched", None))
    with pytest.raises(RuntimeError, match="without a run_id"):
        await h.run(_job(ptc))

    h.get_run.assert_not_called()
    h.merge_payload.assert_not_called()
    # Nothing torn down, no wake — the retry owns the delivery.
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert not cache.client.published


# ---------------------------------------------------------------------------
# _await_run_terminal — the hold-open loop's exits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_terminal_wait_cap_force_clears_stuck_member(monkeypatch):
    """A summary run that never reaches terminal must not wedge the flash
    thread's chain forever."""
    monkeypatch.setattr(report_back, "_RB_TERMINAL_WAIT_CAP", -1.0)
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, run_statuses=("in_progress",))
    await h.run(_job(ptc))

    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert f"ptc_origin:{ptc}" not in cache.kv


@pytest.mark.asyncio
async def test_lease_lost_stands_down_without_clearing():
    """A reclaimed lease means another drainer owns the job now; the loser
    must not tear anything down."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, run_statuses=("in_progress",), lease_ok=False)
    await h.run(_job(ptc, dispatched_run_id="rb-run"))

    # Member intact — the new owner resumes the wait.
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert f"ptc_origin:{ptc}" in cache.kv


@pytest.mark.asyncio
async def test_missing_run_row_is_polled_through_not_torn_down():
    """Dispatched admission returns the run id BEFORE the START transaction
    commits, so an early poll can legitimately see no row — the wait must
    poll through it, never treat it as deletion."""
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, run_statuses=(None, None, "completed"))
    await h.run(_job(ptc, dispatched_run_id="rb-run"))

    assert h.get_run.await_count == 3
    # No teardown: the member awaits the summary run's own watch_clear job.
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert f"ptc_origin:{ptc}" in cache.kv


@pytest.mark.asyncio
async def test_permanently_missing_run_row_force_cleared_at_cap(monkeypatch):
    """Thread deleted mid-summary (rows cascade): the row stays missing and
    the deadline — not the missing row itself — releases the watch + caps."""
    monkeypatch.setattr(report_back, "_RB_TERMINAL_WAIT_CAP", -1.0)
    cache = _FakeCache()
    flash, ptc = "flash-1", "ptc-1"
    _seed_dispatched(cache, flash, [ptc])

    h = _ExecHarness(cache, run_statuses=(None,))
    await h.run(_job(ptc, dispatched_run_id="rb-run"))

    assert not cache.client.sets.get(report_back.flash_watch_key(flash))
    assert f"ptc_origin:{ptc}" not in cache.kv


# ---------------------------------------------------------------------------
# Consumption clear (1.7): the completed report-back flash run enqueues a
# watch_clear outbox job gated on report_back_ptc_thread_id — no in-process
# completion hook remains. Ordering keys follow the one rule: the watching
# flash thread (origin_flash_thread_id, else the run's own thread).
# ---------------------------------------------------------------------------


def test_completed_flash_with_report_back_id_enqueues_consumption_clear():
    from src.server.database.hook_outbox import build_finalize_jobs

    jobs = build_finalize_jobs(
        run_id="run-1",
        thread_id="flash-1",
        msg_type="flash",
        user_id="u-1",
        report_back_ptc_thread_id="ptc-1",
    )("completed")

    clears = [j for j in jobs if j.hook_type == "watch_clear"]
    assert len(clears) == 1
    assert clears[0].payload["ptc_thread_id"] == "ptc-1"
    assert clears[0].payload["error_wake"] is False  # consumption, not failure
    # The summary run IS the watching flash thread — same chain as the
    # report_back job that dispatched it.
    assert clears[0].ordering_key == "flash-1"


def test_completed_flash_without_report_back_id_skips_clear():
    from src.server.database.hook_outbox import build_finalize_jobs

    jobs = build_finalize_jobs(
        run_id="run-1",
        thread_id="flash-1",
        msg_type="flash",
        user_id="u-1",
    )("completed")

    assert not [j for j in jobs if j.hook_type == "watch_clear"]


# ---------------------------------------------------------------------------
# resolve_orphaned_watch — atomic decide+resolve+receipt for a pair whose
# teardown was fenced by a never-admitted reservation (Codex 2.3 round-13
# P0): the caller probes the ledger (fencer's origin_dispatch_gen row, then
# any live run on the thread) and the Lua CASes on the EXACT fenced
# generation and the pre-START intent stamp, so a live or admitted owner is
# never resolved; a resolved phantom is receipted so its late admission is
# refused.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _stub_ledger():
    """resolve_orphaned_watch probes the ledger caller-side; default shape
    here is an idle thread with no admitted row (the phantom baseline)."""
    with (
        patch(
            "src.server.database.turn_lifecycle.thread_has_dispatch_gen",
            AsyncMock(return_value=False),
        ) as has_gen,
        patch(
            "src.server.database.turn_lifecycle.get_active_run",
            AsyncMock(return_value=None),
        ) as active,
    ):
        yield SimpleNamespace(has_gen=has_gen, active=active)


def _seed_phantom(cache, gen: str = "g-PHANTOM", ptr: dict | None = None):
    """Dispatched pair whose origin carries ``gen`` with no admission stamp
    and no ledger row — the lost-409 phantom shape."""
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")]["dispatch_gen"] = gen
    if ptr is not None:
        cache.kv[report_back.flash_rb_run_key("flash-1", "ptc-1")] = ptr


@pytest.mark.asyncio
async def test_resolve_phantom_resolves_pending_spares_origin_and_receipts():
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-9", "request_key": "rk-9"})
    ptr_key = report_back.flash_rb_run_key("flash-1", "ptc-1")

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (True, "rb-9")
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_watch_key("flash-1"), set()
    )
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_user_pending_key("u-1"), set()
    )
    assert ptr_key not in cache.kv
    # The origin is SPARED: only an authorized gen-matched teardown may
    # destroy it. The receipt is what blocks the phantom's late admission.
    assert cache.kv[report_back.ptc_origin_key("ptc-1")] is not None
    receipt_key = report_back.ptc_rb_resolved_key("ptc-1")
    assert "g-PHANTOM" in cache.client.sets[receipt_key]
    assert cache.client.ttls[receipt_key] == report_back._RESOLVED_RECEIPT_TTL
    from src.server.handlers.chat.report_back_keys import flash_rb_done_key

    assert cache.client.lists[flash_rb_done_key("flash-1")] == ["rb-9"]


@pytest.mark.asyncio
async def test_resolve_is_idempotent():
    """A drainer retry (nack/crash between resolve and ack) re-runs safely:
    no duplicate drained record, receipt/memberships already settled."""
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-9"})
    from src.server.handlers.chat.report_back_keys import flash_rb_done_key

    first = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )
    second = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert first == (True, "rb-9")
    assert second == (True, None)
    assert cache.client.lists[flash_rb_done_key("flash-1")] == ["rb-9"]
    assert cache.kv[report_back.ptc_origin_key("ptc-1")] is not None


@pytest.mark.asyncio
async def test_resolve_suppressed_when_origin_moved():
    """A newer reservation replaced the origin between the fenced clear and
    this resolve: the pair belongs to that reservation's lifecycle — nothing
    falls, nothing is receipted."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-NEWER", ptr={"run_id": "rb-9"})

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (False, None)
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_user_pending_key("u-1")]
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") in cache.kv
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


@pytest.mark.asyncio
async def test_resolve_suppressed_when_fencer_was_admitted(_stub_ledger):
    """The fencing generation admitted between the fenced clear and this
    resolve: its START stamped ``origin_dispatch_gen`` on a ledger row, the
    durable admission record — it legitimately owns the pair. Suppress, no
    receipt (its admission already happened; a receipt would be a lie)."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-2")
    _stub_ledger.has_gen.return_value = True

    resolved, _ = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-2"
    )

    assert resolved is False
    _stub_ledger.has_gen.assert_awaited_once_with("ptc-1", "g-2")
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


@pytest.mark.asyncio
async def test_resolve_suppressed_by_live_foreign_run(_stub_ledger):
    """THE round-13 P0 pin (ledger form): origin carries the fenced
    generation but the ledger shows a LIVE run of another lineage on the
    thread. That lineage's own hooks settle the pair — a surrogate
    resolution here could erase the watch membership a live run's
    report-back depends on (execute_report_back acks WITHOUT posting when
    membership is gone)."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-2", ptr={"run_id": "rb-9"})
    _stub_ledger.active.return_value = {
        "conversation_response_id": "run-1",
        "status": "in_progress",
    }

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-2"
    )

    assert (resolved, drained) == (False, None)
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_user_pending_key("u-1")]
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") in cache.kv
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


@pytest.mark.asyncio
async def test_resolve_suppressed_by_pre_start_intent_stamp():
    """Codex round-14 P0 pin, 2.4d form: the admission gate stamps
    ``admitted_gen`` on the origin BEFORE the run STARTs. In the gate→START
    window there is no ledger row and no live run yet, so the Lua's stamp
    check is the only thing standing between a stale fenced clear and
    resolving a generation that is about to legitimately own the pair."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-2", ptr={"run_id": "rb-9"})
    cache.kv[report_back.ptc_origin_key("ptc-1")]["admitted_gen"] = "g-2"
    # No ledger row, no active run — mid-window between gate and START.

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-2"
    )

    assert (resolved, drained) == (False, None)
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_user_pending_key("u-1")]
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") in cache.kv
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


@pytest.mark.asyncio
async def test_resolve_suppressed_when_ledger_probe_fails(_stub_ledger):
    """A failing ledger probe must fail CLOSED (suppress) — resolving blind
    could erase a live admitted lineage's membership. Nothing falls, nothing
    is receipted; the pair waits for a later, informed pass."""
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-9"})
    _stub_ledger.has_gen.side_effect = RuntimeError("ledger unavailable")

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (False, None)
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") in cache.kv
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


@pytest.mark.asyncio
async def test_resolve_records_drained_run_inside_the_script():
    """Codex round-14 P1: pointer delete and the flash_rb_done record happen
    in ONE script — a wrapper crash after the eval can no longer lose the
    drained run's only discovery record (a retry finds the pointer already
    gone and could never reconstruct it)."""
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-9"})

    with patch.object(report_back, "_record_drained_run", AsyncMock()) as rec:
        resolved, drained = await report_back.resolve_orphaned_watch(
            cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
        )

    assert (resolved, drained) == (True, "rb-9")
    rec.assert_not_awaited()
    from src.server.handlers.chat.report_back_keys import flash_rb_done_key

    assert cache.client.lists[flash_rb_done_key("flash-1")] == ["rb-9"]


@pytest.mark.asyncio
async def test_resolve_consults_both_ledger_probes(_stub_ledger):
    """The resolver must ask the ledger both questions — fencer admitted?
    any live run? — before touching pair state. A terminal prior lineage
    (rows exist, none in_progress) is exactly this default shape and does
    not suppress: the phantom still resolves."""
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-9"})

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (True, "rb-9")
    # Both questions are asked twice: once before the destructive script and
    # once after it (the post-resolve revalidation that catches a raced
    # START — the pre-probe's negatives aren't stable through the mutation).
    assert _stub_ledger.has_gen.await_count == 2
    _stub_ledger.has_gen.assert_awaited_with("ptc-1", "g-PHANTOM")
    assert _stub_ledger.active.await_count == 2
    _stub_ledger.active.assert_awaited_with("ptc-1")
    assert "g-PHANTOM" in cache.client.sets[report_back.ptc_rb_resolved_key("ptc-1")]


@pytest.mark.asyncio
async def test_resolve_leaves_memberships_inherited_from_intermediate_lineage():
    """Codex round-15 P0 pin: a refresh phantom G3 inherited the pair state
    (owns nothing) from completed G2 whose report-back is still queued; a
    stale G1 clear fenced by G3 resolves G3 (receipted, so its admission
    refuses) but must NOT touch the memberships or G2's pointer — no
    rollback ever re-adds memberships, and execute_report_back acks without
    posting once membership is gone."""
    cache = _FakeCache()
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")].update(
        dispatch_gen="g-3", owns_watch=False, owns_user=False, prev_gen="g-2"
    )
    cache.kv[report_back.flash_rb_run_key("flash-1", "ptc-1")] = {
        "run_id": "rb-2",
        "dispatch_gen": "g-2",
    }

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-3", job_gen="g-1"
    )

    assert (resolved, drained) == (True, None)
    assert "g-3" in cache.client.sets[report_back.ptc_rb_resolved_key("ptc-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_user_pending_key("u-1")]
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") in cache.kv


@pytest.mark.asyncio
async def test_resolve_surrogate_clears_the_displaced_lineages_state():
    """The canonical lost-409 phantom directly displaced the dying lineage
    (prev_gen == the fenced clear's own generation): resolving it IS that
    lineage's blocked teardown — memberships fall and its pointer drains
    onto the discovery list."""
    cache = _FakeCache()
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")].update(
        dispatch_gen="g-2p", owns_watch=False, owns_user=False, prev_gen="g-1"
    )
    cache.kv[report_back.flash_rb_run_key("flash-1", "ptc-1")] = {
        "run_id": "rb-1",
        "dispatch_gen": "g-1",
    }

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-2p", job_gen="g-1"
    )

    assert (resolved, drained) == (True, "rb-1")
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_watch_key("flash-1"), set()
    )
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_user_pending_key("u-1"), set()
    )
    from src.server.handlers.chat.report_back_keys import flash_rb_done_key

    assert cache.client.lists[flash_rb_done_key("flash-1")] == ["rb-1"]


@pytest.mark.asyncio
async def test_resolve_surrogate_drains_even_a_third_generations_pointer():
    """Codex round-16 P1 pin: a surrogate resolution IS the dying lineage's
    whole-pair teardown, and the gated teardown deletes the pointer
    regardless of generation — memberships must never fall while a pointer
    nobody can discover survives them (/status only reads the pointer behind
    membership, so a spared-but-orphaned pointer is unreachable forever)."""
    cache = _FakeCache()
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")].update(
        dispatch_gen="g-2p", owns_watch=False, owns_user=False, prev_gen="g-1"
    )
    cache.kv[report_back.flash_rb_run_key("flash-1", "ptc-1")] = {
        "run_id": "rb-0",
        "dispatch_gen": "g-0",
    }

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-2p", job_gen="g-1"
    )

    assert (resolved, drained) == (True, "rb-0")
    assert report_back.flash_rb_run_key("flash-1", "ptc-1") not in cache.kv
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_watch_key("flash-1"), set()
    )
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_user_pending_key("u-1"), set()
    )
    from src.server.handlers.chat.report_back_keys import flash_rb_done_key

    assert cache.client.lists[flash_rb_done_key("flash-1")] == ["rb-0"]


@pytest.mark.asyncio
async def test_resolve_surrogate_matches_the_carried_owner_gen():
    """Codex round-16 P2 pin: chained retained phantoms — owner G1's clear is
    fenced by phantom G3, which displaced phantom G2, which displaced G1. G3
    carries owner_gen=G1 (G2 could never produce a clear), so G1's clear
    still surrogate-authorizes the pair teardown instead of stranding the
    memberships and cap slot to the 24h TTL."""
    cache = _FakeCache()
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")].update(
        dispatch_gen="g-3",
        owns_watch=False,
        owns_user=False,
        prev_gen="g-2",
        owner_gen="g-1",
    )

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-3", job_gen="g-1"
    )

    assert (resolved, drained) == (True, None)
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_watch_key("flash-1"), set()
    )
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_user_pending_key("u-1"), set()
    )
    assert "g-3" in cache.client.sets[report_back.ptc_rb_resolved_key("ptc-1")]


@pytest.mark.asyncio
async def test_reserve_records_immediate_prev_and_carries_the_owner_anchor():
    """Codex round-16 P2 pin, via the REAL reserve(): ``prev_gen`` is always
    the immediately displaced gen (it may be mid-admission and still able to
    terminate), while ``owner_gen`` anchors to the nearest displaced ancestor
    certain to produce a clear — carried THROUGH an unadmitted non-owning
    phantom, taken verbatim from an admitted predecessor, and granted to a
    legacy blob without lineage flags (round-15 parity)."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    origin_key = report_back.ptc_origin_key(ptc)

    lineage_fields = ("owns_watch", "owns_user", "prev_gen", "owner_gen", "admitted_gen")

    async def _reserve_over(predecessor: dict) -> dict:
        _seed_dispatched(cache, flash, [ptc], user)
        base = {
            k: v
            for k, v in cache.kv[origin_key].items()
            if k not in lineage_fields
        }
        cache.kv[origin_key] = {**base, **predecessor}
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ):
            async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user) as slot:
                assert slot.error is None
                stored = dict(cache.kv[origin_key])
                assert stored["dispatch_gen"] == slot.dispatch_gen
                return stored

    # Unadmitted, non-owning phantom: immediate prev, carried owner.
    stored = await _reserve_over(
        {
            "dispatch_gen": "g-2",
            "owns_watch": False,
            "owns_user": False,
            "prev_gen": "g-1",
            "owner_gen": "g-1",
        }
    )
    assert stored["prev_gen"] == "g-2"
    assert stored["owner_gen"] == "g-1"

    # Admitted predecessor: anchors itself.
    stored = await _reserve_over(
        {
            "dispatch_gen": "g-2",
            "admitted_gen": "g-2",
            "owns_watch": False,
            "owns_user": False,
            "prev_gen": "g-1",
        }
    )
    assert stored["prev_gen"] == "g-2"
    assert stored["owner_gen"] == "g-2"

    # Legacy blob without lineage flags: anchors itself (round-15 parity).
    stored = await _reserve_over({"dispatch_gen": "g-2"})
    assert stored["prev_gen"] == "g-2"
    assert stored["owner_gen"] == "g-2"


@pytest.mark.asyncio
async def test_refresh_reservation_phantom_resolution_spares_predecessor_delivery():
    """Codex round-15 P0 lifecycle repro, through the REAL reserve/clear/
    resolve/rollback paths: G2 admitted+completed with its report-back still
    queued; G3 refresh-reserves the pair; a stale G1 error clear is fenced
    by G3 and resolved (G3 receipted → its admission 503s → rollback). G2's
    origin AND both memberships must survive the whole dance — its queued
    summary still delivers."""
    cache = _FakeCache()
    flash, ptc, user = "flash-1", "ptc-1", "u-1"
    _seed_dispatched(cache, flash, [ptc], user)
    cache.kv[report_back.ptc_origin_key(ptc)].update(
        dispatch_gen="g-2", admitted_gen="g-2"
    )

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        async with report_back.reserve(flash, ptc, "ws-1", "fws-1", user) as slot:
            assert slot.error is None
            g3 = slot.dispatch_gen
            # G1's stale error clear lands mid-reservation: fenced by G3.
            outcome = await report_back.clear_flash_report_back(
                cache, ptc, flash, expected_gen="g-1"
            )
            assert not outcome
            assert outcome.fencer_gen == g3
            resolved, _ = await report_back.resolve_orphaned_watch(
                cache, ptc, flash, user, fencer_gen=g3, job_gen="g-1"
            )
            assert resolved
            assert g3 in cache.client.sets[report_back.ptc_rb_resolved_key(ptc)]
            # Inherited pair state untouched mid-flight.
            assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
            # No commit → G3's refused admission (receipt → 503) rolls back.

    restored = cache.kv[report_back.ptc_origin_key(ptc)]
    assert restored["dispatch_gen"] == "g-2"
    assert restored["admitted_gen"] == "g-2"
    assert ptc in cache.client.sets[report_back.flash_watch_key(flash)]
    assert ptc in cache.client.sets[report_back.flash_user_pending_key(user)]


@pytest.mark.asyncio
async def test_resolve_suppressed_when_origin_gone():
    """Pair already cleared: nothing to resolve, nothing receipted."""
    cache = _FakeCache()

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (False, None)
    assert report_back.ptc_rb_resolved_key("ptc-1") not in cache.client.sets


# ---------------------------------------------------------------------------
# admit_dispatch_gen / retract_dispatch_gen — the pre-START phantom-refusal
# gate (2.4d): ONE Lua refuses generations the orphan resolver receipted and
# stamps pre-START intent on the origin (first stamp wins per generation);
# the retract CAS releases only the exact (gen, run) that stamped.
# ---------------------------------------------------------------------------


def _gate_cache(gen: str = "g-1"):
    cache = _FakeCache()
    _seed_dispatched(cache, "flash-1", ["ptc-1"])
    cache.kv[report_back.ptc_origin_key("ptc-1")]["dispatch_gen"] = gen
    return cache


@pytest.mark.asyncio
async def test_admit_stamps_pre_start_intent():
    cache = _gate_cache()
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-1") is True
    origin = cache.kv[report_back.ptc_origin_key("ptc-1")]
    assert origin["admitted_gen"] == "g-1"
    assert origin["admitted_run"] == "run-1"


@pytest.mark.asyncio
async def test_admit_refuses_receipted_gen():
    """A generation the orphan resolver receipted as phantom must never
    admit late — its watch state is gone; the turn's report-back would
    silently drop."""
    cache = _gate_cache()
    cache.client.sets[report_back.ptc_rb_resolved_key("ptc-1")] = {"g-1"}
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-1") is False
    assert "admitted_gen" not in cache.kv[report_back.ptc_origin_key("ptc-1")]


@pytest.mark.asyncio
async def test_admit_first_stamp_wins_for_same_gen_retransmit():
    """A same-gen retransmit racing its sibling still admits (the START-txn
    dedup decides the winner) but must not re-token the stamp — else the
    loser's retract could strip it from under the live winner."""
    cache = _gate_cache()
    cache.kv[report_back.ptc_origin_key("ptc-1")]["admitted_gen"] = "g-1"
    cache.kv[report_back.ptc_origin_key("ptc-1")]["admitted_run"] = "run-A"
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-B") is True
    assert cache.kv[report_back.ptc_origin_key("ptc-1")]["admitted_run"] == "run-A"


@pytest.mark.asyncio
async def test_admit_moved_origin_admits_without_stamping():
    """A newer reservation displaced this generation: that lifecycle isn't
    ours to write, so no stamp — but the dispatch still admits and runs as
    an ordinary turn whose report-back finds the pair settled."""
    cache = _gate_cache(gen="g-2")
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-1") is True
    assert "admitted_gen" not in cache.kv[report_back.ptc_origin_key("ptc-1")]


@pytest.mark.asyncio
async def test_admit_fails_closed_when_gate_unavailable():
    disabled = _FakeCache()
    disabled.enabled = False
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=disabled):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-1") is False

    broken = _gate_cache()
    broken.client.eval = AsyncMock(side_effect=ConnectionError("redis down"))
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=broken):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-1") is False


@pytest.mark.asyncio
async def test_retract_releases_only_the_exact_stamp():
    """The retract CAS is scoped to the exact (gen, run) that stamped: a
    foreign run or foreign generation never strips a live sibling's stamp."""
    origin_key = report_back.ptc_origin_key("ptc-1")
    cache = _gate_cache()
    cache.kv[origin_key]["admitted_gen"] = "g-1"
    cache.kv[origin_key]["admitted_run"] = "run-1"

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        await report_back.retract_dispatch_gen("ptc-1", "g-1", "run-2")
        assert cache.kv[origin_key]["admitted_run"] == "run-1"
        await report_back.retract_dispatch_gen("ptc-1", "g-2", "run-1")
        assert cache.kv[origin_key]["admitted_run"] == "run-1"
        await report_back.retract_dispatch_gen("ptc-1", "g-1", "run-1")

    assert "admitted_gen" not in cache.kv[origin_key]
    assert "admitted_run" not in cache.kv[origin_key]
    assert cache.kv[origin_key]["dispatch_gen"] == "g-1"


@pytest.mark.asyncio
async def test_resolve_spares_a_foreign_generations_pointer():
    """The pointer drain is gen-gated: a pointer a THIRD generation just
    re-established survives the phantom's resolution."""
    cache = _FakeCache()
    _seed_phantom(cache, ptr={"run_id": "rb-NEW", "dispatch_gen": "g-3"})
    ptr_key = report_back.flash_rb_run_key("flash-1", "ptc-1")

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (True, None)
    assert cache.kv[ptr_key] == {"run_id": "rb-NEW", "dispatch_gen": "g-3"}


@pytest.mark.asyncio
async def test_resolve_without_flash_thread():
    """Unresolvable flash thread: only the user membership can fall — the ''
    KEYS guards keep the script off the watch/pointer keys entirely."""
    cache = _FakeCache()
    cache.kv[report_back.ptc_origin_key("ptc-1")] = {
        **_origin("ptc-1"),
        "dispatch_gen": "g-PHANTOM",
        "owns_user": True,
    }
    cache.client.sets.setdefault(
        report_back.flash_user_pending_key("u-1"), set()
    ).add("ptc-1")

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", None, "u-1", fencer_gen="g-PHANTOM"
    )

    assert (resolved, drained) == (True, None)
    assert "ptc-1" not in cache.client.sets.get(
        report_back.flash_user_pending_key("u-1"), set()
    )


@pytest.mark.asyncio
async def test_failing_stampers_retract_spares_same_gen_siblings_intent(
    _stub_ledger,
):
    """Pre-START intent is per contender: same-gen admission B admits behind
    first-stamper A; A's priming-failure retract removes only A's own
    ``pending_runs`` entry, so the resolver still defers while B heads to
    START — the Redis first stamper is not necessarily the Postgres START
    winner. Only after B's own retract does the generation resolve as a
    true phantom."""
    cache = _gate_cache()
    watch_key = report_back.flash_watch_key("flash-1")
    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-A") is True
        assert await report_back.admit_dispatch_gen("ptc-1", "g-1", "run-B") is True
        await report_back.retract_dispatch_gen("ptc-1", "g-1", "run-A")

    origin = cache.kv[report_back.ptc_origin_key("ptc-1")]
    assert "admitted_gen" not in origin  # A's stamp CAS-released
    assert origin["pending_runs"] == {"run-B": True}  # B's protection survives

    resolved, _ = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-1"
    )
    assert resolved is False
    assert "ptc-1" in cache.client.sets[watch_key]

    with patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache):
        await report_back.retract_dispatch_gen("ptc-1", "g-1", "run-B")
    resolved, _ = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-1"
    )
    assert resolved is True
    assert "ptc-1" not in cache.client.sets.get(watch_key, set())


@pytest.mark.asyncio
async def test_resolve_restores_memberships_when_same_gen_start_raced(
    _stub_ledger,
):
    """Cross-store TOCTOU: the pre-probe's negatives are not stable through
    the Redis mutation. When the fencer's own admission STARTs in the gap
    (row visible only at the post-resolve revalidation), the resolution is
    withdrawn: memberships restored with TTL, and the receipt un-receipted
    so the live run's retransmits aren't refused."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-1")
    _stub_ledger.has_gen.side_effect = [False, True]  # pre-probe, revalidation

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-1"
    )

    assert (resolved, drained) == (False, None)
    watch_key = report_back.flash_watch_key("flash-1")
    user_key = report_back.flash_user_pending_key("u-1")
    assert "ptc-1" in cache.client.sets[watch_key]
    assert "ptc-1" in cache.client.sets[user_key]
    assert cache.client.ttls[watch_key] == report_back.PTC_ORIGIN_TTL
    assert "g-1" not in cache.client.sets.get(
        report_back.ptc_rb_resolved_key("ptc-1"), set()
    )


@pytest.mark.asyncio
async def test_resolve_restores_memberships_when_foreign_start_raced(
    _stub_ledger,
):
    """An ordinary continuation (never touches the admission gate) STARTing
    in the probe→script gap also withdraws the resolution — its report-back
    needs the memberships the script just dropped. The phantom stays
    receipted (it is still a phantom); the pair settles via the live run's
    own terminal lifecycle."""
    cache = _FakeCache()
    _seed_phantom(cache, gen="g-1")
    _stub_ledger.active.side_effect = [
        None,
        {"conversation_response_id": "run-LIVE"},
    ]

    resolved, drained = await report_back.resolve_orphaned_watch(
        cache, "ptc-1", "flash-1", "u-1", fencer_gen="g-1"
    )

    assert (resolved, drained) == (False, None)
    assert "ptc-1" in cache.client.sets[report_back.flash_watch_key("flash-1")]
    assert "ptc-1" in cache.client.sets[report_back.flash_user_pending_key("u-1")]
    assert "g-1" in cache.client.sets[report_back.ptc_rb_resolved_key("ptc-1")]
