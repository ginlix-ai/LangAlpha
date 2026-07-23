"""Idempotent report-back run claim/release (server-side dedup of a retried POST).

``claim_report_back_run`` is the server-side guard that closes the report-back
double-deliver: a lost-response retry (or a drain re-POST after a crash) must NOT
start a second summary run. One Lua claims the per-(flash, ptc) run pointer;
identity is the POST's deterministic request_key — a prior admission of the SAME
POST makes the retry return that run, while ANY other job's pointer (a stale
incarnation's leftover, or a newer incarnation's terminal pointer a legacy job
must not adopt) is replaced. An incumbent surfaces its RAW pointer bytes and
claimed_at so the caller can gate adoption on run durability (review F1);
``takeover_report_back_run`` CAS-replaces a stale provisional pointer.

The fake faithfully models the contract the helpers depend on: the RAW client
holds JSON strings, and the claim/release/takeover scripts decode or compare
them — exactly how RedisCache splits raw writes from decoded reads.
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services.report_back.flash import keys
from src.server.services.report_back.flash import leases, pointer
from src.server.services.report_back.flash.pointer import (
    claim_report_back_run,
    flash_rb_run_key,
    release_report_back_run,
    takeover_report_back_run,
)

POINTER_MOD = "src.server.services.report_back.flash.pointer"


class _Cache:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.client = self if enabled else None
        self.kv: dict[str, str] = {}
        # The claim's write is membership-gated; every claim test models a
        # live pair unless it removes the member explicitly.
        self.sets: dict[str, set[str]] = {
            keys.flash_watch_key("flash-1"): {"ptc-1"}
        }

    async def get(self, key):
        raw = self.kv.get(key)
        if raw is None:
            return None
        return json.loads(raw) if isinstance(raw, (str, bytes)) else raw

    async def delete(self, key):
        self.kv.pop(key, None)

    def _decoded(self, key):
        raw = self.kv.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    async def eval(self, script, numkeys, *args):
        keys, argv = args[:numkeys], args[numkeys:]
        if script is pointer.CLAIM_POINTER_LUA:
            watch_key, run_key = keys
            ptc_id, value, ttl, request_key, gen = argv
            data = self._decoded(run_key)
            if isinstance(data, dict) and isinstance(data.get("run_id"), str):
                if isinstance(data.get("request_key"), str):
                    if request_key == "" or data["request_key"] == request_key:
                        return [0, self.kv[run_key]]
                elif (request_key == "" and gen == "") or (
                    gen != ""
                    and isinstance(data.get("dispatch_gen"), str)
                    and data["dispatch_gen"] == gen
                ):
                    return [0, self.kv[run_key]]
            if ptc_id not in self.sets.get(watch_key, set()):
                return [2, ""]
            self.kv[run_key] = value
            return [1, ""]
        if script is pointer.POINTER_TAKEOVER_LUA:
            watch_key, run_key = keys
            ptc_id, expected, value, ttl = argv
            current = self.kv.get(run_key)
            if current is None or current != expected:
                return 0
            if ptc_id not in self.sets.get(watch_key, set()):
                return 2
            self.kv[run_key] = value
            return 1
        if script is pointer.POINTER_COMPARE_DELETE_LUA:
            data = self._decoded(keys[0])
            if isinstance(data, dict) and data.get("run_id") == argv[0]:
                self.kv.pop(keys[0], None)
                return 1
            return 0
        raise AssertionError(f"unknown Lua script: {script[:60]!r}")


@pytest.mark.asyncio
async def test_claim_when_no_incumbent_claims_and_writes_pointer():
    cache = _Cache()
    result = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    assert (result.winning_run_id, result.claimed) == ("run-1", True)
    # Pointer persisted in the shape the drain gate reads ({"run_id": ...})
    # plus the priming-lease timestamp.
    stored = json.loads(cache.kv[flash_rb_run_key("flash-1", "ptc-1")])
    assert stored["run_id"] == "run-1"
    assert isinstance(stored["claimed_at"], float)


@pytest.mark.asyncio
async def test_fully_legacy_claim_adopts_fully_legacy_incumbent():
    """Pure pre-deploy path: bare pointer, bare claimer -> adopt, no overwrite."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-A"})
    result = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-B")
    assert (result.winning_run_id, result.claimed) == ("run-A", False)
    assert json.loads(cache.kv[key]) == {"run_id": "run-A"}
    # A pointer predating claimed_at surfaces None — the CM treats unknown
    # age as stale (takeover-eligible) rather than wedging retries.
    assert result.incumbent_claimed_at is None
    assert result.incumbent_raw == cache.kv[key]


@pytest.mark.asyncio
async def test_claim_scopes_idempotency_to_the_request_key():
    """The idempotency identity is the POST's request_key: the same POST
    retried adopts its own prior admission; a DIFFERENT job replaces the
    pointer instead of adopting a summary that already ran (or never will)."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )

    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-A-retry", "g-1", "rk-A"
    )
    assert (result.winning_run_id, result.claimed) == ("run-A", False)

    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-B", "g-2", "rk-B"
    )
    assert (result.winning_run_id, result.claimed) == ("run-B", True)
    stored = json.loads(cache.kv[key])
    assert stored["run_id"] == "run-B"
    assert stored["dispatch_gen"] == "g-2"
    assert stored["request_key"] == "rk-B"


@pytest.mark.asyncio
async def test_legacy_job_never_adopts_another_jobs_pointer():
    """Codex round-5 F3: a LEGACY job (no dispatch_gen — pre-deploy row) still
    carries its own request_key; it must REPLACE a newer incarnation's
    lingering terminal pointer, not adopt it and drop its summary."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-G2-done", "dispatch_gen": "g-2", "request_key": "rk-G2"}
    )
    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-legacy", None, "rk-legacy"
    )
    assert (result.winning_run_id, result.claimed) == ("run-legacy", True)
    assert json.loads(cache.kv[key])["request_key"] == "rk-legacy"


@pytest.mark.asyncio
async def test_claimer_without_request_key_adopts_any_incumbent():
    """A claimer that can't prove identity (manual POST, no request_key)
    adopts whatever is there — stomping a live pointer would be worse."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )
    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-X", None, None
    )
    assert (result.winning_run_id, result.claimed) == ("run-A", False)


@pytest.mark.asyncio
async def test_old_format_pointer_falls_back_to_generation_scoping():
    """Transitional: a pointer written before request_keys existed carries
    only a gen. Same gen adopts; a different generation replaces; and a
    request-key-carrying claimer never adopts on gen absence alone."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-G1", "dispatch_gen": "g-1"})

    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-retry", "g-1", "rk-1"
    )
    assert (result.winning_run_id, result.claimed) == ("run-G1", False)

    # Legacy claimer WITH a request_key vs a generated old-format pointer:
    # replace (adopting an unrelated generated pointer is the F3 hole).
    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-legacy", None, "rk-legacy"
    )
    assert (result.winning_run_id, result.claimed) == ("run-legacy", True)


@pytest.mark.asyncio
async def test_claim_replaces_a_stale_incarnations_pointer():
    """A NEW generation replaces a stale incarnation's old-format pointer
    (its gen-mismatched clear left it behind)."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-G1", "dispatch_gen": "g-1"})
    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-G2", "g-2", "rk-2"
    )
    assert (result.winning_run_id, result.claimed) == ("run-G2", True)
    assert json.loads(cache.kv[key])["run_id"] == "run-G2"


class _RaisingCache(_Cache):
    """The claim script itself fails (transport blip mid-eval)."""

    async def eval(self, script, numkeys, *args):
        raise RuntimeError("redis down")


@pytest.mark.asyncio
async def test_claim_fails_closed_when_script_fails():
    """Codex round-6 F1: the old fail-open here (fabricating claimed=True on
    a Redis error) let a route reach START with NO pointer and NO membership
    check — invisible to the drop teardown's pointer fence, and blind to a
    pair the clear had already settled. The claim now fails closed to the
    unknown shape: not claimed, no winner, nothing written."""
    cache = _RaisingCache()
    result = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    assert (result.winning_run_id, result.claimed) == (None, False)
    assert not result.pair_gone
    assert cache.kv == {}


@pytest.mark.asyncio
async def test_claim_when_cache_disabled_fails_closed():
    """Config-off cache: same fail-closed shape — a report-back POST only
    ever originates from an executor that has Redis, so an unavailable cache
    here is a degraded worker, not a supported mode."""
    cache = _Cache(enabled=False)
    result = await claim_report_back_run(cache, "flash-1", "ptc-1", "run-1")
    assert (result.winning_run_id, result.claimed) == (None, False)
    assert cache.kv == {}


@pytest.mark.asyncio
async def test_cm_script_failure_defers_as_in_flight():
    """The prerequisite pin for the drop-clear pointer fence: a claim-script
    failure surfaces as ``in_flight`` (route 503s BEFORE advancing the flash
    generator — threads.py returns at the in_flight branch, so no run can
    start unfenced), never as a won claim. Covers both orders of the round-6
    interleaving: blip before the clear, and blip after membership fell."""
    cache = _RaisingCache()
    async with pointer.claim(cache, "flash-1", "ptc-1", "run-1") as handle:
        assert handle.in_flight
        assert handle.incumbent is None and not handle.pair_gone
    assert cache.kv == {}

    cleared = _RaisingCache()
    cleared.sets[keys.flash_watch_key("flash-1")].discard("ptc-1")
    async with pointer.claim(cleared, "flash-1", "ptc-1", "run-1") as handle:
        # Membership already fell, but the failed script can't prove it:
        # defer (retriable) rather than misreading the pair as live or gone.
        assert handle.in_flight and not handle.pair_gone
    assert cleared.kv == {}


@pytest.mark.asyncio
async def test_claim_refuses_to_resurrect_a_pointer_after_the_pair_fell():
    """Codex round-17 P1: a resolution (or terminal clear) that dropped the
    watch membership AND the pointer must not have the pointer resurrected by
    a late admission-time claim — the pair reads settled while an orphan
    summary and pointer exist. The write is membership-gated in the same
    script; the route refuses the dispatch on ``pair_gone``."""
    cache = _Cache()
    cache.sets[keys.flash_watch_key("flash-1")].discard("ptc-1")

    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-late", "g-2", "rk-late"
    )

    assert result.pair_gone is True
    assert result.claimed is False
    assert result.winning_run_id is None
    assert cache.kv == {}  # nothing written behind the settled pair

    # A lost-response retry of a PRIOR admission still finds its run: adoption
    # writes nothing, so it stays legal even after the pair fell.
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps(
        {"run_id": "run-A", "dispatch_gen": "g-1", "request_key": "rk-A"}
    )
    result = await claim_report_back_run(
        cache, "flash-1", "ptc-1", "run-A-retry", "g-1", "rk-A"
    )
    assert (result.winning_run_id, result.claimed, result.pair_gone) == (
        "run-A",
        False,
        False,
    )


@pytest.mark.asyncio
async def test_release_deletes_only_when_pointer_is_ours():
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-A"})

    # A release for a different run must not delete someone else's pointer.
    await release_report_back_run(cache, "flash-1", "ptc-1", "run-B")
    assert key in cache.kv

    # Our own release removes it (so a later retry isn't short-circuited).
    await release_report_back_run(cache, "flash-1", "ptc-1", "run-A")
    assert key not in cache.kv


# ---------------------------------------------------------------------------
# Stale-provisional-pointer takeover (review F1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_takeover_cas_replaces_exact_incumbent_bytes():
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    stale = json.dumps({"run_id": "run-dead", "request_key": "rk-A"})
    cache.kv[key] = stale

    outcome = await takeover_report_back_run(
        cache, "flash-1", "ptc-1", stale, "run-B", "g-1", "rk-A"
    )
    assert outcome == "claimed"
    stored = json.loads(cache.kv[key])
    assert stored["run_id"] == "run-B"
    assert isinstance(stored["claimed_at"], float)


@pytest.mark.asyncio
async def test_takeover_loses_when_pointer_changed_under_us():
    """A rival takeover (or re-claim) between probe and CAS wins; we must not
    stomp its pointer — 'lost' surfaces retriable and the retry re-probes."""
    cache = _Cache()
    key = flash_rb_run_key("flash-1", "ptc-1")
    cache.kv[key] = json.dumps({"run_id": "run-rival", "request_key": "rk-A"})

    outcome = await takeover_report_back_run(
        cache,
        "flash-1",
        "ptc-1",
        json.dumps({"run_id": "run-dead", "request_key": "rk-A"}),
        "run-B",
        "g-1",
        "rk-A",
    )
    assert outcome == "lost"
    assert json.loads(cache.kv[key])["run_id"] == "run-rival"


@pytest.mark.asyncio
async def test_takeover_refuses_when_pair_membership_fell():
    cache = _Cache()
    cache.sets[keys.flash_watch_key("flash-1")].discard("ptc-1")
    key = flash_rb_run_key("flash-1", "ptc-1")
    stale = json.dumps({"run_id": "run-dead", "request_key": "rk-A"})
    cache.kv[key] = stale

    outcome = await takeover_report_back_run(
        cache, "flash-1", "ptc-1", stale, "run-B", "g-1", "rk-A"
    )
    assert outcome == "pair_gone"
    assert cache.kv[key] == stale  # nothing written behind the settled pair


@pytest.mark.asyncio
async def test_takeover_surfaces_lost_on_redis_failure():
    cache = _RaisingCache()
    outcome = await takeover_report_back_run(
        cache, "flash-1", "ptc-1", "{}", "run-B", None, None
    )
    assert outcome == "lost"


# ---------------------------------------------------------------------------
# claim() CM — incumbent durability gate (review F1)
# ---------------------------------------------------------------------------

_GET_RUN = "src.server.database.runs.lifecycle.get_run"


def _seed_incumbent(cache: _Cache, claimed_at: float | None) -> str:
    key = flash_rb_run_key("flash-1", "ptc-1")
    value: dict = {"run_id": "run-A", "request_key": "rk-A"}
    if claimed_at is not None:
        value["claimed_at"] = claimed_at
    cache.kv[key] = json.dumps(value)
    return key


@pytest.mark.asyncio
async def test_cm_adopts_incumbent_backed_by_a_ledger_row():
    """A durable incumbent (START committed) is the idempotent answer: the
    retry returns that run, starts nothing, and leaves the pointer alone."""
    cache = _Cache()
    key = _seed_incumbent(cache, time.time())
    row = {"conversation_response_id": "run-A", "status": "in_progress"}
    with patch(_GET_RUN, AsyncMock(return_value=row)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.incumbent == "run-A"
            assert not handle.in_flight and not handle.pair_gone
    assert json.loads(cache.kv[key])["run_id"] == "run-A"


@pytest.mark.asyncio
async def test_cm_defers_on_rowless_incumbent_inside_priming_lease():
    """No ledger row + a young pointer: the prior admission may still be
    priming on another worker. Adopting would ack a run that may never
    exist; taking over would race its START — surface retriable instead."""
    cache = _Cache()
    key = _seed_incumbent(cache, time.time())
    with patch(_GET_RUN, AsyncMock(return_value=None)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.in_flight
            assert handle.incumbent is None and not handle.pair_gone
    assert json.loads(cache.kv[key])["run_id"] == "run-A"  # untouched


@pytest.mark.asyncio
async def test_cm_takes_over_stale_rowless_incumbent():
    """Past the lease with still no row, the prior claim's worker died before
    START (the review F1 scenario): this POST takes the pointer and runs the
    summary itself instead of adopting a run that will never terminate."""
    cache = _Cache()
    key = _seed_incumbent(
        cache, time.time() - leases.RB_POINTER_PRIMING_LEASE_S - 5
    )
    with patch(_GET_RUN, AsyncMock(return_value=None)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.incumbent is None
            assert not handle.in_flight and not handle.pair_gone
            assert json.loads(cache.kv[key])["run_id"] == "run-B"
            handle.consummate()
    assert json.loads(cache.kv[key])["run_id"] == "run-B"


@pytest.mark.asyncio
async def test_cm_takeover_releases_pointer_on_non_consummated_exit():
    """A takeover whose admission then fails must not strand ITS pointer
    either — the release compensates exactly like a fresh claim's."""
    cache = _Cache()
    key = _seed_incumbent(
        cache, time.time() - leases.RB_POINTER_PRIMING_LEASE_S - 5
    )
    with patch(_GET_RUN, AsyncMock(return_value=None)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.incumbent is None and not handle.in_flight
    assert key not in cache.kv  # compare-deleted on exit


@pytest.mark.asyncio
async def test_cm_treats_undated_rowless_pointer_as_stale():
    """A pointer predating claimed_at gives no lease to wait out; with no
    ledger row it must be takeover-eligible, not wedge retries for 24h."""
    cache = _Cache()
    key = _seed_incumbent(cache, None)
    with patch(_GET_RUN, AsyncMock(return_value=None)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.incumbent is None and not handle.in_flight
            handle.consummate()
    assert json.loads(cache.kv[key])["run_id"] == "run-B"


@pytest.mark.asyncio
async def test_cm_defers_when_ledger_probe_fails():
    """DB unreachable: the incumbent can't be proven either way — never ack
    (adopt) on unknown; surface retriable and leave the pointer alone."""
    cache = _Cache()
    key = _seed_incumbent(cache, time.time())
    with patch(_GET_RUN, AsyncMock(side_effect=RuntimeError("db down"))):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.in_flight
    assert json.loads(cache.kv[key])["run_id"] == "run-A"


@pytest.mark.asyncio
async def test_cm_defers_when_takeover_is_lost_to_a_rival():
    """The CAS token no longer matches (a rival re-claimed between probe and
    takeover): defer retriable; the retry re-probes the new pointer."""
    cache = _Cache()
    _seed_incumbent(cache, time.time() - leases.RB_POINTER_PRIMING_LEASE_S - 5)
    with (
        patch(_GET_RUN, AsyncMock(return_value=None)),
        patch(
            f"{POINTER_MOD}.takeover_report_back_run",
            AsyncMock(return_value="lost"),
        ),
    ):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.in_flight
            assert handle.incumbent is None and not handle.pair_gone


@pytest.mark.asyncio
async def test_cm_surfaces_pair_gone_when_membership_fell_before_takeover():
    cache = _Cache()
    _seed_incumbent(cache, time.time() - leases.RB_POINTER_PRIMING_LEASE_S - 5)
    cache.sets[keys.flash_watch_key("flash-1")].discard("ptc-1")
    with patch(_GET_RUN, AsyncMock(return_value=None)):
        async with pointer.claim(
            cache, "flash-1", "ptc-1", "run-B", "g-1", "rk-A"
        ) as handle:
            assert handle.pair_gone
            assert not handle.in_flight and handle.incumbent is None


def test_priming_lease_and_budget_fit_the_admission_lifecycle():
    """Rounds 2+3 F1: the executor's retry budget must fit (a) the priming
    lease — so a rowless crashed pointer becomes takeover-eligible while
    retries are still coming — AND (b) one full post-takeover admission
    attempt (longest legitimate pre-START hold + response/backoff slack) —
    so the drop deadline can't land while the route legitimately holds the
    POST pre-START. Both must hold for ANY configured workflow_timeout
    (the admission holds derive from different config knobs), or the job
    is dropped and acked with no run row: summary permanently lost."""
    hold = leases.admission_hold_bound()
    assert leases.RB_POINTER_PRIMING_LEASE_S <= leases.RB_BUSY_WAIT_CAP / 2
    assert leases.RB_POINTER_PRIMING_LEASE_S <= 900.0
    # Post-lease retry window covers one full admission hold plus slack.
    assert (
        leases.RB_BUSY_WAIT_CAP - leases.RB_POINTER_PRIMING_LEASE_S
        >= hold + leases.RB_ADMISSION_MARGIN_S
    )
    # The floor binds even when workflow_timeout is configured tiny.
    assert leases.RB_BUSY_WAIT_CAP >= 2.0 * (hold + leases.RB_ADMISSION_MARGIN_S)
    # The derivation itself: small budgets scale the lease down; large
    # budgets cap it at the admission-wait ceiling.
    assert leases.derive_priming_lease(600.0) == 300.0
    assert leases.derive_priming_lease(7200.0) == 900.0


def test_admission_hold_bound_matches_admission_margins():
    """_admission_hold_bound derives wait_for_admission's two waits from the
    same runs.admission constants the wait itself reads. The bound is the
    SUM of the two waits — one admission call runs the compaction backstop
    and then the stop-drain sequentially (Codex round-4 F1; the composition
    itself is pinned in runs/test_executor)."""
    from src.config.settings import (
        get_admission_compaction_wait_timeout,
        get_checkpoint_flush_timeout,
        get_compaction_timeout,
    )
    from src.server.services.runs.admission import (
        ADMISSION_TEARDOWN_MARGIN_S,
        COMPACTION_ADMISSION_MARGIN_S,
    )

    stopping = get_checkpoint_flush_timeout() + ADMISSION_TEARDOWN_MARGIN_S
    compaction = max(
        get_admission_compaction_wait_timeout(),
        get_compaction_timeout() + COMPACTION_ADMISSION_MARGIN_S,
    )
    assert leases.admission_hold_bound() == compaction + stopping
