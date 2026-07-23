"""Report-back run-pointer lifecycle: claim, takeover, release, teardown.

Every pointer write is membership-gated Lua — the scripts, their outcome
types, and the claim context manager live together here so no caller can
race the (flash, ptc) pair state with hand-rolled Redis.
"""

from __future__ import annotations

import contextlib
import json
import logging
import time
from typing import NamedTuple

from src.server.services.report_back.flash import leases
from src.server.services.report_back.flash.keys import (
    FLASH_RB_DONE_MAX,
    FLASH_RB_DONE_TTL,
    FLASH_RB_RUN_TTL,
    TEARDOWN_TOMBSTONE_TTL,
    decode,
    flash_rb_done_key,
    flash_rb_run_key,
    flash_user_pending_key,
    flash_watch_key,
    ptc_origin_key,
    ptc_teardown_tombstone_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


# Membership-gated pointer write as one EVAL: a separate SISMEMBER-then-SET
# races a concurrent terminal clear and would resurrect a dead pointer.
# The re-assert is belt-and-suspenders for a degraded-cache admission — it
# must never OVERWRITE a live different-run pointer of the SAME job (a
# stale owner resuming would point clients at its long-dead run), but a
# pointer left behind by a DIFFERENT job's admission (e.g. a previous
# incarnation whose gen-mismatched clear correctly skipped this pair's
# state, pointer included) must be replaced, or this job's clients would
# adopt the dead run. Identity is the POST's deterministic request_key;
# an old-format pointer without one falls back to generation scoping.
# KEYS: 1=flash_watch 2=flash_rb_run
# ARGV: 1=ptc_id 2=run_id 3=json_value 4=ttl 5=request_key ('' = unknown)
#       6=dispatch_gen ('' = legacy)
GATED_POINTER_SET_LUA = """
if redis.call('sismember', KEYS[1], ARGV[1]) == 0 then return 0 end
local v = redis.call('get', KEYS[2])
if v then
  local ok, t = pcall(cjson.decode, v)
  if not (ok and type(t) == 'table') then return 0 end
  if t['run_id'] ~= ARGV[2] then
    if type(t['request_key']) == 'string' then
      if ARGV[5] == '' or t['request_key'] == ARGV[5] then return 0 end
    else
      if ARGV[6] == '' then return 0 end
      if type(t['dispatch_gen']) == 'string' and t['dispatch_gen'] == ARGV[6] then
        return 0
      end
    end
  end
end
redis.call('set', KEYS[2], ARGV[3], 'EX', ARGV[4])
return 1
"""

# Pointer claim at dispatched admission, as one EVAL. The idempotency
# identity is the POST's deterministic request_key (uuid5 of the outbox
# job): an incumbent is adopted ONLY when it carries the SAME request_key —
# i.e. it is a prior admission of this very POST (lost-response retry or
# crash re-drain). Any other job's pointer is replaced: adopting it would
# silently ack a summary that never runs — in particular a LEGACY job (no
# dispatch_gen, but every outbox POST has a request_key) must not adopt a
# newer incarnation's terminal pointer that lingers between that summary's
# completion and its watch_clear. A claimer with no request_key at all
# (manual POST) adopts any incumbent — it can never prove identity, and
# stomping a live pointer is worse. Old-format pointers without a
# request_key fall back to the generation rule; a request-key-carrying
# claimer never adopts one on gen absence alone (that's the legacy-adopts-
# foreign-pointer hole this script exists to close).
# The WRITE is membership-gated like every other pointer write: a resolution
# (or terminal clear) that dropped the pair between this POST's enqueue and
# its admission must not have the pointer resurrected behind it — {2, ''}
# tells the route to refuse the dispatch outright (non-retriable). Adoption
# stays ungated: it writes nothing, and a lost-response retry must still
# find its prior run even if the pair has since fallen.
# An incumbent match returns the RAW pointer value (not just run_id): the
# caller needs claimed_at to judge whether a rowless incumbent is still
# mid-priming, and the exact bytes as the CAS token for a stale takeover.
# KEYS: 1=flash_watch 2=flash_rb_run
# ARGV: 1=ptc_id 2=json_value 3=ttl 4=request_key ('' = unknown)
#       5=dispatch_gen ('' = legacy)
CLAIM_POINTER_LUA = """
local v = redis.call('get', KEYS[2])
if v then
  local ok, t = pcall(cjson.decode, v)
  if ok and type(t) == 'table' and type(t['run_id']) == 'string' then
    if type(t['request_key']) == 'string' then
      if ARGV[4] == '' or t['request_key'] == ARGV[4] then
        return {0, v}
      end
    else
      if (ARGV[4] == '' and ARGV[5] == '')
         or (ARGV[5] ~= '' and type(t['dispatch_gen']) == 'string'
             and t['dispatch_gen'] == ARGV[5]) then
        return {0, v}
      end
    end
  end
end
if redis.call('sismember', KEYS[1], ARGV[1]) == 0 then return {2, ''} end
redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3])
return {1, ''}
"""

# Stale-incumbent takeover: replace the pointer iff it still holds EXACTLY
# the bytes the claim probe returned (a rival takeover or re-claim changes
# them → lost), membership-gated like every pointer write. Closes review F1:
# a provisional pointer whose worker died pre-START would otherwise be
# adopted forever and its summary silently dropped at the terminal-wait cap.
# KEYS: 1=flash_watch 2=flash_rb_run
# ARGV: 1=ptc_id 2=expected_current_value 3=new_json_value 4=ttl
POINTER_TAKEOVER_LUA = """
local v = redis.call('get', KEYS[2])
if not v or v ~= ARGV[2] then return 0 end
if redis.call('sismember', KEYS[1], ARGV[1]) == 0 then return 2 end
redis.call('set', KEYS[2], ARGV[3], 'EX', ARGV[4])
return 1
"""

# Whole-pair teardown gated on the dispatch generation, as ONE script: a
# clear enqueued by an OLD incarnation's terminal (or its dead-letter
# compensation) must not destroy origin/membership/pointer/cap state a newer
# dispatch of the same (flash, ptc) pair has since re-established. A separate
# compare-then-pipeline would race the newer reserve() between the two.
# A GENERATED origin is only ever cleared by a caller presenting ITS gen —
# a gen-less caller (legacy job, unresolvable crash context) clears only
# legacy (gen-less) or absent origins; "I don't know which incarnation"
# must degrade to a bounded TTL leak, never to destroying a live one.
# Empty-string KEYS are absent-state placeholders (no flash / no user).
# A caller whose clear is fenced out records itself in a TOMBSTONE SET
# (its gen; '__legacy__' for a gen-less caller; short TTL): if the fencing
# generation was a PROVISIONAL reserve that later rolls back and restores
# the fenced clear's target as the stashed predecessor, the rollback
# consults the set and completes the clear instead of resurrecting state
# whose only teardown already ran and acked. A SET, not a scalar — two
# stale clears fenced in the same window must not overwrite each other.
# KEYS: 1=ptc_origin 2=flash_rb_run 3=flash_watch 4=flash_user_pending
#       5=tombstone set
# ARGV: 1=expected_gen ('' = legacy-only) 2=ptc_thread_id 3=tombstone_ttl
#       4=refuse_if_pointer ('1' = an OFF-CHAIN caller: a live run pointer
#         means a report-back admission is in flight or already consummated
#         for this pair — refuse the whole teardown, {0, ''}, no tombstone;
#         only that admission's own serialized lifecycle may drain it)
GATED_TEARDOWN_LUA = """
if ARGV[4] == '1' and KEYS[2] ~= '' and redis.call('exists', KEYS[2]) == 1 then
  return {0, ''}
end
local v = redis.call('get', KEYS[1])
if v then
  local ok, t = pcall(cjson.decode, v)
  if ok and type(t) == 'table' and type(t['dispatch_gen']) == 'string' then
    if ARGV[1] == '' or t['dispatch_gen'] ~= ARGV[1] then
      local mark = ARGV[1]
      if mark == '' then mark = '__legacy__' end
      redis.call('sadd', KEYS[5], mark)
      redis.call('expire', KEYS[5], ARGV[3])
      return {0, t['dispatch_gen']}
    end
  end
end
redis.call('del', KEYS[1])
redis.call('del', KEYS[5])
if KEYS[2] ~= '' then redis.call('del', KEYS[2]) end
if KEYS[3] ~= '' then redis.call('srem', KEYS[3], ARGV[2]) end
if KEYS[4] ~= '' then redis.call('srem', KEYS[4], ARGV[2]) end
return 1
"""


class ClearOutcome(NamedTuple):
    """Result of a gated teardown. Truthiness == ``cleared`` so boolean call
    sites read naturally; ``fencer_gen`` is the EXACT origin generation that
    fenced the teardown (set only when not cleared) — the only sound scope
    for a follow-up phantom resolution, since the origin can be replaced by
    a newer reservation the instant the teardown returns."""

    cleared: bool
    fencer_gen: str | None = None

    def __bool__(self) -> bool:
        return self.cleared

# Compare-and-delete for the run pointer: a separate GET-compare-DEL races a
# concurrent claim writing a NEW run id and would delete the newer claim.
# KEYS: 1=flash_rb_run  ARGV: 1=run_id
POINTER_COMPARE_DELETE_LUA = """
local v = redis.call('get', KEYS[1])
if not v then return 0 end
local ok, t = pcall(cjson.decode, v)
if ok and type(t) == 'table' and t['run_id'] == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


def pointer_value(
    run_id: str, dispatch_gen: str | None, request_key: str | None = None
) -> str:
    """Serialized run-pointer value; unknown keys are OMITTED — a JSON null
    would read as a truthy cjson value in the Lua identity checks.
    ``claimed_at`` dates the write so a retry can judge whether a rowless
    incumbent is still inside its priming lease."""
    value: dict = {"run_id": run_id, "claimed_at": time.time()}
    if dispatch_gen:
        value["dispatch_gen"] = dispatch_gen
    if request_key:
        value["request_key"] = request_key
    return json.dumps(value)


async def reassert_run_pointer(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str,
    run_id: str,
    *,
    dispatch_gen: str | None,
    request_key: str,
) -> bool:
    """Re-assert the pair's run pointer for a dispatched summary run.

    One membership-gated EVAL of ``GATED_POINTER_SET_LUA`` (semantics in the
    script's comment): returns False when the pair is gone or a different-run
    pointer of the same job identity refuses the write. Raises on transport
    failure — the caller decides whether the write was best-effort.
    """
    return bool(
        await cache.client.eval(
            GATED_POINTER_SET_LUA,
            2,
            flash_watch_key(flash_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            ptc_thread_id,
            run_id,
            pointer_value(run_id, dispatch_gen, request_key),
            FLASH_RB_RUN_TTL,
            request_key,
            dispatch_gen or "",
        )
    )


class PointerClaim(NamedTuple):
    """Outcome of a dispatched-admission pointer claim.

    On an incumbent (``claimed=False``, not ``pair_gone``),
    ``incumbent_raw`` holds the pointer's exact bytes (the CAS token for a
    stale takeover) and ``incumbent_claimed_at`` its write time (None on
    pointers predating the field)."""

    winning_run_id: str | None
    claimed: bool
    pair_gone: bool = False
    incumbent_raw: str | None = None
    incumbent_claimed_at: float | None = None


async def claim_report_back_run(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str,
    run_id: str,
    dispatch_gen: str | None = None,
    request_key: str | None = None,
) -> PointerClaim:
    """Atomically claim the report-back run pointer for one (flash, ptc) pair.

    ``(run_id, True)`` if we won; ``(incumbent_run_id, False)`` if a prior
    admission of the SAME POST (identified by its deterministic
    ``request_key``) already owns it — making a lost-response retry (or a
    crash re-drain) idempotent. Any other job's pointer is REPLACED instead:
    adopting its long-terminal run would ack a summary that never runs,
    permanently stranding this job's result (the DB-level request_key dedup
    upstream backstops a double-start). ``pair_gone`` means the watch
    membership no longer exists — a resolution or terminal clear settled the
    pair after this POST was enqueued — and the script refused to resurrect
    the pointer behind it; the caller must not schedule a summary. FAILS
    CLOSED when the cache is unavailable or the script errors: the pointer
    is the fence the drop-path teardown checks before settling the pair
    (round-6 F1) — a route allowed to proceed without it (or without the
    membership check) can be pre-START, invisible, when the executor clears.
    The unknown shape maps to ``in_flight`` → 503, inside the executor's
    always-retried set.
    """
    if not (cache.enabled and cache.client):
        return PointerClaim(None, False)
    try:
        state, incumbent = await cache.client.eval(
            CLAIM_POINTER_LUA,
            2,
            flash_watch_key(flash_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            ptc_thread_id,
            pointer_value(run_id, dispatch_gen, request_key),
            FLASH_RB_RUN_TTL,
            request_key or "",
            dispatch_gen or "",
        )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Run-claim failed for {flash_thread_id}/"
            f"{ptc_thread_id}; failing closed (retriable)",
            exc_info=True,
        )
        return PointerClaim(None, False)
    state = int(state)
    if state == 0:
        raw = decode(incumbent)
        incumbent_run: str | None = None
        claimed_at: float | None = None
        try:
            t = json.loads(raw)
            incumbent_run = t.get("run_id")
            if t.get("claimed_at") is not None:
                claimed_at = float(t["claimed_at"])
        except (ValueError, TypeError):
            pass  # Lua validated run_id before returning; belt-and-suspenders
        return PointerClaim(
            incumbent_run,
            False,
            incumbent_raw=raw,
            incumbent_claimed_at=claimed_at,
        )
    if state == 2:
        return PointerClaim(None, False, pair_gone=True)
    return PointerClaim(run_id, True)


async def takeover_report_back_run(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str,
    expected_raw: str,
    run_id: str,
    dispatch_gen: str | None = None,
    request_key: str | None = None,
) -> str:
    """CAS-replace a stale provisional run pointer with this claim's.

    ``'claimed'`` = we own the pointer now; ``'pair_gone'`` = membership fell
    (a resolution settled the pair) so no summary may be scheduled;
    ``'lost'`` = the pointer changed under us (or Redis failed) — the caller
    surfaces retriable and the retry re-probes.
    """
    try:
        state = await cache.client.eval(
            POINTER_TAKEOVER_LUA,
            2,
            flash_watch_key(flash_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            ptc_thread_id,
            expected_raw,
            pointer_value(run_id, dispatch_gen, request_key),
            FLASH_RB_RUN_TTL,
        )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Pointer takeover failed for "
            f"{flash_thread_id}/{ptc_thread_id}; surfacing retriable",
            exc_info=True,
        )
        return "lost"
    state = int(state)
    if state == 1:
        return "claimed"
    if state == 2:
        return "pair_gone"
    return "lost"


async def release_report_back_run(
    cache, flash_thread_id: str, ptc_thread_id: str, run_id: str
) -> None:
    """Delete a just-claimed run pointer iff it still points at ``run_id``.

    Compensates a claim whose admission then failed (e.g. a 409 from the gate) so
    a later retry isn't short-circuited to a run that never started. Atomic
    compare-and-delete (Lua) — a separate get/compare/delete would race a
    concurrent claim's newer pointer.
    """
    if not (cache.enabled and cache.client):
        return
    try:
        await cache.client.eval(
            POINTER_COMPARE_DELETE_LUA,
            1,
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            run_id,
        )
    except Exception:
        pass


class _ReportBackClaim:
    """Handle for the dispatched-flash report-back run claim (see ``claim``).

    ``incumbent`` is a prior admission's run_id (caller short-circuits to it,
    starting no new run) or None (caller proceeds). ``pair_gone`` means the
    pair was settled after this POST was enqueued — the caller must refuse
    the dispatch (non-retriable) instead of scheduling a summary nobody is
    watching for. ``in_flight`` means a prior admission's run may still be
    priming (no ledger row yet, pointer inside its lease) — the caller must
    surface RETRIABLE (503), never adopt or start a second run.
    ``consummate()`` keeps the just-made claim once the run actually starts.
    """

    def __init__(self) -> None:
        self.incumbent: str | None = None
        self.pair_gone = False
        self.in_flight = False
        self._consummated = False

    def consummate(self) -> None:
        """Mark the claim as backing a started run so it isn't released on exit."""
        self._consummated = True


@contextlib.asynccontextmanager
async def claim(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str | None,
    run_id: str,
    dispatch_gen: str | None = None,
    request_key: str | None = None,
):
    """Claim the per-(flash, ptc) report-back run pointer at dispatched admission.

    Closes the report-back double-deliver: a lost-response retry (or a crash
    re-drain) must NOT start a second summary run. On enter, claim the pointer
    (request-key-scoped — see ``claim_report_back_run``); a prior admission of
    the SAME POST surfaces as ``handle.incumbent`` (caller returns that run,
    no new one). Releases the just-made claim on any non-consummated exit so
    a later retry isn't short-circuited to a run that never started. No-op
    when ``ptc_thread_id`` is falsy (ordinary flash dispatch — zero Redis).
    """
    handle = _ReportBackClaim()
    if not ptc_thread_id or cache is None:
        yield handle
        return
    result = await claim_report_back_run(
        cache, flash_thread_id, ptc_thread_id, run_id, dispatch_gen, request_key
    )
    if result.pair_gone:
        handle.pair_gone = True
        yield handle
        return
    if not result.claimed:
        # Durability gate (review F1): adopt an incumbent ONLY when its run
        # has a ledger row. A rowless pointer is a provisional claim whose
        # priming never reached START — adopting it would ack this POST
        # against a run that doesn't exist, and the terminal-wait cap would
        # eventually clear the pair and drop the summary permanently.
        row = None
        row_known = False
        if result.winning_run_id:
            try:
                from src.server.database.runs import lifecycle as tl_db

                row = await tl_db.get_run(result.winning_run_id)
                row_known = True
            except Exception:
                logger.warning(
                    f"[FLASH_REPORT_BACK] Ledger probe for incumbent "
                    f"{result.winning_run_id} failed; deferring adoption",
                    exc_info=True,
                )
        if row is not None:
            handle.incumbent = result.winning_run_id
            yield handle
            return
        if not row_known or result.incumbent_raw is None:
            # Can't prove the pair's state — a failed ledger probe, or a
            # failed/unavailable claim script (fail-closed: no pointer
            # written, membership unchecked) — defer, never ack.
            handle.in_flight = True
            yield handle
            return
        age = time.time() - (result.incumbent_claimed_at or 0.0)
        if (
            result.incumbent_claimed_at is not None
            and age < leases.RB_POINTER_PRIMING_LEASE_S
        ):
            # Plausibly mid-priming on another worker; the retry re-probes.
            handle.in_flight = True
            yield handle
            return
        # Stale (or undated) rowless pointer: its worker died pre-START.
        # Take the pointer over and run the summary ourselves.
        outcome = await takeover_report_back_run(
            cache,
            flash_thread_id,
            ptc_thread_id,
            result.incumbent_raw,
            run_id,
            dispatch_gen,
            request_key,
        )
        if outcome == "pair_gone":
            handle.pair_gone = True
            yield handle
            return
        if outcome != "claimed":
            handle.in_flight = True
            yield handle
            return
        logger.info(
            f"[FLASH_REPORT_BACK] Took over stale provisional pointer for "
            f"ptc={ptc_thread_id} on flash thread {flash_thread_id} "
            f"(rowless incumbent {result.winning_run_id}, age {age:.0f}s)"
        )
        # Fall through: we own the pointer now, same as a won claim.
    try:
        yield handle
    finally:
        if not handle._consummated:
            await release_report_back_run(cache, flash_thread_id, ptc_thread_id, run_id)


async def clear_flash_report_back(
    cache,
    ptc_thread_id: str,
    flash_thread_id: str | None,
    user_id: str | None = None,
    *,
    record_drained: bool = True,
    expected_gen: str | None = None,
    refuse_if_pointer: bool = False,
) -> ClearOutcome:
    """Tear down all report-back state for one PTC thread.

    Idempotent; the whole teardown is one atomic script so a partial failure
    can't leak the per-user cap. ``expected_gen`` fences it to one dispatch
    incarnation: when the origin carries a DIFFERENT dispatch generation
    (the pair was legitimately re-dispatched), nothing is touched and a falsy
    ``ClearOutcome`` carrying that fencing generation is returned — the newer
    incarnation's own lifecycle owns the state. ``expected_gen=None`` clears
    only legacy (gen-less) or absent origins: a caller that cannot name the
    incarnation it means to destroy must degrade to a TTL-bounded leak, never
    to clearing a generated one. ``refuse_if_pointer=True`` is for callers
    OUTSIDE the flash ordering chain (crash teardown): a live run pointer
    means a report-back admission consummated (or is mid-flight) for this
    pair, and only its serialized lifecycle may drain it — the whole
    teardown is refused atomically (falsy outcome, no fencer gen).
    ``user_id`` (else read from ``ptc_origin``)
    releases the cap slot — if unresolvable we WARN rather than silently leak
    it. Does not swallow Redis errors — callers wrap it. The drained run id
    is recorded on ``flash_rb_done`` so a client that missed the wake can
    still find the finished turn; ``record_drained=False`` skips that
    (deleted flash thread — nothing can render those turns).
    """
    origin = await cache.get(ptc_origin_key(ptc_thread_id))
    if user_id is None and isinstance(origin, dict):
        user_id = origin.get("user_id")

    # Read the run pointer BEFORE the teardown deletes it; best-effort.
    drained_run_id = None
    if record_drained and flash_thread_id:
        try:
            ptr = await cache.get(flash_rb_run_key(flash_thread_id, ptc_thread_id))
            if isinstance(ptr, dict):
                drained_run_id = ptr.get("run_id")
        except Exception:
            pass

    if cache.client:
        res = await cache.client.eval(
            GATED_TEARDOWN_LUA,
            5,
            ptc_origin_key(ptc_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id)
            if flash_thread_id
            else "",
            flash_watch_key(flash_thread_id) if flash_thread_id else "",
            flash_user_pending_key(user_id) if user_id else "",
            ptc_teardown_tombstone_key(ptc_thread_id),
            expected_gen or "",
            ptc_thread_id,
            TEARDOWN_TOMBSTONE_TTL,
            "1" if refuse_if_pointer else "0",
        )
        # Gen fence returns {0, fencing_gen}; pointer refusal {0, ''};
        # success returns 1.
        if isinstance(res, (list, tuple)) and res and not int(res[0]):
            fencer_gen = decode(res[1]) if len(res) > 1 and res[1] else None
            if fencer_gen is None:
                logger.info(
                    f"[FLASH_REPORT_BACK] Teardown for {ptc_thread_id} refused: "
                    f"a report-back run pointer is live on flash thread "
                    f"{flash_thread_id}"
                )
            else:
                logger.info(
                    f"[FLASH_REPORT_BACK] Teardown for {ptc_thread_id} skipped: a "
                    f"newer dispatch generation owns the pair on flash thread "
                    f"{flash_thread_id}"
                )
            return ClearOutcome(False, fencer_gen)
        if not user_id and flash_thread_id:
            # The only path that leaks a per-user cap slot: it won't self-heal
            # (later dispatches refresh flash_user_pending's TTL) and can lock
            # the user out at MAX_DISPATCH_PER_USER.
            logger.warning(
                f"[FLASH_REPORT_BACK] Cannot release per-user cap slot for "
                f"{ptc_thread_id} (flash thread {flash_thread_id}): user id "
                f"unresolved (ptc_origin expired/missing); slot may leak"
            )

        if drained_run_id:
            await record_drained_run(cache, flash_thread_id, drained_run_id)
    return ClearOutcome(True)


async def record_drained_run(cache, flash_thread_id: str, run_id: str) -> None:
    """Append a drained report-back run id to the flash thread's discovery
    list. LREM-first dedups a retry; newest first, bounded, TTL'd.
    Best-effort — never breaks the caller's teardown."""
    try:
        done_key = flash_rb_done_key(flash_thread_id)
        pipe = cache.client.pipeline(transaction=True)
        pipe.lrem(done_key, 0, run_id)
        pipe.lpush(done_key, run_id)
        pipe.ltrim(done_key, 0, FLASH_RB_DONE_MAX - 1)
        pipe.expire(done_key, FLASH_RB_DONE_TTL)
        await pipe.execute()
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Failed recording drained run "
            f"{run_id} for flash thread {flash_thread_id}",
            exc_info=True,
        )
