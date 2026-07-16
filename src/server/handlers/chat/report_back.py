"""Concurrent PTC report-back subsystem — sole owner of the machinery.

A flash thread can dispatch N background PTC analyses; each completion "reports
back" as its own ordered flash turn. This module owns the whole lifecycle so no
other layer hand-rolls Redis against the same key namespace: ``reserve()``
(dispatch slot + origin), ``claim()`` (idempotent run-pointer claim at
admission), ``read_report_back_status`` (the ``/status`` slice), the
report-back POST + terminal wait (``execute_report_back``, run under the
hook-outbox drainer's lease), and ``clear_flash_report_back``. Per-flash
ordering comes from the outbox's ordering-key FIFO, not process memory, so
any worker can execute or resume a report-back.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import uuid
from typing import NamedTuple

from src.config.settings import get_workflow_timeout
from src.server.handlers.chat._common import logger
from src.server.handlers.chat.report_back_keys import (
    flash_rb_done_key,
    flash_rb_run_key,
    flash_user_pending_key,
    flash_watch_key,
    ptc_origin_key,
    ptc_rb_resolved_key,
    ptc_teardown_tombstone_key,
    thread_wake_key,
)


# TTL for report-back Redis state (run pointers, watch SET, queue); 24h.
_FLASH_RB_RUN_TTL = 86400

# TTL for the recently-drained run-id list (flash_rb_done); 15 min.
_FLASH_RB_DONE_TTL = 900

# Max recently-drained run ids kept per flash thread (LTRIM bound).
_FLASH_RB_DONE_MAX = 10

# TTL for ptc_origin and flash_watch / flash_user_pending Redis keys (24 hours).
PTC_ORIGIN_TTL = 86400

# TTL for a teardown tombstone (a clear fenced out by a provisional dispatch
# generation). It only needs to outlive that provisional reserve's
# commit-or-rollback window (seconds); an hour is comfortably beyond it.
_TEARDOWN_TOMBSTONE_TTL = 3600

# Caps on concurrent report-back dispatches (cost/DoS guardrail), enforced as an
# atomic reserve-before-dispatch. Known accepted gap: report_back=False
# dispatches skip reserve() entirely and count against neither cap (bounded only
# by per-dispatch HITL approval).
MAX_DISPATCH_PER_FLASH = 5
MAX_DISPATCH_PER_USER = 10

# The whole reservation (cross-flash check, cap check, membership add, origin
# write) is ONE Lua script, so Redis itself serializes rival dispatches —
# across processes, unlike the asyncio lock this replaces. A phased
# reservation left a window where a concurrent teardown deleted the
# membership between the cap phase and the origin write, wiring a dispatch
# whose completion could never deliver.
# The stored origin gains script-injected lineage fields the orphan
# resolver scopes its destruction by: ``owns_watch``/``owns_user`` (did THIS
# reservation create the membership, or inherit pair state a predecessor's
# pending delivery may still depend on), ``prev_gen`` (the immediately
# displaced generation — it may have been mid-admission and can still
# terminate and send its clear), and ``owner_gen`` (the nearest displaced
# ancestor CERTAIN to be able to produce a teardown: admitted, membership
# creator, or a pre-lineage legacy blob; displacing an unadmitted
# non-owning phantom carries ITS owner_gen through, because a phantom that
# never admits never terminates — without the carry, chained retained
# phantoms would strand the pair to the TTL). A phantom holding either gen
# may act as that lineage's surrogate teardown; one that displaced
# somebody ELSE may not touch the pair.
# KEYS: 1=flash_watch 2=flash_user_pending 3=ptc_origin
# ARGV: 1=ptc_thread_id 2=flash_thread_id 3=max_flash 4=max_user 5=ttl
#       6=origin_json
# Returns {status, added_watch, added_user, prev_origin_json_or_''}.
_RESERVE_LUA = """
local prev = redis.call('get', KEYS[3])
local prev_gen = nil
local owner_gen = nil
if prev then
  local ok, t = pcall(cjson.decode, prev)
  if ok and type(t) == 'table' then
    if t['flash_thread_id'] and t['flash_thread_id'] ~= ARGV[2] then
      return {'cross', 0, 0, ''}
    end
    if type(t['dispatch_gen']) == 'string' then
      prev_gen = t['dispatch_gen']
      if t['admitted_gen'] == t['dispatch_gen']
         or t['owns_watch'] == true or t['owns_user'] == true
         or t['owns_watch'] == nil then
        owner_gen = t['dispatch_gen']
      elseif type(t['owner_gen']) == 'string' then
        owner_gen = t['owner_gen']
      end
    end
  end
end
local in_watch = redis.call('sismember', KEYS[1], ARGV[1])
local in_user = redis.call('sismember', KEYS[2], ARGV[1])
if in_watch == 0 and redis.call('scard', KEYS[1]) >= tonumber(ARGV[3]) then
  return {'cap_flash', 0, 0, ''}
end
if in_user == 0 and redis.call('scard', KEYS[2]) >= tonumber(ARGV[4]) then
  return {'cap_user', 0, 0, ''}
end
if in_watch == 0 then redis.call('sadd', KEYS[1], ARGV[1]) end
redis.call('expire', KEYS[1], ARGV[5])
if in_user == 0 then redis.call('sadd', KEYS[2], ARGV[1]) end
redis.call('expire', KEYS[2], ARGV[5])
local og = cjson.decode(ARGV[6])
og['owns_watch'] = (in_watch == 0)
og['owns_user'] = (in_user == 0)
if prev_gen then og['prev_gen'] = prev_gen end
if owner_gen then og['owner_gen'] = owner_gen end
redis.call('set', KEYS[3], cjson.encode(og), 'EX', ARGV[5])
return {'ok', 1 - in_watch, 1 - in_user, prev or ''}
"""

# Reservation rollback, CAS'd on OUR minted generation as one script: it
# reverses the origin (restore the stashed previous incarnation, else
# delete) and removes only the memberships THIS reservation added — but
# only while our generation is still the current one. A later dispatch
# that re-wrote the origin, or a teardown that already consumed our
# incarnation, makes this a no-op; in particular the membership SREM must
# never fire after a rival refresh (it would strand the rival's live
# report-back). A restore first consults the teardown tombstone SET (each
# fenced clear records its identity — a scalar was overwritable by a
# second stale clear, losing the one that mattered): a GENERATED
# predecessor is dead iff a clear presenting exactly ITS gen was fenced; a
# LEGACY (gen-less) predecessor is dead if ANY clear was fenced, since
# every teardown is authorized against a gen-less origin. Restoring either
# would resurrect state whose only clear already ran — complete the clear
# instead (whole pair), returning {2, <consumed run pointer or ''>} so the
# caller can record the drained run id the skipped clear never got to.
# KEYS: 1=flash_watch 2=flash_user_pending 3=ptc_origin 4=tombstone set
#       5=flash_rb_run ('' = no flash)
# ARGV: 1=ptc_thread_id 2=minted_gen 3=prev_origin_json ('' = delete)
#       4=added_watch '1'/'0' 5=added_user '1'/'0' 6=ttl
_ROLLBACK_RESERVE_LUA = """
local cur = redis.call('get', KEYS[3])
if not cur then return 0 end
local ok, t = pcall(cjson.decode, cur)
if not (ok and type(t) == 'table' and t['dispatch_gen'] == ARGV[2]) then
  return 0
end
if ARGV[3] ~= '' then
  local pok, pt = pcall(cjson.decode, ARGV[3])
  local dead
  if pok and type(pt) == 'table' and type(pt['dispatch_gen']) == 'string' then
    dead = redis.call('sismember', KEYS[4], pt['dispatch_gen']) == 1
  else
    dead = redis.call('scard', KEYS[4]) > 0
  end
  if dead then
    local ptr = ''
    if KEYS[5] ~= '' then
      local pv = redis.call('get', KEYS[5])
      if pv then ptr = pv end
      redis.call('del', KEYS[5])
    end
    redis.call('del', KEYS[3])
    redis.call('del', KEYS[4])
    redis.call('srem', KEYS[1], ARGV[1])
    redis.call('srem', KEYS[2], ARGV[1])
    return {2, ptr}
  end
  redis.call('set', KEYS[3], ARGV[3], 'EX', ARGV[6])
else
  redis.call('del', KEYS[3])
end
if ARGV[4] == '1' then redis.call('srem', KEYS[1], ARGV[1]) end
if ARGV[5] == '1' then redis.call('srem', KEYS[2], ARGV[1]) end
return 1
"""

# Namespace for the report-back POST's request_key: uuid5(NS, outbox job id).
# Deterministic per job, so a crash-and-reclaim re-POST dedups to the original
# summary run instead of starting a second one.
RB_REQUEST_NS = uuid.UUID("6f7a2b1c-9d3e-4f50-8a61-c2d4e5f60718")

def _admission_hold_bound() -> float:
    """Longest legitimate server-side pre-START hold of one dispatched POST.

    ``wait_for_admission`` runs its waits SEQUENTIALLY in one call — the
    compaction backstop first, then (when the freed slot carries cancel
    intent) the stop-drain — so the bound is their SUM, not their max
    (round-4 F1: a cancel landing near a compaction window's close chains
    both). The +2/+20 margins mirror
    BackgroundTaskManager._ADMISSION_TEARDOWN_MARGIN_S /
    _COMPACTION_ADMISSION_MARGIN_S (importing BTM here would be circular;
    unit pins guard the mirror and the sequential composition against
    drift)."""
    from src.config.settings import (
        get_admission_compaction_wait_timeout,
        get_checkpoint_flush_timeout,
        get_compaction_timeout,
    )

    stopping = get_checkpoint_flush_timeout() + 2.0
    compaction = max(
        get_admission_compaction_wait_timeout(), get_compaction_timeout() + 20.0
    )
    return compaction + stopping


# Response/backoff slack per admission attempt (30s sock-read + 5s backoff + margin).
_RB_ADMISSION_MARGIN_S = 60.0

# Cap (seconds) on retrying a 409 (flash thread busy with the user's own turn)
# for one item; derived from the workflow timeout so a long user turn is
# waited out, FLOORED so the budget structurally fits the priming lease plus
# one full post-takeover admission attempt (round-3 F1): the drop deadline
# must never land while the route is still inside a legitimate pre-START hold
# entered after takeover became legal — the admission holds derive from
# DIFFERENT config knobs than workflow_timeout, so no timeout value may be
# trusted to cover them.
_RB_BUSY_WAIT_CAP = max(
    float(get_workflow_timeout()),
    2.0 * (_admission_hold_bound() + _RB_ADMISSION_MARGIN_S),
)

# Cap (seconds) on waiting for a POSTed report-back to reach terminal before
# force-clearing it, so a crashed run can't wedge the whole flash queue.
_RB_TERMINAL_WAIT_CAP = _RB_BUSY_WAIT_CAP


def _derive_priming_lease(retry_budget: float) -> float:
    """Priming lease on a run pointer whose run has NO ledger row yet.

    Half the (floored) budget: the lease itself covers the longest legitimate
    pre-START admission wait, and the remaining half guarantees post-takeover
    retries — one full admission hold plus slack — before the drop deadline.
    A rowless crashed pointer therefore always becomes takeover-eligible AND
    completable while 503 retries are still coming; the cost of a small lease
    (takeover racing an unusually slow priming) is bounded by the per-thread
    in_progress slot — one live run per flash thread.
    """
    return min(900.0, retry_budget / 2)


# An incumbent pointer younger than this may still be mid-priming (admission
# wait + START txn) on another worker: retries defer instead of adopting or
# taking over.
_RB_POINTER_PRIMING_LEASE_S = _derive_priming_lease(_RB_BUSY_WAIT_CAP)

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
_GATED_POINTER_SET_LUA = """
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
_CLAIM_POINTER_LUA = """
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
_POINTER_TAKEOVER_LUA = """
local v = redis.call('get', KEYS[2])
if not v or v ~= ARGV[2] then return 0 end
if redis.call('sismember', KEYS[1], ARGV[1]) == 0 then return 2 end
redis.call('set', KEYS[2], ARGV[3], 'EX', ARGV[4])
return 1
"""

# Orphan-membership reap: SREM a member iff its ptc_origin key no longer
# exists, atomically per member (a rival reserve SADDs the member and SETs
# the origin in one script, so exists-then-srem here can never remove a
# freshly reserved member). Needed because the watch/user SETs are shared
# keys whose TTL every reserve refreshes — a member stranded by a lost
# reserve reply (fail-closed, no rollback) would otherwise outlive its
# origin's TTL indefinitely under active use, permanently eating cap slots.
# KEYS: 1=set 2..n+1=the members' origin keys  ARGV: 1..n=members
_REAP_ORPHANS_LUA = """
local removed = 0
for i, member in ipairs(ARGV) do
  if redis.call('exists', KEYS[i + 1]) == 0 then
    removed = removed + redis.call('srem', KEYS[1], member)
  end
end
return removed
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
_GATED_TEARDOWN_LUA = """
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
_POINTER_COMPARE_DELETE_LUA = """
local v = redis.call('get', KEYS[1])
if not v then return 0 end
local ok, t = pcall(cjson.decode, v)
if ok and type(t) == 'table' and t['run_id'] == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


def _decode(value) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


# ---------------------------------------------------------------------------
# ACQUIRE — reserve a dispatch slot under the caps + record the PTC origin
# ---------------------------------------------------------------------------


def _cap_error_flash() -> str:
    return (
        f"too many concurrent analyses on this thread "
        f"(max {MAX_DISPATCH_PER_FLASH}); wait for one to finish"
    )


def _cap_error_user() -> str:
    return (
        f"too many concurrent analyses running "
        f"(max {MAX_DISPATCH_PER_USER}); wait for one to finish"
    )


# How long a resolved-phantom receipt blocks its generation's admission.
# Aligned with _TEARDOWN_TOMBSTONE_TTL: any HTTP admission still in flight
# for a generation that old is dead many times over.
_RESOLVED_RECEIPT_TTL = 3600

# Phantom-refusal admission gate + pre-START intent stamp, ONE script: the
# orphan resolver receipts a generation it resolved as never-admitted — if
# that generation's own HTTP admission then landed, it would run with its
# watch state already erased and its report-back silently dropped. Receipt
# check and stamp are one Redis-side step, so resolution vs late admission
# is a race exactly one side can win.
# The pre-START intent is PER CONTENDER: every admission of the generation
# records its run in ``pending_runs`` on the exact-gen origin blob (KEEPTTL —
# the identity's lifetime), and the resolver defers while ANY entry survives.
# A single first-stamp-wins token is not enough — the Redis first stamper is
# not necessarily the Postgres START winner, so its priming-failure retract
# must not strip the only protection from a same-gen sibling still heading
# to START. The ``admitted_gen``/``admitted_run`` first-stamp is still
# written because ``_RESERVE_LUA`` derives displaced-owner lineage from it;
# as a resolver suppressant it is redundant with ``pending_runs`` and NOT
# sufficient alone (the holder's retract unstamps while siblings remain
# pending) — no resolver may ever read only the stamp. The durable
# admission record is the ledger row's
# ``origin_dispatch_gen`` metadata, which ``resolve_orphaned_watch`` checks
# caller-side. A moved/absent origin skips the intent (that lifecycle isn't
# ours to write) but still admits: the run then executes as an ordinary
# turn whose report-back finds the pair settled.
# KEYS: 1=resolved-receipt set 2=origin key
# ARGV: 1=this dispatch generation 2=this admission's run_id
_ADMISSION_GATE_LUA = """
if redis.call('sismember', KEYS[1], ARGV[1]) == 1 then return 0 end
local o = redis.call('get', KEYS[2])
if o then
  local ok, origin = pcall(cjson.decode, o)
  if ok and type(origin) == 'table' and origin['dispatch_gen'] == ARGV[1] then
    if origin['admitted_gen'] ~= ARGV[1] then
      origin['admitted_gen'] = ARGV[1]
      origin['admitted_run'] = ARGV[2]
    end
    if type(origin['pending_runs']) ~= 'table' then
      origin['pending_runs'] = {}
    end
    origin['pending_runs'][ARGV[2]] = true
    redis.call('set', KEYS[2], cjson.encode(origin), 'KEEPTTL')
  end
end
return 1
"""

# Intent release after a priming failure: removes ONLY this admission's own
# ``pending_runs`` entry (per-contender — a failing same-gen retransmit can
# never strip a live sibling's protection), plus the legacy stamp iff this
# run holds it (exact CAS). Without the release the resolver would read the
# failed gen as pending until the origin TTL and the phantom never settles.
# KEYS: 1=origin key  ARGV: 1=generation 2=run_id
_ADMISSION_RETRACT_LUA = """
local o = redis.call('get', KEYS[1])
if not o then return 0 end
local ok, origin = pcall(cjson.decode, o)
if not ok or type(origin) ~= 'table' then return 0 end
if origin['dispatch_gen'] ~= ARGV[1] then return 0 end
local dirty = false
if origin['admitted_gen'] == ARGV[1] and origin['admitted_run'] == ARGV[2] then
  origin['admitted_gen'] = nil
  origin['admitted_run'] = nil
  dirty = true
end
if type(origin['pending_runs']) == 'table'
   and origin['pending_runs'][ARGV[2]] then
  origin['pending_runs'][ARGV[2]] = nil
  dirty = true
end
if not dirty then return 0 end
redis.call('set', KEYS[1], cjson.encode(origin), 'KEEPTTL')
return 1
"""


async def admit_dispatch_gen(
    ptc_thread_id: str, dispatch_gen: str, run_id: str
) -> bool:
    """Phantom-refusal gate for a dispatched PTC admission, pre-START.

    Refuses a generation the orphan resolver already receipted (its watch
    state is gone — admitting would run a turn whose report-back silently
    drops) and stamps pre-START intent on the origin. Fails CLOSED: an
    unverifiable gate refuses, the caller 503s, and the dispatcher's
    lost-reply probe re-drives it.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return False
    try:
        res = await cache.client.eval(
            _ADMISSION_GATE_LUA,
            2,
            ptc_rb_resolved_key(ptc_thread_id),
            ptc_origin_key(ptc_thread_id),
            dispatch_gen,
            run_id,
        )
        return bool(int(res))
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Admission gate failed for {ptc_thread_id} "
            f"(gen {dispatch_gen}); refusing (retriable)",
            exc_info=True,
        )
        return False


async def retract_dispatch_gen(
    ptc_thread_id: str, dispatch_gen: str, run_id: str
) -> None:
    """Release the pre-START intent stamp after a priming failure.

    Best-effort: a lingering stamp degrades to an origin-TTL-bounded
    'admitted' read (pair waits out the TTL), never to destroying live
    state. Must NOT run on a retransmit-adopt — the adopted run IS this
    generation admitted.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return
    try:
        await cache.client.eval(
            _ADMISSION_RETRACT_LUA,
            1,
            ptc_origin_key(ptc_thread_id),
            dispatch_gen,
            run_id,
        )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Intent-stamp retract failed for "
            f"{ptc_thread_id} (gen {dispatch_gen}); stamp decays at origin TTL",
            exc_info=True,
        )

# Atomic phantom-fence resolution — decide + resolve + receipt in ONE script.
# A separate probe-then-resolve pair races both a newer reservation replacing
# the origin AND the fencer's own late admission; each hazard is closed here:
#   - the origin must still carry EXACTLY the generation that fenced the
#     caller (ARGV[2]) — a moved origin belongs to a newer reservation's
#     lifecycle and is left alone;
#   - the origin's per-contender ``pending_runs`` entries (written by the
#     admission gate, living as long as the origin) are the PRE-START
#     INTENT check: any surviving entry is a same-gen admission mid-priming
#     or running — never phantom. The redundant first-stamp
#     (``admitted_gen``) is honored too, but ``pending_runs`` is the
#     authority — the stamp holder's retract unstamps while siblings remain
#     pending. The DURABLE admission record (a ledger row carrying the
#     generation) is checked by the caller before this script runs; the
#     intent covers only the pre-START window where no row exists yet;
#   - the phantom's generation goes into the resolved-receipt SET FIRST —
#     the endpoint's admission-marker write atomically refuses receipted
#     generations, so resolution vs late admission is a race exactly one
#     side can win;
#   - then the pair's client-facing state resolves, scoped to what the
#     phantom actually OWNS: a membership falls only if this reservation
#     created it (``owns_watch``/``owns_user``) or if the fenced clear's
#     generation is one of the phantom's recorded teardown lineages —
#     ``prev_gen`` (directly displaced; the canonical lost-409 phantom
#     inherits the failing predecessor's pair state, and resolving it IS
#     that predecessor's blocked teardown) or ``owner_gen`` (the carried
#     nearest ancestor certain to produce a clear — everything displaced
#     between it and this phantom was an unadmitted non-owning phantom
#     with no pending delivery, so acting for the owner erases nothing
#     anyone still needs). Pair state
#     inherited past an INTERMEDIATE lineage is left intact — a completed
#     predecessor with a still-queued report-back needs its membership to
#     deliver, and no rollback ever re-adds memberships. The run pointer
#     follows the same authorization: a SURROGATE resolution IS the dying
#     job's whole-pair teardown, which deletes the pointer regardless of
#     generation (mirroring the gated teardown) — memberships must never
#     fall while a pointer nobody can discover survives them; a
#     non-surrogate resolution spares foreign generations' pointers. A
#     drained run id lands on the flash_rb_done discovery list IN THE SAME
#     SCRIPT — a wrapper-side record after the pointer DEL would be lost
#     forever on a crash between the two (the retry finds the pointer
#     already gone).
#     The origin is spared — only a gen-authorized teardown destroys it.
# KEYS: 1=origin 2=watch set|'' 3=user set|''
#       4=run pointer|'' 5=resolved-receipt set 6=flash_rb_done|''
# ARGV: 1=ptc_thread_id 2=fencer gen 3=receipt ttl 4=done max 5=done ttl
#       6=the fenced clear's own generation ('' = unknown)
# Returns {1, ptr_json|'', watch_removed, user_removed} on resolve;
# {0, reason} on suppress.
_ORPHAN_RESOLVE_LUA = """
local o = redis.call('get', KEYS[1])
if not o then return {0, 'origin_gone'} end
local ok, origin = pcall(cjson.decode, o)
if not ok or type(origin) ~= 'table' then return {0, 'origin_unreadable'} end
if origin['dispatch_gen'] ~= ARGV[2] then return {0, 'origin_moved'} end
if origin['admitted_gen'] == ARGV[2] then return {0, 'admitted'} end
if type(origin['pending_runs']) == 'table'
   and next(origin['pending_runs']) ~= nil then
  return {0, 'admitted'}
end
local surrogate = (ARGV[6] ~= ''
  and (origin['prev_gen'] == ARGV[6] or origin['owner_gen'] == ARGV[6]))
redis.call('sadd', KEYS[5], ARGV[2])
redis.call('expire', KEYS[5], tonumber(ARGV[3]))
local watch_removed = 0
local user_removed = 0
if KEYS[2] ~= '' and (origin['owns_watch'] == true or surrogate) then
  watch_removed = redis.call('srem', KEYS[2], ARGV[1])
end
if KEYS[3] ~= '' and (origin['owns_user'] == true or surrogate) then
  user_removed = redis.call('srem', KEYS[3], ARGV[1])
end
local ptr = ''
if KEYS[4] ~= '' then
  local p = redis.call('get', KEYS[4])
  if p then
    local pok, pt = pcall(cjson.decode, p)
    if not surrogate and pok and type(pt) == 'table'
       and type(pt['dispatch_gen']) == 'string'
       and pt['dispatch_gen'] ~= ARGV[2] then
      ptr = ''
    else
      redis.call('del', KEYS[4])
      ptr = p
      if pok and type(pt) == 'table' and type(pt['run_id']) == 'string'
         and KEYS[6] ~= '' then
        redis.call('lrem', KEYS[6], 0, pt['run_id'])
        redis.call('lpush', KEYS[6], pt['run_id'])
        redis.call('ltrim', KEYS[6], 0, tonumber(ARGV[4]) - 1)
        redis.call('expire', KEYS[6], tonumber(ARGV[5]))
      end
    end
  end
end
return {1, ptr, watch_removed, user_removed}
"""


async def resolve_orphaned_watch(
    cache,
    ptc_thread_id: str,
    flash_thread_id: str | None,
    user_id: str | None,
    *,
    fencer_gen: str,
    job_gen: str | None = None,
) -> tuple[bool, str | None]:
    """Durably resolve a pair whose teardown was fenced by a never-admitted
    reservation (lost-409 continuation): the memberships the phantom owns —
    or inherited DIRECTLY from the fenced clear's own dying lineage
    (``job_gen``) — fall, so ``pending_report_back`` flips and the cap slot
    releases; the drained flash run lands on ``flash_rb_done`` (in the same
    script as the pointer delete, so a wrapper crash can't lose the
    discovery record); and the phantom generation is receipted so its late
    admission is refused. Pair state inherited from an INTERMEDIATE lineage
    is left intact (its delivery may still be queued), and the origin is
    deliberately spared. Admission is judged three ways: the ledger row
    carrying this generation (the durable record, checked first — fails
    CLOSED on a DB error, never resolving on a guess), the origin's
    per-contender ``pending_runs`` intent entries (checked atomically in
    the script, covering every same-gen admission's pre-START window —
    surviving its siblings' retracts), and a post-resolve ledger
    revalidation that restores the dropped memberships if a START raced
    the script (the pre-probe's negatives are not stable through the Redis
    mutation). Idempotent; does not swallow Redis errors (the drainer
    nacks and retries). Returns ``(resolved, drained_run_id)``.
    """
    from src.server.database import turn_lifecycle as tl_db

    try:
        admitted = await tl_db.thread_has_dispatch_gen(ptc_thread_id, fencer_gen)
        # A live run on the PTC thread — any lineage — suppresses too: its
        # own hooks settle the pair, and a surrogate resolution could drop
        # a membership that live delivery still needs (round-13 P0, ledger
        # translation of the old live-marker check).
        live = (
            False if admitted
            else await tl_db.get_active_run(ptc_thread_id) is not None
        )
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Ledger probe for phantom-fence resolution of "
            f"{ptc_thread_id} (gen {fencer_gen}) failed; suppressing",
            exc_info=True,
        )
        return False, None
    if admitted or live:
        logger.info(
            f"[FLASH_REPORT_BACK] Phantom-fence resolution for "
            f"{ptc_thread_id} (gen {fencer_gen}) suppressed: "
            f"{'admitted' if admitted else 'live run'} (ledger)"
        )
        return False, None

    res = await cache.client.eval(
        _ORPHAN_RESOLVE_LUA,
        6,
        ptc_origin_key(ptc_thread_id),
        flash_watch_key(flash_thread_id) if flash_thread_id else "",
        flash_user_pending_key(user_id) if user_id else "",
        flash_rb_run_key(flash_thread_id, ptc_thread_id)
        if flash_thread_id
        else "",
        ptc_rb_resolved_key(ptc_thread_id),
        flash_rb_done_key(flash_thread_id) if flash_thread_id else "",
        ptc_thread_id,
        fencer_gen,
        _RESOLVED_RECEIPT_TTL,
        _FLASH_RB_DONE_MAX,
        _FLASH_RB_DONE_TTL,
        job_gen or "",
    )
    if not (isinstance(res, (list, tuple)) and res and int(res[0])):
        reason = (
            _decode(res[1])
            if isinstance(res, (list, tuple)) and len(res) > 1
            else "?"
        )
        logger.info(
            f"[FLASH_REPORT_BACK] Phantom-fence resolution for "
            f"{ptc_thread_id} (gen {fencer_gen}) suppressed: {reason}"
        )
        return False, None

    # Post-resolve revalidation: the pre-probe's negatives (no row, no live
    # run) are point-in-time reads that are not stable through the Redis
    # mutation — a START committing in the gap (a same-gen admission, or an
    # ordinary continuation that never touches the gate) needs the
    # memberships this script just dropped. Pair-consuming outbox jobs are
    # serialized per flash thread by ordering key, so restoring them before
    # this job completes closes the race; a run cannot reach its own
    # terminal hook inside this gap.
    if await _compensate_if_started(
        cache, ptc_thread_id, flash_thread_id, user_id, fencer_gen, res
    ):
        return False, None
    run_id = None
    ptr_raw = res[1] if len(res) > 1 else None
    if ptr_raw:
        try:
            ptr = json.loads(_decode(ptr_raw))
            if isinstance(ptr, dict):
                run_id = ptr.get("run_id")
        except (TypeError, ValueError):
            run_id = None
    return True, run_id


async def _compensate_if_started(
    cache,
    ptc_thread_id: str,
    flash_thread_id: str | None,
    user_id: str | None,
    fencer_gen: str,
    res,
) -> bool:
    """Detect a START that raced the resolve script and restore exactly the
    memberships it removed. Returns True when compensation ran (the caller
    must treat the resolution as suppressed). A probe/restore failure here is
    a logged double-fault residual — the loss it would take requires the
    START to land inside the probe→script gap AND this second read to fail
    milliseconds after the first succeeded."""
    from src.server.database import turn_lifecycle as tl_db

    try:
        admitted_now = await tl_db.thread_has_dispatch_gen(
            ptc_thread_id, fencer_gen
        )
        live_now = (
            True if admitted_now
            else await tl_db.get_active_run(ptc_thread_id) is not None
        )
    except Exception:
        logger.critical(
            f"[FLASH_REPORT_BACK] Post-resolve ledger revalidation failed for "
            f"{ptc_thread_id} (gen {fencer_gen}); if a START raced the "
            f"resolution its report-back membership is lost",
            exc_info=True,
        )
        return False
    if not (admitted_now or live_now):
        return False

    watch_removed = len(res) > 2 and bool(int(res[2]))
    user_removed = len(res) > 3 and bool(int(res[3]))
    try:
        pipe = cache.client.pipeline()
        if watch_removed and flash_thread_id:
            pipe.sadd(flash_watch_key(flash_thread_id), ptc_thread_id)
            pipe.expire(flash_watch_key(flash_thread_id), PTC_ORIGIN_TTL)
        if user_removed and user_id:
            pipe.sadd(flash_user_pending_key(user_id), ptc_thread_id)
            pipe.expire(flash_user_pending_key(user_id), PTC_ORIGIN_TTL)
        if admitted_now:
            # The racer was this very generation: un-receipt it so its
            # retransmits aren't refused while its run lives.
            pipe.srem(ptc_rb_resolved_key(ptc_thread_id), fencer_gen)
        await pipe.execute()
    except Exception:
        logger.critical(
            f"[FLASH_REPORT_BACK] Membership restore after raced resolution "
            f"failed for {ptc_thread_id} (gen {fencer_gen}); report-back for "
            f"the racing run may be lost",
            exc_info=True,
        )
        return True
    logger.warning(
        f"[FLASH_REPORT_BACK] Phantom-fence resolution for {ptc_thread_id} "
        f"(gen {fencer_gen}) raced a START; memberships restored "
        f"(watch={watch_removed}, user={user_removed}, "
        f"same_gen={admitted_now})"
    )
    return True


async def _reap_listed_orphans(cache, set_key: str, members: list[str]) -> int:
    """SREM the given members from ``set_key`` iff their origin is absent AT
    SCRIPT TIME — the per-member EXISTS inside the Lua is the guard, so a
    reserve landing after the caller's stale read is never touched."""
    if not members:
        return 0
    return int(
        await cache.client.eval(
            _REAP_ORPHANS_LUA,
            1 + len(members),
            set_key,
            *[ptc_origin_key(m) for m in members],
            *members,
        )
    )


async def _reap_orphan_members(
    cache, flash_thread_id: str | None, user_id: str | None
) -> int:
    """SREM watch/user members whose ptc_origin key no longer exists.

    Ran under cap pressure and from the status read, so the common path costs
    nothing: a member without an origin is definitionally dead state —
    origins outlive every legitimate lifecycle stage and fall only at
    teardown or TTL — left behind by a lost reserve reply or a clear that
    couldn't resolve the user. Returns how many members were removed.
    """
    removed = 0
    set_keys = []
    if flash_thread_id:
        set_keys.append(flash_watch_key(flash_thread_id))
    if user_id:
        set_keys.append(flash_user_pending_key(user_id))
    for set_key in set_keys:
        members = [_decode(m) for m in (await cache.client.smembers(set_key)) or []]
        removed += await _reap_listed_orphans(cache, set_key, members)
    if removed:
        logger.info(
            f"[FLASH_REPORT_BACK] Reaped {removed} orphaned dispatch member(s) "
            f"for flash={flash_thread_id} user={user_id}"
        )
    return removed


async def check_dispatch_capacity(flash_thread_id: str | None, user_id: str) -> str | None:
    """Advisory cap read for callers about to do expensive pre-dispatch work.

    Returns the cap error ``reserve()`` would raise for a NEW dispatch right
    now, else None. Takes no reservation — ``reserve()`` stays the atomic
    authority — and fails open like it (report_back off / Redis off / error
    -> None). An over-cap read reaps orphaned members once and re-checks, so
    a stranded membership can't wedge the pre-check permanently.
    """
    if not flash_thread_id:
        return None
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not (cache.enabled and cache.client):
            return None
        for attempt in range(2):
            error = None
            if await cache.client.scard(flash_watch_key(flash_thread_id)) >= MAX_DISPATCH_PER_FLASH:
                error = _cap_error_flash()
            elif await cache.client.scard(flash_user_pending_key(user_id)) >= MAX_DISPATCH_PER_USER:
                error = _cap_error_user()
            if error is None:
                return None
            if attempt == 0 and await _reap_orphan_members(cache, flash_thread_id, user_id):
                continue
            return error
        return error
    except Exception as e:
        logger.warning(f"Dispatch capacity pre-check failed: {e}")
        return None


# In-process serialization of same-pair reservation lifecycles (reserve →
# dispatch → commit/rollback). Without it, two overlapping cycles for ONE
# ptc pair can interleave so the later one's rollback restores the earlier
# incarnation's origin AFTER that incarnation itself already failed —
# resurrecting a reservation no run backs (the gen CAS protects against a
# rollback harming a LIVE rival, but cannot know whether the stashed
# predecessor later rolled back). Same-pair overlap is a degenerate case
# (duplicate dispatch tool call), so serializing whole lifecycles is cheap;
# cross-process overlap remains the documented accepted residual
# (last-writer-wins) — reviewed and left standing when 2.4 distributed the
# rest: the gen CAS bounds the damage to the degenerate duplicate-dispatch
# shape, and a resurrected unbacked reservation decays at the origin TTL.
_pair_locks: dict[str, tuple[asyncio.Lock, int]] = {}


@contextlib.asynccontextmanager
async def _pair_lock(ptc_thread_id: str):
    lock, refs = _pair_locks.get(ptc_thread_id) or (asyncio.Lock(), 0)
    _pair_locks[ptc_thread_id] = (lock, refs + 1)
    try:
        async with lock:
            yield
    finally:
        held, remaining = _pair_locks[ptc_thread_id]
        if remaining <= 1:
            del _pair_locks[ptc_thread_id]
        else:
            _pair_locks[ptc_thread_id] = (held, remaining - 1)


class _DispatchSlot:
    """Typed outcome of ``reserve()``.

    ``error`` (an over-cap message or ``"dispatch_failed"``) tells the caller to
    abort; ``wired`` is True only when flash_watch membership is durably in place
    (the completion-time gate can then deliver a report-back). ``commit()`` keeps
    the reservation on the success path; any non-commit exit rolls it back.
    """

    def __init__(self) -> None:
        self.error: str | None = None
        self.wired: bool = False
        self.dispatch_gen: str | None = None
        self._committed = False
        self._reserved = False
        self._prev_origin_raw: str | None = None
        self._added: dict = {"watch": False, "user": False}
        self._cache = None
        self._flash_thread_id: str | None = None
        self._ptc_thread_id: str | None = None
        self._user_id: str | None = None

    def commit(self) -> None:
        """Mark the dispatch as durably started so the reservation is kept."""
        self._committed = True

    async def _rollback(self) -> None:
        # One gen-CAS'd script: origin reversed (previous incarnation
        # restored, else deleted) and only OUR added memberships removed,
        # all only while our generation is still current. A rival refresh
        # or an already-run teardown makes the whole thing a no-op — a
        # non-atomic SREM here would strand a rival's live report-back.
        # A tombstone-completed clear ({2, ptr}) hands back the summary-run
        # pointer the fenced teardown never got to record — record it, or a
        # client that missed the wake loses its only discovery path.
        if not (self._reserved and self.dispatch_gen and self._cache is not None):
            return
        try:
            res = await self._cache.client.eval(
                _ROLLBACK_RESERVE_LUA,
                5,
                flash_watch_key(self._flash_thread_id),
                flash_user_pending_key(self._user_id),
                ptc_origin_key(self._ptc_thread_id),
                ptc_teardown_tombstone_key(self._ptc_thread_id),
                flash_rb_run_key(self._flash_thread_id, self._ptc_thread_id)
                if self._flash_thread_id
                else "",
                self._ptc_thread_id,
                self.dispatch_gen,
                self._prev_origin_raw or "",
                "1" if self._added.get("watch") else "0",
                "1" if self._added.get("user") else "0",
                PTC_ORIGIN_TTL,
            )
            if (
                isinstance(res, (list, tuple))
                and res
                and int(res[0]) == 2
                and self._flash_thread_id
            ):
                ptr_raw = _decode(res[1]) if len(res) > 1 else ""
                run_id = None
                if ptr_raw:
                    try:
                        ptr = json.loads(ptr_raw)
                        if isinstance(ptr, dict):
                            run_id = ptr.get("run_id")
                    except (TypeError, ValueError):
                        run_id = None
                if run_id:
                    await _record_drained_run(
                        self._cache, self._flash_thread_id, run_id
                    )
        except Exception:
            pass


@contextlib.asynccontextmanager
async def reserve(
    flash_thread_id: str | None,
    ptc_thread_id: str,
    ptc_workspace_id: str | None,
    flash_workspace_id: str | None,
    user_id: str,
):
    """Reserve a report-back dispatch slot + record the PTC origin, as a CM.

    The symmetric partner of ``clear_flash_report_back``: yields a typed
    ``_DispatchSlot`` and rolls the reservation back on any non-committed
    exit. ``flash_thread_id`` is None for a non-report-back dispatch — a
    no-op slot (nothing reserved/wired) so the dispatch still POSTs. The
    whole reservation (cross-flash check, caps, memberships, origin +
    minted generation) is ONE Redis script — there is no window where a
    concurrent teardown or rival dispatch can observe it half-applied.
    Redis off by config: proceeds unwired (no report-back system at all).
    Script failure: fail-closed ``slot.error = "dispatch_failed"`` with NO
    blind rollback — the script's effects are unknown, and destroying a
    possibly live incarnation is worse than a TTL-bounded leak (an over-cap
    reserve reaps orphaned members once and retries, so that leak stays
    TTL-bounded rather than wedging the caps). The whole lifecycle — script
    through the caller's dispatch and commit/rollback — holds an in-process
    per-pair lock so overlapping same-pair cycles can't interleave their
    rollbacks (see ``_pair_lock``).
    """
    from src.utils.cache.redis_cache import get_cache_client

    slot = _DispatchSlot()
    slot._flash_thread_id = flash_thread_id
    slot._ptc_thread_id = ptc_thread_id
    slot._user_id = user_id

    # Non-report-back dispatch: nothing to reserve or wire.
    if not flash_thread_id:
        yield slot
        return
    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        yield slot
        return
    slot._cache = cache

    async with _pair_lock(ptc_thread_id):
        try:
            # Record origin BEFORE the caller's POST so a watch member's origin
            # exists by the time its PTC completion can enqueue a report-back.
            # Every reservation cycle mints a NEW dispatch generation: the token
            # rides the dispatch POST into START metadata, and every terminal
            # teardown compares it against the live origin — a stale clear from a
            # PREVIOUS incarnation can then never destroy this one's watch state.
            dispatch_gen = str(uuid.uuid4())
            origin_payload = {
                "origin": "flash",
                "flash_thread_id": flash_thread_id,
                "flash_workspace_id": flash_workspace_id,
                "ptc_thread_id": ptc_thread_id,
                "ptc_workspace_id": ptc_workspace_id,
                "report_back": True,
                "user_id": user_id,
                "dispatch_gen": dispatch_gen,
            }
            status = None
            for attempt in range(2):
                try:
                    res = await cache.client.eval(
                        _RESERVE_LUA,
                        3,
                        flash_watch_key(flash_thread_id),
                        flash_user_pending_key(user_id),
                        ptc_origin_key(ptc_thread_id),
                        ptc_thread_id,
                        flash_thread_id,
                        MAX_DISPATCH_PER_FLASH,
                        MAX_DISPATCH_PER_USER,
                        PTC_ORIGIN_TTL,
                        json.dumps(origin_payload),
                    )
                except Exception as e:
                    logger.warning(f"Failed to reserve PTC dispatch slot: {e}")
                    slot.error = "dispatch_failed"
                    yield slot
                    return
                status = _decode(res[0])
                if status not in ("cap_flash", "cap_user") or attempt == 1:
                    break
                # Over cap: reap members stranded without an origin (lost
                # reserve replies etc. — nothing else removes them, and every
                # reserve refreshes the shared SET's TTL) and retry once.
                try:
                    if not await _reap_orphan_members(cache, flash_thread_id, user_id):
                        break
                except Exception:
                    break
            if status == "cap_flash":
                slot.error = _cap_error_flash()
                yield slot
                return
            if status == "cap_user":
                slot.error = _cap_error_user()
                yield slot
                return
            if status == "cross":
                # A different flash thread already owns this PTC's origin: we
                # can't wire a second one. Nothing was touched; proceed unwired.
                yield slot
                return
            slot._added = {"watch": bool(res[1]), "user": bool(res[2])}
            slot._prev_origin_raw = _decode(res[3]) or None
            slot._reserved = True
            slot.wired = True
            slot.dispatch_gen = dispatch_gen
            yield slot
        finally:
            # Single rollback path: any uncommitted exit (cap-clear, provably
            # undelivered POST) releases the reservation — still under the
            # pair lock, so a queued same-pair reserve observes the settled
            # outcome, never our in-between state. Ambiguous dispatch outcomes
            # (lost body/transport, cancellation mid-exchange) commit first
            # and never reach the rollback.
            if not slot._committed:
                await slot._rollback()


def _pointer_value(
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
            _CLAIM_POINTER_LUA,
            2,
            flash_watch_key(flash_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            ptc_thread_id,
            _pointer_value(run_id, dispatch_gen, request_key),
            _FLASH_RB_RUN_TTL,
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
        raw = _decode(incumbent)
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
    if not (cache.enabled and cache.client):
        return "claimed"
    try:
        state = await cache.client.eval(
            _POINTER_TAKEOVER_LUA,
            2,
            flash_watch_key(flash_thread_id),
            flash_rb_run_key(flash_thread_id, ptc_thread_id),
            ptc_thread_id,
            expected_raw,
            _pointer_value(run_id, dispatch_gen, request_key),
            _FLASH_RB_RUN_TTL,
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
            _POINTER_COMPARE_DELETE_LUA,
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
                from src.server.database import turn_lifecycle as tl_db

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
            and age < _RB_POINTER_PRIMING_LEASE_S
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
            _GATED_TEARDOWN_LUA,
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
            _TEARDOWN_TOMBSTONE_TTL,
            "1" if refuse_if_pointer else "0",
        )
        # Gen fence returns {0, fencing_gen}; pointer refusal {0, ''};
        # success returns 1.
        if isinstance(res, (list, tuple)) and res and not int(res[0]):
            fencer_gen = _decode(res[1]) if len(res) > 1 and res[1] else None
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
            await _record_drained_run(cache, flash_thread_id, drained_run_id)
    return ClearOutcome(True)


async def _record_drained_run(cache, flash_thread_id: str, run_id: str) -> None:
    """Append a drained report-back run id to the flash thread's discovery
    list. LREM-first dedups a retry; newest first, bounded, TTL'd.
    Best-effort — never breaks the caller's teardown."""
    try:
        done_key = flash_rb_done_key(flash_thread_id)
        pipe = cache.client.pipeline(transaction=True)
        pipe.lrem(done_key, 0, run_id)
        pipe.lpush(done_key, run_id)
        pipe.ltrim(done_key, 0, _FLASH_RB_DONE_MAX - 1)
        pipe.expire(done_key, _FLASH_RB_DONE_TTL)
        await pipe.execute()
    except Exception:
        logger.warning(
            f"[FLASH_REPORT_BACK] Failed recording drained run "
            f"{run_id} for flash thread {flash_thread_id}",
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# WAKE — the report-back wake wire-protocol (publish + subscribe), one home
# ---------------------------------------------------------------------------

# SSE event name every report-back wake is delivered under on ``/watch``. The
# frontend watch parser keys on this exact string (web api.ts
# ``REPORT_BACK_WAKE_EVENT``) — keep the two in lockstep.
WAKE_EVENT = "workflow_started"

# ``/watch`` subscriber defaults.
WAKE_KEEPALIVE_INTERVAL = 45  # seconds between keepalive comment frames
WAKE_MAX_WATCH_DURATION = 30 * 60  # auto-close an abandoned watch after 30 min


async def publish_wake(
    cache,
    thread_id: str,
    run_id: str | None = None,
    *,
    error: str | None = None,
    needs_input: str | None = None,
    cleared: bool = False,
) -> None:
    """Publish a report-back wake on a watching thread's channel. Best-effort.

    Single home for the wire payload shape: a normal wake carries
    ``{thread_id, run_id}``; an error wake carries ``{error}``; a HITL pause
    on a dispatched PTC carries ``{needs_input: <ptc thread id>}`` (run_id-less,
    so the client treats it as a /status-refresh nudge); a consumption clear
    carries ``{thread_id, cleared: true}`` (the watcher reconciles and drops
    its pending chip without waiting for the status backstop). Swallows
    publish failures — a dropped nudge degrades to the client's ``/status``
    poll.
    """
    if not (cache and getattr(cache, "client", None)):
        logger.warning(
            f"[RB_WAKE] No cache client; wake for thread {thread_id} not published"
        )
        return
    if error:
        payload = {"error": error}
    elif needs_input:
        payload = {"needs_input": needs_input}
    elif cleared:
        payload = {"thread_id": thread_id, "cleared": True}
    else:
        payload = {"thread_id": thread_id, "run_id": run_id}
    try:
        await cache.client.publish(thread_wake_key(thread_id), json.dumps(payload))
    except Exception:
        logger.warning(
            f"[RB_WAKE] Wake publish failed for thread {thread_id}", exc_info=True
        )


async def watch_wakes(cache, flash_thread_id: str):
    """Yield SSE frames for a flash thread's report-back wake subscription.

    Owns the pub/sub lifecycle, ``WAKE_EVENT`` frame format, keepalives, and the
    max-duration auto-close so the ``/watch`` route stays a thin auth wrapper.
    Forwards EVERY wake, not just the first: N concurrent PTCs' report-backs
    arrive as separate runs and must all be delivered on the one connection.
    """
    import time

    if not (
        cache
        and getattr(cache, "enabled", False)
        and getattr(cache, "client", None)
    ):
        yield 'event: error\ndata: {"error": "watch unavailable"}\n\n'
        return

    channel = thread_wake_key(flash_thread_id)
    pubsub = cache.client.pubsub()
    started_at = time.monotonic()
    try:
        await pubsub.subscribe(channel)
        while True:
            if time.monotonic() - started_at > WAKE_MAX_WATCH_DURATION:
                yield 'event: timeout\ndata: {}\n\n'
                break
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=WAKE_KEEPALIVE_INTERVAL
            )
            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                yield f'event: {WAKE_EVENT}\ndata: {data}\n\n'
            else:
                yield ': ping\n\n'
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


# ---------------------------------------------------------------------------
# READ-MODEL — the ``/status?fields=report_back`` slice
# ---------------------------------------------------------------------------


async def read_report_back_status(thread_id: str) -> dict:
    """Report-back-only status slice for a flash thread.

    The JSON shape is a frontend contract; the recent list is NEWEST FIRST
    (LPUSH order). On its own Redis-read failure ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching), distinct from an explicit
    ``False`` (drained).
    """
    pending_report_back: bool | None = False
    report_back_run_id = None
    recent_report_back_run_ids: list[str] = []
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            # Membership is the source of truth for "pending"; execution
            # progress lives in the durable outbox, not process memory.
            pipe = cache.client.pipeline(transaction=False)
            pipe.smembers(flash_watch_key(thread_id))
            pipe.lrange(flash_rb_done_key(thread_id), 0, _FLASH_RB_DONE_MAX - 1)
            members_raw, recent_raw = await pipe.execute()

            recent_report_back_run_ids = [_decode(r) for r in (recent_raw or [])]
            members = [_decode(m) for m in (members_raw or [])]
            if members:
                # A member without an origin is dead state and must not keep
                # this flash pending forever — under-cap flashes never hit the
                # reserve-path reaper, and successful reserves keep refreshing
                # the shared set's TTL. Filter them out of the derivation and
                # reap them best-effort (the Lua re-checks EXISTS per member,
                # so a racing reserve is never touched).
                origins_raw = await cache.client.mget(
                    [ptc_origin_key(m) for m in members]
                )
                orphans = [m for m, o in zip(members, origins_raw) if o is None]
                if orphans:
                    members = [m for m in members if m not in orphans]
                    try:
                        reaped = await _reap_listed_orphans(
                            cache, flash_watch_key(thread_id), orphans
                        )
                        if reaped:
                            logger.info(
                                f"[FLASH_REPORT_BACK] Status read reaped {reaped} "
                                f"orphaned member(s) for flash={thread_id}"
                            )
                    except Exception:
                        logger.warning(
                            f"Orphan reap during status read failed for {thread_id}",
                            exc_info=True,
                        )
            if members:
                pending_report_back = True
                # Resolve the run to attach to from any live per-(flash, ptc)
                # pointer (written when the report-back run is dispatched).
                # Never a finished run's id. One MGET vs N serial GETs; values
                # are raw serialized JSON.
                ptr_keys = [flash_rb_run_key(thread_id, ptc) for ptc in members]
                for raw in await cache.client.mget(ptr_keys):
                    if raw is None:
                        continue
                    try:
                        ptr = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(ptr, dict) and ptr.get("run_id"):
                        report_back_run_id = ptr["run_id"]
                        break
    except Exception:
        logger.warning(
            f"Report-back status read failed for {thread_id}; reporting unknown",
            exc_info=True,
        )
        pending_report_back = None
        report_back_run_id = None
        recent_report_back_run_ids = []

    return {
        "thread_id": thread_id,
        "pending_report_back": pending_report_back,
        "report_back_run_id": report_back_run_id,
        "recent_report_back_run_ids": recent_report_back_run_ids,
    }


async def execute_report_back(job: dict) -> None:
    """Execute one ``report_back`` outbox job: POST the summary turn, await terminal.

    Sole caller is the hook-outbox drainer — raising nacks the job (retry with
    backoff), returning acks it. Per-flash ordering is the outbox's ordering-key
    FIFO: this job stays open (lease-heartbeated, including through the POST's
    defer loop) until the summary run reaches terminal, so the next report-back
    on the same flash thread can't POST early. Crash-safe resume: the dispatched
    run id is merged into the job payload right after the POST, and the POST
    itself carries a job-deterministic ``request_key`` so a re-POST before that
    merge lands adopts the original run via 409 duplicate_request. Every
    destructive exit re-verifies the lease fence first — a stale owner must
    not tear down state the reclaiming owner is executing against.
    """
    from src.server.database import hook_outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS
    from src.utils.cache.redis_cache import get_cache_client

    payload = job.get("payload") or {}
    ptc_thread_id = payload["ptc_thread_id"]
    dispatch_gen = payload.get("dispatch_gen")
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    # One deterministic request identity per job: dedups the POST at the DB
    # layer AND scopes the run-pointer claim/re-assert to this job.
    job_request_key = str(uuid.uuid5(RB_REQUEST_NS, job_id))

    async def _fence() -> bool:
        """Heartbeat + ownership check in one: extends our lease iff we still
        hold this claim generation."""
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    cache = get_cache_client()
    # Strict read: raises on ANY unavailable state — blip, failed startup
    # connect, config-off — so the drainer nacks instead of acking a dropped
    # dispatch as "not report-back".
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if not origin or origin.get("origin") != "flash" or not origin.get("report_back"):
        return
    flash_thread_id = origin.get("flash_thread_id")
    user_id = origin.get("user_id")
    if not flash_thread_id or not user_id:
        return
    # Already terminal-cleared (duplicate completion event, or a reclaimed job
    # whose summary turn already finished and watch-cleared): nothing to do.
    if not await cache.client.sismember(flash_watch_key(flash_thread_id), ptc_thread_id):
        return
    # Claimed under a stale ordering key (pre-deploy row that finalized
    # unstamped, keyed on its own PTC thread): requeue onto the real flash
    # chain instead of executing here — N such jobs would busy-wait their
    # individual caps CONCURRENTLY against one flash thread's admission gate
    # and can drop summaries permanently, where correctly-keyed rows just
    # wait their turn at the chain head. The fenced ack no-ops afterwards.
    if job.get("ordering_key") != flash_thread_id:
        from src.server.services.hook_outbox import MAX_ATTEMPTS

        requeued = await outbox_db.requeue_job_with_key(
            job_id,
            attempts=attempts,
            ordering_key=flash_thread_id,
            max_attempts=MAX_ATTEMPTS,
        )
        logger.info(
            f"[FLASH_REPORT_BACK] Requeued job {job_id} for {ptc_thread_id} "
            f"onto flash chain {flash_thread_id} "
            f"(stale key {job.get('ordering_key')!r}, status={requeued})"
        )
        return

    rb_run_id = payload.get("dispatched_run_id")
    if rb_run_id:
        logger.info(
            f"[FLASH_REPORT_BACK] Resuming in-flight report-back run {rb_run_id} "
            f"for {ptc_thread_id} on flash thread {flash_thread_id} (no re-dispatch)"
        )
    else:
        outcome, rb_run_id = await _post_report_back(
            cache,
            flash_thread_id,
            ptc_thread_id,
            origin,
            request_key=job_request_key,
            heartbeat=_fence,
            dispatch_gen=dispatch_gen,
        )

        if outcome == "lost":
            # Lease lost inside the POST defer loop: the reclaiming owner is
            # (or will be) executing this job; do nothing further. The
            # drainer's fenced ack no-ops.
            return
        if outcome in ("deleted", "drop", "cap"):
            # Key-lock + row-lock fence held ACROSS the teardown (not just
            # checked before it): while held, no sibling claim on this
            # ordering key can be gated and this row can't be reclaimed — a
            # paused stale owner can't clear state a newer incarnation has
            # re-established.
            async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
                if not owned:
                    return  # reclaimed: the live owner decides the teardown
                if outcome == "deleted":
                    # Flash thread is gone (404). Nothing will consume these
                    # report-backs; clear every watch member.
                    await _discard_flash_thread(cache, flash_thread_id)
                else:
                    # Terminal rejection or exhausted defer-wait. Clear this
                    # member so the chain advances; otherwise the next
                    # report-back on this flash thread would wait behind a
                    # summary turn that never starts. Pointer-gated: a POST's
                    # server route can outlive the client's socket timeout and
                    # still sit pre-START inside a lawful admission hold — the
                    # chain lease serializes THIS executor, not that in-flight
                    # route (round-5 F1). Its live claim fences the teardown
                    # atomically (and a route that has not claimed yet is
                    # refused by the claim script's membership gate once the
                    # clear lands); on refusal we nack, and the retry adopts
                    # the claim's run row, takes over its corpse, or finds
                    # the claim released.
                    cleared = await clear_flash_report_back(
                        cache, ptc_thread_id, flash_thread_id, user_id=user_id,
                        expected_gen=dispatch_gen, refuse_if_pointer=True,
                    )
                    if not cleared and cleared.fencer_gen is None:
                        raise RuntimeError(
                            f"report-back drop for {ptc_thread_id} refused: a "
                            f"live run pointer on flash thread {flash_thread_id} "
                            f"may still be mid-admission; nacking to retry"
                        )
            return

        # outcome == "dispatched"
        if rb_run_id is None:
            # POSTed but the response body didn't yield a run id — we can't
            # await its terminal, and acking would release the chain before
            # this summary finishes. Nack: the retry re-POSTs the SAME
            # request_key and recovers the id via 409 duplicate_request (or
            # the admission run-pointer claim).
            raise RuntimeError(
                f"report-back for {ptc_thread_id} dispatched without a run_id; "
                f"nacking to recover it via request_key dedup"
            )

        # Durable resume pointer FIRST: after this merge lands, a crash-and-
        # reclaim resumes the terminal wait instead of re-POSTing. (Before it
        # lands, the request_key dedup makes the re-POST safe.) A merge failure
        # is therefore tolerable — log, don't nack, the effect already ran.
        try:
            await outbox_db.merge_job_payload(job_id, {"dispatched_run_id": rb_run_id})
        except Exception:
            logger.warning(
                f"[FLASH_REPORT_BACK] Failed persisting dispatched_run_id "
                f"{rb_run_id} on job {job_id}; request_key dedup covers a re-POST",
                exc_info=True,
            )

    # Both paths (fresh dispatch AND crash-resume) confirm the run pointer and
    # publish INSIDE one fence window. The pointer re-assert is belt-and-
    # suspenders for a degraded-cache admission and lets a reloading client
    # reattach via /status; atomically gated on membership + absent-or-same-run
    # (Lua) so a fast-terminal clear is never resurrected and a paused stale
    # owner can't repoint a re-dispatched pair at its dead run. The wake is
    # published only while the fence is held AND the pointer is confirmed
    # current — a stale resume must not wake clients toward a dead run.
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if not owned:
            return  # reclaimed: the live owner publishes and awaits
        pointer_ok = True
        try:
            pointer_ok = bool(
                await cache.client.eval(
                    _GATED_POINTER_SET_LUA,
                    2,
                    flash_watch_key(flash_thread_id),
                    flash_rb_run_key(flash_thread_id, ptc_thread_id),
                    ptc_thread_id,
                    rb_run_id,
                    _pointer_value(rb_run_id, dispatch_gen, job_request_key),
                    _FLASH_RB_RUN_TTL,
                    job_request_key,
                    dispatch_gen or "",
                )
            )
        except Exception:
            pass  # transient Redis failure: the wake is best-effort anyway
        if pointer_ok:
            await publish_wake(cache, flash_thread_id, run_id=rb_run_id)
        else:
            logger.warning(
                f"[FLASH_REPORT_BACK] Pointer for {ptc_thread_id} on flash "
                f"thread {flash_thread_id} refused run {rb_run_id}; not waking"
            )

    await _await_run_terminal(
        job_id, attempts, rb_run_id, flash_thread_id, ptc_thread_id, user_id,
        dispatch_gen,
    )


async def _await_run_terminal(
    job_id: str,
    attempts: int,
    rb_run_id: str,
    flash_thread_id: str,
    ptc_thread_id: str,
    user_id: str | None,
    dispatch_gen: str | None = None,
) -> None:
    """Hold the report-back job open until its summary run reaches terminal.

    Polls the durable run row (NOT watch membership — the member is removed by
    the summary run's own watch_clear job, which queues BEHIND this one on the
    same ordering key; waiting on it would deadlock until the cap). On timeout
    (never terminal, or the row vanished for good — thread deleted) it
    force-clears the pair so the chain and dispatch caps can't stay wedged;
    on a lost lease it stands down with NO teardown — the reclaiming owner
    resumes via ``dispatched_run_id``.
    """
    from src.server.database import hook_outbox as outbox_db
    from src.server.handlers.chat.notify_turn import await_run_terminal
    from src.utils.cache.redis_cache import get_cache_client

    outcome = await await_run_terminal(
        job_id,
        attempts,
        rb_run_id,
        wait_cap=_RB_TERMINAL_WAIT_CAP,
        log_prefix="[FLASH_REPORT_BACK]",
    )
    if outcome != "timeout":
        return
    # Row-lock fence held across the clear (see fenced_job_guard) — a
    # heartbeat alone can't cover a pause between check and mutation.
    logger.warning(
        f"[FLASH_REPORT_BACK] Terminal wait cap hit for {ptc_thread_id} "
        f"on flash thread {flash_thread_id}; clearing"
    )
    cache = get_cache_client()
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if owned:
            await clear_flash_report_back(
                cache, ptc_thread_id, flash_thread_id, user_id=user_id,
                expected_gen=dispatch_gen,
            )


async def _post_report_back(
    cache,
    flash_thread_id: str,
    ptc_thread_id: str,
    origin: dict,
    *,
    request_key: str | None = None,
    heartbeat=None,
    dispatch_gen: str | None = None,
) -> tuple[str, str | None]:
    """POST the synthetic report-back message to the flash thread.

    Builds the flash-specific body (summary prompt, watch-member identity,
    dispatch generation) and delegates the admission-aware defer loop to the
    shared ``post_notification_turn``. Returns its ``(outcome, run_id)``:
    ``"dispatched"`` / ``"drop"``/``"cap"`` (caller clears the member) /
    ``"deleted"`` (caller discards the watch) / ``"lost"`` (caller stops,
    no teardown).
    """
    from src.server.handlers.chat.notify_turn import post_notification_turn

    ws_label = origin.get("ptc_workspace_id") or "an auto-created workspace"
    message = (
        "<system>\n"
        f"The analysis you dispatched (thread {ptc_thread_id} in workspace "
        f"{ws_label}) has completed. Use agent_output to retrieve and "
        f"summarize the results for the user.\n"
        "</system>"
    )
    body = {
        "messages": [{"role": "user", "content": message}],
        "agent_mode": "flash",
        "workspace_id": origin.get("flash_workspace_id"),
        "query_type": "system",
        # Lets the report-back flash run identify which watch member to clear
        # on its own completion.
        "report_back_ptc_thread_id": ptc_thread_id,
        # The pair's dispatch generation rides into the summary run's START
        # metadata so its consumption watch_clear is fenced to THIS incarnation.
        "origin_dispatch_gen": dispatch_gen,
    }
    if request_key:
        body["request_key"] = request_key
    return await post_notification_turn(
        thread_id=flash_thread_id,
        body=body,
        user_id=origin.get("user_id"),
        wait_cap=_RB_BUSY_WAIT_CAP,
        heartbeat=heartbeat,
        log_prefix="[FLASH_REPORT_BACK]",
        subject=f"PTC thread {ptc_thread_id}",
    )


async def _discard_flash_thread(cache, flash_thread_id: str) -> None:
    """Flash thread deleted (404): disposition every snapshotted watch member.

    Raises on any failure so the drainer nacks and retries — a swallowed
    member clear here would strand that member's origin/pointer/cap state
    forever; that is also why the origin reads are STRICT — a transient
    read degrading to a gen-less clear would refuse a generated origin.
    Each member is cleared against the generation its origin holds RIGHT
    NOW (observed-gen CAS); a member whose origin moved to a different
    flash thread loses only OUR stale watch reference — its live dispatch
    owns the rest. There is deliberately NO final DEL of the watch set: a
    member SADDed by a concurrent reserve after our snapshot must survive
    (its completion would otherwise ack as a non-member and drop the
    summary). Every snapshotted member is removed individually — the
    clear's teardown Lua SREMs it, the cross-flash branches SREM it, and
    an origin that vanished mid-walk means a rival teardown already did —
    and Redis drops an empty set automatically; any residue is an orphan
    the reapers collect.
    """
    watch_key = flash_watch_key(flash_thread_id)
    members = await cache.client.smembers(watch_key)
    for member in members or []:
        ptc_tid = _decode(member)
        origin = await cache.get_strict(ptc_origin_key(ptc_tid))
        if isinstance(origin, dict) and origin.get("flash_thread_id") not in (
            None,
            flash_thread_id,
        ):
            await cache.client.srem(watch_key, ptc_tid)
            continue
        observed_gen = (
            origin.get("dispatch_gen") if isinstance(origin, dict) else None
        )
        # No drained-run record: the flash thread is gone, so nothing can
        # ever render these turns.
        cleared = await clear_flash_report_back(
            cache,
            ptc_tid,
            flash_thread_id,
            record_drained=False,
            expected_gen=observed_gen,
        )
        if not cleared:
            # The origin's generation moved between our read and the CAS.
            # Re-read: moved cross-flash -> drop only our reference; gone ->
            # already torn down; still ours -> retry the whole job rather
            # than delete the watch set out from under live state.
            fresh = await cache.get_strict(ptc_origin_key(ptc_tid))
            if isinstance(fresh, dict) and fresh.get("flash_thread_id") not in (
                None,
                flash_thread_id,
            ):
                await cache.client.srem(watch_key, ptc_tid)
                continue
            if fresh is not None:
                raise RuntimeError(
                    f"discard of flash thread {flash_thread_id}: clear of "
                    f"member {ptc_tid} refused (generation moved); nacking"
                )

