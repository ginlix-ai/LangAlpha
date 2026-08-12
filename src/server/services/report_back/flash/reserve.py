"""Report-back dispatch reservation: caps, origin lineage, admission gate.

``reserve()`` is the atomic authority for taking a dispatch slot (cap check +
watch/user membership + origin write in one Lua script) and rolling it back;
``admit_dispatch_gen``/``retract_dispatch_gen`` fence the pre-START window;
``resolve_orphaned_watch`` and the reapers settle pairs whose delivery can no
longer arrive. All mutation of the reservation key namespace lives here.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid

from src.server.services.report_back.flash import pointer
from src.server.services.report_back.flash.keys import (
    FLASH_RB_DONE_MAX,
    FLASH_RB_DONE_TTL,
    PTC_ORIGIN_TTL,
    decode,
    flash_rb_done_key,
    flash_rb_run_key,
    flash_user_pending_key,
    flash_watch_key,
    ptc_origin_key,
    ptc_rb_resolved_key,
    ptc_teardown_tombstone_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


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
RESERVE_LUA = """
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
ROLLBACK_RESERVE_LUA = """
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

# Orphan-membership reap: SREM a member iff its ptc_origin key no longer
# exists, atomically per member (a rival reserve SADDs the member and SETs
# the origin in one script, so exists-then-srem here can never remove a
# freshly reserved member). Needed because the watch/user SETs are shared
# keys whose TTL every reserve refreshes — a member stranded by a lost
# reserve reply (fail-closed, no rollback) would otherwise outlive its
# origin's TTL indefinitely under active use, permanently eating cap slots.
# KEYS: 1=set 2..n+1=the members' origin keys  ARGV: 1..n=members
REAP_ORPHANS_LUA = """
local removed = 0
for i, member in ipairs(ARGV) do
  if redis.call('exists', KEYS[i + 1]) == 0 then
    removed = removed + redis.call('srem', KEYS[1], member)
  end
end
return removed
"""

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
RESOLVED_RECEIPT_TTL = 3600

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
# written because ``RESERVE_LUA`` derives displaced-owner lineage from it;
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
ADMISSION_GATE_LUA = """
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
ADMISSION_RETRACT_LUA = """
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
            ADMISSION_GATE_LUA,
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
            ADMISSION_RETRACT_LUA,
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
ORPHAN_RESOLVE_LUA = """
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
    from src.server.database.runs import lifecycle as tl_db

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
        ORPHAN_RESOLVE_LUA,
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
        RESOLVED_RECEIPT_TTL,
        FLASH_RB_DONE_MAX,
        FLASH_RB_DONE_TTL,
        job_gen or "",
    )
    if not (isinstance(res, (list, tuple)) and res and int(res[0])):
        reason = (
            decode(res[1])
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
            ptr = json.loads(decode(ptr_raw))
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
    from src.server.database.runs import lifecycle as tl_db

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


async def reap_listed_orphans(cache, set_key: str, members: list[str]) -> int:
    """SREM the given members from ``set_key`` iff their origin is absent AT
    SCRIPT TIME — the per-member EXISTS inside the Lua is the guard, so a
    reserve landing after the caller's stale read is never touched."""
    if not members:
        return 0
    return int(
        await cache.client.eval(
            REAP_ORPHANS_LUA,
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
        members = [decode(m) for m in (await cache.client.smembers(set_key)) or []]
        removed += await reap_listed_orphans(cache, set_key, members)
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
                ROLLBACK_RESERVE_LUA,
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
                ptr_raw = decode(res[1]) if len(res) > 1 else ""
                run_id = None
                if ptr_raw:
                    try:
                        ptr = json.loads(ptr_raw)
                        if isinstance(ptr, dict):
                            run_id = ptr.get("run_id")
                    except (TypeError, ValueError):
                        run_id = None
                if run_id:
                    await pointer.record_drained_run(
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
                        RESERVE_LUA,
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
                status = decode(res[0])
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
            slot._prev_origin_raw = decode(res[3]) or None
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
