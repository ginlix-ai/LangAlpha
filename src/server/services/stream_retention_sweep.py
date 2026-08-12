"""Background sweep putting a TTL on event streams that never got one.

Scope is deliberately narrow — root ``workflow:stream:{tid}:{run_id}`` and v2
``subagent:stream:{tid}:{task_run_id}`` only. Both insert their ledger row
BEFORE the run's first XADD, which is what makes "no row ⇒ garbage" sound. v1
task-keyed subagent streams are EXCLUDED: they can legitimately have no ledger
row at all (CLI and other no-ledger admits), and their key is reusable across
a resume's reset-and-refill, so a stale terminal reading could stamp a stream
that has since been reborn.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from typing import Optional

from src.utils.concurrency import cancel_and_join

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 600.0
SCAN_BATCH = 500
KEYS_PER_CYCLE = 5000
# Round-trip ceiling per cycle, independent of how many keys MATCH. At
# SCAN_BATCH this examines ~100k keys — far past any real stream population —
# and the cursor persists, so a bigger keyspace just takes more cycles.
_MAX_SCAN_ROUNDS = 200

# One worker sweeps per cycle. The lease outlives a cycle so a crashed sweeper
# doesn't hand the keyspace to a second worker mid-pass.
_RELEASE_LEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

_LEASE_KEY = "sweep:stream_retention:lease"
_LEASE_TTL_S = 600
# Beside the lease, not in process memory: the next cycle is very likely a
# different worker, and a cursor that resets every cycle would re-scan the
# head of the keyspace forever and never reach the tail.
_CURSOR_KEY = "sweep:stream_retention:cursor"
_CURSOR_TTL_S = 86400

# Keyspace-driven with the ledger as oracle, rather than ledger-driven: the run
# tables carry no "finalized_at", so there is no way to enumerate the runs whose
# streams should have been stamped. Scanning the keyspace and asking the ledger
# about what it finds inverts that cleanly.
#
# One pattern, so one cursor is enough: a SCAN cursor is a position in the
# keyspace, not in a filtered view, and juggling several against one cursor
# means no pattern ever completes a pass. ``_stamp_batch`` does the precise
# root-vs-v2-vs-v1 discrimination.
_STREAM_PATTERN = "*:stream:*"
_ROOT_PREFIX = "workflow:stream:"
_SUBAGENT_PREFIX = "subagent:stream:"

# LEGACY_META_COMPAT — written by workers on the previous release; nothing
# reads them. Swept for one release so a rolling deploy's leftovers expire.
#
# Removal checklist, one release after this ships (grep LEGACY_META_COMPAT):
#   1. ``stream_writer.stream_meta_key`` + its EXPIRE in ``append_run_end_event``
#   2. ``redis_stream.task_meta_key`` + its EXPIRE in ``stamp_task_retention``
#   3. ``_LEGACY_META_PATTERNS``, ``_sweep_legacy_meta`` and the ``legacy`` tally
_LEGACY_META_PATTERNS = ("workflow:events:meta:*", "subagent:events:meta:*")

STOP_GRACE = 30.0


def _canonical_uuid(value: str) -> bool:
    """True only for a canonically-formatted UUID.

    This is the v1/v2 discriminator: v2 keys end in a ``task_run_id`` (UUID),
    v1 keys in a ``token_urlsafe`` task id. Strict equality against the
    round-tripped form, because ``UUID()`` also accepts braced, urn-prefixed
    and undashed spellings that no key builder produces.
    """
    try:
        # Not ``value.lower()``: an uppercase spelling round-trips here but
        # then misses the ledger lookup, which is keyed by the exact string
        # scanned while Postgres answers in lowercase — a live run read as an
        # orphan. No key builder emits one, so reject the shape outright.
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError, TypeError):
        return False


def _decode(value) -> str:
    return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value


class StreamRetentionSweeper:
    _instance: Optional["StreamRetentionSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "StreamRetentionSweeper":
        if cls._instance is None:
            cls._instance = StreamRetentionSweeper()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="stream-retention-sweeper"
        )
        logger.info(
            f"[StreamRetentionSweeper] started (interval={self._interval:.0f}s)"
        )

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning(
                "[StreamRetentionSweeper] sweep exceeded stop grace; cancelling"
            )
            await cancel_and_join(self._loop_task)
        except Exception:
            logger.warning("[StreamRetentionSweeper] stop failed", exc_info=True)
        self._loop_task = None

    async def _loop(self) -> None:
        # Stagger workers so they don't all contend for the lease on the same
        # tick after a simultaneous restart.
        await self._sleep(random.uniform(0, min(30.0, self._interval)))
        while not self._stop_event.is_set():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.warning("[StreamRetentionSweeper] cycle failed", exc_info=True)
            await self._sleep(self._interval)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except (TimeoutError, asyncio.TimeoutError):
            pass

    # ---------------------------------------------------------------- sweep

    async def sweep_once(self) -> dict:
        """One leased pass over a slice of the keyspace. Returns a tally."""
        from src.config.settings import get_redis_ttl_workflow_events
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or cache.client is None:
            return {}
        client = cache.client

        holder = uuid.uuid4().hex
        if not await client.set(_LEASE_KEY, holder, nx=True, ex=_LEASE_TTL_S):
            return {}

        ttl = get_redis_ttl_workflow_events()
        tally = {"scanned": 0, "stamped": 0, "skipped_v1": 0, "legacy": 0}
        try:
            cursor = int(_decode(await client.get(_CURSOR_KEY)) or 0)
            cursor = await self._sweep_streams(client, cursor, ttl, tally)
            await client.set(_CURSOR_KEY, str(cursor), ex=_CURSOR_TTL_S)
            tally["legacy"] = await self._sweep_legacy_meta(client, ttl)
        except Exception:
            logger.warning("[StreamRetentionSweeper] pass failed", exc_info=True)
        finally:
            # Only release a lease we still hold: a pass that overran its TTL
            # has already handed the keyspace to another worker. Compare and
            # delete in one script — a GET then DELETE can expire in between
            # and drop the NEXT owner's lease, putting two sweepers on the
            # shared cursor at once.
            try:
                await client.eval(_RELEASE_LEASE_LUA, 1, _LEASE_KEY, holder)
            except Exception:
                pass

        if tally["stamped"] or tally["legacy"]:
            logger.info(
                "[StreamRetentionSweeper] scanned=%d stamped=%d skipped_v1=%d "
                "legacy=%d",
                tally["scanned"],
                tally["stamped"],
                tally["skipped_v1"],
                tally["legacy"],
            )
        return tally

    async def _sweep_streams(
        self, client, cursor: int, ttl: int, tally: dict
    ) -> int:
        """SCAN a slice of the stream keyspace, stamping the orphans."""
        seen = 0
        rounds = 0
        while seen < KEYS_PER_CYCLE and not self._stop_event.is_set():
            # MATCH filters server-side, so a batch can come back empty while
            # the cursor still advances. Bounding only on matched keys lets one
            # cycle walk a whole large keyspace looking for a handful of
            # streams — cap the round trips too.
            if rounds >= _MAX_SCAN_ROUNDS:
                break
            rounds += 1
            cursor, keys = await client.scan(
                cursor=cursor, match=_STREAM_PATTERN, count=SCAN_BATCH
            )
            seen += len(keys)
            tally["scanned"] += len(keys)
            await self._stamp_batch(client, keys, ttl, tally)
            if cursor == 0:
                break
        return cursor

    async def _stamp_batch(self, client, keys, ttl: int, tally: dict) -> None:
        root_by_run: dict[str, str] = {}
        v2_by_run: dict[str, str] = {}
        for raw in keys:
            key = _decode(raw)
            parts = key.split(":")
            if len(parts) != 4:
                continue
            tail = parts[3]
            # Both branches gate on the id shape the ledger can actually be
            # asked about. An unparseable id is silently dropped by the oracle,
            # and absence is what this sweep reads as garbage — so an unknown
            # shape must never reach it.
            if key.startswith(_ROOT_PREFIX):
                if _canonical_uuid(tail):
                    root_by_run[tail] = key
            elif key.startswith(_SUBAGENT_PREFIX):
                if _canonical_uuid(tail):
                    v2_by_run[tail] = key
                else:
                    # v1 task-keyed stream — out of scope, see module docstring.
                    tally["skipped_v1"] += 1

        expiring: list[str] = []
        if root_by_run:
            from src.server.database.runs import lifecycle as tl_db

            expiring += await self._orphans(
                root_by_run, tl_db.get_run_statuses, "root"
            )
        if v2_by_run:
            from src.server.database.runs import subagent_runs as sr_db

            expiring += await self._orphans(
                v2_by_run, sr_db.get_task_run_statuses, "v2"
            )
        if not expiring:
            return

        async with client.pipeline(transaction=False) as pipe:
            for key in expiring:
                # NX: never lengthen a retention window a collector already
                # stamped. This sweep is a floor, not an authority.
                pipe.expire(key, ttl, nx=True)
            results = await pipe.execute()
        tally["stamped"] += sum(1 for r in results if r)

    async def _orphans(self, by_run: dict[str, str], oracle, label: str) -> list[str]:
        """Keys whose run is terminal or gone. Empty on a failed oracle read."""
        try:
            statuses = await oracle(list(by_run))
        except Exception:
            # Never read a failed query as "every row is missing" — that would
            # expire the streams of every run currently in flight.
            logger.warning(
                "[StreamRetentionSweeper] %s ledger read failed; skipping batch",
                label,
                exc_info=True,
            )
            return []
        # Terminal-or-gone is the whole test. Active streams carry no TTL by
        # design — retention is stamped at the run's terminal, so a live
        # consumer can never have its backlog expire underneath it. A run that
        # dies before that terminal (crash, OOM, kill -9) leaves its stream
        # resident forever, and on a long-lived deployment those orphans grew to
        # dominate the keyspace.
        return [
            key
            for run_id, key in by_run.items()
            if statuses.get(run_id, "") != "in_progress"
        ]

    async def _sweep_legacy_meta(self, client, ttl: int) -> int:
        """Expire the write-only meta hashes a previous release still writes.

        Cursor per pattern, persisted for the same reason the stream sweep
        persists its own: restarting at 0 every cycle re-walks the head of the
        keyspace and never reaches the tail.
        """
        stamped = 0
        for pattern in _LEGACY_META_PATTERNS:
            cursor_key = f"{_CURSOR_KEY}:legacy:{pattern}"
            cursor = int(_decode(await client.get(cursor_key)) or 0)
            seen = 0
            rounds = 0
            while seen < KEYS_PER_CYCLE and not self._stop_event.is_set():
                if rounds >= _MAX_SCAN_ROUNDS:
                    break
                rounds += 1
                cursor, keys = await client.scan(
                    cursor=cursor, match=pattern, count=SCAN_BATCH
                )
                seen += len(keys)
                if keys:
                    async with client.pipeline(transaction=False) as pipe:
                        for key in keys:
                            pipe.expire(key, ttl, nx=True)
                        stamped += sum(1 for r in await pipe.execute() if r)
                if cursor == 0:
                    break
            await client.set(cursor_key, str(cursor), ex=_CURSOR_TTL_S)
        return stamped
