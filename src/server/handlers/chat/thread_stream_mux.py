"""Multiplexed per-thread SSE stream: task channels + watch on one socket.

Replaces the per-task GETs + the watch socket with a single connection so the
browser's per-host HTTP/1.1 budget stays constant in task count. The main run
stream is deliberately NOT carried here (v1): the foreground POST owns it, and
muxing it would double-consume the same Redis stream.

Wire contract (M0, scratchpad/mux_build_plan.md):
- Channels: ``task:<task_id>`` and ``watch``.
- Task-frame SSE id line: ``id: <chan>@<epoch>#<entry_id>#<logical>`` where
  ``epoch`` is the writer round's ``spawned_run_id`` (resume mints a new one
  and deletes the stream — an epoch mismatch means a client cursor predates
  the current stream incarnation and must be discarded), ``entry_id`` is the
  opaque Redis entry ID (the transport cursor — valid for auto-ID entries
  that carry no integer seq), and ``logical`` is the producer's original
  ``id: N`` value (``-`` when absent) for UI ordering.
- Control frames (never advance cursors): ``chan_open``, ``chan_close``,
  ``stream_gap``, ``transport_error``, ``timeout``.
- Tri-state liveness: a probe answering "unknown" never closes a channel;
  only an XREAD-level Redis failure ends the socket, as a retryable
  ``transport_error`` with client cursors intact. ``chan_close terminal``
  requires positive evidence (sentinel, or settled probe after quiescence).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import AsyncGenerator, Optional

from fastapi import HTTPException

from ptc_agent.agent.middleware.background_subagent.registry import (
    spawn_nudge_channel,
)
from src.server.services.background_registry_store import BackgroundRegistryStore
from src.utils.cache.redis_cache import get_cache_client

from ._common import logger
from .report_back import watch_wakes
from .stream_from_log import (
    _PAYLOAD_RECORD,
    _PAYLOAD_SENTINEL,
    _classify_subagent_payload,
    _record_to_sse,
    _xread_block_ms,
)

WATCH_CHANNEL = "watch"
_TASK_CHAN_RE = re.compile(r"^task:([A-Za-z0-9_-]{1,12})$")
_ENTRY_ID_RE = re.compile(r"^\d{1,15}-\d{1,10}$")
_EPOCH_RE = re.compile(r"^[A-Za-z0-9-]{1,40}$")

_MAX_CHANNELS = 32
_MAX_CURSORS_LEN = 4096
# Per-channel entries per XREAD round. XREAD's COUNT applies per stream, so
# this IS the fairness bound: a flooding task hands the loop at most this
# many frames before other channels get their turn.
_XREAD_COUNT = 32
# Empty XREAD rounds on a channel before its (rate-limited) settled probe may
# close it. Two rounds mirrors the single-stream handshake's dwell.
_QUIESCE_EMPTY_ROUNDS = 2
_LIVENESS_PROBE_MIN_INTERVAL_S = 5.0
_RESCAN_INTERVAL_S = 30.0
_KEEPALIVE_S = 25.0
_MAX_DURATION_S = 30 * 60
_OUT_QUEUE_MAX = 256

_NO_EPOCH = "-"


def parse_mux_cursors(raw: Optional[str]) -> dict[str, tuple[str, str]]:
    """Parse ``?cursors=task:X@<epoch>#<entry_id>,…`` → {chan: (epoch, entry_id)}.

    Rejects (400): oversize input, bad grammar, duplicate channels, cursors
    for ``watch`` (pub/sub — nothing to resume), more than ``_MAX_CHANNELS``.
    """
    if not raw:
        return {}
    if len(raw) > _MAX_CURSORS_LEN:
        raise HTTPException(status_code=400, detail="cursors too large")
    out: dict[str, tuple[str, str]] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        chan, sep, tail = part.partition("@")
        epoch, sep2, entry_id = tail.partition("#")
        if not sep or not sep2:
            raise HTTPException(
                status_code=400, detail=f"malformed cursor: {part[:80]!r}"
            )
        if chan == WATCH_CHANNEL:
            raise HTTPException(
                status_code=400, detail="watch channel takes no cursor"
            )
        if not _TASK_CHAN_RE.match(chan):
            raise HTTPException(
                status_code=400, detail=f"unknown channel: {chan[:80]!r}"
            )
        if not _EPOCH_RE.match(epoch) or not _ENTRY_ID_RE.match(entry_id):
            raise HTTPException(
                status_code=400, detail=f"malformed cursor: {part[:80]!r}"
            )
        if chan in out:
            raise HTTPException(
                status_code=400, detail=f"duplicate channel: {chan}"
            )
        if len(out) >= _MAX_CHANNELS:
            raise HTTPException(status_code=400, detail="too many channels")
        out[chan] = (epoch, entry_id)
    return out


@dataclass
class _Chan:
    task_id: str
    epoch: str
    stream_key: bytes
    cursor: bytes
    empty_rounds: int = 0
    last_probe_at: float = field(default=0.0)
    # Terminal closes MARK the channel instead of popping it: the identity
    # must survive in the map so a queued frame from an OLDER generation
    # keeps failing the dequeue identity check even after this generation
    # settles (ABA). A resume rotation overwrites the entry wholesale.
    closed: bool = False

    @property
    def name(self) -> str:
        return f"task:{self.task_id}"


def _control(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _chan_close(chan: str, reason: str) -> str:
    return _control("chan_close", {"chan": chan, "reason": reason})


def _mux_frame(chan: str, epoch: str, entry_id: str, payload: str) -> str:
    """Stamp a rendered SSE payload with the composite transport id.

    The producer's own ``id: N`` first line (when present) is demoted into
    the composite as the logical id; seq-less payloads get ``-``.
    """
    first, nl, rest = payload.partition("\n")
    if first.startswith("id: "):
        logical = first[4:].strip() or _NO_EPOCH
        body = rest
    else:
        logical = _NO_EPOCH
        body = payload
    return f"id: {chan}@{epoch}#{entry_id}#{logical}\n{body}"


async def _read_task_meta_tristate(
    thread_id: str, task_id: str
) -> tuple[str, Optional[dict]]:
    """('ok', meta) | ('absent', None) | ('unavailable', None).

    ``read_task_meta`` collapses outage and absence into None — callers that
    must not treat a Redis outage as "settled" (the whole point of tri-state
    liveness) need the distinction, so this reads the hash directly.
    """
    try:
        cache = get_cache_client()
        if not getattr(cache, "enabled", False) or not cache.client:
            return "unavailable", None
        raw = await cache.client.hgetall(f"subagent:meta:{thread_id}:{task_id}")
    except Exception:
        return "unavailable", None
    if not raw:
        return "absent", None
    meta = {
        (k.decode() if isinstance(k, bytes) else str(k)): (
            v.decode() if isinstance(v, bytes) else str(v)
        )
        for k, v in raw.items()
    }
    return "ok", meta


async def _task_liveness(thread_id: str, task_id: str) -> str:
    """'live' | 'settled' | 'unknown' — lock-first, then meta, never guessing.

    Mirrors ``_subagent_writer_settled`` but keeps the third state: an
    unavailable lock probe + unreadable meta is 'unknown', not 'settled'.
    """
    registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)
    if registry is not None:
        task = await registry.get_task_by_task_id(task_id)
        if task is not None:
            ato = task.asyncio_task
            if ato is not None and not ato.done():
                return "live"
    try:
        from src.server.services.writer_guard import held_task_namespaces

        held = await held_task_namespaces(thread_id, [task_id])
    except Exception:
        held = None
    if held is not None:
        return "live" if task_id in held else "settled"
    status, meta = await _read_task_meta_tristate(thread_id, task_id)
    if status == "unavailable":
        return "unknown"
    if meta is None:
        return "settled"
    return "live" if meta.get("status") == "running" else "settled"


async def _discover_tasks(thread_id: str) -> dict[str, str]:
    """{task_id: epoch} for every currently-running task, local + cross-worker."""
    out: dict[str, str] = {}
    registry = await BackgroundRegistryStore.get_instance().get_registry(thread_id)
    if registry is not None:
        for task in await registry.get_all_tasks():
            ato = task.asyncio_task
            if ato is not None and not ato.done():
                out[task.task_id] = task.spawned_run_id or _NO_EPOCH
    try:
        cache = get_cache_client()
        if getattr(cache, "enabled", False) and cache.client:
            members = await cache.client.smembers(f"subagent:active:{thread_id}")
            for member in members or ():
                task_id = member.decode() if isinstance(member, bytes) else str(member)
                if task_id in out:
                    continue
                status, meta = await _read_task_meta_tristate(thread_id, task_id)
                if status == "ok" and meta is not None:
                    if meta.get("status") == "running":
                        out[task_id] = meta.get("spawned_run_id") or _NO_EPOCH
                elif status == "absent":
                    continue
    except Exception:
        logger.warning(
            "[mux] active-set discovery failed for %s", thread_id, exc_info=True
        )
    return out


async def _attach_gap_probe(cache, chan: _Chan) -> Optional[str]:
    """stream_gap frame when the resume cursor's successor was trimmed away.

    Only meaningful for explicit ``seq-0`` cursors (auto-ID cursors carry no
    logical position to compare). Mirrors the single-stream trimmed-head
    probe; the mux variant just scopes the frame to a channel.
    """
    major_s, _, minor_s = chan.cursor.decode().partition("-")
    try:
        major = int(major_s)
    except ValueError:
        return None
    if minor_s != "0" or major >= 10_000_000_000:  # auto-ID (ms) cursor
        return None
    try:
        head = await cache.client.xrange(chan.stream_key, count=1)
    except Exception:
        return None
    if not head:
        return None
    payload = head[0][1].get(b"event")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    if not payload or not payload.startswith("id: "):
        return None
    first_line, _, _ = payload.partition("\n")
    try:
        first_seq = int(first_line[4:].strip())
    except ValueError:
        return None
    if first_seq > major + 1:
        return _control(
            "stream_gap",
            {
                "chan": chan.name,
                "expected_from": major + 1,
                "first_available": first_seq,
            },
        )
    return None


async def stream_thread_mux(
    thread_id: str, cursor_map: dict[str, tuple[str, str]]
) -> AsyncGenerator[str, None]:
    """The mux generator: attach, then pump task streams + watch into one SSE."""
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        yield _control("transport_error", {"retryable": True})
        return

    out_q: asyncio.Queue = asyncio.Queue(maxsize=_OUT_QUEUE_MAX)
    channels: dict[str, _Chan] = {}
    new_chan_evt = asyncio.Event()
    attach_frames: list[str] = []

    def _open_channel(task_id: str, epoch: str, cursor: bytes) -> _Chan:
        chan = _Chan(
            task_id=task_id,
            epoch=epoch,
            stream_key=f"subagent:stream:{thread_id}:{task_id}".encode(),
            cursor=cursor,
        )
        channels[task_id] = chan
        new_chan_evt.set()
        return chan

    # ---- attach: discovery + per-channel cursor classification ------------
    running = await _discover_tasks(thread_id)
    for chan_name, (epoch, entry_id) in cursor_map.items():
        task_id = chan_name[5:]
        if task_id in running:
            continue
        liveness = await _task_liveness(thread_id, task_id)
        if liveness == "unknown":
            yield _control("transport_error", {"retryable": True})
            return
        if liveness == "live":
            # Lock held but not yet in meta/active-set (spawn window):
            # epoch unknown — replay from 0 below by leaving it out of
            # ``running`` with a placeholder epoch.
            running[task_id] = _NO_EPOCH
            continue
        # Settled: open for drain — the retained stream ends in the
        # producer's sentinel, which closes the channel after the tail is
        # delivered. An expired stream closes on the first settled probe.
        status, meta = await _read_task_meta_tristate(thread_id, task_id)
        current_epoch = (meta or {}).get("spawned_run_id") or _NO_EPOCH
        cursor = entry_id.encode() if current_epoch == epoch else b"0"
        chan = _open_channel(task_id, current_epoch, cursor)
        attach_frames.append(
            _control(
                "chan_open",
                {"chan": chan.name, "epoch": chan.epoch, "mode": "drain"},
            )
        )

    for task_id, epoch in running.items():
        chan_name = f"task:{task_id}"
        client = cursor_map.get(chan_name)
        if client is not None and client[0] == epoch and epoch != _NO_EPOCH:
            cursor, mode = client[1].encode(), "resume"
        else:
            cursor, mode = b"0", "replay"
        chan = _open_channel(task_id, epoch, cursor)
        attach_frames.append(
            _control(
                "chan_open", {"chan": chan.name, "epoch": epoch, "mode": mode}
            )
        )
        if mode == "resume":
            gap = await _attach_gap_probe(cache, chan)
            if gap is not None:
                attach_frames.append(gap)

    for frame in attach_frames:
        yield frame

    # ---- pumps ------------------------------------------------------------
    block_ms = _xread_block_ms()

    async def _close_channel(chan: _Chan, reason: str) -> None:
        # Identity-guarded: a resume rotates channels[task_id] to a NEW _Chan
        # on the SAME stream key, so a stale chan (from an older pump
        # snapshot) must neither evict its successor nor emit a close for a
        # channel that is no longer this incarnation.
        if channels.get(chan.task_id) is not chan or chan.closed:
            return
        chan.closed = True
        # Tagged like content frames: a close whose put suspended across a
        # rotation must not overtake-and-kill the successor client-side —
        # the dequeue filter drops it unless THIS chan still owns the slot.
        await out_q.put(("task_frame", (chan, _chan_close(chan.name, reason))))

    async def _task_pump() -> None:
        while True:
            # Closed entries stay in the map for generation identity only —
            # they must not be read (a settled stream would spin the pump).
            snapshot = [c for c in channels.values() if not c.closed]
            if not snapshot:
                new_chan_evt.clear()
                try:
                    await asyncio.wait_for(new_chan_evt.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue
            streams = {c.stream_key: c.cursor for c in snapshot}
            try:
                result = await asyncio.wait_for(
                    cache.client.xread(
                        streams, block=block_ms, count=_XREAD_COUNT
                    ),
                    timeout=(block_ms / 1000.0) + 2.0,
                )
            except asyncio.TimeoutError:
                continue  # wedged read — loop; socket keepalive covers liveness
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception:
                logger.warning(
                    "[mux] XREAD failed for %s", thread_id, exc_info=True
                )
                await out_q.put(("transport", None))
                return

            got: set[str] = set()
            for stream_name, entries in result or ():
                chan = next(
                    (c for c in snapshot if c.stream_key == stream_name), None
                )
                # `is` check, not membership: _admit rotates in a NEW _Chan
                # for the same task on resume — a stale snapshot chan must
                # not emit the successor's entries under the old epoch (or
                # advance a cursor nobody reads).
                if chan is None or channels.get(chan.task_id) is not chan:
                    continue
                got.add(chan.task_id)
                chan.empty_rounds = 0
                closed = False
                for entry_id, fields in entries:
                    chan.cursor = entry_id
                    payload = fields.get(b"event")
                    if payload is None:
                        continue
                    if isinstance(payload, bytes):
                        try:
                            payload = payload.decode("utf-8")
                        except UnicodeDecodeError:
                            continue
                    kind, parsed = _classify_subagent_payload(payload)
                    if kind == _PAYLOAD_SENTINEL:
                        await _close_channel(chan, "terminal")
                        closed = True
                        break
                    if kind == _PAYLOAD_RECORD and parsed is not None:
                        payload = _record_to_sse(parsed, thread_id, chan.task_id)
                    entry_id_s = (
                        entry_id.decode()
                        if isinstance(entry_id, bytes)
                        else str(entry_id)
                    )
                    # Revalidate per entry: a put on a full out_q suspends
                    # mid-batch and a resume can rotate the channel at that
                    # await. This pre-put check is only a fast-path bail —
                    # asyncio.Queue does NOT order a woken putter ahead of a
                    # fresh put, so a suspended put can still land after the
                    # successor's chan_open. The dequeue-side identity filter
                    # in the drain loop is the authoritative guard.
                    if channels.get(chan.task_id) is not chan:
                        break
                    await out_q.put(
                        (
                            "task_frame",
                            (
                                chan,
                                _mux_frame(
                                    chan.name, chan.epoch, entry_id_s, payload
                                ),
                            ),
                        )
                    )
                if closed:
                    continue

            # Sentinel-less terminal fallback, per channel: only channels
            # that stayed empty this round, only after two empty rounds,
            # probes rate-limited, and 'unknown' NEVER closes.
            now = time.monotonic()
            for chan in snapshot:
                if chan.task_id in got or channels.get(chan.task_id) is not chan:
                    continue
                chan.empty_rounds += 1
                if chan.empty_rounds < _QUIESCE_EMPTY_ROUNDS:
                    continue
                if now - chan.last_probe_at < _LIVENESS_PROBE_MIN_INTERVAL_S:
                    continue
                chan.last_probe_at = now
                if await _task_liveness(thread_id, chan.task_id) == "settled":
                    await _close_channel(chan, "terminal")

    async def _watch_pump() -> None:
        backoff = 1.0
        while True:
            gen = watch_wakes(cache, thread_id)
            try:
                async for frame in gen:
                    backoff = 1.0
                    if frame.startswith(": ping"):
                        continue  # mux emits its own keepalive
                    if frame.startswith("event: timeout"):
                        await out_q.put(("timeout", None))
                        return
                    await out_q.put(("frame", frame))
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception:
                logger.warning(
                    "[mux] watch pump failed for %s", thread_id, exc_info=True
                )
            finally:
                await gen.aclose()
            # Closed without its 30-min timeout (unconfirmed subscribe or
            # transient error): paced resubscribe, like the standalone
            # watch client would.
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    async def _discovery_pump() -> None:
        pubsub = cache.client.pubsub()
        try:
            await pubsub.subscribe(spawn_nudge_channel(thread_id))
            last_scan = time.monotonic()
            while True:
                try:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=5.0
                    )
                except (asyncio.CancelledError, GeneratorExit):
                    raise
                except Exception:
                    await asyncio.sleep(2.0)
                    msg = None
                if msg and msg.get("type") == "message":
                    data = msg.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8", errors="replace")
                    try:
                        nudge = json.loads(data)
                        task_id = str(nudge.get("task_id") or "")
                        epoch = str(nudge.get("epoch") or _NO_EPOCH)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    await _admit(task_id, epoch)
                if time.monotonic() - last_scan >= _RESCAN_INTERVAL_S:
                    last_scan = time.monotonic()
                    for task_id, epoch in (
                        await _discover_tasks(thread_id)
                    ).items():
                        await _admit(task_id, epoch)
        finally:
            try:
                await pubsub.unsubscribe(spawn_nudge_channel(thread_id))
                await pubsub.aclose()
            except Exception:
                pass

    async def _admit(task_id: str, epoch: str) -> None:
        """Open a channel for a newly-seen task (or re-open across a resume)."""
        if not task_id or not _TASK_CHAN_RE.match(f"task:{task_id}"):
            return
        existing = channels.get(task_id)
        if existing is not None and (existing.epoch == epoch or epoch == _NO_EPOCH):
            return
        # First sighting — or a new epoch re-incarnating the stream after a
        # resume. Either way just (re)open: _open_channel replaces any stale
        # channel and the fresh chan_open supersedes it client-side. Never
        # chan_close(terminal) on rotation: `terminal` means the task
        # settled, and a resume is a continuation — a false terminal made
        # clients mark live resumed tasks completed.
        chan = _open_channel(task_id, epoch, b"0")
        await out_q.put(
            (
                "frame",
                _control(
                    "chan_open",
                    {"chan": chan.name, "epoch": epoch, "mode": "replay"},
                ),
            )
        )

    started_at = time.monotonic()
    pumps = [
        asyncio.create_task(_task_pump()),
        asyncio.create_task(_watch_pump()),
        asyncio.create_task(_discovery_pump()),
    ]
    try:
        while True:
            if time.monotonic() - started_at > _MAX_DURATION_S:
                yield _control("timeout", {})
                return
            try:
                kind, payload = await asyncio.wait_for(
                    out_q.get(), timeout=_KEEPALIVE_S
                )
            except asyncio.TimeoutError:
                yield ": ping\n\n"
                continue
            if kind == "frame":
                yield payload
            elif kind == "task_frame":
                # Authoritative stale-generation filter: the enqueue-side
                # check cannot cover a put that suspended on a full queue
                # across a rotation (a fresh put can overtake the woken
                # putter), so a rotated-out frame can sit behind the
                # successor's chan_open. Terminal closes mark instead of pop,
                # so the slot always holds the latest generation — a frame
                # emits iff its own chan still owns the slot (drops lose
                # nothing: a successor replays its stream from 0).
                chan, frame = payload
                if channels.get(chan.task_id) is chan:
                    yield frame
            elif kind == "timeout":
                yield _control("timeout", {})
                return
            elif kind == "transport":
                yield _control("transport_error", {"retryable": True})
                return
    finally:
        for pump in pumps:
            pump.cancel()
