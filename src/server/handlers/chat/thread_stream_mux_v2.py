"""Mux v2: every lane of a thread — main runs, task runs, watch — on one
socket, read from immutable run-scoped streams (STREAM_CONTRACT_V2.md).

What dissolves relative to v1: epochs, rotation, and the ABA generation
machinery (a run stream is born once and never reset — the channel key IS
the run identity); the subscribe-after-snapshot discovery race (the control
lane is a durable stream, so admission reads a backlog, not a pub/sub
window); and the sentinel handshake (the cursor-bearing ``run_end`` frame
closes a channel positively).

Wire shape:
- Cursors: ``?cursors=run:<run_id>#<entry_id>,…`` — resume is exclusive.
- Frames: ``id: run:<run_id>#<entry_id>`` + the contract envelope
  ``{run_id, seq, lane, type, payload}`` as data. Control frames
  (``chan_open``, ``chan_close``, ``resync_required``, ``transport_error``,
  ``timeout``) carry no cursor.
- Close: the ``run_end`` frame, or — for a worker that died between the
  terminal CAS and the append — a rate-limited ledger-row probe emitting
  ``chan_close{reason: terminal}`` from row truth. 'unknown' never closes.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from typing import AsyncGenerator, Optional

from fastapi import HTTPException

from src.server.services.thread_control_stream import control_stream_key
from src.utils.cache.redis_cache import get_cache_client

from ._common import logger
from .report_back import watch_wakes
from .stream_from_log import _xread_block_ms
from .thread_stream_mux import (
    _KEEPALIVE_S,
    _MAX_CHANNELS,
    _MAX_CURSORS_LEN,
    _MAX_DURATION_S,
    _OUT_QUEUE_MAX,
    _QUIESCE_EMPTY_ROUNDS,
    _RESCAN_INTERVAL_S,
    _XREAD_COUNT,
    _control,
)

_RUN_CHAN_RE = re.compile(r"^run:([A-Za-z0-9-]{1,40})$")
_ENTRY_ID_RE = re.compile(r"^\d{1,15}-\d{1,10}$")
_PROBE_MIN_INTERVAL_S = 5.0
# Discovery grace: the rescan admits task runs finalized this recently (capped
# to the socket's own attach age plus the client's declared snapshot lag) —
# closing the settled-before-seed race and the lost-announce case with
# positive closure. Runs settled before the client's knowledge horizon are
# already terminal in its history projection and are never re-announced.
# Must exceed the rescan interval so a run that both spawned and settled
# between rescans is seen at least once.
_RECENT_TERMINAL_SEED_S = 60
# Clock slack between the app's attach moment and Postgres finalized_at.
_ATTACH_GRACE_S = 5
# Abuse bound on the client-declared ?since_age_s knowledge-horizon lag.
_MAX_SNAPSHOT_LAG_S = 600
# Deferred admissions outlive their socket in a per-thread Redis hash: the
# announce is consumed at defer time, so a socket that dies before its retry
# succeeds would otherwise strand the run outside every recovery window
# (open-run seed excludes terminal; recent-terminal is age-bounded; the
# client's horizon advanced legitimately on delivered frames). The debt is
# per-CLIENT even though the hash is per-thread — admission on one socket
# proves nothing about delivery to another socket's client — so nothing ever
# deletes a field: TTL is the only eraser, refreshed by every re-defer, and
# redundant re-admission on sockets that already delivered the run is
# filtered client-side (drain frames for history-terminal tasks).
_PENDING_TTL_S = 3600


def _pending_key(thread_id: str) -> str:
    return f"mux2:pending:{thread_id}"


async def _store_pending(cache, thread_id: str, run_id: str, lane: str) -> None:
    try:
        key = _pending_key(thread_id)
        await cache.client.hset(key, run_id, lane)
        await cache.client.expire(key, _PENDING_TTL_S)
    except Exception:
        logger.warning(
            "[mux2] pending store failed for %s/%s", thread_id, run_id,
            exc_info=True,
        )


async def _load_pending(cache, thread_id: str) -> Optional[dict[str, str]]:
    """The thread's durable debt, {} when none — None on a failed read.

    The distinction is load-bearing: an error mistaken for emptiness would
    silently strand every debt this socket was meant to inherit, so callers
    must treat None as unknown and retry until a read succeeds."""
    try:
        raw = await cache.client.hgetall(_pending_key(thread_id))
    except Exception:
        logger.warning(
            "[mux2] pending load failed for %s", thread_id, exc_info=True
        )
        return None
    out: dict[str, str] = {}
    for k, v in (raw or {}).items():
        out[k.decode() if isinstance(k, bytes) else k] = (
            v.decode() if isinstance(v, bytes) else v
        )
    return out


def parse_mux_cursors_v2(raw: Optional[str]) -> dict[str, str]:
    """``?cursors=run:<run_id>#<entry_id>,…`` → {run_id: entry_id}."""
    if not raw:
        return {}
    if len(raw) > _MAX_CURSORS_LEN:
        raise HTTPException(status_code=400, detail="cursors too large")
    out: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        chan, sep, entry_id = part.partition("#")
        m = _RUN_CHAN_RE.match(chan)
        if not sep or not m or not _ENTRY_ID_RE.match(entry_id):
            raise HTTPException(
                status_code=400, detail=f"malformed cursor: {part[:80]!r}"
            )
        run_id = m.group(1)
        if run_id in out:
            raise HTTPException(
                status_code=400, detail=f"duplicate channel: {chan}"
            )
        if len(out) >= _MAX_CHANNELS:
            raise HTTPException(status_code=400, detail="too many channels")
        out[run_id] = entry_id
    return out


@dataclass
class _RunChan:
    run_id: str
    lane: str  # "main" | "task:<task_id>"
    stream_key: bytes
    cursor: bytes
    empty_rounds: int = 0
    last_probe_at: float = field(default=0.0)
    # Closed channels stay in the map: membership is the admission dedup
    # (immutable streams never reopen), they just drop out of the XREAD.
    closed: bool = False
    # Ledger started_at (epoch ms), declared on chan_open: the client
    # orders per-task outcome votes by run start, never by close order.
    started_ms: Optional[float] = None

    @property
    def name(self) -> str:
        return f"run:{self.run_id}"


def _run_stream_key(thread_id: str, chan_lane: str, run_id: str) -> bytes:
    if chan_lane == "main":
        return f"workflow:stream:{thread_id}:{run_id}".encode()
    return f"subagent:stream:{thread_id}:{run_id}".encode()


def _envelope(chan: _RunChan, entry_id: str, ftype: str, payload: str) -> str:
    """Contract frame around an already-JSON payload (no re-parse)."""
    head = json.dumps(
        {"run_id": chan.run_id, "seq": entry_id, "lane": chan.lane, "type": ftype},
        ensure_ascii=False,
    )
    data = f'{head[:-1]}, "payload": {payload or "null"}}}'
    return f"id: {chan.name}#{entry_id}\nevent: {ftype}\ndata: {data}\n\n"


def _parse_main_entry(fields: dict) -> Optional[tuple[str, str]]:
    """(type, payload_json) from a main-lane entry's rendered-SSE field."""
    raw = fields.get(b"event")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not raw:
        return None
    ftype, payload = "message", ""
    for line in raw.split("\n"):
        if line.startswith("event: "):
            ftype = line[7:].strip()
        elif line.startswith("data: "):
            payload = line[6:]
    return ftype, payload


def _parse_task_entry(fields: dict) -> Optional[tuple[str, str]]:
    ftype = fields.get(b"type")
    payload = fields.get(b"payload")
    if isinstance(ftype, bytes):
        ftype = ftype.decode("utf-8", errors="replace")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    if not ftype:
        return None
    return ftype, payload or ""


async def _run_row(run_id: str, lane: str) -> Optional[dict]:
    """Ledger row for a run, or None when unknown/unreadable."""
    try:
        if lane == "main":
            from src.server.database import turn_lifecycle as tl_db

            return await tl_db.get_run(run_id)
        from src.server.database import subagent_runs as sr_db

        return await sr_db.get_task_run(run_id)
    except Exception:
        return None


def _row_outcome(row: Optional[dict]) -> Optional[str]:
    """Terminal status from a row, or None while open/unknown."""
    if row is None:
        return None
    status = str(row.get("status") or "")
    return status if status and status != "in_progress" else None


def _row_started_ms(row: Optional[dict]) -> Optional[float]:
    """Run start as epoch ms — the client orders outcome votes by this."""
    ts = row.get("started_at") if row else None
    try:
        return ts.timestamp() * 1000.0 if ts is not None else None
    except (AttributeError, ValueError, OSError):
        return None


async def _run_row_outcome(run_id: str, lane: str) -> Optional[str]:
    """Terminal row status, or None while open/unknown — the crash-window
    backstop for streams whose writer died between CAS and run_end."""
    return _row_outcome(await _run_row(run_id, lane))


async def _admission_meta(
    run_id: str, lane: str, settled: bool, started_ms: Optional[float]
) -> Optional[tuple[bool, Optional[float]]]:
    """Row-truth (settled, started_ms) for an admission; None = defer.

    A run already terminal at admission is a backlog, not a live lane —
    callers can't know that (a delayed announce or a rescan seed can both
    arrive after the run finalized), so the row decides, and the same row
    supplies the start order the client votes outcomes by. Drain only on
    positive terminal evidence: an unknown row stays live. A task lane whose
    start order can't be established is deferred — admitting it would vote
    as older-than-everything and could crown a stale predecessor's outcome.
    started_at is NOT NULL, so that only happens on a failed row read; the
    rescan retries once the read heals."""
    if not settled or started_ms is None:
        row = await _run_row(run_id, lane)
        if not settled:
            settled = _row_outcome(row) is not None
        if started_ms is None:
            started_ms = _row_started_ms(row)
    if lane != "main" and started_ms is None:
        return None
    return settled, started_ms


async def _seed_channels(
    thread_id: str,
) -> list[tuple[str, str, Optional[float]]]:
    """(run_id, lane, started_ms) for every open run — the attach-time
    reconciliation.

    Deliberately open runs only: a run already terminal at attach belongs to
    the history projection, and re-announcing it would replay content the
    client just projected. Runs that settle AFTER attach are healed by the
    rescan's recent-terminal admission below."""
    out: list[tuple[str, str, Optional[float]]] = []
    try:
        from src.server.database import turn_lifecycle as tl_db

        root = await tl_db.get_active_run(thread_id)
        if root is not None:
            out.append((str(root["conversation_response_id"]), "main", None))
    except Exception:
        logger.warning(
            "[mux2] main-lane seed failed for %s", thread_id, exc_info=True
        )
    try:
        from src.server.database import subagent_runs as sr_db

        for run in await sr_db.list_open_runs_for_thread(thread_id):
            out.append(
                (
                    str(run["task_run_id"]),
                    f"task:{run['task_id']}",
                    _row_started_ms(run),
                )
            )
    except Exception:
        logger.warning(
            "[mux2] ledger seed failed for %s", thread_id, exc_info=True
        )
    return out


async def _resolve_lane(
    thread_id: str, run_id: str
) -> Optional[tuple[str, Optional[float]]]:
    """(lane, started_ms) for a client-cursor run the seed didn't cover
    (settled runs)."""
    try:
        from src.server.database import subagent_runs as sr_db

        row = await sr_db.get_task_run(run_id)
        if row is not None and str(row.get("thread_id")) == thread_id:
            return f"task:{row['task_id']}", _row_started_ms(row)
        from src.server.database import turn_lifecycle as tl_db

        root = await tl_db.get_run(run_id)
        if root is not None and str(root.get("conversation_thread_id")) == thread_id:
            return "main", None
    except Exception:
        logger.warning(
            "[mux2] lane resolve failed for %s", run_id, exc_info=True
        )
    return None


def _entry_after(entry_id: bytes | str, cursor: str) -> bool:
    """True when entry_id > cursor (Redis major-minor numeric order)."""
    a = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
    try:
        am, _, an = a.partition("-")
        cm, _, cn = cursor.partition("-")
        return (int(am), int(an or 0)) > (int(cm), int(cn or 0))
    except ValueError:
        return False


async def stream_thread_mux_v2(
    thread_id: str, cursor_map: dict[str, str], since_age_s: float = 0.0
) -> AsyncGenerator[str, None]:
    """Attach (seed + control snapshot), then one XREAD pump over control +
    every open run stream, plus the watch relay.

    ``since_age_s`` is the client's declared knowledge-horizon lag: seconds
    between its status/history snapshot (or last received frame) and this
    connect. The settled-run catch-up windows anchor at that horizon — not
    the socket's attach moment — so a task that settled during a slow
    history load still gets positive closure."""
    cache = get_cache_client()
    if not cache.enabled or not cache.client:
        yield _control("transport_error", {"retryable": True})
        return

    started_at = time.monotonic()
    snapshot_lag = int(min(max(since_age_s, 0.0), _MAX_SNAPSHOT_LAG_S))
    if since_age_s > _MAX_SNAPSHOT_LAG_S:
        # The client's horizon predates the catch-up cap (tab asleep, long
        # outage): runs that started AND settled early in the gap are beyond
        # every recovery path here — only a projection reload can see them.
        # Serve the stream anyway; the client reloads history in parallel.
        yield _control("resync_required", {"scope": "thread"})
    out_q: asyncio.Queue = asyncio.Queue(maxsize=_OUT_QUEUE_MAX)
    channels: dict[str, _RunChan] = {}
    new_chan_evt = asyncio.Event()
    attach_frames: list[str] = []
    control_key = control_stream_key(thread_id).encode()

    # Control cursor FIRST, then the row seeds: any run announced after this
    # snapshot is seen by the seed reads below or by the XREAD from here —
    # a durable stream leaves no window between the two.
    control_cursor = b"$"
    try:
        tail = await cache.client.xrevrange(control_key, count=1)
        control_cursor = tail[0][0] if tail else b"0"
    except Exception:
        logger.warning(
            "[mux2] control snapshot failed for %s", thread_id, exc_info=True
        )

    def _open(
        run_id: str,
        lane: str,
        cursor: bytes,
        started_ms: Optional[float] = None,
    ) -> _RunChan:
        chan = _RunChan(
            run_id=run_id,
            lane=lane,
            stream_key=_run_stream_key(thread_id, lane, run_id),
            cursor=cursor,
            started_ms=started_ms,
        )
        channels[run_id] = chan
        new_chan_evt.set()
        return chan

    def _chan_open_frame(chan: _RunChan, mode: str) -> str:
        payload: dict = {"chan": chan.name, "lane": chan.lane, "mode": mode}
        if chan.started_ms is not None:
            payload["started"] = chan.started_ms
        return _control("chan_open", payload)

    def _chan_close_frame(chan: _RunChan, reason: str, **extra) -> str:
        return _control(
            "chan_close", {"chan": chan.name, "reason": reason, **extra}
        )

    def _open_count() -> int:
        # The cap bounds concurrent XREAD fan-out, so it counts open channels
        # only: closed ones stay in the map purely as the admission dedup and
        # must not starve later runs on a long-lived socket.
        return sum(1 for c in channels.values() if not c.closed)

    # ---- attach ----------------------------------------------------------
    for run_id, lane, started_ms in await _seed_channels(thread_id):
        if _open_count() >= _MAX_CHANNELS:
            break
        cursor = cursor_map.get(run_id)
        mode = "resume" if cursor is not None else "replay"
        chan = _open(run_id, lane, (cursor or "0").encode(), started_ms)
        attach_frames.append(_chan_open_frame(chan, mode))

    for run_id, entry_id in cursor_map.items():
        if run_id in channels:
            continue
        resolved = await _resolve_lane(thread_id, run_id)
        if resolved is None:
            attach_frames.append(
                _control(
                    "chan_close",
                    {"chan": f"run:{run_id}", "reason": "unknown_run"},
                )
            )
            continue
        # Settled run the client is still behind on: drain the retained
        # stream to its run_end; an expired stream closes via the row probe.
        lane, started_ms = resolved
        chan = _open(run_id, lane, entry_id.encode(), started_ms)
        attach_frames.append(_chan_open_frame(chan, "drain"))

    # Knowledge-horizon catch-up: runs that settled between the client's
    # snapshot and this attach have live-'active' cards with no closure
    # coming from the open-run seed. Admit them as drain backlogs now — the
    # first rescan is 30s out, far too late for a card wedged on page load.
    try:
        from src.server.database import subagent_runs as sr_db

        recent = await sr_db.list_recently_finalized_runs_for_thread(
            thread_id,
            # Include the server-side attach elapsed too: a run can finalize
            # during a stalled seed/resolve above, after the horizon.
            within_seconds=snapshot_lag
            + int(time.monotonic() - started_at)
            + _ATTACH_GRACE_S,
        )
    except Exception:
        recent = []
        logger.warning(
            "[mux2] attach recent-terminal seed failed for %s",
            thread_id,
            exc_info=True,
        )
    for run in recent:
        run_id = str(run["task_run_id"])
        if run_id in channels or _open_count() >= _MAX_CHANNELS:
            continue
        chan = _open(
            run_id, f"task:{run['task_id']}", b"0", _row_started_ms(run)
        )
        attach_frames.append(_chan_open_frame(chan, "drain"))

    # Pending admissions inherited from prior sockets: a defer whose socket
    # died before its retry succeeded is invisible to every window above
    # (announce consumed, run possibly terminal, horizon legitimately
    # advanced) — only the durable identity brings it back. Unresolvable
    # entries stay pending on this socket for the rescan to retry. A failed
    # load is UNKNOWN debt, not absent debt: the rescan re-reads until one
    # load succeeds (pending_synced), and since nothing deletes hash fields,
    # a debt can never be erased out from under a socket that hasn't seen it.
    pending_admissions: dict[str, str] = {}
    inherited = await _load_pending(cache, thread_id)
    pending_synced = inherited is not None
    for run_id, lane in (inherited or {}).items():
        if run_id in channels:
            continue
        if _open_count() >= _MAX_CHANNELS:
            pending_admissions[run_id] = lane
            continue
        meta = await _admission_meta(run_id, lane, False, None)
        if meta is None:
            pending_admissions[run_id] = lane
            continue
        settled, started_ms = meta
        chan = _open(run_id, lane, b"0", started_ms)
        attach_frames.append(
            _chan_open_frame(chan, "drain" if settled else "replay")
        )

    # Head probe for every resumed cursor. On an immutable stream the head
    # is the run's first frame forever, so head > cursor means entries at or
    # below a once-real cursor were lost externally (expiry) — resync, never
    # gap-and-continue. An absent stream with a terminal row aged out after
    # its grace window: close, replay owns the transcript.
    for chan in list(channels.values()):
        if chan.cursor in (b"0", b"$") or chan.closed:
            continue
        try:
            head = await cache.client.xrange(chan.stream_key, count=1)
        except Exception:
            continue
        if not head:
            outcome = await _run_row_outcome(chan.run_id, chan.lane)
            if outcome is not None:
                chan.closed = True
                attach_frames.append(
                    _chan_close_frame(chan, "terminal", outcome=outcome)
                )
            continue
        if _entry_after(head[0][0], chan.cursor.decode()):
            chan.closed = True
            attach_frames.append(
                _control("resync_required", {"chan": chan.name})
            )
            attach_frames.append(_chan_close_frame(chan, "resync_required"))

    for frame in attach_frames:
        yield frame

    # ---- pumps -----------------------------------------------------------
    block_ms = _xread_block_ms()

    # A deferred admission must outlive every bounded recovery window AND
    # this socket: its announce is already behind control_cursor, a run that
    # finalizes during the outage ages out of the seed and recent-terminal
    # queries, and the client's horizon keeps advancing on delivered frames.
    # The local map drives this socket's 30s rescan retries; the durable
    # copy (_store_pending) lets the next socket inherit the debt.
    if pending_admissions:
        logger.warning(
            "[mux2] inherited %d pending admissions for %s",
            len(pending_admissions),
            thread_id,
        )

    async def _admit(
        run_id: str,
        lane: str,
        *,
        settled: bool = False,
        started_ms: Optional[float] = None,
    ) -> None:
        if run_id in channels:
            pending_admissions.pop(run_id, None)
            return
        if _open_count() >= _MAX_CHANNELS:
            pending_admissions[run_id] = lane
            await _store_pending(cache, thread_id, run_id, lane)
            logger.warning(
                "[mux2] channel cap reached for %s; deferring %s",
                thread_id,
                run_id,
            )
            return
        meta = await _admission_meta(run_id, lane, settled, started_ms)
        if meta is None:
            pending_admissions[run_id] = lane
            await _store_pending(cache, thread_id, run_id, lane)
            logger.warning(
                "[mux2] deferring %s (%s) for %s: start order unavailable",
                run_id,
                lane,
                thread_id,
            )
            return
        settled, started_ms = meta
        # Local retirement only — the durable field expires by TTL. Deleting
        # it here would erase a debt other sockets' clients still owe.
        pending_admissions.pop(run_id, None)
        chan = _open(run_id, lane, b"0", started_ms)
        await out_q.put(
            ("frame", _chan_open_frame(chan, "drain" if settled else "replay"))
        )

    async def _close(chan: _RunChan, reason: str, **extra) -> None:
        if chan.closed:
            return
        chan.closed = True
        await out_q.put(("frame", _chan_close_frame(chan, reason, **extra)))

    async def _read_pump() -> None:
        nonlocal control_cursor, pending_synced
        last_scan = time.monotonic()
        while True:
            open_chans = [c for c in channels.values() if not c.closed]
            streams: dict[bytes, bytes] = {control_key: control_cursor}
            for c in open_chans:
                streams[c.stream_key] = c.cursor
            try:
                result = await asyncio.wait_for(
                    cache.client.xread(
                        streams, block=block_ms, count=_XREAD_COUNT
                    ),
                    timeout=(block_ms / 1000.0) + 2.0,
                )
            except asyncio.TimeoutError:
                result = None
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception:
                logger.warning(
                    "[mux2] XREAD failed for %s", thread_id, exc_info=True
                )
                await out_q.put(("transport", None))
                return

            got: set[str] = set()
            for stream_name, entries in result or ():
                if stream_name == control_key:
                    for entry_id, fields in entries:
                        control_cursor = entry_id
                        ftype = fields.get(b"type") or b""
                        run_id = fields.get(b"run_id") or b""
                        run_id = (
                            run_id.decode() if isinstance(run_id, bytes) else run_id
                        )
                        if ftype == b"run_started":
                            await _admit(run_id, "main")
                        elif ftype == b"task_run_started":
                            task_id = fields.get(b"task_id") or b""
                            task_id = (
                                task_id.decode()
                                if isinstance(task_id, bytes)
                                else task_id
                            )
                            await _admit(run_id, f"task:{task_id}")
                    continue
                chan = next(
                    (c for c in open_chans if c.stream_key == stream_name), None
                )
                if chan is None or chan.closed:
                    continue
                got.add(chan.run_id)
                chan.empty_rounds = 0
                for entry_id, fields in entries:
                    chan.cursor = entry_id
                    parsed = (
                        _parse_main_entry(fields)
                        if chan.lane == "main"
                        else _parse_task_entry(fields)
                    )
                    if parsed is None:
                        continue
                    ftype, payload = parsed
                    entry_id_s = (
                        entry_id.decode()
                        if isinstance(entry_id, bytes)
                        else str(entry_id)
                    )
                    await out_q.put(
                        ("frame", _envelope(chan, entry_id_s, ftype, payload))
                    )
                    if ftype == "run_end":
                        await _close(chan, "terminal")
                        break

            # Crash-window backstop: a terminal ledger row whose stream
            # never got its run_end closes the channel from row truth.
            now = time.monotonic()
            for chan in open_chans:
                if chan.run_id in got or chan.closed:
                    continue
                chan.empty_rounds += 1
                if chan.empty_rounds < _QUIESCE_EMPTY_ROUNDS:
                    continue
                if now - chan.last_probe_at < _PROBE_MIN_INTERVAL_S:
                    continue
                chan.last_probe_at = now
                outcome = await _run_row_outcome(chan.run_id, chan.lane)
                if outcome is not None:
                    await _close(chan, "terminal", outcome=outcome)

            if time.monotonic() - last_scan >= _RESCAN_INTERVAL_S:
                last_scan = time.monotonic()
                if not pending_synced:
                    inherited = await _load_pending(cache, thread_id)
                    if inherited is not None:
                        pending_synced = True
                        for run_id, lane in inherited.items():
                            await _admit(run_id, lane)
                for run_id, lane in list(pending_admissions.items()):
                    await _admit(run_id, lane)
                for run_id, lane, started_ms in await _seed_channels(thread_id):
                    await _admit(run_id, lane, started_ms=started_ms)
                # Recent-terminal reconciliation, bounded to runs that could
                # only have settled AFTER the client's knowledge horizon
                # (its snapshot lag plus this socket's age) — anything older
                # was terminal in the projection it attached with, and
                # re-announcing it would duplicate the transcript. Heals the
                # settled-before-seed race and a lost control announce with
                # positive closure; channel membership dedups re-admission.
                window = min(
                    _RECENT_TERMINAL_SEED_S + snapshot_lag,
                    int(time.monotonic() - started_at)
                    + snapshot_lag
                    + _ATTACH_GRACE_S,
                )
                try:
                    from src.server.database import subagent_runs as sr_db

                    recent = await sr_db.list_recently_finalized_runs_for_thread(
                        thread_id, within_seconds=window
                    )
                except Exception:
                    recent = []
                    logger.warning(
                        "[mux2] recent-terminal rescan failed for %s",
                        thread_id,
                        exc_info=True,
                    )
                for run in recent:
                    await _admit(
                        str(run["task_run_id"]),
                        f"task:{run['task_id']}",
                        settled=True,
                        started_ms=_row_started_ms(run),
                    )

    async def _watch_pump() -> None:
        backoff = 1.0
        while True:
            gen = watch_wakes(cache, thread_id)
            try:
                async for frame in gen:
                    backoff = 1.0
                    if frame.startswith(": ping"):
                        continue
                    if frame.startswith("event: timeout"):
                        await out_q.put(("timeout", None))
                        return
                    await out_q.put(("frame", frame))
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception:
                logger.warning(
                    "[mux2] watch pump failed for %s", thread_id, exc_info=True
                )
            finally:
                await gen.aclose()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    pumps = [
        asyncio.create_task(_read_pump()),
        asyncio.create_task(_watch_pump()),
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
            elif kind == "timeout":
                yield _control("timeout", {})
                return
            elif kind == "transport":
                yield _control("transport_error", {"retryable": True})
                return
    finally:
        for pump in pumps:
            pump.cancel()
