"""A subagent's captured-event archive: read back from Redis, recorded on the turn.

The per-task capture stream is written by the agent-side spill and read back
here once, post-terminal; what the turn keeps of it is each lane's provenance
and the images its text references. Separated from the collection lifecycle
because both halves carry their own failure contract: an archive that cannot
be read to the end is refused whole, never served as a prefix, and the
capture streams stay the only copy until every lane replayed from them has
been recorded.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from ptc_agent.agent.middleware.background_subagent.redis_stream import (
    task_stream_key,
)
from src.server.database.conversation.responses import replace_lane_provenance
from src.utils.cache.redis_cache import get_cache_client

logger = logging.getLogger(__name__)


class SubagentArchiveReadError(RuntimeError):
    """The captured-event archive could not be read to the end.

    Raised rather than yielding a prefix: a truncated archive is
    indistinguishable from a short one at the call site, so persisting half a
    subagent's history — and the usage totals derived from it — would look
    like success.
    """


# Entries per XRANGE round. Reading the whole stream in one call topped the
# SLOWLOG on a busy deployment (MAXLEN allows 300k entries, hundreds of MB) and
# pinned a connection for the length of the transfer.
_ARCHIVE_PAGE = 1000


async def iter_subagent_events_full(
    thread_id: str, task
) -> AsyncIterator[dict]:
    """Yield the current round's captured records for a subagent, in seq order.

    Raises ``SubagentArchiveReadError`` if the stream cannot be read in full.
    """
    if task is None or not thread_id:
        return

    high_water = int(getattr(task, "captured_event_seq", 0) or 0)
    # A resume that could not confirm its spool delete keeps the sequence
    # running rather than restarting it, so the retained prior round sits under
    # ids <= base. Reading from base makes the archive identical either way.
    base = int(getattr(task, "captured_event_seq_base", 0) or 0)
    if high_water <= base:
        return

    try:
        cache = get_cache_client()
    except Exception as exc:
        logger.warning(
            "[SubagentCollector] Failed to obtain cache client for "
            f"task {getattr(task, 'task_id', '?')}: {exc}"
        )
        return
    if cache is None or not getattr(cache, "enabled", False) or cache.client is None:
        return

    sa_stream_key = task_stream_key(thread_id, task.task_id)
    # The v1 spill writes explicit ``{seq}-0`` ids, so both round bounds are
    # directly expressible as a range — everything past the high-water is a
    # later epoch's refill and was never ours to read, everything at or below
    # the base is a previous round's.
    upper = f"{high_water}-0"
    cursor = "-" if base <= 0 else f"({base}-0"
    yielded = 0
    while True:
        try:
            page = await cache.client.xrange(
                sa_stream_key, min=cursor, max=upper, count=_ARCHIVE_PAGE
            )
        except Exception as exc:
            raise SubagentArchiveReadError(
                f"XRANGE failed for {sa_stream_key}: {exc}"
            ) from exc

        for entry_id, fields in page or []:
            try:
                seq_part = (
                    entry_id.decode("utf-8")
                    if isinstance(entry_id, bytes)
                    else entry_id
                )
                seq = int(seq_part.split("-", 1)[0])
            except (ValueError, AttributeError):
                continue
            if seq <= base or seq > high_water:
                continue
            raw = fields.get(b"record")
            if raw is None:
                continue
            try:
                payload = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                record = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(record, dict):
                continue
            yielded += 1
            yield record

        # A short page means the range is exhausted. Looping until an EMPTY
        # page instead would spin forever whenever the final page happens to
        # land exactly on the boundary.
        if not page or len(page) < _ARCHIVE_PAGE:
            break
        last_id = page[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode("utf-8")
        cursor = f"({last_id}"  # exclusive, so the last entry isn't re-read

    expected = high_water - base
    if yielded < expected:
        logger.warning(
            "subagent_history_truncated",
            extra={
                "thread_id": thread_id,
                "task_id": getattr(task, "task_id", None),
                "expected": expected,
                "recovered": yielded,
                "missing": expected - yielded,
                "redis_write_failed": bool(getattr(task, "redis_write_failed", False)),
            },
        )


def record_to_persist_event(record: dict, thread_id: str) -> dict:
    """Convert a captured-event record to persistence shape ``{event, data}``,
    with the ``ts`` it went out at, which a provenance row keeps."""
    data = dict(record.get("data") or {})
    data["thread_id"] = thread_id
    event = {"event": record.get("event"), "data": data}
    if record.get("ts") is not None:
        event["ts"] = record["ts"]
    return event


async def persist_collected_events(
    events: list[dict],
    response_id: str,
    thread_id: str,
    workspace_id: str,
    sandbox=None,
) -> bool:
    """Record the lanes ``events`` hold on the turn: where their images were
    captured to, and their provenance for the tool calls each task's
    checkpoints saved.

    Returns True once both landed; callers must not retire the Redis capture
    streams on False, since they are the only remaining source of either.
    """
    images: dict[str, dict[str, str]] = {}
    if sandbox:
        try:
            images = await _capture_lane_images(
                events, sandbox, thread_id, workspace_id
            )
        except Exception:
            logger.warning(
                "[IMAGE_CAPTURE] Hook B failed", exc_info=True,
            )

    lanes = _task_lanes(events)
    if not lanes:
        return True
    from src.server.services.history.committed import saved_lane_provenance
    from src.server.services.subagent_run_coordinator import record_lane_images

    # One bounded retry: a transiently failed write must not cost the lanes
    # their images or provenance. Images merge into the runs' facts and
    # provenance replaces each lane's rows whole, so a repeat never
    # duplicates them. A failed checkpoint read is no failure here: that
    # lane's sources are written unchecked. Provenance goes first, so images
    # that keep failing still leave the lanes their sources.
    for attempt in (1, 2):
        try:
            # A subagent that failed streamed what it fetched in its last
            # step, which its transcript never shows.
            provenance = await saved_lane_provenance(
                thread_id, events, log_tag="[SubagentCollector]"
            )
            if not await replace_lane_provenance(response_id, provenance, lanes):
                raise RuntimeError(f"no response row for {response_id}")
            for task_id, lane_images in images.items():
                await record_lane_images(
                    thread_id, response_id, task_id, lane_images
                )
            logger.info(
                f"[SubagentCollector] Recorded {len(provenance)} provenance "
                f"entries for {len(lanes)} lanes, response_id={response_id}"
            )
            return True
        except Exception as e:
            if attempt == 1:
                await asyncio.sleep(2.0)
                continue
            logger.error(
                f"[SubagentCollector] Failed to record lanes for "
                f"response_id={response_id}: {e}",
                exc_info=True,
            )
    return False


def _task_lanes(events: list[dict]) -> dict[str, str]:
    """Each ``task:{id}`` lane the events name, to its task id."""
    lanes: dict[str, str] = {}
    for event in events:
        data = event.get("data")
        agent = data.get("agent") if isinstance(data, dict) else None
        if isinstance(agent, str) and agent.startswith("task:"):
            lanes[agent] = agent.removeprefix("task:")
    return lanes


async def _capture_lane_images(
    events: list[dict],
    sandbox,
    thread_id: str,
    workspace_id: str,
) -> dict[str, dict[str, str]]:
    """Hook B: capture the images the lanes reference; each lane's
    path-to-URL map, by task id, for the runs the turn launched for it.

    Not on the turn's response row with the main lane's map: these lanes
    settle after that row did.
    """
    from src.server.services.persistence.image_capture import (
        capture_images,
        sandbox_image_paths,
    )

    by_task: dict[str, list[dict]] = {}
    for event in events:
        data = event.get("data")
        agent = data.get("agent") if isinstance(data, dict) else None
        if isinstance(agent, str) and agent.startswith("task:"):
            by_task.setdefault(agent.removeprefix("task:"), []).append(event)
    paths = {task_id: sandbox_image_paths(lane) for task_id, lane in by_task.items()}
    path_to_url = await capture_images(
        set().union(*paths.values()),
        sandbox,
        thread_id=thread_id,
        workspace_id=workspace_id,
    )
    out: dict[str, dict[str, str]] = {}
    for task_id, lane_paths in paths.items():
        images = {p: path_to_url[p] for p in lane_paths if p in path_to_url}
        if images:
            out[task_id] = images
    return out


@dataclass
class ArchiveBuffer:
    """One collection's archive writes: the events not yet landed, and
    whether a replay was withheld.

    Each task's events arrive in one batch and a write replaces its agents'
    rows whole, so a landed batch is never sent again and a failed one rides
    the next write. A withheld replay never entered the buffer, which leaves
    the capture streams the only copy of that task whatever later writes do.
    """

    response_id: str
    thread_id: str
    workspace_id: str
    user_id: str
    sandbox: Any
    unwritten: list[dict] = field(default_factory=list)
    withheld: bool = False

    async def flush(self) -> None:
        """Write the unwritten events, and forget them once they land."""
        if self.unwritten and await persist_collected_events(
            self.unwritten, self.response_id, self.thread_id,
            self.workspace_id, sandbox=self.sandbox,
        ):
            self.unwritten.clear()

    @property
    def complete(self) -> bool:
        """Every event replayed from the capture streams has landed, so the
        streams can be retired."""
        return not self.withheld and not self.unwritten
