"""A subagent's captured-event archive: read back from Redis, written to the turn.

The per-task capture stream is written by the agent-side spill and read back
here once, post-terminal, to rebuild the subagent's history; the collected
events then land in the turn's ``sse_events``. Separated from the collection
lifecycle because both halves carry their own failure contract: an archive
that cannot be read to the end is refused whole, never served as a prefix, and
the capture streams stay the transcript's copy until every event replayed from
them has been written.
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
from src.server.database.conversation.responses import replace_agent_events
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
    """Convert a captured-event record to persistence shape ``{event, data}``."""
    data = dict(record.get("data") or {})
    data["thread_id"] = thread_id
    return {"event": record.get("event"), "data": data}


async def persist_collected_events(
    events: list[dict],
    response_id: str,
    thread_id: str,
    workspace_id: str,
    sandbox=None,
) -> bool:
    """Write subagent events into the turn's sse_events, replacing their agents' rows.

    Returns True once the write landed; callers must not retire the Redis
    capture streams on False, since they are the only remaining source of the
    captured transcript.
    """
    if sandbox:
        try:
            await _capture_lane_images(
                events, sandbox, response_id, thread_id, workspace_id
            )
        except Exception:
            logger.warning(
                "[IMAGE_CAPTURE] Hook B failed", exc_info=True,
            )

    # One bounded retry: the replay cache gate holds the turn uncacheable
    # until these rows land, so a transiently failed write must not leave
    # the turn rebuilding on every read for its lifetime. Each attempt runs
    # in one row-locked transaction: concurrent atomic appends
    # (compact/offload context_window) serialize on the lock instead of being
    # erased, and any rows the turn itself saved for these agents give way to
    # the full capture rather than duplicate it.
    for attempt in (1, 2):
        try:
            if await replace_agent_events(response_id, events):
                logger.info(
                    f"[SubagentCollector] Updated sse_events for "
                    f"response_id={response_id} (+{len(events)} events)"
                )
                return True
            raise RuntimeError(f"no response row for {response_id}")
        except Exception as e:
            if attempt == 1:
                await asyncio.sleep(2.0)
                continue
            logger.error(
                f"[SubagentCollector] Failed to update sse_events "
                f"response_id={response_id}: {e}",
                exc_info=True,
            )
    return False


async def _capture_lane_images(
    events: list[dict],
    sandbox,
    response_id: str,
    thread_id: str,
    workspace_id: str,
) -> None:
    """Hook B: capture the images the lanes reference and rewrite them in the
    events, recording each lane's path-to-URL map on the runs the turn
    launched for it.

    Not on the thread's checkpoint, as the main turn's capture is: these
    lanes settle after their turn did, and a record appended now would land
    on whatever turn is newest, after the stored projection of this one.
    """
    from src.server.services.persistence.image_capture import (
        capture_images,
        sandbox_image_paths,
    )
    from src.server.services.subagent_run_coordinator import record_lane_images

    by_task: dict[str, list[dict]] = {}
    for event in events:
        data = event.get("data")
        agent = data.get("agent") if isinstance(data, dict) else None
        if isinstance(agent, str) and agent.startswith("task:"):
            by_task.setdefault(agent.removeprefix("task:"), []).append(event)
    # Read before the capture: it rewrites the paths it resolves.
    paths = {task_id: sandbox_image_paths(lane) for task_id, lane in by_task.items()}
    path_to_url = await capture_images(
        events, sandbox, thread_id=thread_id, workspace_id=workspace_id
    )
    for task_id, lane_paths in paths.items():
        images = {p: path_to_url[p] for p in lane_paths if p in path_to_url}
        if images:
            await record_lane_images(thread_id, response_id, task_id, images)


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
