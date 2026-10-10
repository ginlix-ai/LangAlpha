"""Each thread's transcript: JSONL the agent can grep, rendered from the
checkpoint into the store, where the computer's file mount serves it.

The checkpoint is the record and these files are a rendering of it, made when
it changes (a turn end, a task finishing) rather than when it is read. Each
agent of a thread (its own, and each background task's) is stored on its own
with a fingerprint of what it was rendered from, the thread's checkpoint or
the task's latest run, so an agent already current costs no state read and a
save touches only its agent's files. A render renders only the segments whose
shape changed since the stored copy, and a save writes only the rows that
differ. A small file's bytes sit in its row; a larger one's in the per-user
blob registry when there is object storage.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AnyMessage

from ptc_agent.agent.transcript import (
    EarlierTurnsMissing,
    TranscriptTarget,
    Window,
    build_directory,
    load_manifest,
)
from ptc_agent.agent.transcript.render import SCHEMA_VERSION
from ptc_agent.core.paths import WorkspaceLayout

if TYPE_CHECKING:
    from src.server.database.thread_transcripts import TaskRun

logger = logging.getLogger(__name__)

INDEX = "threads.jsonl"

# Files up to this size keep their bytes in the row even with object storage:
# most turns fit, and a row costs a turn end no upload and a read no fetch.
INLINE_FILE_MAX_BYTES = 64 * 1024

# Threads a bring-up exports at once. Each holds one checkpointer connection
# while it reads, and one agent's history at a time.
_EXPORT_CONCURRENCY = 3
_BLOB_UPLOAD_CONCURRENCY = 8


def index_path(root: str) -> str:
    """The computer's thread index, beside every workspace folder rather than in one."""
    return f"{root.rstrip('/')}/{WorkspaceLayout.AGENTS_DIR}/{INDEX}"


def _task_run(task_id: str, row: dict[str, Any]) -> TaskRun:
    from src.server.database.thread_transcripts import TaskRun

    return TaskRun(
        task_id, row.get("latest_run_id"), row.get("status"), row.get("final_checkpoint_id")
    )


def _task_print(run: TaskRun) -> str:
    """What a task's fingerprint is taken from: the run its save checks."""
    return "|".join(
        str(value or "") for value in (run.run_id, run.status, run.final_checkpoint_id)
    )


def _fingerprint(source: str | None) -> str:
    """An agent's copy is current while this matches: ``source`` is the
    thread's checkpoint for its own agent, a task's print for a task."""
    payload = json.dumps([SCHEMA_VERSION, source])
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _newer(held: Any, candidate: Any) -> bool:
    """Whether a copy rendered at ``held`` is past ``candidate``. Checkpoint
    ids sort by time, so an export racing a newer one (another worker's turn
    end) stands down instead of undoing it."""
    return bool(held and candidate and held > candidate)


def _live_at(copy: Any, checkpoint_id: str | None) -> bool:
    """Whether ``copy`` is the thread's own, saved live past ``checkpoint_id``:
    the store keeps it over a render from there, so none is made. Without a
    checkpoint a render reads the latest one, which is not behind it."""
    from src.server.database.thread_transcripts import LIVE_FINGERPRINT

    return (
        copy is not None
        and checkpoint_id is not None
        and copy.fingerprint == LIVE_FINGERPRINT
        and copy.checkpoint_id == checkpoint_id
    )


@dataclass
class _Target:
    """Whose store a render lands in."""

    workspace_id: str
    user_id: str
    #: No object storage: every file's bytes go in the rows.
    inline: bool


async def _target(workspace_id: str) -> _Target | None:
    from src.server.database.workspace import workspace_owner
    from src.utils.storage import is_storage_enabled

    try:
        user_id = await workspace_owner(workspace_id)
    except LookupError:
        return None
    return _Target(workspace_id, user_id, not is_storage_enabled())


@dataclass
class _Rendered:
    """One agent's copy as a render leaves it, and the blob bytes to store."""

    copy: Any
    blobs: dict[str, bytes]


@dataclass
class _Job:
    """One agent's render: what it renders from and the copy it replaces."""

    agent: TranscriptTarget
    messages: list[Any]
    header: dict[str, Any]
    fingerprint: str
    checkpoint_id: str | None
    #: The manifest of the stored copy this one replaces, if there is one.
    previous: str | None
    #: What the window trimmed from the head of ``messages``.
    window: Window
    #: A task's render from the checkpoint: the run it read.
    run: TaskRun | None = None

    def render(self, *, inline: bool, full: bool = False) -> _Rendered:
        """The copy, each file named by digest. Only the segments that changed
        since ``previous`` render and carry bytes; the rest are carried over.
        CPU only, so it runs in a worker thread. Raises
        ``EarlierTurnsMissing`` when ``previous`` cannot stand in for the
        trimmed runs (see ``_whole``)."""
        from src.server.database.thread_transcripts import StoredTranscript

        directory = build_directory(
            self.messages,
            unit=self.agent.unit,
            header=self.header,
            previous=None if full else load_manifest(self.previous),
            window=self.window,
        )
        copy = StoredTranscript(
            self.fingerprint, self.checkpoint_id, directory.manifest, run=self.run
        )
        # Always in its row: the next render reads it from there.
        manifest = directory.manifest.encode()
        manifest_path = self.agent.prefix + self.agent.manifest
        copy.files[manifest_path] = (hashlib.sha256(manifest).hexdigest(), len(manifest))
        copy.inline[manifest_path] = manifest
        blobs: dict[str, bytes] = {}
        for name, (sha, size) in directory.files.items():
            path = self.agent.prefix + name
            copy.files[path] = (sha, size)
            data = directory.rendered.get(name)
            if data is None:
                copy.carried.add(path)
            elif inline or size <= INLINE_FILE_MAX_BYTES:
                copy.inline[path] = data
            else:
                blobs[sha] = data
        return _Rendered(copy, blobs)


async def _whole(job: _Job) -> _Job:
    """``job`` over the thread's whole message list: the runs the window
    trimmed are read back from the turn slices, for a render with no stored
    copy to carry them from (a first export, a schema change, a fork that
    dropped the copy, a copy moved under its render)."""
    from src.server.services.history.window import earlier_runs

    earlier = await earlier_runs(job.agent.thread_id, job.checkpoint_id, job.window)
    if earlier is None:
        raise EarlierTurnsMissing(
            f"{job.agent.directory}: {job.window.runs} trimmed runs unreadable"
        )
    return replace(job, messages=[*earlier, *job.messages], window=Window())


async def _render_job(job: _Job, *, inline: bool, full: bool = False) -> _Rendered:
    try:
        return await asyncio.to_thread(job.render, inline=inline, full=full)
    except EarlierTurnsMissing:
        logger.info(f"Transcript {job.agent.directory} renders its trimmed runs from slices")
    whole = await _whole(job)
    return await asyncio.to_thread(whole.render, inline=inline, full=full)


async def _store_blobs(user_id: str, contents: dict[str, bytes]) -> None:
    from src.server.database.workspace_file_blobs import registered_blobs, store_blobs

    if not contents:
        return
    registered = await registered_blobs(user_id, list(contents))
    await store_blobs(
        user_id,
        {sha: data for sha, data in contents.items() if sha not in registered},
        concurrency=_BLOB_UPLOAD_CONCURRENCY,
    )


async def _save(target: _Target, job: _Job, rendered: _Rendered) -> bool:
    """Store one agent's copy. One that carried files over from rows another
    save has since moved is rendered again in full."""
    from src.server.database.thread_transcripts import StaleCopy, save_stored

    agent = job.agent
    try:
        await _store_blobs(target.user_id, rendered.blobs)
        return await save_stored(agent.thread_id, agent.prefix, target.user_id, rendered.copy)
    except StaleCopy:
        if job.previous is None:
            raise
        logger.info(f"Transcript {agent.directory} moved under its render; rendering it in full")
    full = await _render_job(job, inline=target.inline, full=True)
    await _store_blobs(target.user_id, full.blobs)
    return await save_stored(agent.thread_id, agent.prefix, target.user_id, full.copy)


@dataclass
class Behind:
    """A thread some of whose agents' stored copies are behind."""

    thread_id: str
    checkpoint_id: str | None
    #: Every task of the thread, by id, with its latest run.
    tasks: dict[str, dict[str, Any]]
    #: The agents to render, by prefix ("" for the thread's own).
    agents: set[str]
    #: Stored agents whose task is gone (a truncation deleted it).
    gone: set[str]


async def behind_in_store(threads: list[tuple[str, str | None]]) -> list[Behind]:
    """Which of these (thread id, checkpoint id) pairs the store is behind
    on, and on which of each thread's agents, in the order given. The store
    is read before the tasks, so a task stored here is one already created:
    missing from the tasks, it was deleted."""
    from src.server.database.runs.subagent_runs import list_thread_tasks
    from src.server.database.thread_transcripts import stored_fingerprints

    ids = [thread_id for thread_id, _ in threads]
    stored = await stored_fingerprints(ids)
    tasks: dict[str, dict[str, dict[str, Any]]] = {}
    for row in await list_thread_tasks(ids):
        tasks.setdefault(str(row["thread_id"]), {})[row["task_id"]] = row
    behind = []
    for thread_id, checkpoint_id in threads:
        mine = tasks.get(thread_id, {})
        wanted = {"": _fingerprint(checkpoint_id)} | {
            TranscriptTarget(thread_id, task_id).prefix: _fingerprint(
                _task_print(_task_run(task_id, row))
            )
            for task_id, row in mine.items()
        }
        have = stored.get(thread_id, {})
        agents = {prefix for prefix, print_ in wanted.items() if have.get(prefix) != print_}
        gone = have.keys() - wanted.keys()
        if agents or gone:
            behind.append(Behind(thread_id, checkpoint_id, mine, agents, gone))
    return behind


async def _render_and_save(target: _Target, job: _Job) -> bool:
    rendered = await _render_job(job, inline=target.inline)
    return await _save(target, job, rendered)


async def _render_own(
    target: _Target, thread: Behind, own: Any, reader: Any
) -> bool | None:
    """Render the thread's own agent from its checkpoint and store it:
    whether the save landed. None when it stands down, the stored copy being
    newer or the checkpoint empty."""
    state = await reader.aget_state(thread.thread_id, thread.checkpoint_id)
    rendered_at = (state.config or {}).get("configurable", {}).get("checkpoint_id")
    if own is not None and _newer(own.checkpoint_id, rendered_at):
        return None
    values = state.values or {}
    messages = list(values.get("messages") or [])
    if not messages:
        # Nothing to show; a copy already stored is left for the next
        # render that has something, rather than emptied.
        return None
    job = _Job(
        TranscriptTarget(thread.thread_id),
        messages,
        {"thread_id": thread.thread_id, "checkpoint_id": rendered_at},
        _fingerprint(thread.checkpoint_id),
        rendered_at,
        own.manifest if own is not None else None,
        Window.of(values),
    )
    return await _render_and_save(target, job)


async def _render_task(
    target: _Target, thread: Behind, task_id: str, previous: Any, reader: Any
) -> bool:
    """Render one task from its runs and store it, at its own namespace's
    checkpoint: the thread's says nothing of how far a task running beside
    it has got."""
    row = thread.tasks[task_id]
    run = _task_run(task_id, row)
    history = await reader.aget_task_history(thread.thread_id, task_id)
    header = {
        "task_id": task_id,
        "description": row.get("description"),
        "subagent_type": row.get("subagent_type"),
        "status": row.get("status"),
        "created_at": _iso(row.get("created_at")),
        "launch_call_id": row.get("launch_tool_call_id"),
    }
    job = _Job(
        TranscriptTarget(thread.thread_id, task_id),
        history.messages,
        header,
        _fingerprint(_task_print(run)),
        history.checkpoint_id,
        previous.manifest if previous is not None else None,
        # Only the main agent's checkpoint is trimmed (``with_window``).
        Window(),
        run,
    )
    return await _render_and_save(target, job)


async def _render(target: _Target, thread: Behind, stored: dict[str, Any]) -> bool:
    """Render a thread's behind agents and replace their stored copies,
    ``stored`` being the headers the thread holds now by prefix, and drop the
    copies of tasks that are gone. The thread's checkpoint is read only when
    its own agent is behind and not saved live past it, and only the runs of
    the tasks rendered are. One agent is read, rendered and saved at a time,
    so a thread with many long tasks holds one history, not all of them.
    Returns whether every save landed."""
    from src.server.database.thread_transcripts import delete_stored
    from src.server.services.history.reader import CheckpointHistoryReader

    thread_id = thread.thread_id
    own = stored.get("")
    if own is not None and _newer(own.checkpoint_id, thread.checkpoint_id):
        return False
    agents = thread.agents
    if _live_at(own, thread.checkpoint_id):
        agents = agents - {""}
        if not agents and not thread.gone:
            return False
    reader = CheckpointHistoryReader.get_instance()
    landed: list[bool] = []
    if "" in agents:
        saved = await _render_own(target, thread, own, reader)
        if saved is None:
            return False
        landed.append(saved)
    for task_id in sorted(thread.tasks):
        prefix = TranscriptTarget(thread_id, task_id).prefix
        if prefix in agents:
            landed.append(
                await _render_task(target, thread, task_id, stored.get(prefix), reader)
            )
    if thread.gone:
        await delete_stored(thread_id, thread.gone)
    return all(landed)


async def _export(thread_id: str, workspace_id: str | None) -> dict[str, int]:
    """Render the thread's agents whose stored copy is behind its latest
    checkpoint. Only ``_export_once`` calls this."""
    from src.server.database.conversation import (
        get_thread_by_id,
        get_thread_checkpoint_id,
    )
    from src.server.database.thread_transcripts import load_stored

    counts = {"stored": 0, "failed": 0}
    if workspace_id is None:
        thread = await get_thread_by_id(thread_id)
        if not thread or not thread.get("workspace_id"):
            return counts
        workspace_id = str(thread["workspace_id"])
    checkpoint_id, target = await asyncio.gather(
        get_thread_checkpoint_id(thread_id), _target(workspace_id)
    )
    if target is None:
        return counts
    behind = await behind_in_store([(thread_id, checkpoint_id)])
    if not behind:
        return counts
    stored = await load_stored([thread_id])
    try:
        counts["stored"] += await _render(target, behind[0], stored.get(thread_id, {}))
    except Exception as e:
        counts["failed"] += 1
        logger.warning(f"Transcript export failed for thread {thread_id}: {e}")
    return counts


# One export of a thread at a time across workers, to spare them the same
# render: two at once are safe, since a save refuses a task's copy whose run
# moved since its render read it. The worker holding the lock exports until
# the flag is down; one that finds it held only raises the flag, and the
# holder checks it again after letting go, so an export asked for mid-render
# still runs. Both keys expire: a worker that dies holding the lock delays the
# thread's next export rather than stopping it. Without Redis, exports run
# unlocked.
_LOCK_TTL_MS = 300_000
_FLAG_TTL_S = 3600


def _cache() -> Any:
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
    except Exception:
        return None
    return cache if getattr(cache, "enabled", False) and cache.client else None


async def _export_once(thread_id: str, workspace_id: str | None) -> dict[str, int]:
    """Export a thread under its lock when Redis can take it. Returns what
    this caller stored: nothing when it left the export to the lock's holder,
    or its save was refused for a task whose run moved, the copy then being
    the export's that the move asked for."""
    counts = {"stored": 0, "failed": 0}

    async def export() -> None:
        for key, value in (await _export(thread_id, workspace_id)).items():
            counts[key] += value

    cache = _cache()
    lock = f"transcripts:export:{thread_id}"
    flag = f"transcripts:export-again:{thread_id}"
    holder = uuid.uuid4().hex
    while True:
        acquired = None
        if cache is not None:
            try:
                await cache.client.set(flag, "1", ex=_FLAG_TTL_S)
            except Exception as e:
                logger.warning(f"Transcript export of {thread_id} runs unlocked: {e}")
            else:
                # None: Redis failed it, and the client logged why.
                acquired = await cache.acquire_lock(lock, holder, _LOCK_TTL_MS)
                if acquired is False:
                    return counts
        if not acquired:
            await export()
            return counts
        try:
            while await cache.client.delete(flag):
                await export()
        finally:
            await cache.release_lock(lock, holder)
        if not await cache.client.exists(flag):
            return counts


async def export_thread(workspace_id: str, thread_id: str) -> dict[str, int]:
    """Bring one thread's stored transcript up to date, for the backfill."""
    return await _export_once(thread_id, workspace_id)


async def sync_workspace(workspace_id: str) -> dict[str, int]:
    """Render the workspace's threads whose stored copy is behind their
    checkpoint (a turn end whose render failed, a thread from before
    transcripts were stored), newest first. The store is read for all of them
    at once, and each behind thread exported under its lock.
    """
    from src.server.database.thread_transcripts import workspace_checkpoints

    counts = {"stored": 0, "failed": 0}
    if await _target(workspace_id) is None:
        return counts
    # A thread with no stamped checkpoint has not finished a turn; its first
    # turn end exports it.
    behind = await behind_in_store(await workspace_checkpoints(workspace_id))
    gate = asyncio.Semaphore(_EXPORT_CONCURRENCY)

    async def one(thread_id: str) -> None:
        async with gate:
            try:
                for key, value in (await _export_once(thread_id, workspace_id)).items():
                    counts[key] += value
            except Exception as e:
                counts["failed"] += 1
                logger.warning(f"Transcript export failed for thread {thread_id}: {e}")

    await asyncio.gather(*(one(thread.thread_id) for thread in behind))
    return counts


async def save_live(
    transcript: TranscriptTarget, messages: Sequence[AnyMessage], *, window: Window
) -> bool:
    """Store one agent's transcript from messages in hand, ahead of the render
    from the checkpoint: compaction points the model at it mid-turn, before
    the turn end renders it. Only the segments that changed since the stored
    copy render. The copy takes the live fingerprint and the latest checkpoint
    of its agent's source (the thread's, or the task's namespace), or its
    stored one's when that is later. The messages in hand hold everything up
    to that checkpoint, so a render from there (a turn end whose export has
    not landed, an export that read a running task before this compaction)
    leaves the copy in place; the next checkpoint the agent writes still
    renders over it, and only what came after this save. ``window`` was
    trimmed from the head of ``messages``. Returns whether it landed or had
    nothing to change."""
    from src.server.database.conversation import (
        get_thread_by_id,
        get_thread_checkpoint_id,
    )
    from src.server.database.thread_transcripts import LIVE_FINGERPRINT, load_stored
    from src.server.services.history.reader import CheckpointHistoryReader

    thread_id = transcript.thread_id
    thread = await get_thread_by_id(thread_id)
    if not thread or not thread.get("workspace_id"):
        return False
    target = await _target(str(thread["workspace_id"]))
    if target is None:
        return False
    prefix = transcript.prefix
    stored = (await load_stored([thread_id], prefix)).get(thread_id, {}).get(prefix)
    held = load_manifest(stored.manifest) if stored is not None else {}
    header = {k: v for k, v in held.items() if k not in ("schema", "segments")}
    reader = CheckpointHistoryReader.get_instance()
    if transcript.task_id is None:
        header["thread_id"] = thread_id
        # A first turn has no stamp yet, and a copy labelled with none is
        # replaced by any render, one that read the checkpoint before this
        # compaction included. The tip is the label then. Not later: a label
        # past the stamp would stand the turn end's render down with it.
        latest = await get_thread_checkpoint_id(
            thread_id
        ) or await reader.alatest_checkpoint_id(thread_id)
    else:
        header["task_id"] = transcript.task_id
        latest = await reader.alatest_checkpoint_id(thread_id, transcript.checkpoint_ns)
    held_at = stored.checkpoint_id if stored is not None else None
    checkpoint_id = max(filter(None, (held_at, latest)), default=None)
    job = _Job(
        transcript,
        list(messages),
        header,
        LIVE_FINGERPRINT,
        checkpoint_id,
        stored.manifest if stored is not None else None,
        window,
    )
    rendered = await _render_job(job, inline=target.inline)
    if stored is not None and rendered.copy.manifest == stored.manifest:
        return True
    return await _save(target, job, rendered)


_exports: dict[str, asyncio.Task] = {}
_again: set[str] = set()


def schedule_thread_export(thread_id: str, workspace_id: str | None = None) -> None:
    """Export in the background, coalescing a burst (several tasks finishing
    together) into one export plus at most one rerun, and one worker at a
    time. At a turn end, and for a change that lands outside a turn."""
    running = _exports.get(thread_id)
    if running is not None and not running.done():
        _again.add(thread_id)
        return

    async def _run() -> None:
        try:
            while True:
                _again.discard(thread_id)
                await _export_once(thread_id, workspace_id)
                if thread_id not in _again:
                    break
        except Exception as e:
            logger.warning(f"Transcript export failed for thread {thread_id}: {e}")
        finally:
            _exports.pop(thread_id, None)

    _exports[thread_id] = asyncio.create_task(_run())
