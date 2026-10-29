"""Which save of an agent's stored transcript lands, against real PostgreSQL.

A turn end, another worker's turn end and compaction's live save can all
write one agent's copy at once, so the header row's upsert condition decides
the winner in a single statement, and a task's copy checks its run under that
row's lock; only Postgres can show that they do. Each test makes its own
user, workspace and thread and removes them afterwards.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from psycopg.rows import dict_row, tuple_row

from src.server.database.thread_transcripts import (
    LIVE_FINGERPRINT,
    StoredTranscript,
    drop_own_past,
    drop_rewound,
    save_stored,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# Checkpoint ids sort by the time they were taken.
CP_OLD = "1f000000-0000-6000-8000-000000000001"
CP_NEW = "1f000000-0000-6000-8000-000000000002"
TASK = "tasks/k1/"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _copy(
    checkpoint_id: str | None = CP_OLD, *, inline=None, files=None, live=False, run=None
):
    """A stored copy; ``inline`` files carry their bytes, ``files`` only a sha."""
    inline = inline or {}
    listed = {path: (_sha(data), len(data)) for path, data in inline.items()}
    listed.update(files or {})
    return StoredTranscript(
        fingerprint=LIVE_FINGERPRINT if live else f"fp-{checkpoint_id}",
        checkpoint_id=checkpoint_id,
        manifest="{}",
        files=listed,
        inline=dict(inline),
        run=run,
    )


@pytest_asyncio.fixture(loop_scope="session")
async def thread(patched_get_db_connection, test_db_pool):
    from src.server.database.conversation import create_thread
    from src.server.database.user import create_user
    from src.server.database.workspace import create_workspace

    user_id = f"transcript-test-{uuid.uuid4().hex[:12]}"
    await create_user(user_id=user_id, email=f"{user_id}@example.com", name="Test")
    workspace = await create_workspace(user_id=user_id, name="Transcripts", status="running")
    workspace_id = str(workspace["workspace_id"])
    thread_id = str(uuid.uuid4())
    await create_thread(
        conversation_thread_id=thread_id,
        workspace_id=workspace_id,
        current_status="completed",
    )

    async def read(prefix: str = ""):
        """One agent's header row, and every file row of the thread."""
        async with test_db_pool.connection() as conn:
            header = await (
                await conn.execute(
                    "SELECT checkpoint_id, fingerprint FROM thread_transcripts "
                    "WHERE conversation_thread_id = %s AND prefix = %s",
                    (thread_id, prefix),
                )
            ).fetchone()
            rows = await (
                await conn.execute(
                    "SELECT path, sha256, content FROM thread_transcript_files "
                    "WHERE conversation_thread_id = %s",
                    (thread_id,),
                )
            ).fetchall()
        files = {
            r["path"]: (r["sha256"], None if r["content"] is None else bytes(r["content"]))
            for r in rows
        }
        return header, files

    try:
        yield SimpleNamespace(
            id=thread_id, user_id=user_id, workspace_id=workspace_id, read=read
        )
    finally:
        async with test_db_pool.connection() as conn:
            await conn.execute(
                "DELETE FROM conversation_threads WHERE workspace_id = %s", (workspace_id,)
            )
            await conn.execute("DELETE FROM workspaces WHERE workspace_id = %s", (workspace_id,))
            await conn.execute("DELETE FROM users WHERE user_id = %s", (user_id,))


async def _save(thread, copy: StoredTranscript, prefix: str = "") -> bool:
    return await save_stored(thread.id, prefix, thread.user_id, copy)


# -- racing saves --------------------------------------------------------------


async def test_a_save_older_than_the_stored_checkpoint_loses(thread):
    await _save(thread, _copy(CP_NEW, inline={"turn-0001.jsonl": b"new"}))

    assert not await _save(thread, _copy(CP_OLD, inline={"turn-0001.jsonl": b"old"}))

    header, files = await thread.read()
    assert header["checkpoint_id"] == CP_NEW
    assert files["turn-0001.jsonl"][1] == b"new"


@pytest.mark.parametrize("checkpoint_id", [CP_OLD, CP_NEW], ids=["same", "newer"])
async def test_a_save_at_or_after_the_stored_checkpoint_replaces_it(thread, checkpoint_id):
    await _save(thread, _copy(CP_OLD))

    assert await _save(thread, _copy(checkpoint_id, inline={"turn-0001.jsonl": b"x"}))

    header, files = await thread.read()
    assert header["fingerprint"] == f"fp-{checkpoint_id}"
    assert files["turn-0001.jsonl"][1] == b"x"


@pytest.mark.parametrize(
    ("stored", "saved"), [(None, CP_OLD), (CP_NEW, None)], ids=["stored", "saved"]
)
async def test_a_null_checkpoint_on_either_side_lets_the_save_land(thread, stored, saved):
    await _save(thread, _copy(stored))

    assert await _save(thread, _copy(saved))

    header, _ = await thread.read()
    assert header["checkpoint_id"] == saved


# -- compaction's live save ----------------------------------------------------


@pytest.mark.parametrize("prefix", ["", TASK], ids=["own", "task"])
async def test_a_render_at_a_live_copys_checkpoint_leaves_it_in_place(thread, prefix):
    """The live copy holds the messages past its checkpoint: mid-turn the
    thread's is still the last turn's end, and an export can read a running
    task before its compaction saves live."""
    first, second = f"{prefix}turn-0001.jsonl", f"{prefix}turn-0002.jsonl"
    await _save(thread, _copy(inline={first: b"a"}), prefix)
    turn = {first: b"a", second: b"so far"}
    assert await _save(thread, _copy(inline=turn, live=True), prefix)

    assert not await _save(thread, _copy(inline={first: b"a"}), prefix)

    header, files = await thread.read(prefix)
    assert header["fingerprint"] == LIVE_FINGERPRINT
    assert files[second][1] == b"so far"


@pytest.mark.parametrize(
    ("prefix", "live", "replacement"),
    [
        ("", CP_OLD, _copy(CP_OLD, live=True)),
        ("", CP_OLD, _copy(CP_NEW)),
        # A first turn's: the thread has no checkpoint yet, and an export
        # renders its latest one.
        ("", None, _copy(CP_OLD)),
        (TASK, CP_OLD, _copy(CP_NEW)),
    ],
    ids=["another-live-save", "a-newer-render", "first-turn", "a-tasks-newer-render"],
)
async def test_what_replaces_a_live_copy(thread, prefix, live, replacement):
    await _save(thread, _copy(live, live=True), prefix)

    assert await _save(thread, replacement, prefix)

    header, _ = await thread.read(prefix)
    assert header["fingerprint"] == replacement.fingerprint


# -- one agent's copy ----------------------------------------------------------


async def test_a_save_leaves_the_other_agents_alone(thread):
    await _save(thread, _copy(CP_NEW, inline={f"{TASK}run-0001.jsonl": b"task"}), TASK)
    await _save(thread, _copy(CP_OLD, inline={"turn-0001.jsonl": b"a"}))

    # Each agent is ordered on its own: the task's newer checkpoint does not
    # hold the thread's own copy back.
    assert await _save(thread, _copy(CP_OLD, inline={"turn-0002.jsonl": b"b"}))

    task, files = await thread.read(TASK)
    assert task["checkpoint_id"] == CP_NEW
    assert set(files) == {"turn-0002.jsonl", f"{TASK}run-0001.jsonl"}


async def test_a_file_resent_without_bytes_keeps_them_when_its_sha_is_unchanged(thread):
    await _save(thread, _copy(inline={"turn-0001.jsonl": b"kept"}))

    carried = {"turn-0001.jsonl": (_sha(b"kept"), 4)}
    assert await _save(thread, _copy(CP_NEW, files=carried))

    _, files = await thread.read()
    assert files["turn-0001.jsonl"] == (_sha(b"kept"), b"kept")


async def test_a_file_resent_without_bytes_under_a_new_sha_drops_the_old_bytes(thread):
    await _save(thread, _copy(inline={"turn-0001.jsonl": b"before"}))

    changed = {"turn-0001.jsonl": (_sha(b"after"), 5)}
    assert await _save(thread, _copy(CP_NEW, files=changed))

    _, files = await thread.read()
    assert files["turn-0001.jsonl"] == (_sha(b"after"), None)


async def test_files_no_longer_in_the_copy_are_deleted(thread):
    await _save(thread, _copy(inline={"turn-0001.jsonl": b"a", "turn-0002.jsonl": b"b"}))

    assert await _save(thread, _copy(CP_NEW, inline={"turn-0001.jsonl": b"a"}))

    _, files = await thread.read()
    assert set(files) == {"turn-0001.jsonl"}


async def test_a_save_for_a_deleted_thread_returns_false(thread, test_db_pool):
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "DELETE FROM conversation_threads WHERE conversation_thread_id = %s",
            (thread.id,),
        )

    assert not await _save(thread, _copy(inline={"turn-0001.jsonl": b"late"}))


# -- a fork rewinding the thread -----------------------------------------------


@pytest.mark.parametrize(("fork_at", "kept"), [(CP_OLD, False), (CP_NEW, True)])
async def test_a_fork_drops_the_own_copy_rendered_past_it(thread, test_db_pool, fork_at, kept):
    """A copy past the fork shows the turns it discards, and the render at the
    older checkpoint would lose to it; the tasks' copies stay for the render's
    own sweep of the tasks the fork removed."""
    await _save(thread, _copy(CP_NEW, inline={"turn-0002.jsonl": b"discarded"}))
    await _save(thread, _copy(CP_NEW, inline={"run-0001.jsonl": b"task"}), prefix=TASK)

    async with test_db_pool.connection() as conn:
        await drop_rewound(thread.id, fork_at, conn=conn)

    header, files = await thread.read()
    assert (header is not None) is kept
    assert ("turn-0002.jsonl" in files) is kept
    assert (await thread.read(TASK))[0] is not None
    assert await _save(thread, _copy(CP_OLD, inline={"turn-0001.jsonl": b"x"})) is not kept


@pytest.mark.parametrize(("live", "kept"), [(True, False), (False, True)], ids=["live", "rendered"])
async def test_a_fork_drops_a_live_copy_labelled_at_it(thread, test_db_pool, live, kept):
    """A copy saved live holds the messages past its checkpoint, the turns the
    fork discards; one rendered there shows only the turns it keeps."""
    await _save(thread, _copy(CP_OLD, inline={"turn-0002.jsonl": b"so far"}, live=live))

    async with test_db_pool.connection() as conn:
        await drop_rewound(thread.id, CP_OLD, conn=conn)

    header, _ = await thread.read()
    assert (header is not None) is kept


async def _point(test_db_pool, thread, checkpoint_id: str) -> None:
    async with test_db_pool.connection() as conn:
        await conn.execute(
            "UPDATE conversation_threads SET latest_checkpoint_id = %s "
            "WHERE conversation_thread_id = %s",
            (checkpoint_id, thread.id),
        )


async def _admit(thread) -> None:
    from src.server.database.runs.lifecycle import start_run

    await start_run(
        run_id=str(uuid.uuid4()),
        thread_id=thread.id,
        request_key=str(uuid.uuid4()),
        metadata={"msg_type": "ptc"},
    )


@pytest.mark.parametrize("case", ["idle", "run open", "pointer moved"])
async def test_a_copy_past_the_pointer_is_dropped_only_while_nothing_can_be_saving_it(
    thread, test_db_pool, case
):
    """Past an idle pointer the copy shows turns a fork replaced. With a run
    open it may be that run's live save, and a pointer that moved since it
    was read means a turn landed past it."""
    await _point(test_db_pool, thread, CP_NEW if case == "pointer moved" else CP_OLD)
    await _save(thread, _copy(CP_NEW, inline={"turn-0002.jsonl": b"replaced"}))
    if case == "run open":
        await _admit(thread)

    dropped = await drop_own_past(thread.id, CP_OLD)

    header, files = await thread.read()
    assert dropped is (case == "idle")
    assert (header is None) is (case == "idle")
    assert ("turn-0002.jsonl" in files) is (case != "idle")


async def test_a_run_admitted_during_the_drop_keeps_the_copy_it_saves_live(
    thread, test_db_pool, test_db_uri, monkeypatch, tuple_rows
):
    """The drop waits on the stale copy's row while a run is admitted and its
    compaction saves over that row live, labelled past the pointer as a live
    save labels it. That copy is the run's and must survive the drop, which
    read no run open before the run's START committed."""
    import psycopg
    from langchain_core.messages import AIMessage, HumanMessage

    from ptc_agent.agent.transcript import TranscriptTarget, Window
    from src.server.services import transcripts

    monkeypatch.setattr(
        transcripts,
        "_target",
        AsyncMock(return_value=transcripts._Target(thread.workspace_id, thread.user_id, True)),
    )
    # The pointer is set, so a live save never asks the checkpointer.
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: SimpleNamespace(),
    )

    async def save_live() -> None:
        said = [HumanMessage(content="q", id="h-1"), AIMessage(content="so far", id="a-1")]
        assert await transcripts.save_live(TranscriptTarget(thread.id), said, window=Window())

    await _point(test_db_pool, thread, CP_OLD)
    await _save(thread, _copy(CP_NEW, inline={"turn-0002.jsonl": b"replaced"}))

    async def blocked_by(probe, pid: int) -> list[int]:
        rows = await (
            await probe.execute(
                "SELECT pid FROM pg_stat_activity WHERE %s = ANY(pg_blocking_pids(pid))",
                (pid,),
            )
        ).fetchall()
        return [r[0] for r in rows]

    async with (
        await psycopg.AsyncConnection.connect(test_db_uri) as holder,
        await psycopg.AsyncConnection.connect(test_db_uri, autocommit=True) as probe,
    ):
        # A key-share lock holds the drop's delete at the copy's row and lets
        # a save update it, so the run can be admitted and save live while
        # the delete waits.
        await holder.execute(
            "SELECT 1 FROM thread_transcripts "
            "WHERE conversation_thread_id = %s AND prefix = '' FOR KEY SHARE",
            (thread.id,),
        )
        drop = asyncio.create_task(drop_own_past(thread.id, CP_OLD))
        for _ in range(500):
            waiting = await blocked_by(probe, holder.info.backend_pid)
            if waiting:
                break
            await asyncio.sleep(0.01)
        assert len(waiting) == 1, "the drop never reached the copy's row"

        admit = asyncio.create_task(_admit(thread))
        for _ in range(500):
            if admit.done() or await blocked_by(probe, waiting[0]):
                break
            await asyncio.sleep(0.01)
        saved = admit.done()
        if saved:
            await admit
            await save_live()
        await holder.rollback()
        await drop
        await admit
        if not saved:
            await save_live()

    header, files = await thread.read()
    assert header is not None and header["fingerprint"] == LIVE_FINGERPRINT
    assert "turn-0002.jsonl" not in files


# -- serving ---------------------------------------------------------------------


@pytest.fixture
def tuple_rows(monkeypatch, test_db_pool):
    """The readers index rows as the app pool returns them, not as the test
    pool's dict rows."""

    @asynccontextmanager
    async def connection():
        async with test_db_pool.connection() as conn:
            conn.row_factory = tuple_row
            try:
                yield conn
            finally:
                conn.row_factory = dict_row

    monkeypatch.setattr(
        "src.server.database.thread_transcripts.get_db_connection", connection
    )


async def test_a_directory_two_threads_share_serves_neither(thread, tuple_rows):
    from src.server.database.conversation import create_thread
    from src.server.database.thread_transcripts import (
        list_transcript,
        list_transcript_tree,
        load_transcript_file,
    )

    short_id, segment = thread.id[:8], "turn-0001.jsonl"
    await _save(thread, _copy(inline={segment: b"mine"}))
    served = await load_transcript_file(thread.workspace_id, short_id, segment)
    assert served is not None and served.content == b"mine"
    assert [f.thread_id for f in await list_transcript_tree(thread.workspace_id, None, 10)] == [
        thread.id
    ]

    twin = SimpleNamespace(id=f"{short_id}-ffff-4fff-bfff-ffffffffffff", user_id=thread.user_id)
    await create_thread(
        conversation_thread_id=twin.id,
        workspace_id=thread.workspace_id,
        current_status="completed",
    )
    await _save(twin, _copy(inline={segment: b"theirs"}))

    assert await list_transcript(thread.workspace_id, short_id) is None
    assert await load_transcript_file(thread.workspace_id, short_id, segment) is None
    assert await list_transcript_tree(thread.workspace_id, None, 10) == []
    assert await list_transcript_tree(thread.workspace_id, short_id, 10) == []


# -- a task's run moving under its render --------------------------------------


@pytest.mark.parametrize("move", ["finished", "resumed", "deleted"])
async def test_a_tasks_copy_lands_only_while_its_run_reads_as_rendered(
    thread, test_db_pool, move
):
    """A task's header comes from its latest run, which moves without a new
    checkpoint, so a render at the stored checkpoint can still be stale: the
    save checks the run again under the header row's lock."""
    from src.server.database.runs import subagent_runs
    from src.server.database.thread_transcripts import TaskRun

    first = str(uuid.uuid4())
    await subagent_runs.start_task_run(
        task_run_id=first, thread_id=thread.id, task_id="k1", cause="init"
    )
    read = TaskRun("k1", first, "in_progress", None)
    segment = f"{TASK}run-0001.jsonl"
    assert await _save(thread, _copy(inline={segment: b"running"}, run=read), TASK)

    await subagent_runs.finalize_task_run(
        task_run_id=first, status="completed", final_checkpoint_id=CP_OLD
    )
    if move == "resumed":
        await subagent_runs.start_task_run(
            task_run_id=str(uuid.uuid4()),
            thread_id=thread.id,
            task_id="k1",
            cause="resume",
            predecessor_run_id=first,
        )
    elif move == "deleted":
        # As a truncation's repair drops a task left with no run.
        async with test_db_pool.connection() as conn:
            await conn.execute("DELETE FROM subagent_tasks WHERE thread_id = %s", (thread.id,))
    stale = _copy(inline={segment: b"stale"}, run=read)
    stale.fingerprint = "stale"

    assert not await _save(thread, stale, TASK)

    header, files = await thread.read(TASK)
    assert header["fingerprint"] == f"fp-{CP_OLD}"
    assert files[segment][1] == b"running"


# -- exports overlapping -------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session")
async def redis_cache():
    """The cache client on a real Redis at ``REDIS_URL``, or None without one."""
    from src.utils.cache.redis_cache import RedisCacheClient

    client = RedisCacheClient(
        url=os.getenv("REDIS_URL", "redis://localhost:6379/0"), max_connections=4
    )
    try:
        await client.connect()
    except Exception:
        yield None
        return
    try:
        yield client if client.enabled and client.client else None
    finally:
        await client.disconnect()


class _Unreachable:
    """A worker's Redis failing every call, while another worker's answers."""

    enabled = True

    def __init__(self) -> None:
        self.client = self

    async def set(self, *args, **kwargs):
        raise ConnectionError("Redis is unreachable")


@pytest.mark.parametrize("overlap", ["no-redis", "one-without-redis", "lock-expired"])
async def test_a_task_finishing_mid_export_ends_stored_finished(
    thread, tuple_rows, monkeypatch, redis_cache, overlap
):
    """One export reads a task running; the task finishes, and the export its
    finish asks for renders it finished at the same checkpoint and saves
    first. Nothing keeps the two apart when Redis is down for either, or the
    first outlives its lock, so the first's save is refused instead: the run
    it read is no longer the task's. Refused is not failed, since the copy
    stored is current."""
    from langchain_core.messages import AIMessage, HumanMessage

    from ptc_agent.agent.transcript import TranscriptTarget, load_manifest
    from src.server.database.runs import subagent_runs
    from src.server.database.thread_transcripts import load_stored
    from src.server.services import transcripts

    lock = f"transcripts:export:{thread.id}"
    while_first_reads = None
    if overlap == "no-redis":
        caches = [None, None]
    elif redis_cache is None:
        pytest.skip("Redis is not reachable at REDIS_URL")
    elif overlap == "one-without-redis":
        caches = [redis_cache, _Unreachable()]
    else:
        caches = [redis_cache, redis_cache]
        monkeypatch.setattr(transcripts, "_LOCK_TTL_MS", 200)

        async def while_first_reads():
            async with asyncio.timeout(5):
                while await redis_cache.client.exists(lock):
                    await asyncio.sleep(0.05)

    run_id = str(uuid.uuid4())
    await subagent_runs.start_task_run(
        task_run_id=run_id, thread_id=thread.id, task_id="k1", cause="init"
    )
    await _save(thread, StoredTranscript(transcripts._fingerprint(CP_OLD), CP_OLD, "{}"))
    first_reading, first_may_go = asyncio.Event(), asyncio.Event()
    reads = []

    class Reader:
        async def aget_task_history(self, thread_id, task_id):
            reads.append(task_id)
            if len(reads) == 1:
                first_reading.set()
                await first_may_go.wait()
            messages = [HumanMessage("go"), AIMessage("done")]
            return SimpleNamespace(messages=messages, checkpoint_id=CP_NEW)

    target = transcripts._Target(thread.workspace_id, thread.user_id, True)
    monkeypatch.setattr(transcripts, "_cache", lambda: caches.pop(0))
    monkeypatch.setattr(transcripts, "_target", AsyncMock(return_value=target))
    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_checkpoint_id",
        AsyncMock(return_value=CP_OLD),
    )
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: Reader(),
    )

    first = asyncio.create_task(transcripts.export_thread(thread.workspace_id, thread.id))
    await first_reading.wait()
    if while_first_reads is not None:
        await while_first_reads()
    await subagent_runs.finalize_task_run(
        task_run_id=run_id, status="completed", final_checkpoint_id=CP_NEW
    )
    second = asyncio.create_task(transcripts.export_thread(thread.workspace_id, thread.id))
    # The second saves the finish while the first is still reading.
    assert (await asyncio.wait({second}, timeout=5))[0]
    first_may_go.set()
    counts = await asyncio.gather(first, second)

    prefix = TranscriptTarget(thread.id, "k1").prefix
    stored = (await load_stored([thread.id], prefix))[thread.id][prefix]
    assert load_manifest(stored.manifest)["status"] == "completed"
    assert not await transcripts.behind_in_store([(thread.id, CP_OLD)])
    assert counts == [{"stored": 0, "failed": 0}, {"stored": 1, "failed": 0}]
