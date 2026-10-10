"""Which agents get rendered into the store, and what a render keeps.

Rendering reads the whole checkpoint, so an agent whose stored copy is current
is never rendered again, a render older than the stored copy stands down
before it reads, and a task finishing reads its own run, not the thread's.
Every export, a bring-up's and the backfill's among them, takes the thread's
lock, and one agent is read, rendered and saved at a time.
A save writes only the rows that changed. Compaction's live save replaces only
its own agent's copy and leaves a fingerprint that makes the next render from
the checkpoint redo it.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.transcript import TranscriptTarget, Window, load_manifest
from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from src.server.database.conversation import ThreadPrefixes
from src.server.database.thread_transcripts import (
    StaleCopy,
    StoredTranscript,
    _changed_rows,
)
from src.server.services import transcripts
from src.server.services.computer_manager import _bringup, _thread_dirs
from src.server.services.livefs.history import index_content
from src.server.services.transcripts import (
    INLINE_FILE_MAX_BYTES,
    Behind,
    _fingerprint,
    _Job,
    _render,
    _Target,
)

ROOT = "/home/workspace"
LAYOUT = SandboxLayout.for_root(ROOT).for_workspace("research")
T1 = "11111111-0000-0000-0000-000000000000"
T2 = "22222222-0000-0000-0000-000000000000"
FP = _fingerprint("cp-1")
NOW = datetime(2026, 9, 26, tzinfo=timezone.utc)


class _Runtime:
    def __init__(self, stdout: str = "", *, rm_exit: int = 0) -> None:
        self.stdout = stdout
        self.rm_exit = rm_exit
        self.commands: list[str] = []

    async def exec(self, command: str, timeout: int = 60):
        self.commands.append(command)
        # The delete after the fence is one line; the listing opens with its sweep.
        if "rm -rf" in command and "\n" not in command:
            return SimpleNamespace(stdout="", stderr="", exit_code=self.rm_exit)
        if "mktemp" in command:
            return SimpleNamespace(stdout=_thread_dirs._SET_ASIDE_END, exit_code=0)
        return SimpleNamespace(stdout=self.stdout, exit_code=0)


def _target(*, inline: bool = False) -> _Target:
    return _Target("ws-1", "user-1", inline)


@pytest.fixture(autouse=True)
def folder(monkeypatch):
    """Where the workspace's folder is when a step reads it under its hold."""
    state = SimpleNamespace(layout=LAYOUT)

    @asynccontextmanager
    async def held(workspace_id, root):
        yield state.layout

    monkeypatch.setattr(
        "src.server.services.workspace_layout.held_workspace_layout", held
    )
    monkeypatch.setattr(_bringup, "held_workspace_layout", held)
    return state


def _stored(checkpoint_id: str = "cp-1", manifest: str = "{}", **files) -> StoredTranscript:
    return StoredTranscript(
        fingerprint=_fingerprint(checkpoint_id),
        checkpoint_id=checkpoint_id,
        manifest=manifest,
        files={
            name.replace("__", "/"): (sha, 10)
            for name, sha in (files or {"turn-0001.jsonl": "a" * 64}).items()
        },
    )


# -- which agents render -------------------------------------------------------


class _Cache:
    """The shared cache client: its lock, and the raw keys the rerun flag uses."""

    enabled = True

    def __init__(self) -> None:
        self.keys: dict[str, str] = {}
        self.client = self

    async def set(self, key, value, ex=None):
        self.keys[key] = value

    async def delete(self, key):
        return int(self.keys.pop(key, None) is not None)

    async def exists(self, key):
        return int(key in self.keys)

    async def acquire_lock(self, key, token, ttl_ms):
        return self.keys.setdefault(key, token) == token

    async def release_lock(self, key, token):
        if self.keys.get(key) == token:
            del self.keys[key]


@pytest.fixture
def store():
    state = SimpleNamespace(
        in_store={}, tasks=[], renders=[], cache=_Cache(), during_render=None
    )

    async def render(target, thread, stored):
        state.renders.append((thread.thread_id, thread.agents))
        if state.during_render is not None:
            await state.during_render()
        return True

    async def stored_fingerprints(ids):
        return {t: fp for t, fp in state.in_store.items() if t in ids}

    async def load_stored(ids, prefix=None):
        return {}

    with (
        patch.object(transcripts, "_render", render),
        patch.object(transcripts, "_target", AsyncMock(return_value=_target())),
        patch(
            "src.server.database.conversation.get_thread_checkpoint_id",
            AsyncMock(return_value="cp-1"),
        ),
        patch("src.utils.cache.redis_cache.get_cache_client", lambda: state.cache),
        patch(
            "src.server.database.thread_transcripts.stored_fingerprints",
            stored_fingerprints,
        ),
        patch("src.server.database.thread_transcripts.load_stored", load_stored),
        patch(
            "src.server.database.runs.subagent_runs.list_thread_tasks",
            AsyncMock(side_effect=lambda ids: state.tasks),
        ),
    ):
        yield state


def _workspace_threads(*threads):
    return patch(
        "src.server.database.thread_transcripts.workspace_checkpoints",
        AsyncMock(return_value=list(threads)),
    )


@pytest.mark.asyncio
async def test_a_thread_current_in_the_store_costs_nothing(store):
    store.in_store[T1] = {"": FP}
    counts = await transcripts.export_thread("ws-1", T1)
    assert counts == {"stored": 0, "failed": 0}
    assert store.renders == []


@pytest.mark.asyncio
async def test_the_sync_exports_only_the_threads_the_store_is_behind_on(store):
    store.in_store[T1] = {"": FP}
    store.in_store[T2] = {"": "stale"}
    with _workspace_threads((T1, "cp-1"), (T2, "cp-1")):
        counts = await transcripts.sync_workspace("ws-1")
    assert store.renders == [(T2, {""})]
    assert counts == {"stored": 1, "failed": 0}
    assert store.cache.keys == {}


@pytest.mark.asyncio
async def test_a_sync_leaves_a_thread_whose_export_is_held_to_its_holder(store):
    """Another worker is rendering the thread: the sync renders nothing over
    it, and the flag it raises makes the holder run again."""
    store.in_store[T1] = {"": "stale"}
    store.cache.keys[f"transcripts:export:{T1}"] = "another-worker"
    with _workspace_threads((T1, "cp-1")):
        counts = await transcripts.sync_workspace("ws-1")
    assert store.renders == []
    assert counts == {"stored": 0, "failed": 0}
    assert f"transcripts:export-again:{T1}" in store.cache.keys


@pytest.mark.asyncio
async def test_an_export_asked_for_mid_render_runs_again_under_the_same_hold(store):
    store.in_store[T1] = {"": "stale"}
    asked = []

    async def ask_again():
        if not asked:
            asked.append(await transcripts.export_thread("ws-1", T1))

    store.during_render = ask_again
    counts = await transcripts.export_thread("ws-1", T1)

    assert asked == [{"stored": 0, "failed": 0}]
    assert [thread_id for thread_id, _ in store.renders] == [T1, T1]
    assert counts == {"stored": 2, "failed": 0}
    assert store.cache.keys == {}


@pytest.mark.asyncio
async def test_a_task_that_moved_renders_that_task_alone(store):
    task = {"thread_id": T1, "task_id": "k1", "latest_run_id": "r2", "status": "done"}
    store.tasks = [task]
    store.in_store[T1] = {"": FP, "tasks/k1/": "run r1"}
    await transcripts.export_thread("ws-1", T1)
    assert store.renders == [(T1, {"tasks/k1/"})]


@pytest.mark.asyncio
async def test_the_copy_of_a_task_a_truncation_deleted_is_dropped(monkeypatch, store):
    store.in_store[T1] = {"": FP, "tasks/k9/": "run r1"}
    [behind] = await transcripts.behind_in_store([(T1, "cp-1")])
    assert behind.agents == set() and behind.gone == {"tasks/k9/"}

    state = SimpleNamespace(
        config={"configurable": {"checkpoint_id": "cp-1"}},
        values={"messages": [_message("hi")]},
    )
    reader = SimpleNamespace(aget_state=AsyncMock(return_value=state))
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    delete = AsyncMock()
    monkeypatch.setattr("src.server.database.thread_transcripts.delete_stored", delete)
    await _render(_target(), behind, {})
    delete.assert_awaited_once_with(T1, {"tasks/k9/"})


@pytest.mark.asyncio
async def test_a_render_older_than_the_stored_copy_stands_down_before_reading(monkeypatch):
    reader = SimpleNamespace(aget_state=AsyncMock())
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    save = AsyncMock()
    monkeypatch.setattr("src.server.database.thread_transcripts.save_stored", save)

    saved = await transcripts._render(
        _target(), Behind(T1, "cp-1", {}, {""}, set()), {"": _stored(checkpoint_id="cp-2")}
    )
    assert saved is False
    reader.aget_state.assert_not_awaited()
    save.assert_not_called()


@pytest.mark.asyncio
async def test_a_finished_task_renders_from_its_run_alone(monkeypatch):
    """The thread's own copy is current, so its checkpoint is not read, and
    the meta names the call that launched the run."""
    task = {
        "thread_id": T1,
        "task_id": "k1",
        "latest_run_id": "r1",
        "status": "completed",
        "launch_tool_call_id": "call-1",
    }
    meta = '{"schema": 2, "task_id": "k1", "launch_call_id": "call-1"}'
    reader = SimpleNamespace(
        aget_state=AsyncMock(),
        aget_task_history=AsyncMock(
            return_value=SimpleNamespace(messages=[_message("go")], checkpoint_id="task-cp-1")
        ),
    )
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    saves = []

    async def save_stored(thread_id, prefix, user_id, copy):
        saves.append((prefix, copy))
        return True

    monkeypatch.setattr("src.server.database.thread_transcripts.save_stored", save_stored)

    behind = Behind(T1, "cp-1", {"k1": task}, {"tasks/k1/"}, set())
    stored = {"": _stored(), "tasks/k1/": _stored(manifest=meta)}
    assert await _render(_target(inline=True), behind, stored)

    reader.aget_state.assert_not_awaited()
    [(prefix, copy)] = saves
    assert prefix == "tasks/k1/"
    assert load_manifest(copy.manifest)["launch_call_id"] == "call-1"
    # Ordered by its own run, not the thread's checkpoint beside it.
    assert copy.checkpoint_id == "task-cp-1"


@pytest.mark.asyncio
async def test_a_threads_agents_are_read_and_saved_one_at_a_time(monkeypatch):
    """A long thread's task histories are never all in memory at once."""
    order = []
    tasks = {
        k: {"thread_id": T1, "task_id": k, "latest_run_id": "r1", "status": "done"}
        for k in ("k1", "k2")
    }

    async def aget_state(thread_id, checkpoint_id):
        order.append("read own")
        return SimpleNamespace(
            config={"configurable": {"checkpoint_id": "cp-1"}},
            values={"messages": [_message("hi")]},
        )

    async def aget_task_history(thread_id, task_id):
        order.append(f"read {task_id}")
        return SimpleNamespace(messages=[_message(task_id)], checkpoint_id="task-cp-1")

    async def save_stored(thread_id, prefix, user_id, copy):
        order.append(f"save {prefix or 'own'}")
        return True

    reader = SimpleNamespace(aget_state=aget_state, aget_task_history=aget_task_history)
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    monkeypatch.setattr("src.server.database.thread_transcripts.save_stored", save_stored)

    behind = Behind(T1, "cp-1", tasks, {"", "tasks/k1/", "tasks/k2/"}, set())
    assert await _render(_target(inline=True), behind, {})
    assert order == [
        "read own",
        "save own",
        "read k1",
        "save tasks/k1/",
        "read k2",
        "save tasks/k2/",
    ]


# -- what a save writes --------------------------------------------------------


def _copy(**files: str) -> StoredTranscript:
    return StoredTranscript(
        "fp", "cp-1", "{}", files={path: (sha * 64, 1) for path, sha in files.items()}
    )


def test_a_save_writes_only_the_rows_that_changed():
    copy = _copy(**{"turn-0001.jsonl": "a", "turn-0002.jsonl": "b", "turn-0003.jsonl": "c"})
    copy.carried = {"turn-0001.jsonl"}
    copy.inline = {"turn-0002.jsonl": b"b", "turn-0003.jsonl": b"c"}
    held = {
        "turn-0001.jsonl": ("a" * 64, "user-1", False),
        "turn-0002.jsonl": ("b" * 64, "user-1", True),
        "turn-0003.jsonl": ("x" * 64, "user-1", True),
    }
    rows = _changed_rows(T1, "", "user-1", copy, held)
    assert [row[2] for row in rows] == ["turn-0003.jsonl"]


def test_a_carried_file_whose_row_moved_is_a_stale_copy():
    copy = _copy(**{"turn-0001.jsonl": "a"})
    copy.carried = {"turn-0001.jsonl"}
    held = {"turn-0001.jsonl": ("z" * 64, "user-1", True)}
    with pytest.raises(StaleCopy):
        _changed_rows(T1, "", "user-1", copy, held)


def _job(messages) -> _Job:
    return _Job(TranscriptTarget(T1), messages, {"thread_id": T1}, FP, "cp-1", None, Window())


def _big():
    from langchain_core.messages import AIMessage

    return AIMessage(content="x" * (INLINE_FILE_MAX_BYTES + 1), id="m-big")


def test_small_files_ride_the_rows_even_with_object_storage():
    rendered = _job([_message("hi"), _message("again"), _big()]).render(inline=False)
    assert set(rendered.copy.inline) == {"manifest.json", "turn-0001.jsonl"}
    [(sha, data)] = rendered.blobs.items()
    assert rendered.copy.files["turn-0002.jsonl"] == (sha, len(data))


def test_without_object_storage_every_file_rides_the_rows():
    rendered = _job([_message("hi"), _big()]).render(inline=True)
    assert rendered.blobs == {}
    assert rendered.copy.inline.keys() == rendered.copy.files.keys()


# -- the index -----------------------------------------------------------------


def _row(thread_id: str, *, stored: bool = True, dir_name: str | None = "research"):
    return {
        "conversation_thread_id": thread_id,
        "title": "t",
        "workspace_id": "ws-1",
        "workspace_name": "Research",
        "dir_name": dir_name,
        "created_at": NOW,
        "updated_at": NOW,
        "has_transcript": stored,
    }


def test_the_index_names_a_path_only_for_a_stored_transcript():
    content = index_content(
        ROOT,
        [_row(T1), _row(T2, stored=False), _row("33333333-0000", dir_name=None)],
    )
    lines = content.splitlines()
    assert len(lines) == 2
    assert f'"transcript": "{LAYOUT.transcripts}/{T1[:8]}"' in lines[0]
    assert '"transcript": null' in lines[1]


def test_the_index_names_no_transcript_two_threads_share_a_directory_for():
    twin = T1[:8] + "-ffff-ffff-ffff-ffffffffffff"
    elsewhere = T1[:8] + "-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    content = index_content(
        ROOT,
        [
            _row(T1),
            _row(twin),
            _row(T2),
            _row(elsewhere, dir_name="other") | {"workspace_id": "ws-2"},
        ],
    )
    served = ['"transcript": null' not in line for line in content.splitlines()]
    assert served == [False, False, True, True]


# -- the sync at bring-up ------------------------------------------------------


def _prefixes(*open_ids: str, archived: tuple[str, ...] = ()) -> ThreadPrefixes:
    return ThreadPrefixes(all=frozenset((*open_ids, *archived)), open=frozenset(open_ids))


def _thread_reads(monkeypatch, plain: ThreadPrefixes, fenced: ThreadPrefixes | None = None) -> None:
    """The plain read and the fenced one, which a run admitted in between can
    make differ."""
    monkeypatch.setattr(
        "src.server.database.conversation.get_workspace_thread_prefixes",
        AsyncMock(return_value=plain),
    )

    @asynccontextmanager
    async def fenced_read(_workspace_id):
        yield plain if fenced is None else fenced

    monkeypatch.setattr(
        "src.server.database.conversation.fenced_workspace_thread_prefixes", fenced_read
    )


def _set_aside(runtime) -> list[str]:
    """The dirs the prune's set-aside script moves, in order."""
    (script,) = [c for c in runtime.commands if "mktemp" in c]
    return [line.split(" ")[1] for line in script.splitlines() if line.startswith("move ")]


PRUNED = _thread_dirs._remove_set_aside(LAYOUT)


# The listing leads each dir's line with the base it sits under.
D = WorkspaceLayout.THREADS_DIR
R = WorkspaceLayout.LARGE_TOOL_RESULTS_DIR
S = WorkspaceLayout.SCRATCHPAD_DIR


@pytest.mark.asyncio
async def test_the_prune_clears_dead_dirs(monkeypatch):
    runtime = _Runtime(stdout=f"{D} {T1[:8]}\n{D} 33333333\n{R} 33333333\n")
    _thread_reads(monkeypatch, _prefixes(T1[:8], T2[:8]))

    live, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")

    assert live.all == {T1[:8], T2[:8]}
    assert pruned
    assert _set_aside(runtime) == [
        f"{LAYOUT.threads}/33333333",
        f"{LAYOUT.large_tool_results}/33333333",
    ]
    assert runtime.commands[-1] == PRUNED


@pytest.mark.asyncio
async def test_a_dir_judged_dead_before_the_fence_stays_if_a_run_took_it_since(monkeypatch):
    """The plain read only decides whether to take the fence: a turn admitted
    on the archived thread before it is live under the fenced read."""
    runtime = _Runtime(stdout=f"{S} {T2[:8]}\n")
    _thread_reads(
        monkeypatch,
        _prefixes(T1[:8], archived=(T2[:8],)),
        fenced=_prefixes(T1[:8], T2[:8]),
    )

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")

    assert pruned
    assert len(runtime.commands) == 1


@pytest.mark.asyncio
async def test_nothing_dead_takes_no_fence(monkeypatch):
    runtime = _Runtime(stdout=f"{D} {T1[:8]}\n{S} {T1[:8]}\n")
    monkeypatch.setattr(
        "src.server.database.conversation.get_workspace_thread_prefixes",
        AsyncMock(return_value=_prefixes(T1[:8])),
    )
    fenced = AsyncMock(side_effect=AssertionError("fenced"))
    monkeypatch.setattr(
        "src.server.database.conversation.fenced_workspace_thread_prefixes", fenced
    )

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")

    assert pruned
    fenced.assert_not_called()


@pytest.mark.asyncio
async def test_an_archived_thread_keeps_its_scratch_and_loses_its_scratchpad(monkeypatch):
    runtime = _Runtime(stdout=f"{D} {T2[:8]}\n{R} {T2[:8]}\n{S} {T1[:8]}\n{S} {T2[:8]}\n")
    _thread_reads(monkeypatch, _prefixes(T1[:8], archived=(T2[:8],)))

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")

    assert pruned
    assert _set_aside(runtime) == [f"{LAYOUT.scratchpad}/{T2[:8]}"]
    assert runtime.commands[-1] == PRUNED


@pytest.mark.asyncio
async def test_a_prune_whose_removal_failed_says_so(monkeypatch):
    runtime = _Runtime(stdout=f"{D} 33333333\n", rm_exit=-1)
    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    live, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")

    assert live.all == {T1[:8]}
    assert not pruned


class _Shell:
    """A sandbox runtime that runs each script in a local shell."""

    async def exec(self, script, timeout=60):
        import subprocess

        ran = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
        return SimpleNamespace(stdout=ran.stdout, stderr=ran.stderr, exit_code=ran.returncode)


@pytest.mark.asyncio
async def test_the_set_aside_and_the_removal_run_in_a_real_shell(monkeypatch, tmp_path):
    """Both bases hold a dir of the same name, so each needs its own slot in
    the set-aside folder; the live thread's dirs stay where they are."""
    from pathlib import Path

    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    for path in (
        layout.join(D, T1[:8], "keep.txt"),
        layout.join(D, "33333333", "a.txt"),
        layout.join(R, "33333333", "b.txt"),
    ):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("x")

    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(_Shell(), layout, "ws-1")

    assert pruned
    assert Path(layout.join(D, T1[:8], "keep.txt")).exists()
    assert not Path(layout.join(D, "33333333")).exists()
    assert not Path(layout.join(R, "33333333")).exists()
    assert not list(Path(layout.agents).glob(".pruned.*"))


@pytest.mark.asyncio
async def test_what_an_earlier_prune_could_not_delete_goes_with_the_next_listing(
    monkeypatch, tmp_path
):
    """With nothing newly dead no set-aside runs, and no later judgment would
    find the folder a failed delete left: one the agent made read-only, here."""
    from pathlib import Path

    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    left = Path(layout.agents, ".pruned.abc123", "0", "note.md")
    left.parent.mkdir(parents=True)
    left.write_text("x")
    left.parent.chmod(0o500)
    Path(layout.join(D, T1[:8])).mkdir(parents=True)

    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(_Shell(), layout, "ws-1")

    assert pruned
    assert not list(Path(layout.agents).glob(".pruned.*"))
    assert Path(layout.join(D, T1[:8])).is_dir()


@pytest.mark.asyncio
async def test_a_set_aside_that_starts_too_late_moves_nothing(monkeypatch, tmp_path):
    """A timed-out exec may still run once the fence is gone, when a turn
    admitted since could be writing in the dir it would move."""
    from pathlib import Path

    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    dead = Path(layout.join(D, "33333333", "a.txt"))
    dead.parent.mkdir(parents=True)
    dead.write_text("x")
    monkeypatch.setattr(_thread_dirs, "_SET_ASIDE_MOVE_S", -5)
    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(_Shell(), layout, "ws-1")

    assert not pruned
    assert dead.exists()
    assert not list(Path(layout.agents).glob(".pruned.*"))


@pytest.mark.asyncio
async def test_a_set_aside_that_stalls_past_its_deadline_moves_nothing_more(
    monkeypatch, tmp_path
):
    """Started in time is not enough: a stalled exec runs on once the fence
    is gone, so each move looks at the clock again."""
    import shutil
    from pathlib import Path

    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    dead = [Path(layout.join(base, "33333333", "a.txt")) for base in (D, R)]
    for path in dead:
        path.parent.mkdir(parents=True)
        path.write_text("x")
    # The clock reads true for the start check and the first move, then
    # jumps past the deadline, as it would after a stall.
    bin_dir, calls = tmp_path / "bin", tmp_path / "date-calls"
    bin_dir.mkdir()
    (bin_dir / "date").write_text(
        f'#!/bin/sh\nn=$(cat {calls} 2>/dev/null || echo 0)\necho $((n + 1)) > {calls}\n'
        f'[ "$n" -lt 2 ] && exec {shutil.which("date")} "$@"\necho 9999999999\n'
    )
    (bin_dir / "date").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    _, pruned = await _thread_dirs.prune_dead_thread_dirs(_Shell(), layout, "ws-1")

    assert not pruned
    assert [path.exists() for path in dead].count(True) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["raised", "no word of its end"])
async def test_the_fence_outlasts_a_set_aside_that_may_still_be_running(monkeypatch, ending):
    """An exec that fails early can leave the script running in the sandbox
    until its deadline, so admission stays fenced through the timeout."""
    import time

    monkeypatch.setattr(_thread_dirs, "_SET_ASIDE_TIMEOUT_S", 0.3)
    live = _prefixes(T1[:8])
    monkeypatch.setattr(
        "src.server.database.conversation.get_workspace_thread_prefixes",
        AsyncMock(return_value=live),
    )
    held = {}

    @asynccontextmanager
    async def fenced_read(_workspace_id):
        held["from"] = time.monotonic()
        try:
            yield live
        finally:
            held["for"] = time.monotonic() - held["from"]

    monkeypatch.setattr(
        "src.server.database.conversation.fenced_workspace_thread_prefixes", fenced_read
    )

    class _Failing(_Runtime):
        async def exec(self, command, timeout=60):
            if "mktemp" not in command:
                return await super().exec(command, timeout)
            if ending == "raised":
                raise ConnectionError("exec stream closed")
            return SimpleNamespace(stdout="", stderr="", exit_code=-1)

    runtime = _Failing(stdout=f"{D} 33333333\n")
    if ending == "raised":
        with pytest.raises(ConnectionError):
            await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")
    else:
        _, pruned = await _thread_dirs.prune_dead_thread_dirs(runtime, LAYOUT, "ws-1")
        assert not pruned

    assert held["for"] >= 0.3


@pytest.mark.asyncio
@pytest.mark.skipif(os.geteuid() == 0, reason="root deletes past any mode")
async def test_a_sweep_that_cannot_finish_leaves_the_workspace_unpruned(monkeypatch, tmp_path):
    """The set-aside is still on disk, so no bring-up may stamp the workspace
    done and stop coming back to it."""
    from pathlib import Path

    layout = SandboxLayout.for_root(str(tmp_path)).for_workspace("My Workspace")
    left = Path(layout.agents, ".pruned.abc123")
    Path(left, "0").mkdir(parents=True)
    Path(left, "0", "note.md").write_text("x")
    Path(layout.agents).chmod(0o500)
    _thread_reads(monkeypatch, _prefixes(T1[:8]))
    try:
        _, pruned = await _thread_dirs.prune_dead_thread_dirs(_Shell(), layout, "ws-1")
    finally:
        Path(layout.agents).chmod(0o755)

    assert not pruned
    assert left.exists()


@pytest.mark.asyncio
async def test_a_deleted_threads_dirs_leave_a_running_machine(monkeypatch):
    from src.server.services.computer_manager._bringup import BringUpMixin

    runtime = _Runtime(stdout=f"{D} {T1[:8]}\n{D} {T2[:8]}\n{R} {T2[:8]}\n")

    @asynccontextmanager
    async def computer_runtime(computer, sandbox_id):
        assert sandbox_id == "sb-1"
        yield runtime

    manager = SimpleNamespace(_computer_runtime=computer_runtime)
    monkeypatch.setattr(
        _bringup,
        "get_computer_for_workspace",
        AsyncMock(
            return_value={
                "computer_id": "c-1",
                "provider_ref": "sb-1",
                "status": "running",
                "root_dir": ROOT,
            }
        ),
    )
    _thread_reads(monkeypatch, _prefixes(T1[:8]))

    await BringUpMixin.prune_thread_dirs_if_running(manager, "ws-1")

    assert _set_aside(runtime) == [
        f"{LAYOUT.threads}/{T2[:8]}",
        f"{LAYOUT.large_tool_results}/{T2[:8]}",
    ]
    assert runtime.commands[-1] == PRUNED


@pytest.mark.asyncio
@pytest.mark.parametrize(("archived_at", "pruned"), [(datetime.now(timezone.utc), True), (None, False)])
async def test_a_run_end_prunes_only_an_archived_thread(monkeypatch, archived_at, pruned):
    import uuid

    from src.server.services.computer_manager._bringup import BringUpMixin

    workspace_id = uuid.uuid4()
    manager = SimpleNamespace(prune_thread_dirs_if_running=AsyncMock())
    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": workspace_id, "archived_at": archived_at}),
    )

    await BringUpMixin._prune_if_archived(manager, T1)

    assert manager.prune_thread_dirs_if_running.call_args_list == (
        [((str(workspace_id),),)] if pruned else []
    )


# -- compaction's live save ----------------------------------------------------


def _message(text: str):
    from langchain_core.messages import HumanMessage

    return HumanMessage(content=text, id=f"m-{text}")


@pytest.fixture
def live(monkeypatch):
    """One thread's stored agents by prefix, and the saves that land."""
    state = SimpleNamespace(stored={}, saves=[])

    async def load_stored(ids, prefix=None):
        return {T1: {p: s for p, s in state.stored.items() if prefix in (None, p)}}

    async def save_stored(thread_id, prefix, user_id, copy):
        state.saves.append((prefix, copy))
        return True

    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_by_id",
        AsyncMock(return_value={"workspace_id": "ws-1"}),
    )
    state.stamped = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.server.database.conversation.get_thread_checkpoint_id", state.stamped
    )
    state.tip = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: SimpleNamespace(
            alatest_checkpoint_id=state.tip,
        ),
    )
    monkeypatch.setattr(
        transcripts, "_target", AsyncMock(return_value=_target(inline=True))
    )
    monkeypatch.setattr("src.server.database.thread_transcripts.load_stored", load_stored)
    monkeypatch.setattr("src.server.database.thread_transcripts.save_stored", save_stored)
    return state


@pytest.mark.asyncio
async def test_a_live_save_keeps_the_rendered_checkpoint_and_empties_the_fingerprint(live):
    live.stored[""] = _stored(manifest='{"thread_id": "%s", "checkpoint_id": "cp-1"}' % T1)
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("hi")], window=Window())
    [(prefix, copy)] = live.saves
    assert prefix == ""
    assert copy.fingerprint == "" and copy.checkpoint_id == "cp-1"
    assert copy.files["turn-0001.jsonl"] != ("a" * 64, 10)
    assert load_manifest(copy.manifest)["checkpoint_id"] == "cp-1"


@pytest.mark.asyncio
async def test_a_live_save_takes_the_threads_checkpoint_when_its_export_is_late(live):
    """A render at the thread's checkpoint lacks this turn, so the live copy
    must not look older than it."""
    live.stored[""] = _stored(checkpoint_id="cp-1")
    live.stamped.return_value = "cp-2"
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("hi")], window=Window())
    [(_, copy)] = live.saves
    assert copy.checkpoint_id == "cp-2"


@pytest.mark.asyncio
async def test_a_first_turn_live_save_takes_the_checkpoint_tip(live):
    """Unstamped, the copy was labelled with no checkpoint, which any render
    replaced: an export that read the checkpoint before this compaction then
    dropped what the live save had added until the turn ended."""
    live.tip.return_value = "cp-3"
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("hi")], window=Window())
    [(_, copy)] = live.saves
    assert copy.checkpoint_id == "cp-3"
    live.tip.assert_awaited_once_with(T1)

    # Stamped, the stamp stays the label: one past it would stand the turn
    # end's render down too.
    live.saves.clear()
    live.stamped.return_value = "cp-2"
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("bye")], window=Window())
    [(_, copy)] = live.saves
    assert copy.checkpoint_id == "cp-2"
    live.tip.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_render_at_a_live_copys_checkpoint_renders_only_the_tasks(monkeypatch):
    task = {"thread_id": T1, "task_id": "k1", "latest_run_id": "r1", "status": "done"}
    reader = SimpleNamespace(
        aget_state=AsyncMock(),
        aget_task_history=AsyncMock(
            return_value=SimpleNamespace(messages=[_message("go")], checkpoint_id="task-cp-1")
        ),
    )
    monkeypatch.setattr(
        "src.server.services.history.reader.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    save = AsyncMock(return_value=True)
    monkeypatch.setattr("src.server.database.thread_transcripts.save_stored", save)
    own = _stored()
    own.fingerprint = ""

    behind = Behind(T1, "cp-1", {"k1": task}, {"", "tasks/k1/"}, set())
    assert await _render(_target(inline=True), behind, {"": own})

    reader.aget_state.assert_not_awaited()
    assert [c.args[1] for c in save.await_args_list] == ["tasks/k1/"]


@pytest.mark.asyncio
async def test_a_live_save_of_a_task_replaces_only_that_task(live):
    live.stored["tasks/k1/"] = _stored(
        manifest='{"task_id": "k1", "description": "d"}',
        **{"tasks__k1__run-0001.jsonl": "b" * 64},
    )
    assert await transcripts.save_live(TranscriptTarget(T1, "k1"), [_message("go")], window=Window())
    [(prefix, copy)] = live.saves
    assert prefix == "tasks/k1/"
    assert copy.files.keys() == {"tasks/k1/meta.json", "tasks/k1/run-0001.jsonl"}
    assert load_manifest(copy.manifest)["description"] == "d"


@pytest.mark.asyncio
async def test_a_live_save_of_a_task_takes_the_tasks_latest_checkpoint(live):
    """An export that read the running task before this compaction renders
    at that checkpoint or an older one, and must not replace what it lacks."""
    live.stored["tasks/k1/"] = _stored(checkpoint_id="task-cp-1")
    live.stamped.return_value = "cp-9"
    live.tip.return_value = "task-cp-2"
    assert await transcripts.save_live(TranscriptTarget(T1, "k1"), [_message("go")], window=Window())
    [(_, copy)] = live.saves
    assert copy.fingerprint == "" and copy.checkpoint_id == "task-cp-2"
    live.tip.assert_awaited_once_with(T1, "task:k1")
    live.stamped.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_live_save_with_nothing_new_writes_nothing(live):
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("hi")], window=Window())
    [(_, first)] = live.saves
    live.stored[""] = first
    assert await transcripts.save_live(TranscriptTarget(T1), [_message("hi")], window=Window())
    assert len(live.saves) == 1
