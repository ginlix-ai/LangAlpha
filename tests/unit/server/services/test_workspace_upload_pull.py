"""Assembling an upload's parts, pulling it into the folder, and cleaning up after.

Assembling trusts the store's own listing, never the client, and a pull's end
is recorded against the attempt that ran it, so a retry, a refusal and a
shutdown each leave the row where the next worker can carry on.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.database.workspace_file import WorkspaceSyncBusy
from src.server.services import workspace_upload_pull as upload_pull
from src.server.services.workspace_upload_pull import (
    _LOCAL_PULLS_MAX,
    _POPULATED_DIRECTORY,
    _RETRY_STEP,
    COMPLETE_LEASE,
    KEEP_ALIVE_EVERY,
    MAX_PULL_ATTEMPTS,
    PULL_LEASE,
    SWEEP_BATCH_LIMIT,
    UPLOAD_FAILED,
    Assembly,
    _assemble,
    _ClaimLost,
    _clean_store,
    _heartbeat,
    _pull_once,
    _Refused,
    assemble_and_pull,
    launch_pull,
    pull_upload,
    sweep_once,
)

UPLOAD_ID = "00000000-0000-4000-8000-000000000001"
KEY = f"uploads/user-1/{UPLOAD_ID}"
STORE_ID = "store-upload-1"


def _row(**overrides) -> dict:
    # Three parts of 10, 10 and 5 bytes: the sizes are the row's, not the store's.
    return {
        "upload_id": UPLOAD_ID,
        "user_id": "user-1",
        "workspace_id": "ws-1",
        "path": "data/big.bin",
        "size": 25,
        "part_size": 10,
        "part_count": 3,
        "object_key": KEY,
        "store_upload_id": STORE_ID,
        "attempts": 0,
        **overrides,
    }


def _held(*sizes: int) -> list[dict]:
    return [{"part_number": n, "size": s, "etag": f"e{n}"} for n, s in enumerate(sizes, 1)]


def _store(*, parts, held_size, completed=True):
    return (
        patch.object(upload_pull, "list_uploaded_parts", MagicMock(return_value=parts)),
        patch.object(upload_pull, "object_size", MagicMock(return_value=held_size)),
        patch.object(upload_pull, "complete_multipart_upload", MagicMock(return_value=completed)),
    )


# --- assembling ------------------------------------------------------------------


def test_every_part_at_its_size_assembles_in_order():
    listing, size, complete = _store(parts=_held(10, 10, 5), held_size=25)
    with listing, size, complete as completed:
        assert _assemble(_row()) == Assembly(ok=True)
    completed.assert_called_once_with(KEY, STORE_ID, [(1, "e1"), (2, "e2"), (3, "e3")])


@pytest.mark.parametrize(
    "parts, missing",
    [
        ([p for p in _held(10, 10, 5) if p["part_number"] != 2], [2]),
        (_held(10, 9, 5), [2]),
        # The last part is the remainder; a full-size one would make the file long.
        (_held(10, 10, 10), [3]),
        ([], [1, 2, 3]),
    ],
    ids=["one-not-sent", "one-short", "last-too-long", "none-sent"],
)
def test_parts_the_store_lacks_or_holds_at_another_size_are_missing(parts, missing):
    listing, size, complete = _store(parts=parts, held_size=None)
    with listing, size, complete as completed:
        assert _assemble(_row()) == Assembly(missing=missing)
    completed.assert_not_called()


@pytest.mark.parametrize(
    "held_size, assembly",
    [
        (25, Assembly(ok=True)),
        (24, Assembly(mismatch=True)),
        (None, Assembly(unreachable=True)),
    ],
    ids=["assembled-before", "other-object", "nothing-held"],
)
def test_an_upload_the_store_no_longer_knows_is_settled_by_the_object(held_size, assembly):
    """An earlier attempt may have assembled it before its answer was lost."""
    listing, size, complete = _store(parts=None, held_size=held_size)
    with listing, size, complete as completed:
        assert _assemble(_row()) == assembly
    completed.assert_not_called()


@pytest.mark.parametrize(
    "held_size, assembly",
    [(24, Assembly(mismatch=True)), (None, Assembly(unreachable=True))],
    ids=["wrong-size", "no-head"],
)
def test_after_assembling_the_object_has_to_be_the_declared_size(held_size, assembly):
    listing, size, complete = _store(parts=_held(10, 10, 5), held_size=held_size)
    with listing, size, complete:
        assert _assemble(_row()) == assembly


# --- pulling ---------------------------------------------------------------------


def _pull_db(ended: dict | None = None):
    finish = AsyncMock(return_value=ended if ended is not None else _row(state="done"))
    release = AsyncMock(return_value=True)
    return (
        finish,
        release,
        patch.object(upload_pull.db, "finish_pull", finish),
        patch.object(upload_pull.db, "release_pull", release),
    )


async def _run_pull(attempts: int, outcome, **row):
    finish, release, finish_patch, release_patch = _pull_db()
    clean = AsyncMock()
    with (
        finish_patch,
        release_patch,
        patch.object(upload_pull, "_pull_once", AsyncMock(side_effect=outcome)),
        patch.object(upload_pull, "clean_up", clean),
    ):
        await pull_upload(_row(state="pulling", attempts=attempts, **row))
    return finish, release, clean


@pytest.mark.asyncio
async def test_a_pull_that_placed_the_file_ends_done_and_cleans_the_store():
    finish, release, clean = await _run_pull(1, None)
    finish.assert_awaited_once_with(UPLOAD_ID, 1, state="done", error=None)
    clean.assert_awaited_once_with(finish.return_value)
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_refused_pull_fails_with_its_reason():
    finish, release, clean = await _run_pull(1, _Refused("no room"))
    finish.assert_awaited_once_with(UPLOAD_ID, 1, state="failed", error="no room")
    clean.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", range(1, MAX_PULL_ATTEMPTS))
async def test_a_failed_pull_waits_longer_each_time_before_another(attempt):
    finish, release, clean = await _run_pull(attempt, RuntimeError("pull timed_out"))
    release.assert_awaited_once_with(
        UPLOAD_ID, attempt, retry_in=_RETRY_STEP * attempt, refund=False
    )
    finish.assert_not_awaited()
    clean.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_last_attempt_that_fails_fails_the_upload():
    finish, release, clean = await _run_pull(MAX_PULL_ATTEMPTS, RuntimeError("pull timed_out"))
    finish.assert_awaited_once_with(
        UPLOAD_ID, MAX_PULL_ATTEMPTS, state="failed", error=UPLOAD_FAILED
    )
    clean.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_last_attempt_that_began_placing_leaves_the_row_one_more():
    """That one fetches nothing: it settles by the folder what this one could
    not record, the file it may already have placed included."""

    async def fails_placing(_row, held):
        held["placing"] = True
        raise RuntimeError("[Errno 5] Input/output error")

    finish, release, _ = await _run_pull(MAX_PULL_ATTEMPTS, fails_placing)
    release.assert_awaited_once_with(
        UPLOAD_ID, MAX_PULL_ATTEMPTS, retry_in=_RETRY_STEP * MAX_PULL_ATTEMPTS, refund=False
    )
    finish.assert_not_awaited()

    finish, release, _ = await _run_pull(
        MAX_PULL_ATTEMPTS + 1, RuntimeError("pull timed_out"), placing_inode=INODE
    )
    finish.assert_awaited_once_with(
        UPLOAD_ID, MAX_PULL_ATTEMPTS + 1, state="failed", error=UPLOAD_FAILED
    )
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_cancelled_pull_hands_the_attempt_back_uncounted():
    started = asyncio.Event()

    async def hang(_row, _held):
        started.set()
        await asyncio.Event().wait()

    finish, release, finish_patch, release_patch = _pull_db()
    with finish_patch, release_patch, patch.object(upload_pull, "_pull_once", hang):
        task = asyncio.create_task(pull_upload(_row(state="pulling", attempts=2)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    release.assert_awaited_once_with(UPLOAD_ID, 2, retry_in=timedelta(0), refund=True)
    finish.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_pull_whose_claim_another_worker_took_stops_and_records_nothing():
    started, stopped = asyncio.Event(), asyncio.Event()

    async def hang(_row, _held):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    finish, release, finish_patch, release_patch = _pull_db()
    with (
        finish_patch,
        release_patch,
        patch.object(upload_pull, "_pull_once", hang),
        patch.object(upload_pull, "_HEARTBEAT_S", 0.01),
        patch.object(upload_pull.db, "renew_pull", AsyncMock(return_value=False)),
    ):
        await asyncio.wait_for(pull_upload(_row(state="pulling", attempts=2)), 1)

    assert started.is_set() and stopped.is_set()
    finish.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_folder_busy_syncing_hands_the_attempt_back_uncounted():
    finish, release, clean = await _run_pull(3, WorkspaceSyncBusy("ws-1"))
    release.assert_awaited_once_with(UPLOAD_ID, 3, retry_in=_RETRY_STEP, refund=True)
    finish.assert_not_awaited()
    clean.assert_not_awaited()


STAGED = f".wsfiles-upload-{UPLOAD_ID}"
TARGET = "/home/u/Research/data/big.bin"
INODE = 4242


def _sandbox_pull(
    placed: dict,
    fetched: dict | None = None,
    sandbox=None,
    events: list | None = None,
    *,
    found: str = "none",
    holds_claim: bool = True,
):
    """``_pull_once`` up to the sandbox's answers: ``fetched`` to the fetch,
    ``placed`` to the placement, ``found`` to clearing the staging; ``events``
    records each step and the path lock."""
    from contextlib import asynccontextmanager

    events = [] if events is None else events
    if sandbox is None:
        sandbox = MagicMock()

    async def runtime_call(_fn, command, *_args, **_kwargs):
        if command.startswith("rm -f"):
            events.append(("remove", command))
            return MagicMock(stdout="", exit_code=0)
        if command.startswith("ls -di"):
            events.append(("inode",))
            return MagicMock(stdout=f"{INODE} {STAGED}\n", exit_code=0)
        events.append(("clear", STAGED in command))
        return MagicMock(stdout=f"{found}\n", exit_code=0)

    sandbox._runtime_call = AsyncMock(side_effect=runtime_call)

    async def free_disk(*_args):
        events.append(("disk",))
        return None

    async def mark_placing(upload_id, attempt, inode):
        events.append(("placing", attempt, inode))
        return holds_claim

    @asynccontextmanager
    async def path_lock(path):
        events.append(("lock", path))
        yield
        events.append(("unlock", path))

    sandbox.normalize_path = lambda path: path
    sandbox.path_write_lock = path_lock

    @asynccontextmanager
    async def acquire(*_args):
        yield sandbox, {"workspace_id": "ws-1"}

    @asynccontextmanager
    async def lock(_workspace_id):
        yield None

    async def pull(_sandbox, items, *, layout):
        (item,) = items
        fetch = "url" in item
        events.append(("fetch" if fetch else "place", item["path"], item.get("file")))
        return {item["path"]: (fetched or {"status": "ok"}) if fetch else placed}

    layout = MagicMock(workspace="/home/u/Research")
    return (
        patch.object(
            upload_pull,
            "owned_workspace_on_computer",
            AsyncMock(return_value={"workspace_id": "ws-1", "status": "running"}),
        ),
        patch.object(upload_pull, "acquire_sandbox_to_change", acquire),
        patch.object(upload_pull, "owner_layout", MagicMock(return_value=layout)),
        patch.object(upload_pull, "previous_dir_names_of", MagicMock(return_value=())),
        patch.object(upload_pull, "contained_target", AsyncMock(return_value=TARGET)),
        patch.object(upload_pull, "workspace_sync_lock", lock),
        patch.object(upload_pull, "free_disk_bytes", free_disk),
        patch.object(upload_pull, "get_signed_url", MagicMock(return_value="https://store.invalid/o")),
        patch.object(upload_pull, "pull_direct", pull),
        patch.object(upload_pull.db, "mark_placing", mark_placing),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetched, placed, refused",
    [
        (None, {"status": "failed", "error": _POPULATED_DIRECTORY}, True),
        (None, {"status": "failed", "error": "path escapes root"}, True),
        ({"status": "mismatch", "error": "got sha256=0 bytes=1"}, None, True),
        ({"status": "failed", "http": 503, "error": "HTTP 503"}, None, False),
    ],
    ids=["populated-directory", "escapes-root", "other-bytes", "store-503"],
)
async def test_a_sandbox_answer_no_retry_can_change_is_refused_at_once(fetched, placed, refused):
    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in _sandbox_pull(placed, fetched):
            stack.enter_context(p)
        with pytest.raises(_Refused if refused else RuntimeError):
            await _pull_once(_row(state="pulling", attempts=1), {})


@pytest.mark.asyncio
async def test_the_file_is_fetched_without_its_path_lock_and_placed_under_it():
    """The file tools hold the lock from an edit's read to its write, so a
    rename between the two would be written over with the edit's stale bytes;
    the transfer itself outlasts the lock's lease."""
    from contextlib import ExitStack

    events: list = []
    with ExitStack() as stack:
        for p in _sandbox_pull({"status": "ok"}, events=events):
            stack.enter_context(p)
        await _pull_once(_row(state="pulling", attempts=1), {})
    assert events == [
        # Every leftover goes before the disk is measured, this upload's own
        # earlier copy included, which would otherwise count against it.
        ("clear", False),
        ("disk",),
        ("fetch", STAGED, None),
        ("inode",),
        ("placing", 1, INODE),
        ("lock", TARGET),
        ("place", "data/big.bin", STAGED),
        ("unlock", TARGET),
    ]


@pytest.mark.asyncio
async def test_a_placement_refused_removes_the_fetched_copy():
    from contextlib import ExitStack

    events: list = []
    with ExitStack() as stack:
        for p in _sandbox_pull({"status": "failed", "error": _POPULATED_DIRECTORY}, events=events):
            stack.enter_context(p)
        with pytest.raises(_Refused):
            await _pull_once(_row(state="pulling", attempts=1), {})
    assert events[-1] == ("remove", f"rm -f -- /home/u/Research/{STAGED}")


@pytest.mark.asyncio
async def test_a_placement_a_retry_can_rescue_keeps_the_fetched_copy_for_it():
    from contextlib import ExitStack

    events: list = []
    placed = {"status": "failed", "error": "[Errno 5] Input/output error"}
    with ExitStack() as stack:
        for p in _sandbox_pull(placed, events=events):
            stack.enter_context(p)
        with pytest.raises(RuntimeError):
            await _pull_once(_row(state="pulling", attempts=1), {})
    assert not [e for e in events if e[0] == "remove"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "found, steps, outcome",
    [
        ("placed", [("clear", True)], None),
        (
            "staged",
            [
                ("clear", True),
                ("inode",),
                ("placing", 2, INODE),
                ("lock", TARGET),
                ("place", "data/big.bin", STAGED),
                ("unlock", TARGET),
            ],
            None,
        ),
        ("none", [("clear", True)], _Refused),
    ],
    ids=["file-in-place", "copy-still-staged", "neither"],
)
async def test_a_pull_after_one_that_began_placing_goes_by_what_the_folder_holds(
    found, steps, outcome
):
    """The earlier attempt's end was never recorded, and the file it placed
    may have been changed since, so it is never fetched and placed again."""
    from contextlib import ExitStack

    events: list = []
    row = _row(state="pulling", attempts=2, placing_inode=INODE)
    with ExitStack() as stack:
        for p in _sandbox_pull({"status": "ok"}, events=events, found=found):
            stack.enter_context(p)
        if outcome is None:
            await _pull_once(row, {})
        else:
            with pytest.raises(outcome):
                await _pull_once(row, {})
    assert events == steps


def _shell_sandbox():
    """A sandbox whose commands run in this machine's shell."""
    import subprocess
    from types import SimpleNamespace

    async def runtime_call(_fn, command, *_args, **_kwargs):
        done = subprocess.run(["sh", "-c", command], capture_output=True, text=True)
        return SimpleNamespace(stdout=done.stdout, exit_code=done.returncode)

    return SimpleNamespace(_runtime_call=runtime_call, runtime=SimpleNamespace(exec=None))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "before, found",
    [
        ("staged", "staged"),
        ("placed", "placed"),
        ("replaced-file-only", "none"),
    ],
)
async def test_only_the_copy_the_attempt_was_placing_counts_as_placed(tmp_path, before, found):
    """A sweep may take the staged copy before the rename ran, leaving the
    file the upload would have replaced at its path."""
    from src.server.services.workspace_upload_pull import _clear_staging, _inode_of

    (tmp_path / "data").mkdir()
    target = tmp_path / "data" / "big.bin"
    staged = tmp_path / STAGED
    staged.write_bytes(b"the upload")
    inode = await _inode_of(_shell_sandbox(), str(staged))
    if before == "placed":
        staged.replace(target)
    elif before == "replaced-file-only":
        target.write_bytes(b"the file it replaces")
        staged.unlink()
    (tmp_path / ".wsfiles-left").mkdir()
    (tmp_path / "data" / ".wsfiles-mine").write_bytes(b"a user's own")

    answer = await _clear_staging(
        _shell_sandbox(), str(tmp_path), keep=STAGED, target=str(target), placed_inode=inode
    )

    assert answer == found
    assert not (tmp_path / ".wsfiles-left").exists()
    assert (tmp_path / "data" / ".wsfiles-mine").exists()


@pytest.mark.asyncio
async def test_a_pull_whose_claim_is_gone_by_placement_places_nothing_and_records_nothing():
    from contextlib import ExitStack

    events: list = []
    finish, release, finish_patch, release_patch = _pull_db()
    with ExitStack() as stack:
        for p in (*_sandbox_pull({"status": "ok"}, events=events, holds_claim=False), finish_patch, release_patch):
            stack.enter_context(p)
        with pytest.raises(_ClaimLost):
            await _pull_once(_row(state="pulling", attempts=1), {})
        await pull_upload(_row(state="pulling", attempts=1))
    assert ("lock", TARGET) not in events
    finish.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_workspace_stopped_since_the_upload_began_is_not_started_again():
    from contextlib import ExitStack

    from src.server.services.workspace_files import not_running_for_upload

    stopped = {"workspace_id": "ws-1", "status": "stopped"}
    acquired = MagicMock()
    with ExitStack() as stack:
        for p in _sandbox_pull({"status": "ok"}):
            stack.enter_context(p)
        stack.enter_context(
            patch.object(upload_pull, "owned_workspace_on_computer", AsyncMock(return_value=stopped))
        )
        stack.enter_context(patch.object(upload_pull, "acquire_sandbox_to_change", acquired))
        with pytest.raises(_Refused) as refused:
            await _pull_once(_row(state="pulling", attempts=1), {})
    assert refused.value.message == not_running_for_upload(stopped).detail
    acquired.assert_not_called()


@pytest.mark.asyncio
async def test_a_slow_keep_alive_does_not_hold_the_claim_back():
    renew = AsyncMock(return_value=True)

    async def stalls(*_args, **_kwargs):
        await asyncio.Event().wait()

    manager = MagicMock()
    manager.keep_sandbox_alive = stalls
    held = {"sandbox": MagicMock(sandbox_id="sb-1")}
    with (
        patch.object(upload_pull, "_HEARTBEAT_S", 0.01),
        patch.object(upload_pull, "_KEEP_ALIVE_TIMEOUT_S", 0.02),
        patch.object(upload_pull.db, "renew_pull", renew),
        patch.object(upload_pull.db, "claim_touch", AsyncMock(return_value=True)),
        patch.object(upload_pull.WorkspaceManager, "get_instance", return_value=manager),
    ):
        pull = MagicMock()
        beat = asyncio.create_task(
            _heartbeat(_row(state="pulling", attempts=1), 1, held, pull)
        )
        await asyncio.sleep(0.25)
        beat.cancel()
        with pytest.raises(asyncio.CancelledError):
            await beat
    assert renew.await_count >= 3
    pull.cancel.assert_not_called()


# --- starting the pull ---------------------------------------------------------


@pytest.mark.asyncio
async def test_an_attempt_taken_over_from_a_pull_still_running_here_starts():
    """The sweep here can claim a pull of this worker's own whose renewals
    failed; the stale one stops at its next heartbeat, and the new one must
    not have been folded into it."""
    gate = asyncio.Event()
    ran: list[int] = []

    async def pull(row):
        ran.append(row["attempts"])
        await gate.wait()

    with (
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "pull_upload", pull),
    ):
        launch_pull(_row(state="pulling", attempts=1))
        launch_pull(_row(state="pulling", attempts=2))
        for _ in range(5):
            await asyncio.sleep(0)
        assert ran == [1, 2]
        assert upload_pull._running_pulls == {(UPLOAD_ID, 1), (UPLOAD_ID, 2)}
        gate.set()
        for _ in range(5):
            await asyncio.sleep(0)
        assert upload_pull._running_pulls == set()


def _assembled():
    return patch.object(upload_pull, "_assemble", MagicMock(return_value=Assembly(ok=True)))


@pytest.mark.asyncio
async def test_completions_at_once_take_no_more_pulls_than_the_worker_has_room_for():
    # The slot is promised before the claim's await, so two completions that
    # find one slot free cannot both take it.
    gate = asyncio.Event()
    claims: list = []

    async def start_pull(upload_id, lease):
        claims.append(lease)
        await gate.wait()
        return _row(state="pulling", attempts=1 if lease else 0, lease_until=lease)

    launch = MagicMock()
    with (
        _assembled(),
        patch.object(upload_pull, "_running_pulls", {f"local-{i}" for i in range(_LOCAL_PULLS_MAX - 1)}),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "start_pull", start_pull),
        patch.object(upload_pull, "launch_pull", launch),
    ):
        first = asyncio.create_task(assemble_and_pull(_row(state="completing")))
        second = asyncio.create_task(assemble_and_pull(_row(state="completing")))
        while len(claims) < 2:
            await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(first, second)
        assert upload_pull._reserved_slots == 0

    assert sorted(claims, key=bool) == [None, PULL_LEASE]
    launch.assert_called_once()


@pytest.mark.asyncio
async def test_a_pull_the_database_left_due_is_not_launched():
    # Asked for a claim, but the user already has a pull live: the row says so.
    launch = MagicMock()
    with (
        _assembled(),
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(
            upload_pull.db,
            "start_pull",
            AsyncMock(return_value=_row(state="pulling", attempts=0, lease_until=None)),
        ),
        patch.object(upload_pull, "launch_pull", launch),
    ):
        started, _assembly = await assemble_and_pull(_row(state="completing"))
    assert started["state"] == "pulling"
    launch.assert_not_called()


# --- cleaning up -----------------------------------------------------------------


def _clean(*, aborted: bool, listing, deleted: bool = True):
    calls: list[str] = []

    def record(name, value):
        def call(*_args):
            calls.append(name)
            return value

        return call

    patches = (
        patch.object(upload_pull, "abort_multipart_upload", record("abort", aborted)),
        patch.object(upload_pull, "list_uploaded_parts", record("list", listing)),
        patch.object(upload_pull, "delete_object", record("delete", deleted)),
    )
    return calls, patches


def test_an_unassembled_upload_is_aborted_before_its_object_is_deleted():
    calls, (abort, listing, delete) = _clean(aborted=True, listing=None)
    with abort, listing, delete:
        assert _clean_store(_row(attempts=0)) is True
    assert calls == ["abort", "delete"]


@pytest.mark.parametrize("listing", [_held(10), None], ids=["parts-held", "store-silent"])
def test_an_abort_the_store_did_not_confirm_leaves_the_row_for_the_sweep(listing):
    # A listing that fails says nothing about the parts, so only a confirmed
    # abort (an upload the store no longer knows counts) lets the row go.
    calls, (abort, listing_patch, delete) = _clean(aborted=False, listing=listing)
    with abort, listing_patch, delete:
        assert _clean_store(_row(attempts=0)) is False
    assert calls == ["abort"]


def test_a_pulled_upload_only_deletes_its_object():
    calls, (abort, listing, delete) = _clean(aborted=True, listing=None, deleted=False)
    with abort, listing, delete:
        assert _clean_store(_row(attempts=1)) is False
    assert calls == ["delete"]


# --- sweep -----------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("running", [0, _LOCAL_PULLS_MAX - 1, _LOCAL_PULLS_MAX])
async def test_the_sweep_takes_over_only_the_pulls_this_worker_has_room_for(running):
    lapsed_pull = _row(state="pulling", attempts=2)

    async def claim_lapsed(state, *, lease, limit, max_attempts=None):
        return [lapsed_pull] if state == "pulling" else []

    claim = AsyncMock(side_effect=claim_lapsed)
    launch = MagicMock()
    with (
        patch.object(upload_pull, "_running_pulls", {f"local-{i}" for i in range(running)}),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", claim),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_waiting_touches", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
        patch.object(upload_pull, "launch_pull", launch),
    ):
        await sweep_once()

    pulling = [c for c in claim.await_args_list if c.args == ("pulling",)]
    room = _LOCAL_PULLS_MAX - running
    if room:
        assert [c.kwargs for c in pulling] == [
            {"lease": PULL_LEASE, "limit": room, "max_attempts": MAX_PULL_ATTEMPTS}
        ]
        launch.assert_called_once_with(lapsed_pull)
    else:
        assert pulling == []
        launch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_max, slots", [(50, _LOCAL_PULLS_MAX), (4, 2), (2, 1), (1, 1)])
async def test_a_worker_takes_on_no_more_pulls_than_its_pool_can_spare(monkeypatch, pool_max, slots):
    # Each pull holds a pooled connection for the folder's sync lock while it
    # runs, so pulls that filled the pool would leave the lease heartbeat and
    # every other request on the worker waiting for one.
    monkeypatch.setenv("POSTGRES_CONVERSATION_POOL_MAX", str(pool_max))
    claim = AsyncMock(return_value=[])
    with (
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", claim),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_waiting_touches", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
    ):
        await sweep_once()

    pulling = [c for c in claim.await_args_list if c.args == ("pulling",)]
    assert [c.kwargs["limit"] for c in pulling] == [slots]


@pytest.mark.asyncio
async def test_the_sweep_assembles_lapsed_completions_and_cleans_ended_rows():
    lapsed, exhausted, ended = (
        _row(state="completing"),
        _row(state="failed", attempts=MAX_PULL_ATTEMPTS),
        _row(state="aborted"),
    )

    async def claim_lapsed(state, *, lease, limit, max_attempts=None):
        return [lapsed] if state == "completing" else []

    claim = AsyncMock(side_effect=claim_lapsed)
    assemble, clean = AsyncMock(return_value=(None, Assembly(ok=True))), AsyncMock()
    with (
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", claim),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[exhausted])),
        patch.object(upload_pull.db, "claim_waiting_touches", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=[ended])),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
        patch.object(upload_pull, "assemble_and_pull", assemble),
        patch.object(upload_pull, "clean_up", clean),
    ):
        await sweep_once()

    assert claim.await_args_list[0].args == ("completing",)
    assert claim.await_args_list[0].kwargs["lease"] == COMPLETE_LEASE
    assemble.assert_awaited_once_with(lapsed)
    assert [c.args for c in clean.await_args_list] == [(exhausted,), (ended,)]


@pytest.mark.asyncio
async def test_a_pull_waiting_its_turn_counts_as_use_of_its_computer():
    """Nothing else keeps that computer in use while the user's live pull
    keeps another one, and the idle sweep would stop it under the upload."""
    waiting = _row(state="pulling", attempts=0, lease_until=None, sandbox_id="sb-2")
    touches = AsyncMock(return_value=[waiting])
    started = {}
    count = AsyncMock()
    with (
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_waiting_touches", touches),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
        patch.object(upload_pull, "schedule_start", lambda key, start: started.setdefault(key, start)),
        patch.object(upload_pull, "count_as_use", count),
    ):
        await sweep_once()
        (start,) = started.values()
        await start()

    touches.assert_awaited_once_with(KEEP_ALIVE_EVERY, SWEEP_BATCH_LIMIT)
    count.assert_awaited_once_with(waiting["workspace_id"], "sb-2")


@pytest.mark.asyncio
async def test_a_completion_during_the_sweeps_claim_finds_the_slots_promised():
    gate = asyncio.Event()
    claims: list = []

    async def claim_lapsed(state, *, lease, limit, max_attempts=None):
        if state == "pulling":
            await gate.wait()
        return []

    async def start_pull(upload_id, lease):
        claims.append(lease)
        return _row(state="pulling", attempts=0, lease_until=lease)

    with (
        _assembled(),
        patch.object(upload_pull, "_running_pulls", {"local-0", "local-1"}),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", claim_lapsed),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_waiting_touches", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
        patch.object(upload_pull.db, "start_pull", start_pull),
        patch.object(upload_pull, "launch_pull", MagicMock()),
    ):
        sweep = asyncio.create_task(sweep_once())
        while upload_pull._reserved_slots == 0:
            await asyncio.sleep(0)
        await assemble_and_pull(_row(state="completing"))
        gate.set()
        await sweep
    assert claims == [None]


@pytest.mark.asyncio
async def test_one_row_that_raises_does_not_stop_the_sweep():
    first, second = _row(upload_id="a", state="completing"), _row(upload_id="b", state="completing")
    pulls = [_row(upload_id="p", state="pulling"), _row(upload_id="q", state="pulling")]
    ended = [_row(upload_id="x", state="aborted"), _row(upload_id="y", state="aborted")]

    async def claim_lapsed(state, *, lease, limit, max_attempts=None):
        return [first, second] if state == "completing" else pulls

    assemble = AsyncMock(side_effect=[RuntimeError("store gone"), (None, Assembly(ok=True))])
    clean = AsyncMock(side_effect=[RuntimeError("store gone"), None])
    launch = MagicMock(side_effect=[RuntimeError("no loop"), None])
    with (
        patch.object(upload_pull, "_running_pulls", set()),
        patch.object(upload_pull, "_reserved_slots", 0),
        patch.object(upload_pull.db, "expire_idle", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_lapsed", claim_lapsed),
        patch.object(upload_pull.db, "fail_exhausted_pulls", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_waiting_touches", AsyncMock(return_value=[])),
        patch.object(upload_pull.db, "claim_cleanups", AsyncMock(return_value=ended)),
        patch.object(upload_pull.db, "prune_cleaned", AsyncMock(return_value=0)),
        patch.object(upload_pull, "assemble_and_pull", assemble),
        patch.object(upload_pull, "clean_up", clean),
        patch.object(upload_pull, "launch_pull", launch),
    ):
        await sweep_once()
    assert [c.args[0]["upload_id"] for c in assemble.await_args_list] == ["a", "b"]
    assert [c.args[0]["upload_id"] for c in launch.call_args_list] == ["p", "q"]
    assert [c.args[0]["upload_id"] for c in clean.await_args_list] == ["x", "y"]
