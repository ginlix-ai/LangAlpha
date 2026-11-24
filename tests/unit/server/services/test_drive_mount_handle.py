"""What one worker believes about a computer's Drive mount, and what it adds
to a command's result.

A tool asks before a command or file call that names the Drive whether it is
on disk: a start not yet asked for is asked off the turn's path, a start
under way is waited out, one that failed is tried again at most every 30 s,
and a machine the file mount never serves has the feature decide. After the
command, a Drive found dead is restarted, files a link set aside are named,
and saves the server never answered are reported, unless the command's own
saves were settled first. The sandbox's daemon commands and Redis are
stood in for.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import drive_mount as D
from ptc_agent.core.sandbox.drive_mount import NO_DRIVE, DriveDisk
from ptc_agent.core.sandbox.livefs_mount import MountOutcome
from src.server.services.computer_manager._livefs import LivefsMixin
from src.server.services.livefs import drive_mount as M
from src.server.services.livefs import drive_notices as N

ROOT = "/home/workspace"
LAYOUT = SandboxLayout.for_root(ROOT)
COMPUTER = "comp-drive-1"
USER = "user-drive-1"
LINK = f"{ROOT}/.agents/drive"
DEAD = f"ls: {LINK}/personal: Transport endpoint is not connected"
MISSING = f"cat: {LINK}/personal/a.txt: No such file or directory"
NAMES_DRIVE = "ls .agents/drive/personal"


def _sandbox(*, mounts: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        mounts_livefs=mounts,
        runtime=object(),
        layout=LAYOUT,
        sandbox_id="sb-1",
        drive=NO_DRIVE,
    )


#: One worker's machines, as the manager keeps them.
MACHINES: dict[str, SimpleNamespace] = {}
MANAGER = SimpleNamespace(
    _livefs_provider=lambda sandbox: "docker",
    _machine=lambda computer_id: MACHINES.setdefault(
        computer_id, SimpleNamespace(drive=M.DriveRuns())
    ),
)


def _handle(sandbox, user: str = USER):
    return LivefsMixin._hand_drive(MANAGER, COMPUTER, user, sandbox)


async def _push(sandbox, text: str, call_id: str | None) -> bool:
    """The guard's step before the mount's report: settle the call's saves."""
    return await sandbox.drive.push_saves(text, call_id)


async def _after(
    sandbox, output: str, *, text: str = "", call_id=None, settled=False
) -> str:
    return await sandbox.drive.after(
        output, text=text, call_id=call_id, settled=settled
    )


async def _on_disk(sandbox, wait_s: float = 0.0) -> DriveDisk:
    return await sandbox.drive.on_disk(wait_s)


class Daemon:
    """The Drive daemon's host commands, as the host sees their answers."""

    def __init__(self) -> None:
        self.start = AsyncMock(return_value=MountOutcome(ok=True, started=True))
        self.link = AsyncMock(return_value=MountOutcome(ok=True))
        self.stop = AsyncMock(return_value=MountOutcome(ok=True))
        self.answers = AsyncMock(return_value=True)
        self.settle = AsyncMock(return_value=[])
        self.take_notices = AsyncMock(return_value=[])


@pytest.fixture
def daemon():
    found = Daemon()
    with (
        patch.object(D, "start", found.start),
        patch.object(D, "link", found.link),
        patch.object(D, "stop", found.stop),
        patch.object(D, "answers", found.answers),
        patch.object(D, "settle", found.settle),
        patch.object(D, "take_notices", found.take_notices),
    ):
        yield found


@pytest.fixture
def memo():
    """Redis as a dict: what converge brought about, or that it stopped it."""
    store: dict[str, object] = {}

    async def get(key):
        return store.get(key)

    async def put(key, value, ttl=None):
        store[key] = value

    async def drop(*keys):
        for key in keys:
            store.pop(key, None)

    client = SimpleNamespace(get=get, set=put, delete=drop)
    MACHINES.clear()
    with patch.object(M, "get_cache_client", lambda: client):
        yield store


@pytest.fixture
def wanted():
    with patch.object(M, "wanted", AsyncMock(return_value=True)) as found:
        yield found


@pytest.fixture
def folders():
    """The computer's folder lock, free unless a test holds it."""
    state = SimpleNamespace(busy=False)

    @asynccontextmanager
    async def lock(computer_id, *, wait_s):
        yield None if state.busy else object()

    with (
        patch.object(M, "workspace_folders_lock", lock),
        patch.object(M, "targets", AsyncMock(return_value=[LINK])),
        patch.object(M, "effective_relay_base_url", lambda provider: "http://relay"),
    ):
        yield state


@pytest.fixture
def filed():
    with (
        patch.object(N, "seen", AsyncMock(return_value=True)) as seen,
        patch.object(N, "file", AsyncMock(return_value=[])) as file,
    ):
        yield SimpleNamespace(seen=seen, file=file)


@pytest.fixture
def stack(daemon, memo, wanted, folders, filed):
    return SimpleNamespace(
        daemon=daemon, memo=memo, wanted=wanted, folders=folders, filed=filed
    )


async def _serving(sandbox) -> None:
    _handle(sandbox)
    assert await _on_disk(sandbox, wait_s=2) is DriveDisk.ON


class TestOnDisk:
    @pytest.mark.asyncio
    async def test_a_sandbox_handed_nothing_has_no_drive_of_its_own(self):
        sandbox = _sandbox()
        assert await _on_disk(sandbox) is DriveDisk.OFF
        assert await _push(sandbox, NAMES_DRIVE, "c1") is False
        assert await _after(sandbox, DEAD, text=NAMES_DRIVE, call_id="c1") == ""

    @pytest.mark.asyncio
    async def test_the_first_ask_starts_it_and_waits_for_it(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.daemon.start.assert_awaited_once()
        stack.daemon.link.assert_awaited_once_with(sandbox, [LINK])
        # Kept: the next ask asks the sandbox nothing.
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.ON
        stack.daemon.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_start_under_way_is_waited_out_only_with_a_wait(self, stack):
        release = asyncio.Event()

        async def slow_start(sandbox, *, base_url):
            await release.wait()
            return MountOutcome(ok=True, started=True)

        stack.daemon.start.side_effect = slow_start
        sandbox = _sandbox()
        _handle(sandbox)
        assert await _on_disk(sandbox) is DriveDisk.UNAVAILABLE
        waiting = asyncio.create_task(_on_disk(sandbox, wait_s=2))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        release.set()
        assert await waiting is DriveDisk.ON

    @pytest.mark.asyncio
    async def test_nothing_is_asked_while_the_file_mounts_start_has_yet_to_answer(
        self, stack
    ):
        # A turn handed the file mount's start off its path: the Drive runs
        # on the token that start installs.
        sandbox = _sandbox()
        sandbox.livefs = SimpleNamespace(serving=None)
        _handle(sandbox)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.UNAVAILABLE
        stack.daemon.start.assert_not_awaited()
        sandbox.livefs = SimpleNamespace(serving=True)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.ON
        stack.daemon.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_feature_off_answers_off(self, stack):
        stack.wanted.return_value = False
        sandbox = _sandbox()
        _handle(sandbox)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.OFF
        stack.daemon.start.assert_not_awaited()
        # Not known stopped, so stopped once: a no-op where it never ran.
        stack.daemon.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_computer_that_had_it_has_it_stopped_when_the_feature_goes_off(
        self, stack
    ):
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.wanted.return_value = False
        other = _sandbox()
        _handle(other)
        assert await _on_disk(other, wait_s=2) is DriveDisk.OFF
        stack.daemon.stop.assert_awaited_once()
        assert list(stack.memo.values()) == [True]

    @pytest.mark.asyncio
    async def test_the_feature_off_stops_a_mount_redis_forgot(self, stack):
        """Redis flushed or evicted while the computer serves: the cached
        files stay readable until a stop, so its absence must not skip one."""
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.memo.clear()
        stack.wanted.return_value = False
        other = _sandbox()
        _handle(other)
        assert await _on_disk(other, wait_s=2) is DriveDisk.OFF
        stack.daemon.stop.assert_awaited_once()
        # Remembered stopped: the next turn asks the sandbox nothing.
        again = _sandbox()
        _handle(again)
        assert await _on_disk(again, wait_s=2) is DriveDisk.OFF
        stack.daemon.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_machine_never_mounted_has_the_feature_decide(self, stack):
        sandbox = _sandbox(mounts=False)
        _handle(sandbox)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.UNAVAILABLE
        stack.wanted.return_value = False
        other = _sandbox(mounts=False)
        _handle(other)
        assert await _on_disk(other, wait_s=2) is DriveDisk.OFF
        stack.daemon.start.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_stop_never_lands_between_a_start_and_its_memory(self, stack):
        """CR043: a converge still linking publishes nothing over a stop."""
        held = asyncio.Lock()

        @asynccontextmanager
        async def exclusive(computer_id, *, wait_s):
            if held.locked():
                yield None
                return
            async with held:
                yield object()

        linking, go = asyncio.Event(), asyncio.Event()

        async def slow_link(sandbox, targets):
            linking.set()
            await go.wait()
            return MountOutcome(ok=True)

        stack.daemon.link.side_effect = slow_link

        async def converge():
            return await M.converge(
                _sandbox(), computer_id=COMPUTER, user_id=USER, provider="docker"
            )

        with patch.object(M, "workspace_folders_lock", exclusive):
            up = asyncio.create_task(converge())
            await linking.wait()
            stack.wanted.return_value = False
            await converge()
            go.set()
            await up
            await converge()
        assert stack.daemon.stop.await_count >= 1
        assert M._memo_key(COMPUTER) not in stack.memo

    @pytest.mark.asyncio
    async def test_a_new_sandbox_gets_a_run_of_its_own(self, stack):
        """CR044: an old sandbox's run in flight answers for that sandbox only."""
        go = asyncio.Event()

        async def slow_start(sandbox, *, base_url):
            await go.wait()
            return MountOutcome(ok=True, started=True)

        stack.daemon.start.side_effect = slow_start
        old = _sandbox()
        _handle(old)
        assert await _on_disk(old) is DriveDisk.UNAVAILABLE
        new = SimpleNamespace(**{**vars(_sandbox()), "sandbox_id": "sb-2"})
        _handle(new)
        asked = asyncio.create_task(_on_disk(new, wait_s=2))
        await asyncio.sleep(0)
        go.set()
        assert await asked is DriveDisk.ON
        started_on = [c.args[0].sandbox_id for c in stack.daemon.start.await_args_list]
        assert started_on == ["sb-1", "sb-2"]

    @pytest.mark.asyncio
    async def test_a_failed_start_is_tried_again_after_its_backoff(
        self, stack, monkeypatch
    ):
        assert M._RETRY_EVERY_S == 30
        stack.daemon.start.return_value = MountOutcome(ok=False, reason="no fuse")
        sandbox = _sandbox()
        _handle(sandbox)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.UNAVAILABLE
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.UNAVAILABLE
        assert stack.daemon.start.await_count == 1
        monkeypatch.setattr(M, "_RETRY_EVERY_S", 0.0)
        stack.daemon.start.return_value = MountOutcome(ok=True, started=True)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.ON
        assert stack.daemon.start.await_count == 2

    @pytest.mark.asyncio
    async def test_folders_held_past_every_try_leave_it_unavailable(
        self, stack, monkeypatch
    ):
        monkeypatch.setattr(M, "_BUSY_PAUSE_S", 0.0)
        stack.folders.busy = True
        sandbox = _sandbox()
        _handle(sandbox)
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.UNAVAILABLE
        stack.daemon.start.assert_not_awaited()


class TestAfter:
    @pytest.mark.asyncio
    async def test_a_dead_drive_is_restarted_and_the_command_asked_to_run_again(
        self, stack
    ):
        sandbox = _sandbox()
        await _serving(sandbox)
        note = await _after(sandbox, DEAD, text=NAMES_DRIVE)
        assert note == "The Drive had stopped and is serving again; rerun the command."
        assert stack.daemon.start.await_count == 2

    @pytest.mark.asyncio
    async def test_a_dead_drive_that_will_not_start_says_so(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.daemon.start.return_value = MountOutcome(ok=False, reason="gone")
        note = await _after(sandbox, DEAD, text=NAMES_DRIVE)
        assert note == "The Drive stopped and could not be started again (failed)."
        assert await _on_disk(sandbox) is DriveDisk.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_a_missing_drive_path_is_probed_at_most_every_30s(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.daemon.answers.return_value = True
        assert await _after(sandbox, MISSING, text=NAMES_DRIVE) == ""
        stack.daemon.answers.return_value = False
        # Probed a moment ago: a missing file is just missing until the next.
        assert await _after(sandbox, MISSING, text=NAMES_DRIVE) == ""
        assert stack.daemon.answers.await_count == 1
        assert stack.daemon.start.await_count == 1

    @pytest.mark.asyncio
    async def test_a_missing_drive_path_the_probe_finds_gone_restarts_it(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        stack.daemon.answers.return_value = False
        note = await _after(sandbox, MISSING, text=NAMES_DRIVE)
        assert note == "The Drive had stopped and is serving again; rerun the command."

    @pytest.mark.asyncio
    async def test_files_a_link_set_aside_are_named_once(self, stack):
        stack.daemon.link.return_value = MountOutcome(
            ok=True, set_aside={LINK: f"{LINK}.local"}
        )
        sandbox = _sandbox()
        await _serving(sandbox)
        note = await _after(sandbox, "ok")
        assert note == (
            f"What was at {LINK} before the Drive was linked there is now at {LINK}.local."
        )
        assert await _after(sandbox, "ok") == ""

    @pytest.mark.asyncio
    async def test_unanswered_saves_are_taken_when_the_command_names_the_drive(
        self, stack
    ):
        unsaved = {
            "id": "n1",
            "kind": "unsaved",
            "space": "personal",
            "path": "a.txt",
            "call": "c1",
        }
        stack.daemon.take_notices.return_value = [unsaved]
        stack.filed.file.return_value = [
            {"op": "write", "ok": True, "path": "p", "report": "NOT SAVED YET: p"}
        ]
        sandbox = _sandbox()
        await _serving(sandbox)
        note = await _after(sandbox, "ok", text=NAMES_DRIVE, call_id="c1")
        assert "NOT SAVED YET: p" in note
        stack.filed.file.assert_awaited_once_with(
            COMPUTER, ROOT, [unsaved], own_call="c1"
        )

    @pytest.mark.asyncio
    async def test_unanswered_saves_are_taken_when_the_daemon_is_not_seen(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        assert await _after(sandbox, "ok", text="ls", call_id="c1") == ""
        stack.daemon.take_notices.assert_not_awaited()
        stack.filed.seen.return_value = False
        await _after(sandbox, "ok", text="ls", call_id="c2")
        stack.daemon.take_notices.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_drive_not_serving_adds_nothing(self, stack):
        stack.daemon.start.return_value = MountOutcome(ok=False)
        sandbox = _sandbox()
        _handle(sandbox)
        await _on_disk(sandbox, wait_s=2)
        assert await _after(sandbox, DEAD, text=NAMES_DRIVE, call_id="c1") == ""
        stack.daemon.take_notices.assert_not_awaited()
        assert stack.daemon.start.await_count == 1


class TestPushSaves:
    @pytest.mark.asyncio
    async def test_a_command_naming_the_drive_has_its_saves_settled_and_filed(
        self, stack
    ):
        notes = [
            {
                "id": "n1",
                "kind": "saved",
                "space": "personal",
                "path": "a.txt",
                "call": "c1",
            }
        ]
        stack.daemon.settle.return_value = notes
        sandbox = _sandbox()
        await _serving(sandbox)
        settled = await _push(sandbox, NAMES_DRIVE, "c1")
        assert settled is True
        stack.daemon.settle.assert_awaited_once_with(sandbox, "c1")
        stack.filed.file.assert_awaited_once_with(COMPUTER, ROOT, notes)
        # Its notices were filed under their calls already: not taken again.
        stack.filed.seen.return_value = False
        assert (
            await _after(sandbox, "ok", text=NAMES_DRIVE, call_id="c1", settled=settled)
            == ""
        )
        stack.daemon.take_notices.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_command_not_naming_the_drive_pays_nothing(self, stack):
        sandbox = _sandbox()
        await _serving(sandbox)
        assert await _push(sandbox, "ls", "c1") is False
        assert await _push(sandbox, NAMES_DRIVE, None) is False
        stack.daemon.settle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_drive_not_serving_settles_nothing(self, stack):
        stack.wanted.return_value = False
        sandbox = _sandbox()
        _handle(sandbox)
        await _on_disk(sandbox, wait_s=2)
        assert await _push(sandbox, NAMES_DRIVE, "c1") is False
        stack.daemon.settle.assert_not_awaited()


class TestHanded:
    def test_kept_for_its_owner_and_made_anew_for_another(self):
        sandbox = _sandbox()
        first = _handle(sandbox)
        assert _handle(sandbox) is first
        other = _handle(sandbox, "user-drive-2")
        assert other is not first
        assert sandbox.drive is other

    @pytest.mark.asyncio
    async def test_a_sandbox_joining_a_converge_under_way_takes_its_answer(self, stack):
        release = asyncio.Event()

        async def slow_start(sandbox, *, base_url):
            await release.wait()
            return MountOutcome(ok=True, started=True)

        stack.daemon.start.side_effect = slow_start
        first, second = _sandbox(), _sandbox()
        _handle(first)
        _handle(second)
        asking = asyncio.create_task(_on_disk(first, wait_s=2))
        await asyncio.sleep(0.01)
        # A reconnect's sandbox object asks while the first one's runs: one
        # converge per computer, and both are told what it did.
        joining = asyncio.create_task(_on_disk(second, wait_s=2))
        await asyncio.sleep(0.01)
        release.set()
        assert await asking is DriveDisk.ON
        assert await joining is DriveDisk.ON
        stack.daemon.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_forced_converge_runs_after_the_one_under_way(self, stack):
        release = asyncio.Event()
        calls: list[str] = []

        async def slow_start(sandbox, *, base_url):
            calls.append("start")
            await release.wait()
            return MountOutcome(ok=True, started=True)

        stack.daemon.start.side_effect = slow_start
        sandbox = _sandbox()
        handle = _handle(sandbox)
        handle.converge_soon()
        await asyncio.sleep(0.01)
        handle.converge_soon(force=True)
        await asyncio.sleep(0.01)
        assert calls == ["start"]
        release.set()
        assert await _on_disk(sandbox, wait_s=2) is DriveDisk.ON
        for _ in range(20):
            await asyncio.sleep(0.01)
        assert calls == ["start", "start"]

    @pytest.mark.asyncio
    async def test_a_dead_drive_found_while_a_converge_runs_restarts_after_it(
        self, stack
    ):
        """Recovery goes through the computer's runs: queued behind the one
        under way, never beside it, and the command waits for its answer."""
        sandbox = _sandbox()
        await _serving(sandbox)
        release = asyncio.Event()
        order: list[str] = []

        async def held_targets(computer_id, layout):
            order.append("targets")
            await release.wait()
            return [LINK]

        async def start(sandbox, *, base_url):
            order.append("start")
            return MountOutcome(ok=True, started=True)

        stack.daemon.start.side_effect = start
        with patch.object(M, "targets", held_targets):
            # A reacquire's converge, which a served Drive does not wait for.
            sandbox.drive.converge_soon()
            await asyncio.sleep(0.01)
            recovering = asyncio.create_task(_after(sandbox, DEAD, text=NAMES_DRIVE))
            await asyncio.sleep(0.01)
            assert order == ["targets"]
            assert not recovering.done()
            release.set()
            note = await recovering
        assert note == "The Drive had stopped and is serving again; rerun the command."
        assert order == ["targets", "start", "targets", "start"]
        assert await _on_disk(sandbox) is DriveDisk.ON
