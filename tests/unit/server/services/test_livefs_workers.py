"""A computer's file mount as several server workers see it.

A request, its commands and the next turn may each land on a different
worker, so what decides a turn's mount work is what every worker recorded:
the last start's answer on the token row and the folders a link laid, read
once at a turn's start. Each worker here is its own ``ComputerManager`` with
its own sandbox object, as a reconnect gives it; they share one sandbox and
one set of rows (``_Rows``, whose conditions are the SQL's, pinned against
Postgres in ``tests/integration/test_livefs_mount_rows.py``). An exec into
the sandbox is what each test counts.
"""

from __future__ import annotations

import asyncio
import json
import shlex
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import mcp_setup
from ptc_agent.core.sandbox.livefs_mount import MountOutcome
from ptc_agent.core.sandbox.livefs_runtime.protocol import MountError
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import ExecResult, RuntimeState
from src.server.database.livefs_tokens import Served, TokenRow
from src.server.services.computer_manager import ComputerBinding, ComputerManager
from src.server.services.livefs import mount
from src.server.services.livefs.tokens import MintedToken
from src.server.services.livefs.tree import POINTS
from src.server.services.platform_secret_sweep import PlatformSecretSweeper

ROOT = "/home/workspace"
LAYOUT = SandboxLayout.for_root(ROOT)
COMPUTER = "comp-test-1"
USER = "user-test-1"
SANDBOX = "sb-test-1"
WS_A = "ws-test-a"
WS_B = "ws-test-b"
WS_C = "ws-test-c"
URL = "http://relay.test"
DEAD = "cat: .agents/user/memory/notes.md: Transport endpoint is not connected"


class _Rows:
    """The rows every worker shares, with the conditions their SQL writes
    them under."""

    def __init__(self) -> None:
        self.token: dict | None = None
        self.links: set[tuple[str, str | None]] = set()
        # One for the set: a record deletes every row laid under another.
        self.layout: str | None = None
        self.bound: set[str] = {WS_A, WS_B}
        self.reads = 0
        self.downs = 0
        self.down_fails = False
        self._minted = 0

    def row(self) -> TokenRow:
        t = self.token
        if t is None:
            return TokenRow()
        if t["served_by"] is None or t["served"] is None:
            return TokenRow(t["expires_at"], t["held_by"])
        return TokenRow(
            t["expires_at"], t["held_by"], t["served_by"], t["served"], t["served_until"]
        )

    async def current_token(self, _computer_id: str) -> TokenRow:
        return self.row()

    async def mint_token(self, computer_id: str, _user_id: str) -> MintedToken:
        self._minted += 1
        expires_at = datetime.now(UTC) + timedelta(minutes=60, seconds=self._minted)
        t = self.token
        if t is None:
            self.token = {
                "expires_at": expires_at,
                "held_by": None,
                "served_by": None,
                "served": None,
                "served_until": None,
            }
        else:
            if t["held_by"] is None:
                t["served_by"] = None
            t["held_by"], t["expires_at"] = None, expires_at
        return MintedToken(f"lfs1.{computer_id}.s{self._minted}", expires_at)

    def _serving(self, sandbox_id: str, served: Served | None) -> None:
        t = self.token
        t["served_by"] = sandbox_id if served else None
        t["served"] = served
        t["served_until"] = t["expires_at"] if served else None

    async def mark_held(self, _c, sandbox_id, expires_at, served) -> None:
        if self.token is not None and self.token["expires_at"] == expires_at:
            self.token["held_by"] = sandbox_id
            self._serving(sandbox_id, served)

    async def mark_served(self, _c, sandbox_id, expires_at, served) -> None:
        t = self.token
        if t is not None and t["held_by"] == sandbox_id and t["expires_at"] == expires_at:
            self._serving(sandbox_id, served)

    async def mark_down(self, _c, sandbox_id: str) -> None:
        if self.down_fails:
            raise ConnectionError("the database is unreachable")
        self.downs += 1
        if self.token is not None and self.token["served_by"] == sandbox_id:
            self.token["served_by"] = None

    async def revoke(self, _computer_id: str) -> None:
        self.token = None

    async def mount_view(self, _c, sandbox_id: str, layout: str):
        self.reads += 1
        if layout != self.layout:
            return self.row(), frozenset()
        return self.row(), frozenset(w for s, w in self.links if s == sandbox_id)

    async def save_links(self, _c, sandbox_id: str, linked, layout: str) -> frozenset:
        kept = frozenset(w for w in linked if w is None or w in self.bound)
        self.links = {(sandbox_id, w) for w in kept}
        self.layout = layout
        return kept

    def leave(self, workspace_id: str) -> None:
        """The workspace's delete, or its move off the computer: either takes
        its link rows in the same transaction."""
        self.bound.discard(workspace_id)
        self.links = {(s, w) for s, w in self.links if w != workspace_id}


class _Box:
    """The one sandbox every worker reaches, counting each exec into it."""

    def __init__(self) -> None:
        self.actions: list[str] = []
        self.daemon_up = False
        self.answers: dict[str, list[dict]] = {}
        self.during: dict[str, Callable[[], Awaitable[None]]] = {}

    def started(self) -> int:
        return self.actions.count("start")

    async def exec(self, command: str) -> dict:
        argv = shlex.split(command)
        action = argv[argv.index("-c") + 6]
        self.actions.append(action)
        # Long enough for a worker waiting on the folders lock to queue.
        for _ in range(3):
            await asyncio.sleep(0)
        if (during := self.during.pop(action, None)) is not None:
            await during()
        if queued := self.answers.get(action):
            return queued.pop(0)
        if action == "start":
            started, self.daemon_up = not self.daemon_up, True
            return {"ok": True, "started": started}
        return {"ok": True}


class _Sandbox:
    """One worker's object for the shared sandbox."""

    def __init__(self, box: _Box, *, lost: bool = False) -> None:
        self.sandbox_id = SANDBOX
        self.layout = LAYOUT
        self.livefs = None
        self.livefs_lost = lost
        self._box = box
        self.runtime = SimpleNamespace(exec_as_root=self._exec)

    async def _runtime_call(self, func, *args, retry_policy):
        return await func(*args)

    async def _exec(self, command: str, timeout: int, env: dict | None = None):
        return SimpleNamespace(stdout=json.dumps(await self._box.exec(command)), stderr="")


@pytest.fixture
def rows(monkeypatch) -> _Rows:
    rows = _Rows()
    # The advisory lock, which every worker takes for the same computer.
    folders = asyncio.Lock()

    @asynccontextmanager
    async def workspace_folders_lock(computer_id, wait_s=None):
        async with folders:
            yield object()

    class _Tree:
        def __init__(self, identity, store) -> None:
            pass

        async def links(self):
            linked = {None: [("user", LAYOUT.join(".agents/user"))]}
            for w in sorted(rows.bound):
                linked[w] = [(f"workspaces/{w}/transcripts", f"{ROOT}/{w}/t")]
            return linked

    monkeypatch.setattr(mount, "LivefsTree", _Tree)
    monkeypatch.setattr(mount, "workspace_folders_lock", workspace_folders_lock)
    monkeypatch.setattr(mount, "effective_relay_base_url", lambda _p: URL)
    monkeypatch.setattr(mount.db, "current_token", rows.current_token)
    monkeypatch.setattr(mount.db, "mark_held", rows.mark_held)
    monkeypatch.setattr(mount.db, "mark_served", rows.mark_served)
    monkeypatch.setattr(mount.db, "mark_down", rows.mark_down)
    monkeypatch.setattr(mount.links_db, "mount_view", rows.mount_view)
    monkeypatch.setattr(mount.links_db, "save_links", rows.save_links)
    monkeypatch.setattr(mount.tokens, "mint_token", rows.mint_token)
    monkeypatch.setattr(mount.tokens, "revoke", rows.revoke)
    return rows


def _worker() -> ComputerManager:
    return ComputerManager(SimpleNamespace(sandbox=SimpleNamespace(provider="docker")))


async def _synced() -> str:
    await asyncio.sleep(0)
    return "synced"


async def _turn(
    worker: ComputerManager, sandbox: _Sandbox, workspace_id: str, *, bring_up=False
) -> None:
    """A turn's share of the mount on ``worker``: a bring-up's when the turn
    reconnects (a worker's first turn on the computer), then the attach every
    turn makes; and the links it left going in."""
    if bring_up:
        await worker._livefs_beside(
            COMPUTER, USER, sandbox, _synced(), workspace_id=workspace_id
        )
    await worker._load_livefs(COMPUTER, USER, sandbox)
    if worker._livefs_owed(COMPUTER, workspace_id, sandbox):
        await worker._keep_livefs(COMPUTER, USER, sandbox, workspace_id)
    await _seen(worker).linked()


def _seen(worker: ComputerManager):
    return worker._machine(COMPUTER).livefs


async def _serving_everywhere(rows: _Rows, box: _Box, *workers: ComputerManager):
    """The first worker brings the mount up and links every folder; each
    other one then runs a turn, as a reconnect does. Answers each worker's
    sandbox object."""
    sandboxes = []
    for worker in workers:
        sandbox = _Sandbox(box)
        await _turn(worker, sandbox, WS_A, bring_up=True)
        sandboxes.append(sandbox)
    assert box.actions == ["start", "link"]
    assert rows.links == {(SANDBOX, None), (SANDBOX, WS_A), (SANDBOX, WS_B)}
    return sandboxes


# -- a worker trusts what another recorded -------------------------------------


@pytest.mark.asyncio
async def test_a_fresh_worker_on_a_serving_linked_computer_asks_the_sandbox_nothing(
    rows,
):
    box = _Box()
    await _serving_everywhere(rows, box, _worker())
    fresh, sandbox = _worker(), _Sandbox(box)

    await _turn(fresh, sandbox, WS_A, bring_up=True)
    reads = rows.reads
    command = await sandbox.livefs.ready(WS_A)

    assert command
    assert box.actions == ["start", "link"]
    # A command goes on with what its turn read.
    assert rows.reads == reads


@pytest.mark.asyncio
async def test_a_warm_turn_reads_the_rows_once_and_asks_the_sandbox_nothing(rows):
    box = _Box()
    worker = _worker()
    (sandbox,) = await _serving_everywhere(rows, box, worker)
    reads = rows.reads

    await _turn(worker, sandbox, WS_B)

    assert rows.reads == reads + 1
    assert box.actions == ["start", "link"]


@pytest.mark.parametrize("together", [False, True], ids=["one-after-another", "at-once"])
@pytest.mark.asyncio
async def test_new_daemon_code_is_served_by_the_first_worker_and_read_by_the_rest(
    rows, monkeypatch, together
):
    """A deploy brings every worker the new code at once. The first turn on
    the computer serves it; a worker that read the old answer before that
    one landed finds it under the lock and asks nothing."""
    box = _Box()
    workers = [_worker(), _worker(), _worker()]
    sandboxes = await _serving_everywhere(rows, box, *workers)
    monkeypatch.setattr(mount.livefs_mount, "code_version", lambda: "next-code")

    turns = [_turn(w, s, WS_A) for w, s in zip(workers, sandboxes, strict=True)]
    if together:
        await asyncio.gather(*turns)
    else:
        for turn in turns:
            await turn

    assert box.actions == ["start", "link", "start"]
    assert rows.row().served == Served("next-code", URL)
    for worker, sandbox in zip(workers, sandboxes, strict=True):
        assert _seen(worker).serves(sandbox, WS_A)


@pytest.mark.asyncio
async def test_a_stop_on_one_worker_is_not_serving_on_every_other(rows):
    box = _Box()
    stopping, other = _worker(), _worker()
    _, sandbox = await _serving_everywhere(rows, box, stopping, other)

    await stopping._revoke_livefs(COMPUTER)

    await other._load_livefs(COMPUTER, USER, sandbox)
    assert other._livefs_owed(COMPUTER, WS_A, sandbox)
    assert not _seen(other).serving(sandbox)
    assert _seen(other).serve_due(sandbox)


# -- a folder that left the computer --------------------------------------------


@pytest.mark.asyncio
async def test_a_workspace_deleted_on_another_worker_is_not_linked_on_this_one(rows):
    """Its folder is gone; one that takes its name is made anew, without the
    mount's links until a link lays them."""
    box = _Box()
    here, there = _worker(), _worker()
    sandbox, _ = await _serving_everywhere(rows, box, here, there)

    rows.leave(WS_B)
    there._forget_project(WS_B)
    rows.bound.add(WS_C)
    await _turn(here, sandbox, WS_A)

    assert not _seen(here).serves(sandbox, WS_B)
    await _turn(here, sandbox, WS_C)
    assert box.actions == ["start", "link", "link"]
    assert _seen(here).serves(sandbox, WS_C)


@pytest.mark.asyncio
async def test_a_workspace_moved_off_and_back_on_another_worker_is_linked_again(rows):
    box = _Box()
    here, there = _worker(), _worker()
    sandbox, _ = await _serving_everywhere(rows, box, here, there)

    rows.leave(WS_B)
    there._forget_project(WS_B)
    rows.bound.add(WS_B)
    await _turn(here, sandbox, WS_B)

    assert box.actions == ["start", "link", "link"]
    assert _seen(here).serves(sandbox, WS_B)


@pytest.mark.asyncio
async def test_a_link_answer_landing_after_a_workspace_left_adds_it_on_no_worker(rows):
    """The link read the folders before the workspace was deleted or moved
    off, on another worker, and answers after: the record leaves it out for
    every worker, the one that linked included."""
    box = _Box()
    linking, leaving_on, fresh = _worker(), _worker(), _worker()
    sandbox, _ = await _serving_everywhere(rows, box, linking, leaving_on)
    rows.bound.add(WS_C)

    async def leave() -> None:
        rows.leave(WS_C)
        leaving_on._forget_project(WS_C)

    box.during["link"] = leave
    await _turn(linking, sandbox, WS_C)

    assert box.actions == ["start", "link", "link"]
    assert not _seen(linking).serves(sandbox, WS_C)
    assert _seen(linking).serves(sandbox, WS_A)
    for worker in (leaving_on, fresh):
        other = _Sandbox(box)
        await _turn(worker, other, WS_A, bring_up=True)
        assert not _seen(worker).serves(other, WS_C)
    assert (SANDBOX, WS_C) not in rows.links


# -- the sandbox stays the truth ------------------------------------------------


@pytest.mark.asyncio
async def test_a_sandbox_booted_by_a_worker_is_served_whatever_the_rows_say(rows):
    """A sandbox that stopped on its own leaves rows saying it serves; the
    worker whose reconnect booted it asks it to serve anyway."""
    box = _Box()
    await _serving_everywhere(rows, box, _worker())
    box.daemon_up = False
    booting, booted = _worker(), _Sandbox(box, lost=True)

    await _turn(booting, booted, WS_A, bring_up=True)

    assert box.actions == ["start", "link", "start"]
    assert box.daemon_up and await booted.livefs.ready(WS_A)


@pytest.mark.asyncio
async def test_a_dead_daemon_a_command_met_is_recorded_for_every_worker(rows):
    """The rows said it served, so only the command could find it dead; the
    restart asks the sandbox whatever they say, and what it answers is what
    every other worker reads next."""
    box = _Box()
    healing, other = _worker(), _worker()
    sandbox, elsewhere = await _serving_everywhere(rows, box, healing, other)
    box.daemon_up = False
    box.answers["start"] = [
        {"ok": False, "error": "start_failed", "reason": "fusermount: permission denied"}
    ]

    with patch.object(mount.outcomes, "collect", AsyncMock(return_value=[])):
        await sandbox.livefs.report("c0ffee00c0ffee00", DEAD)

    assert box.actions == ["start", "link", "start"]
    assert rows.row().served_by is None
    await other._load_livefs(COMPUTER, USER, elsewhere)
    assert other._livefs_owed(COMPUTER, WS_A, elsewhere)
    assert not _seen(other).serving(elsewhere)


@pytest.mark.parametrize("answer", ["busy", "raised", "stale_code"])
@pytest.mark.asyncio
async def test_a_restart_a_reconnect_found_is_recorded_whatever_its_serve_answers(
    rows, monkeypatch, answer
):
    """The rows said it served before the boot. The worker that learned of
    the boot records it before anything else reads them, so another worker
    whose reconnect learned nothing serves it rather than hand out a mount
    that is not there, whatever the first one's own serve came to."""
    box = _Box()
    await _serving_everywhere(rows, box, _worker())
    box.daemon_up = False
    failing = True
    lock, start = mount.workspace_folders_lock, mount.livefs_mount.start

    @asynccontextmanager
    async def settling(computer_id, wait_s=None):
        if failing and answer == "busy":
            yield None
            return
        async with lock(computer_id, wait_s) as held:
            yield held

    async def starting(sandbox, **kwargs):
        if failing and answer == "raised":
            raise ConnectionError("the exec dropped")
        if failing and answer == "stale_code":
            return MountOutcome(ok=False, error=MountError.STALE_CODE)
        return await start(sandbox, **kwargs)

    monkeypatch.setattr(mount, "workspace_folders_lock", settling)
    monkeypatch.setattr(mount.livefs_mount, "start", starting)
    booting, booted = _worker(), _Sandbox(box, lost=True)

    await _turn(booting, booted, WS_A, bring_up=True)

    assert booted.livefs is None and not booted.livefs_lost
    assert rows.row().served_by is None
    failing = False
    fresh, sandbox = _worker(), _Sandbox(box)
    await _turn(fresh, sandbox, WS_A, bring_up=True)
    assert box.actions == ["start", "link", "start"]
    assert box.daemon_up and await sandbox.livefs.ready(WS_A)


@pytest.mark.asyncio
async def test_a_restart_whose_record_fails_hands_out_no_mount(rows):
    """Unrecorded, the rows still say the mount serves: the worker that
    learned of the boot reads nothing and serves nothing until it records
    it, and keeps the report until then. Its failed serve backs off as any
    does; the record lets every other worker serve meanwhile."""
    box = _Box()
    await _serving_everywhere(rows, box, _worker())
    box.daemon_up = False
    rows.down_fails = True
    worker, booted = _worker(), _Sandbox(box, lost=True)

    await _turn(worker, booted, WS_A, bring_up=True)

    assert booted.livefs is None and booted.livefs_lost
    assert box.actions == ["start", "link"]
    assert rows.row().served_by == SANDBOX
    rows.down_fails = False
    await _turn(worker, booted, WS_A)
    assert not booted.livefs_lost and rows.downs == 1
    assert rows.row().served_by is None
    other, sandbox = _worker(), _Sandbox(box)
    await _turn(other, sandbox, WS_A, bring_up=True)
    assert box.actions == ["start", "link", "start"]
    assert box.daemon_up and await sandbox.livefs.ready(WS_A)


@pytest.mark.asyncio
async def test_a_scrub_restart_is_recorded_for_every_worker(rows):
    """The platform-secret scrub restarts the sandbox off every turn, so no
    worker's sandbox object learns of it; a worker whose cached one never
    reconnects would go on with a mount that is gone."""
    box = _Box()
    here, there = _worker(), _worker()
    sandbox, elsewhere = await _serving_everywhere(rows, box, here, there)

    async def scrub(runtime, **kwargs):
        box.daemon_up = False

    sweeper = PlatformSecretSweeper()
    idle = SimpleNamespace(has_active_tasks_for_computer=AsyncMock(return_value=False))
    provider = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(capabilities=set())))
    behind = {"workspace_id": None, "platform_secret_version": 0, "is_always_on": False}
    with (
        patch(
            "ptc_agent.core.sandbox.platform_secrets.converge_sandbox_platform_secrets",
            AsyncMock(side_effect=scrub),
        ),
        patch(
            "src.server.services.platform_secret_rollout.stamp_platform_secret_version",
            AsyncMock(),
        ),
        patch(
            "src.server.services.runs.executor.LocalRunExecutor.get_instance",
            return_value=idle,
        ),
        patch.object(sweeper, "_drop_local_session", AsyncMock()),
    ):
        assert await sweeper._converge_locked(
            COMPUTER,
            SANDBOX,
            behind,
            SimpleNamespace(placeholders={}, bindings={}, generation=1),
            provider,
        )

    assert rows.row().served_by is None
    await _turn(here, sandbox, WS_A)
    await _turn(there, elsewhere, WS_A)
    assert box.actions == ["start", "link", "start"]
    assert box.daemon_up and await elsewhere.livefs.ready(WS_A)


@pytest.mark.parametrize("lost", [False, True], ids=["running", "booted"])
@pytest.mark.asyncio
async def test_an_attach_whose_tool_overlay_fails_still_reads_the_rows(
    rows, monkeypatch, lost
):
    """The overlay failing ends the attach, not the turn: the turn's
    commands still get the mount the rows say serves, and a restart its
    reconnect found is recorded for every other worker."""
    box = _Box()
    await _serving_everywhere(rows, box, _worker())
    worker, sandbox = _worker(), _Sandbox(box, lost=lost)
    monkeypatch.setattr(worker, "_maybe_restore_files", AsyncMock(return_value=True))
    monkeypatch.setattr(worker, "_ensure_project_tool_overlay", AsyncMock(return_value=False))
    keep = AsyncMock()
    monkeypatch.setattr(worker, "_keep_livefs", keep)

    await worker._ensure_project_attached(
        ComputerBinding(WS_A, COMPUTER), SimpleNamespace(sandbox=sandbox), user_id=USER
    )

    keep.assert_not_awaited()
    assert box.actions == ["start", "link"]
    if lost:
        assert rows.row().served_by is None and sandbox.livefs is None
    else:
        assert await sandbox.livefs.ready(WS_A)


@pytest.mark.asyncio
async def test_an_attach_ending_early_takes_back_a_mount_the_rows_say_is_down(
    rows, monkeypatch
):
    """This worker's warm sandbox object was handed the mount on an earlier
    turn, and a restart was recorded since. An attach that ends before any
    serve must not leave the turn's prompt saying the files are mounted."""
    box = _Box()
    worker = _worker()
    (sandbox,) = await _serving_everywhere(rows, box, worker)
    box.daemon_up = False
    await mount.restarted(COMPUTER, SANDBOX)
    monkeypatch.setattr(worker, "_maybe_restore_files", AsyncMock(return_value=True))
    monkeypatch.setattr(worker, "_ensure_project_tool_overlay", AsyncMock(return_value=False))

    await worker._ensure_project_attached(
        ComputerBinding(WS_A, COMPUTER), SimpleNamespace(sandbox=sandbox), user_id=USER
    )

    assert sandbox.livefs is None
    await _turn(worker, sandbox, WS_A)
    assert box.actions == ["start", "link", "start"]
    assert box.daemon_up and await sandbox.livefs.ready(WS_A)


# -- the link layout -------------------------------------------------------------


def test_the_link_layout_follows_the_declared_points_and_the_daemon_code():
    code = mount.livefs_mount.code_version()
    assert mount._layout(POINTS, code) == mount.LINK_LAYOUT
    assert mount._layout(POINTS[:-1], code) != mount.LINK_LAYOUT
    moved = (replace(POINTS[0], target=".agents/elsewhere"), *POINTS[1:])
    assert mount._layout(moved, code) != mount.LINK_LAYOUT
    assert mount._layout(POINTS, "next-release") != mount.LINK_LAYOUT


@pytest.mark.asyncio
async def test_a_release_with_another_link_layout_links_every_sandbox_again(
    rows, monkeypatch
):
    box = _Box()
    worker = _worker()
    (sandbox,) = await _serving_everywhere(rows, box, worker)

    await _turn(worker, sandbox, WS_A)
    assert box.actions == ["start", "link"]

    monkeypatch.setattr(mount, "LINK_LAYOUT", "next-release")
    fresh, other = _worker(), _Sandbox(box)
    await _turn(fresh, other, WS_A, bring_up=True)
    assert box.actions == ["start", "link", "link"]
    assert rows.layout == "next-release" and await other.livefs.ready(WS_B)
    await _turn(fresh, other, WS_B)
    assert box.actions == ["start", "link", "link"]


# -- the boot signal ------------------------------------------------------------


def _config() -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


class _Booting(Exception):
    pass


@pytest.mark.asyncio
async def test_a_reconnect_that_boots_the_sandbox_marks_it_with_no_mount():
    class _Runtime:
        async def get_state(self):
            return RuntimeState.STOPPED

        async def start(self, timeout):
            raise _Booting

    class _Provider:
        async def get(self, sandbox_id, **kwargs):
            return _Runtime()

        async def prepare_reconnect(self, runtime, *, tier=None):
            pass

        def is_transient_error(self, exc):
            return False

        def is_host_unavailable(self, exc):
            return False

    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _Provider()
    sandbox.livefs = object()

    with pytest.raises(_Booting):
        await sandbox.reconnect(SANDBOX)

    assert sandbox.livefs_lost and sandbox.livefs is None


class _Running:
    async def get_state(self):
        return RuntimeState.RUNNING

    async def fetch_working_dir(self):
        return ROOT


@pytest.mark.parametrize(
    "answered, lost", [(True, False), (False, True), (None, False)], ids=["up", "gone", "unasked"]
)
@pytest.mark.asyncio
async def test_a_reconnect_to_a_running_sandbox_reads_the_mount_off_its_one_exec(
    answered, lost
):
    """A sandbox running again since the rows were written may hold no
    mount; the supervisor start every reconnect runs says, at no exec more."""

    class _Provider:
        async def get(self, sandbox_id, **kwargs):
            return _Running()

        async def prepare_reconnect(self, runtime, *, tier=None):
            pass

        def is_transient_error(self, exc):
            return False

    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _Provider()
    handle = object()
    sandbox.livefs = handle
    sandbox._start_internal_mcp_servers = AsyncMock(return_value=answered)

    await sandbox.reconnect(SANDBOX)

    sandbox._start_internal_mcp_servers.assert_awaited_once()
    assert sandbox.livefs_lost is lost
    assert sandbox.livefs is (None if lost else handle)


@pytest.mark.parametrize(
    "stdout, answered",
    [("livefs-mount-answers\n", True), ("", False), (None, None)],
    ids=["answers", "absent", "exec-failed"],
)
@pytest.mark.asyncio
async def test_the_supervisor_start_asks_whether_the_mount_answers(stdout, answered):
    commands: list[str] = []

    async def exec_(command):
        commands.append(command)
        if stdout is None:
            raise ConnectionError("the exec dropped")
        return ExecResult(stdout=stdout, stderr="", exit_code=0)

    async def runtime_call(func, *args, retry_policy):
        return await func(*args)

    sandbox = SimpleNamespace(
        runtime=SimpleNamespace(exec=exec_), _runtime_call=runtime_call, _work_dir=ROOT
    )

    assert await mcp_setup._start_internal_mcp_servers(sandbox) is answered
    assert len(commands) == 1 and "test -e /mnt/livefs/." in commands[0]
