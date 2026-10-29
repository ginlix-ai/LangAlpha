"""Bringing a computer's file mount up, and when a warm turn asks again.

Serving (the token and the daemon) and the links are asked of the sandbox
apart, each only when due, since every ask is an exec into the sandbox. A
bring-up serves beside its asset sync and lays the links off the turn's
path once the daemon answered, serving or not; a command in a workspace
waits for that workspace's links and runs without the mount while they are
not in. A token running low is renewed in the background, once per
computer, while turns and commands go on with it. A mount failure never
costs a turn: the file tools still reach these files through the store.

These pin one worker's own decisions: the rows every worker records and
reads at a turn's start are stood in for as unreadable, which leaves the
worker with what it saw itself. ``test_livefs_workers`` has several workers
share them.
"""

from __future__ import annotations

import asyncio
import json
import shlex
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime import lifecycle
from ptc_agent.core.sandbox.livefs_runtime.protocol import CONFIG_ENV, MountError
from src.server.database.livefs_tokens import Served, TokenRow
from src.server.services import transcripts
from src.server.services.computer_manager import ComputerManager
from src.server.services.livefs import mount, tracker
from src.server.services.livefs.mount import LinkState, MountState
from src.server.services.livefs.tokens import MintedToken
from src.server.services.livefs.tracker import MountTracker

ROOT = "/home/workspace"
LAYOUT = SandboxLayout.for_root(ROOT)
INDEX = transcripts.index_path(ROOT)
COMPUTER = "comp-test-1"
USER = "user-test-1"
WS_A = "ws-test-a"
WS_B = "ws-test-b"
FOLDER_A = LAYOUT.for_workspace("research-a")
FOLDER_B = LAYOUT.for_workspace("research-b")
DEAD = "cat: .agents/user/memory/notes.md: Transport endpoint is not connected"
#: The rows' read, which every test here but the ones on it stands in for.
VIEW = mount.view


def _in(minutes: float) -> datetime:
    return datetime.now(UTC) + timedelta(minutes=minutes)


class _Sandbox:
    """What a daemon command touches, answering each in turn."""

    mounts_livefs = True

    def __init__(self, *answers: dict, sandbox_id: str = "sb-test-1") -> None:
        self.sandbox_id = sandbox_id
        self.layout = LAYOUT
        self.livefs = None
        self.commands: list[str] = []
        #: The token each command carried in its environment, if any.
        self.configs: list[dict | None] = []
        self._answers = list(answers) or [{"ok": True}]
        self.runtime = SimpleNamespace(exec_as_root=self._exec)

    async def _runtime_call(self, func, *args, retry_policy):
        return await func(*args)

    async def _exec(self, command: str, timeout: int, env: dict | None = None):
        self.commands.append(command)
        sent = (env or {}).get(CONFIG_ENV)
        self.configs.append(json.loads(sent) if sent else None)
        answer = self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]
        return SimpleNamespace(stdout=json.dumps(answer), stderr="")

    def argv(self, index: int = -1) -> list[str]:
        return shlex.split(self.commands[index])


@pytest.fixture
def server(monkeypatch):
    """The server's side of a bring-up, with its database and tree stood in for."""
    state = SimpleNamespace(
        links={
            None: [("user", LAYOUT.join(".agents/user")), ("computer/threads.jsonl", INDEX)],
            WS_A: [(f"workspaces/{WS_A}/transcripts", FOLDER_A.transcripts)],
        },
        expires_at=_in(59),
        mint=AsyncMock(return_value=MintedToken("lfs1.comp-test-1.fresh", _in(60))),
        settling=False,
        #: The sandbox that took the current token, as its row says.
        held_by="sb-test-1",
        #: The sandbox whose daemon the last start answered serving, and with
        #: what; none until one answers.
        served_by=None,
        served=None,
        mark_held=AsyncMock(),
        mark_served=AsyncMock(),
        #: Records every folder linked; ``_Rows`` in test_livefs_workers
        #: leaves out the ones that left the computer.
        save_links=AsyncMock(side_effect=lambda _c, _s, linked, _layout: linked),
        #: Each folders lock taken, by its wait ("full" for the default).
        waits=[],
    )

    class _Tree:
        def __init__(self, identity, store) -> None:
            pass

        async def links(self):
            return {key: list(group) for key, group in state.links.items()}

    @asynccontextmanager
    async def workspace_folders_lock(computer_id, wait_s="full"):
        state.waits.append(wait_s)
        yield None if state.settling else object()

    monkeypatch.setattr(mount, "LivefsTree", _Tree)
    monkeypatch.setattr(mount, "workspace_folders_lock", workspace_folders_lock)
    monkeypatch.setattr(
        mount.db,
        "current_token",
        AsyncMock(
            side_effect=lambda _c: TokenRow(
                state.expires_at,
                state.held_by,
                state.served_by,
                state.served,
                state.expires_at,
            )
        ),
    )
    monkeypatch.setattr(mount.db, "mark_held", state.mark_held)
    monkeypatch.setattr(mount.db, "mark_served", state.mark_served)
    monkeypatch.setattr(mount.links_db, "save_links", state.save_links)
    monkeypatch.setattr(mount.tokens, "mint_token", state.mint)
    monkeypatch.setattr(mount, "effective_relay_base_url", lambda _p: "http://relay.test")
    return state


@pytest.fixture(autouse=True)
def _rows_unread(monkeypatch):
    monkeypatch.setattr(mount, "view", AsyncMock(return_value=None))


async def _serve(sandbox: _Sandbox, **kwargs) -> MountState:
    return await mount.serve(
        sandbox, computer_id=COMPUTER, user_id=USER, provider="daytona", **kwargs
    )


async def _link(sandbox: _Sandbox) -> LinkState:
    return await mount.link(sandbox, computer_id=COMPUTER, user_id=USER)


async def _renew(sandbox: _Sandbox):
    return await mount.renew(
        sandbox, computer_id=COMPUTER, user_id=USER, provider="daytona"
    )


# -- serving -------------------------------------------------------------------


@pytest.mark.parametrize("ask", [_serve, _link], ids=["serve", "link"])
@pytest.mark.asyncio
async def test_a_settle_holding_the_folders_leaves_the_mount_alone(server, ask):
    server.settling = True
    sandbox = _Sandbox()

    state = await ask(sandbox)

    assert state.error == MountError.BUSY
    assert sandbox.commands == []


@pytest.mark.asyncio
async def test_serve_runs_the_code_this_host_ships_and_links_nothing(server):
    sandbox = _Sandbox({"ok": True, "started": True})

    state = await _serve(sandbox)

    argv = sandbox.argv()
    assert "start" in argv and "--link" not in argv
    # Root runs the host's own boot text, never the shipped package.
    boot = argv.index("-c")
    assert argv[boot - 1] == "-I" and "PYTHONPATH" not in sandbox.commands[-1]
    assert argv[boot + 4] == lifecycle.code_version()
    assert json.loads(argv[boot + 5]) == lifecycle.code_manifest()
    assert state.mounted and state.started
    assert server.waits == [mount.TURN_LOCK_WAIT_S]


@pytest.mark.parametrize(
    ("state", "settled"),
    [
        (MountState(True), True),
        (MountState(False, error=MountError.UNSUPPORTED), True),
        (MountState(False, error=MountError.INSTALLING), True),
        (MountState(False, error=MountError.UNREACHABLE), True),
        (MountState(False, error=MountError.BAD_CONFIG), True),
        (MountState(False, error=MountError.STALE_CODE), False),
        (MountState(False, error=MountError.START_FAILED), False),
        (MountState(False, error=MountError.UNANSWERED), False),
        (MountState(False, error=MountError.BUSY), False),
        (MountState(False, reason="exec lost"), False),
    ],
)
def test_a_serve_is_settled_unless_asking_again_at_once_may_answer_otherwise(
    state, settled
):
    """An asset sync may be replacing the daemon's code, a settle may let go
    of the folders, and an exec that lost its answer may get one."""
    assert state.settled is settled


# -- the links -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_lays_what_the_tree_declares_and_carries_no_token(server):
    sandbox = _Sandbox()

    state = await _link(sandbox)

    argv = sandbox.argv()
    assert "link" in argv
    assert f"computer/threads.jsonl:{INDEX}" in argv
    assert "--base-url" not in argv
    assert sandbox.configs == [None]
    server.mint.assert_not_awaited()
    assert state == LinkState(frozenset({None, WS_A}))
    assert server.waits == [mount.TURN_LOCK_WAIT_S]


@pytest.mark.asyncio
async def test_a_folder_whose_link_failed_is_not_linked_and_the_rest_are(server):
    server.links[WS_B] = [(f"workspaces/{WS_B}/transcripts", FOLDER_B.transcripts)]
    sandbox = _Sandbox(
        {
            "ok": False,
            "error": "link_failed",
            "failed": {FOLDER_A.transcripts: "Permission denied"},
        }
    )

    state = await _link(sandbox)

    assert state.linked == {None, WS_B} and state.failed == {WS_A}
    assert state.error == MountError.LINK_FAILED


@pytest.mark.asyncio
async def test_a_failed_link_of_the_computers_own_counts_against_every_folder(server):
    """A folder that owns the root reads its files through those links."""
    sandbox = _Sandbox(
        {"ok": False, "error": "link_failed", "failed": {INDEX: "Permission denied"}}
    )

    state = await _link(sandbox)

    assert state.linked == frozenset() and state.failed == {None, WS_A}


@pytest.mark.parametrize(
    "stdout",
    [json.dumps({"ok": False, "error": "stale_code"}), ""],
    ids=["stale-code", "unanswered"],
)
@pytest.mark.asyncio
async def test_a_link_the_daemon_did_not_answer_for_says_nothing_of_what_is_linked(
    server, stdout
):
    sandbox = _Sandbox()
    sandbox.runtime.exec_as_root = AsyncMock(
        return_value=SimpleNamespace(stdout=stdout, stderr="killed")
    )

    state = await _link(sandbox)

    assert state.linked is None
    assert state.error in (MountError.STALE_CODE, MountError.UNANSWERED)


# -- the token the sandbox holds -----------------------------------------------


@pytest.mark.asyncio
async def test_a_serve_keeps_a_token_with_time_left_and_the_server_address_goes_along(
    server,
):
    sandbox = _Sandbox()

    state = await _serve(sandbox)

    server.mint.assert_not_awaited()
    assert sandbox.configs == [None]
    argv = sandbox.argv()
    assert argv[argv.index("--base-url") + 1] == "http://relay.test"
    assert state.expires_at == server.expires_at


@pytest.mark.asyncio
async def test_a_turn_keeps_a_token_that_only_runs_low(server):
    """It still carries a command; the background renewal replaces it."""
    server.expires_at = _in(5)
    sandbox = _Sandbox()

    state = await _serve(sandbox)

    server.mint.assert_not_awaited()
    assert sandbox.configs == [None] and state.expires_at == server.expires_at


@pytest.mark.asyncio
async def test_a_token_as_good_as_gone_is_replaced(server):
    server.expires_at = _in(0.5)
    sandbox = _Sandbox()

    await _serve(sandbox)

    assert sandbox.configs == [
        {"base_url": "http://relay.test", "token": "lfs1.comp-test-1.fresh"}
    ]


@pytest.mark.asyncio
async def test_a_fresh_token_the_sandbox_never_took_is_replaced(server):
    """A publish can fail after its mint, so a token with time left is kept
    only while its row says this sandbox took it."""
    server.held_by = None
    sandbox = _Sandbox()

    await _serve(sandbox)

    server.mint.assert_awaited_once()
    assert sandbox.configs[-1] is not None
    server.mark_held.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_sandbox_that_lost_its_token_is_given_a_new_one(server):
    sandbox = _Sandbox({"ok": False, "error": "no_config"}, {"ok": True})

    state = await _serve(sandbox)

    assert state.mounted
    server.mint.assert_awaited_once()
    assert sandbox.configs[0] is None and sandbox.configs[1] is not None


# -- the background renewal ----------------------------------------------------


@pytest.mark.asyncio
async def test_a_token_another_worker_renewed_is_adopted_without_an_exec(server):
    sandbox = _Sandbox()

    assert await _renew(sandbox) == MountState(True, server.expires_at)

    server.mint.assert_not_awaited()
    assert sandbox.commands == [] and server.waits == []


@pytest.mark.asyncio
async def test_a_renewal_publishes_a_new_token_and_leaves_the_links(server):
    server.expires_at = _in(20)
    sandbox = _Sandbox({"ok": True})

    renewed = await _renew(sandbox)

    assert renewed.mounted
    assert renewed.expires_at == server.mint.return_value.expires_at
    argv = sandbox.argv()
    assert "start" in argv and "--link" not in argv and sandbox.configs[-1] is not None
    assert server.waits == ["full"]
    server.mark_held.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_renewal_the_sandbox_refused_is_not_recorded_as_held(server):
    server.expires_at = _in(20)
    sandbox = _Sandbox({"ok": False, "error": "unanswered"})

    assert await _renew(sandbox) is None

    server.mark_held.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_renewal_the_daemon_cannot_serve_with_answers_the_mount_down(server):
    """The sandbox holds the new token, but a mount that cannot reach the
    server serves nothing, and the tracker has to hear so."""
    server.expires_at = _in(20)
    sandbox = _Sandbox({"ok": False, "error": "unreachable"})

    renewed = await _renew(sandbox)

    assert not renewed.mounted and renewed.error == MountError.UNREACHABLE
    server.mark_held.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_renewal_for_a_computer_leaving_service_renews_nothing(server):
    """A renewal that waited on the lock while a stop revoked the token must
    not bring one back; the mint refuses it."""
    server.expires_at = None
    server.mint.side_effect = mount.tokens.NotServing("stopping")
    sandbox = _Sandbox()

    assert await _renew(sandbox) is None

    assert sandbox.commands == []
    server.mark_held.assert_not_awaited()


# -- failures ------------------------------------------------------------------


@pytest.mark.parametrize("ask", [_serve, _link], ids=["serve", "link"])
@pytest.mark.asyncio
async def test_nothing_raises_when_the_sandbox_call_fails(server, caplog, ask):
    sandbox = _Sandbox()
    sandbox.runtime.exec_as_root = AsyncMock(side_effect=RuntimeError("exec lost"))

    state = await ask(sandbox)

    assert state.reason == "exec lost" and state.error is None
    assert "exec lost" in caplog.text


@pytest.mark.asyncio
async def test_a_refused_mount_is_logged_and_reported_with_the_daemons_code(
    server, caplog
):
    sandbox = _Sandbox({"ok": False, "error": "unsupported", "reason": "no /dev/fuse"})

    state = await _serve(sandbox)

    assert not state.mounted
    assert (state.error, state.reason) == ("unsupported", "no /dev/fuse")
    assert "unsupported" in caplog.text


# -- what a warm turn asks -----------------------------------------------------


@pytest.fixture
def clock(monkeypatch):
    """The tracker's monotonic clock, set by hand; the event loop keeps its own."""
    now = SimpleNamespace(t=1000.0)
    monkeypatch.setattr(tracker, "time", SimpleNamespace(monotonic=lambda: now.t))
    return now


def _manager() -> ComputerManager:
    return ComputerManager(SimpleNamespace(sandbox=SimpleNamespace(provider="docker")))


def _up(minutes_left: float = 59, *, started: bool = False) -> MountState:
    return MountState(True, _in(minutes_left), started=started)


def _links(*workspace_ids: str) -> LinkState:
    """Every link went in: the computer's own and ``workspace_ids``'."""
    return LinkState(frozenset({None, *workspace_ids}))


async def _served(manager, sandbox, state: MountState) -> None:
    """Let the manager record one serve whose answer is ``state``."""
    with patch.object(mount, "serve", AsyncMock(return_value=state)):
        await manager._serve_livefs(COMPUTER, USER, sandbox)


async def _linked(manager, sandbox, state: LinkState | None) -> None:
    """Record one link whose answer is ``state`` (None: it raised)."""
    _seen(manager).saw_links(sandbox, state)


def _seen(manager) -> MountTracker:
    return manager._machine(COMPUTER).livefs


@pytest.mark.asyncio
async def test_a_serving_mount_with_the_workspace_linked_is_not_asked_again(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up(minutes_left=31))
    await _linked(manager, sandbox, _links(WS_A))

    assert sandbox.livefs is not None
    assert not manager._livefs_owed(COMPUTER, WS_A, sandbox)


@pytest.mark.asyncio
async def test_a_different_sandbox_is_due(clock):
    manager = _manager()
    await _served(manager, _Sandbox(), _up())

    assert _seen(manager).serve_due(_Sandbox(sandbox_id="sb-test-2"))


@pytest.mark.asyncio
async def test_a_workspace_not_yet_linked_owes_a_link_and_no_serve(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(manager, sandbox, _links(WS_A))

    seen = _seen(manager)
    assert not seen.serve_due(sandbox)
    assert seen.link_owed(sandbox, WS_B) and not seen.link_owed(sandbox, WS_A)


@pytest.mark.parametrize(
    ("answer", "owed"),
    [
        (MountState(False, error=MountError.UNREACHABLE), True),
        (MountState(False, error=MountError.UNSUPPORTED), True),
        (MountState(False, reason="folders are moving", error=MountError.BUSY), False),
        (MountState(False, reason="exec lost"), False),
    ],
    ids=["unreachable", "unsupported", "busy", "raised"],
)
@pytest.mark.asyncio
async def test_a_mount_that_does_not_serve_still_owes_its_links(clock, answer, owed):
    """Unserved, a link dangles, so code that builds a served path is
    refused rather than writing files the store never sees. A serve that
    never reached the daemon says nothing of the sandbox."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, answer)

    assert _seen(manager).link_owed(sandbox, WS_A) is owed
    assert _seen(manager).link_owed(sandbox, None) is owed


@pytest.mark.asyncio
async def test_a_token_under_thirty_minutes_is_renewed_in_the_background_not_due(
    clock,
):
    manager, sandbox = _manager(), _Sandbox()
    renew = AsyncMock(return_value=MountState(True, _in(60)))
    with patch.object(mount, "renew", renew):
        await _served(manager, sandbox, _up(minutes_left=29))
        assert not _seen(manager).serve_due(sandbox)
        await asyncio.sleep(0)

    renew.assert_awaited_once()
    assert not _seen(manager).renewal_owed(sandbox)


@pytest.mark.asyncio
async def test_a_token_as_good_as_gone_is_due(clock):
    manager, sandbox = _manager(), _Sandbox()
    with patch.object(mount, "renew", AsyncMock(return_value=None)):
        await _served(manager, sandbox, _up(minutes_left=0.5))

    assert _seen(manager).serve_due(sandbox)


@pytest.mark.asyncio
async def test_a_reconnect_the_rows_say_is_served_is_handed_the_mount(clock, monkeypatch):
    """The daemon outlives a sandbox object, so a reconnect's new one is
    handed the serving mount at the turn's read of the rows, without asking
    the sandbox again. (It once served again: one exec per reconnect, on
    every worker.)"""
    manager = _manager()
    await _served(manager, _Sandbox(), _up())
    await _linked(manager, _Sandbox(), _links(WS_A))
    reconnected, serve = _Sandbox(), AsyncMock()
    rows = mount.MountView(True, _in(59), frozenset({None, WS_A}))
    monkeypatch.setattr(mount, "view", AsyncMock(return_value=rows))

    await manager._load_livefs(COMPUTER, USER, reconnected)

    assert not manager._livefs_owed(COMPUTER, WS_A, reconnected)
    with patch.object(mount, "serve", serve):
        await manager._keep_livefs(COMPUTER, USER, reconnected, WS_A)
    serve.assert_not_awaited()
    assert await reconnected.livefs.ready(WS_A)


@pytest.mark.parametrize(
    "rows",
    [None, mount.MountView(False, None, frozenset({None, WS_A}))],
    ids=["unread", "not-serving"],
)
@pytest.mark.asyncio
async def test_a_reconnect_is_handed_nothing_the_rows_do_not_say_serves(
    clock, monkeypatch, rows
):
    """What this worker saw before the reconnect is no answer: the reconnect
    may have found the mount gone, which only the rows then say."""
    manager = _manager()
    await _served(manager, _Sandbox(), _up())
    reconnected = _Sandbox()
    monkeypatch.setattr(mount, "view", AsyncMock(return_value=rows))

    await manager._load_livefs(COMPUTER, USER, reconnected)

    assert reconnected.livefs is None
    assert _seen(manager).serve_due(reconnected)


@pytest.mark.asyncio
async def test_a_restart_a_reconnect_reported_is_recorded_before_a_serve_reads_the_row(
    server, monkeypatch
):
    """No mount survives a boot, and a command meets no error there to heal
    by, so the row saying the daemon serves is no answer once one is
    reported: the serve asks the sandbox."""
    server.served_by = "sb-test-1"
    server.served = Served(mount._code(), "http://relay.test")

    async def mark_down(_c, sandbox_id):
        if server.served_by == sandbox_id:
            server.served_by = None

    monkeypatch.setattr(mount.db, "mark_down", AsyncMock(side_effect=mark_down))
    sandbox = _Sandbox({"ok": True, "started": True})
    sandbox.livefs_lost = True

    state = await _serve(sandbox)

    assert state.mounted and state.started
    assert len(sandbox.commands) == 1 and "start" in sandbox.argv()
    mount.db.mark_down.assert_awaited_once_with(COMPUTER, "sb-test-1")
    assert not sandbox.livefs_lost


@pytest.mark.asyncio
async def test_a_reported_restart_whose_record_fails_is_read_by_nothing(monkeypatch):
    """The report stays for the next read, and a read that cannot record it
    answers nothing, so no worker hands out the mount the row still says
    serves."""
    monkeypatch.setattr(
        mount.db, "mark_down", AsyncMock(side_effect=ConnectionError("unreachable"))
    )
    mount_view = AsyncMock()
    monkeypatch.setattr(mount.links_db, "mount_view", mount_view)
    sandbox = _Sandbox()
    sandbox.livefs_lost = True

    view = await VIEW(sandbox, computer_id=COMPUTER, provider="docker")

    assert view is None and sandbox.livefs_lost
    mount_view.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_mount_is_retried_after_ten_minutes(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, MountState(False, error=MountError.UNREACHABLE))

    assert sandbox.livefs is None
    clock.t += 599
    assert not _seen(manager).serve_due(sandbox)
    clock.t += 1
    assert _seen(manager).serve_due(sandbox)


@pytest.mark.asyncio
async def test_a_sandbox_installing_fuse_is_retried_after_twenty_seconds(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, MountState(False, error=MountError.INSTALLING))

    clock.t += 19
    assert not _seen(manager).serve_due(sandbox)
    clock.t += 1
    assert _seen(manager).serve_due(sandbox)


@pytest.mark.asyncio
async def test_a_serve_held_off_by_a_settle_keeps_a_serving_mount_and_asks_after_twenty_seconds(
    clock,
):
    manager, sandbox = _manager(), _Sandbox()
    with patch.object(mount, "renew", AsyncMock(return_value=None)):
        await _served(manager, sandbox, _up(minutes_left=0.5))
    handle = sandbox.livefs

    await _served(manager, sandbox, MountState(False, error=MountError.BUSY))

    assert sandbox.livefs is handle
    clock.t += 19
    assert not _seen(manager).serve_due(sandbox)
    clock.t += 1
    assert _seen(manager).serve_due(sandbox)


@pytest.mark.parametrize(
    "answer",
    [
        LinkState(error=MountError.BUSY, reason="folders are moving"),
        LinkState(error=MountError.STALE_CODE),
        LinkState(error=MountError.UNANSWERED),
        LinkState(reason="exec lost"),
        None,
    ],
    ids=["busy", "stale-code", "unanswered", "failed", "raised"],
)
@pytest.mark.asyncio
async def test_links_the_daemon_did_not_answer_for_are_asked_again_after_twenty_seconds(
    clock, answer
):
    """Not by every command meanwhile: each would wait on the folders lock
    and an exec, then go without."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(manager, sandbox, answer)

    seen = _seen(manager)
    clock.t += 19
    assert not seen.link_owed(sandbox, WS_A) and not seen.link_owed(sandbox, None)
    clock.t += 1
    assert seen.link_owed(sandbox, WS_A)


@pytest.mark.asyncio
async def test_a_link_that_failed_in_one_folder_leaves_the_mount_serving_the_rest(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())

    await _linked(
        manager,
        sandbox,
        LinkState(frozenset({None, WS_B}), frozenset({WS_A}), MountError.LINK_FAILED),
    )

    seen = _seen(manager)
    assert not seen.serve_due(sandbox)
    assert seen.serves(sandbox, WS_B) and not seen.serves(sandbox, WS_A)
    # An error that outlasts the turn waits ten minutes, like a failed serve.
    clock.t += 599
    assert not seen.link_owed(sandbox, WS_A)
    clock.t += 1
    assert seen.link_owed(sandbox, WS_A)


@pytest.mark.asyncio
async def test_a_workspace_that_joined_after_a_failed_link_is_not_held_off_by_it(clock):
    """The failed link never read its folder, so it says nothing about it."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(
        manager,
        sandbox,
        LinkState(frozenset({None}), frozenset({WS_A}), MountError.LINK_FAILED),
    )

    seen = _seen(manager)
    assert not seen.link_owed(sandbox, WS_A)
    assert seen.link_owed(sandbox, WS_B)


@pytest.mark.asyncio
async def test_a_workspace_unlinked_while_a_link_runs_stays_unlinked_by_its_answer(clock):
    """The running link may have read the folder that left; its answer must
    not bring back a workspace whose folder is made anew without links."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    seen = _seen(manager)
    link, land = _held(_links(WS_A, WS_B))
    seen.link_soon(sandbox, WS_A, link)
    await asyncio.sleep(0)

    manager._forget_project(WS_A)
    land.set()
    await seen.linked()

    assert link.landed == 1
    assert seen.serves(sandbox, WS_B) and not seen.serves(sandbox, WS_A)
    assert seen.link_owed(sandbox, WS_A)


@pytest.mark.asyncio
async def test_a_link_answer_the_daemon_gave_for_no_list_keeps_what_was_linked(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(manager, sandbox, _links(WS_A))

    await _linked(manager, sandbox, LinkState(error=MountError.UNANSWERED))

    assert _seen(manager).serves(sandbox, WS_A)


@pytest.mark.asyncio
async def test_a_link_answer_from_a_sandbox_replaced_meanwhile_is_dropped(clock):
    manager, old, rebuilt = _manager(), _Sandbox(), _Sandbox(sandbox_id="sb-test-2")
    await _served(manager, rebuilt, _up())

    await _linked(manager, old, _links(WS_A))

    assert not _seen(manager).serves(rebuilt, WS_A)


@pytest.mark.asyncio
async def test_an_unsupported_sandbox_is_never_retried_but_its_rebuild_is(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, MountState(False, error=MountError.UNSUPPORTED))

    clock.t += 10**9
    assert not _seen(manager).serve_due(sandbox)
    assert _seen(manager).serve_due(_Sandbox(sandbox_id="sb-test-2"))


@pytest.mark.asyncio
async def test_a_project_this_worker_forgot_is_linked_again_on_its_next_turn(clock):
    """A workspace that leaves a computer and comes back gets a new folder
    there, without the links its old one had; what forgets its folder
    forgets those too."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(manager, sandbox, _links(WS_A, WS_B))

    manager._forget_project(WS_A)

    seen = _seen(manager)
    assert seen.link_owed(sandbox, WS_A) and not seen.link_owed(sandbox, WS_B)
    assert manager._livefs_owed(COMPUTER, WS_A, sandbox)


@pytest.mark.asyncio
async def test_a_forgotten_project_is_unlinked_on_every_computer_this_worker_knows(
    clock,
):
    """A user's workspace can sit on any of their computers; forgetting it
    reaches each one's view, whichever it was on."""
    manager = _manager()
    first, second = _Sandbox(), _Sandbox(sandbox_id="sb-test-2")
    other = "comp-test-2"
    await _served(manager, first, _up())
    with patch.object(mount, "serve", AsyncMock(return_value=_up())):
        await manager._serve_livefs(other, USER, second)
    _seen(manager).saw_links(first, _links(WS_A))
    manager._machine(other).livefs.saw_links(second, _links(WS_A, WS_B))

    manager._forget_project(WS_A)

    assert not _seen(manager).serves(first, WS_A)
    assert not manager._machine(other).livefs.serves(second, WS_A)
    assert manager._machine(other).livefs.serves(second, WS_B)


# -- the mount beside an asset sync, and off the turn's path --------------------


def _held(*answers: LinkState) -> tuple[AsyncMock, asyncio.Event]:
    """A ``link`` holding its answers until the test lets them land. Each
    answer given is counted in ``landed``."""
    land = asyncio.Event()
    replies = list(answers)

    async def link(*args, **kwargs):
        await land.wait()
        mock.landed += 1
        return replies.pop(0) if len(replies) > 1 else replies[0]

    mock = AsyncMock(side_effect=link)
    mock.landed = 0
    return mock, land


async def _synced() -> str:
    await asyncio.sleep(0)
    return "synced"


async def _beside(manager, sandbox, *, workspace_id: str | None = None) -> str:
    """A bring-up the turn waits on returns before its links land."""
    return await asyncio.wait_for(
        manager._livefs_beside(
            COMPUTER, USER, sandbox, _synced(), workspace_id=workspace_id
        ),
        timeout=1,
    )


@pytest.mark.asyncio
async def test_a_wake_hands_the_turn_the_mount_while_the_links_go_in(clock):
    manager, sandbox = _manager(), _Sandbox()
    link, land = _held(_links(WS_A))

    with (
        patch.object(mount, "serve", AsyncMock(return_value=_up(started=True))),
        patch.object(mount, "link", link),
    ):
        assert await _beside(manager, sandbox) == "synced"
        # The turn builds its prompt with the mount; a command waits for it.
        assert sandbox.livefs is not None
        command = asyncio.ensure_future(sandbox.livefs.ready(WS_A))
        await asyncio.sleep(0)
        assert not command.done()
        land.set()
        assert await command

    link.assert_awaited_once()
    assert not manager._livefs_owed(COMPUTER, WS_A, sandbox)


@pytest.mark.parametrize(
    "first",
    [
        MountState(False, error=MountError.STALE_CODE),
        MountState(False, error=MountError.START_FAILED),
        MountState(False, error=MountError.UNANSWERED),
        MountState(False, reason="folders are moving", error=MountError.BUSY),
        MountState(False, reason="exec lost"),
    ],
    ids=["stale-code", "start-failed", "unanswered", "busy", "raised"],
)
@pytest.mark.asyncio
async def test_a_serve_the_sync_stood_in_the_way_of_is_asked_once_more_after_it(
    clock, first
):
    """The prompt says whether the files are mounted for the rest of the
    conversation, so a serve that may answer otherwise once the sync is done
    is asked again before the turn goes on."""
    manager, sandbox = _manager(), _Sandbox()
    serve = AsyncMock(side_effect=[first, _up(started=True)])

    with (
        patch.object(mount, "serve", serve),
        patch.object(mount, "link", AsyncMock(return_value=_links(WS_A))),
    ):
        await _beside(manager, sandbox)
        await _seen(manager).linked()

    assert serve.await_count == 2
    assert await sandbox.livefs.ready(WS_A)


@pytest.mark.parametrize(
    "error",
    [MountError.UNREACHABLE, MountError.UNSUPPORTED, MountError.INSTALLING],
)
@pytest.mark.asyncio
async def test_a_serve_that_would_answer_the_same_is_not_asked_again_and_still_links(
    clock, error
):
    """The links dangle until a mount serves, as they do once one goes
    down, and the next turn owes the sandbox nothing."""
    manager, sandbox = _manager(), _Sandbox()
    serve = AsyncMock(return_value=MountState(False, error=error))
    link = AsyncMock(return_value=_links(WS_A))

    with patch.object(mount, "serve", serve), patch.object(mount, "link", link):
        await _beside(manager, sandbox, workspace_id=WS_A)
        await _seen(manager).linked()

    serve.assert_awaited_once()
    link.assert_awaited_once()
    assert sandbox.livefs is None
    assert not manager._livefs_owed(COMPUTER, WS_A, sandbox)


@pytest.mark.parametrize(
    "answer",
    [
        MountState(False, reason="folders are moving", error=MountError.BUSY),
        MountState(False, reason="exec lost"),
    ],
    ids=["busy", "raised"],
)
@pytest.mark.asyncio
async def test_a_serve_that_asked_the_daemon_nothing_links_nothing(clock, answer):
    """A settle owns the folders, or the serve never reached the sandbox."""
    manager, sandbox = _manager(), _Sandbox()
    serve, link = AsyncMock(return_value=answer), AsyncMock()

    with patch.object(mount, "serve", serve), patch.object(mount, "link", link):
        await _beside(manager, sandbox, workspace_id=WS_A)
        await asyncio.sleep(0)

    assert serve.await_count == 2
    link.assert_not_awaited()
    assert sandbox.livefs is None


@pytest.mark.asyncio
async def test_a_sync_on_a_mount_serving_there_asks_no_serve(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    serve, link = AsyncMock(), AsyncMock(return_value=_links(WS_A, WS_B))

    with patch.object(mount, "serve", serve), patch.object(mount, "link", link):
        await _beside(manager, sandbox, workspace_id=WS_B)
        await _seen(manager).linked()

    serve.assert_not_awaited()
    link.assert_awaited_once()


@pytest.mark.parametrize(
    "down",
    [
        LinkState(frozenset({None, WS_B}), frozenset({WS_A}), MountError.LINK_FAILED),
        # A settle held the folders, so no link went in.
        LinkState(error=MountError.BUSY, reason="folders are moving"),
    ],
    ids=["failed", "busy"],
)
@pytest.mark.asyncio
async def test_links_that_did_not_go_in_keep_the_mount_from_that_workspaces_commands(
    clock, down
):
    manager, sandbox = _manager(), _Sandbox()
    link, land = _held(down)

    with (
        patch.object(mount, "serve", AsyncMock(return_value=_up(started=True))),
        patch.object(mount, "link", link),
    ):
        await _beside(manager, sandbox, workspace_id=WS_A)
        land.set()
        assert not await sandbox.livefs.ready(WS_A)

    # The mount serves on for the computer; only this folder goes without.
    assert sandbox.livefs is not None
    link.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_warm_turn_in_a_workspace_not_yet_linked_goes_on_while_it_is(clock):
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, _up())
    await _linked(manager, sandbox, _links(WS_A))
    serve, (link, land) = AsyncMock(), _held(_links(WS_A, WS_B))

    with patch.object(mount, "serve", serve), patch.object(mount, "link", link):
        await asyncio.wait_for(
            manager._keep_livefs(COMPUTER, USER, sandbox, WS_B), timeout=1
        )
        assert _seen(manager).linking()
        land.set()
        assert await sandbox.livefs.ready(WS_B)

    serve.assert_not_awaited()
    assert not manager._livefs_owed(COMPUTER, WS_B, sandbox)


@pytest.mark.asyncio
async def test_a_warm_turn_on_a_serving_mount_whose_token_is_gone_goes_on(clock):
    """The renewal replaces the token off the turn's path, and a command
    waits for that renewal rather than serving again."""
    manager, sandbox = _manager(), _Sandbox()
    with patch.object(mount, "renew", AsyncMock(return_value=None)):
        await _served(manager, sandbox, _up(minutes_left=0.5))
        await asyncio.sleep(0)
    await _linked(manager, sandbox, _links(WS_A))
    clock.t += tracker._RENEW_RETRY_S
    renewed = asyncio.Event()

    async def renewing(*args, **kwargs):
        await renewed.wait()
        return MountState(True, _in(60))

    serve, renew = AsyncMock(), AsyncMock(side_effect=renewing)
    with patch.object(mount, "serve", serve), patch.object(mount, "renew", renew):
        assert manager._livefs_owed(COMPUTER, WS_A, sandbox)
        await asyncio.wait_for(
            manager._keep_livefs(COMPUTER, USER, sandbox, WS_A), timeout=1
        )
        command = asyncio.ensure_future(sandbox.livefs.ready(WS_A))
        await asyncio.sleep(0)
        assert not command.done()
        renewed.set()
        assert await command

    serve.assert_not_awaited()
    renew.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_warm_turn_on_a_mount_due_to_serve_waits_for_it(clock):
    """Whether the files are mounted goes into the prompt."""
    manager, sandbox = _manager(), _Sandbox()
    await _served(manager, sandbox, MountState(False, error=MountError.INSTALLING))
    clock.t += 20
    serve = AsyncMock(return_value=_up(started=True))

    with (
        patch.object(mount, "serve", serve),
        patch.object(mount, "link", AsyncMock(return_value=_links(WS_A))),
    ):
        await manager._keep_livefs(COMPUTER, USER, sandbox, WS_A)
        assert sandbox.livefs is not None
        await _seen(manager).linked()

    serve.assert_awaited_once()
    assert await sandbox.livefs.ready(WS_A)


@pytest.mark.parametrize(
    ("first", "runs"),
    [(_links(WS_A, WS_B), 1), (_links(WS_A), 2)],
    ids=["linked-by-the-first", "joined-after-its-read"],
)
@pytest.mark.asyncio
async def test_a_turn_arriving_while_links_go_in_waits_for_none_and_repeats_none(
    clock, first, runs
):
    """It runs after the link in flight, and only when that one, having read
    the folders before this workspace joined, left it out."""
    manager, sandbox = _manager(), _Sandbox()
    link, land = _held(first, _links(WS_A, WS_B))

    with (
        patch.object(mount, "serve", AsyncMock(return_value=_up(started=True))),
        patch.object(mount, "link", link),
    ):
        await _beside(manager, sandbox, workspace_id=WS_A)
        assert manager._livefs_owed(COMPUTER, WS_B, sandbox)
        await asyncio.wait_for(
            manager._keep_livefs(COMPUTER, USER, sandbox, WS_B), timeout=1
        )
        await asyncio.sleep(0)
        assert link.await_count == 1
        land.set()
        assert await sandbox.livefs.ready(WS_B)

    assert link.await_count == runs
    assert not manager._livefs_owed(COMPUTER, WS_B, sandbox)


@pytest.mark.asyncio
async def test_a_machine_forgotten_while_its_links_go_in_keeps_nothing_of_them(clock):
    """A stop revokes the token; links laid after it are for nothing."""
    manager, sandbox = _manager(), _Sandbox()
    link, land = _held(_links(WS_A))

    with (
        patch.object(mount, "serve", AsyncMock(return_value=_up(started=True))),
        patch.object(mount, "link", link),
    ):
        await _beside(manager, sandbox)
        await asyncio.sleep(0)
        _seen(manager).forget()
        land.set()
        for _ in range(3):
            await asyncio.sleep(0)

    seen = _seen(manager)
    assert link.await_count == 1 and link.landed == 0
    assert not seen.linking() and not seen.serving(sandbox)
    assert not await sandbox.livefs.ready(WS_A)


@pytest.mark.asyncio
async def test_a_forget_ends_the_links_queued_behind_the_running_one(clock):
    manager, sandbox = _manager(), _Sandbox()
    link, land = _held(_links(WS_A))

    with (
        patch.object(mount, "serve", AsyncMock(return_value=_up(started=True))),
        patch.object(mount, "link", link),
    ):
        await _beside(manager, sandbox, workspace_id=WS_A)
        await manager._keep_livefs(COMPUTER, USER, sandbox, WS_B)
        await asyncio.sleep(0)
        _seen(manager).forget()
        land.set()
        for _ in range(5):
            await asyncio.sleep(0)

    assert link.await_count == 1 and link.landed == 0
    assert not _seen(manager).linking()


# -- the handle the file tools hold --------------------------------------------


def _serving(sandbox: _Sandbox, *linked: str, minutes_left: float = 59) -> MountTracker:
    """A tracker that saw ``sandbox`` serve and ``linked`` linked in."""
    seen = MountTracker()
    sandbox.livefs = object()
    seen.saw(sandbox, _up(minutes_left))
    seen.saw_links(sandbox, _links(*linked))
    return seen


def _handle(
    seen: MountTracker,
    sandbox: _Sandbox,
    *,
    serve: AsyncMock | None = None,
    link: AsyncMock | None = None,
    renew: AsyncMock | None = None,
) -> mount.Handle:
    return mount.Handle(
        COMPUTER,
        seen,
        sandbox,
        serve=serve or AsyncMock(),
        link=link or AsyncMock(return_value=LinkState()),
        renew=renew or AsyncMock(return_value=None),
    )


def _serve_recorded(seen: MountTracker, sandbox: _Sandbox, state: MountState) -> AsyncMock:
    """A serve that records ``state``, as the manager's does."""

    async def serve():
        await asyncio.sleep(0)
        seen.saw(sandbox, state)
        return state

    return AsyncMock(side_effect=serve)


@pytest.mark.asyncio
async def test_a_command_gets_the_mount_only_in_a_workspace_linked_in():
    """One whose folder is not linked yet asks for the links and waits."""
    sandbox = _Sandbox()
    link = AsyncMock(side_effect=[_links(WS_A), _links(WS_A, WS_B)])
    handle = _handle(_serving(sandbox, WS_A), sandbox, link=link)

    assert await handle.ready(WS_A) and await handle.ready(None)
    link.assert_not_awaited()

    # A folder the link left out (still moving) goes without, until one
    # that has it lands.
    assert not await handle.ready(WS_B)
    assert await handle.ready(WS_B)
    assert link.await_count == 2


@pytest.mark.asyncio
async def test_a_command_in_a_linked_workspace_never_waits_on_links_for_another():
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A)
    link, land = _held(_links(WS_A, WS_B))
    seen.link_soon(sandbox, WS_B, link)
    assert seen.linking()

    handle = _handle(seen, sandbox, link=link)
    assert await asyncio.wait_for(handle.ready(WS_A), timeout=1)

    land.set()
    await seen.linked()


@pytest.mark.asyncio
async def test_a_failed_link_of_the_computers_own_keeps_the_root_from_commands(clock):
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A)
    seen.saw_links(
        sandbox,
        LinkState(frozenset(), frozenset({None, WS_A}), MountError.LINK_FAILED),
    )
    handle = _handle(seen, sandbox, link=AsyncMock(return_value=_links(WS_A)))

    assert not await handle.ready(None) and not await handle.ready(WS_A)
    clock.t += 600
    assert await handle.ready(None) and await handle.ready(WS_A)


@pytest.mark.asyncio
async def test_a_mount_found_down_before_a_command_keeps_it_from_the_path_guard():
    """The token is refreshed before the tool decides whether the command
    may name the store-backed files, not after it let one through."""
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=0.5)
    down = MountState(False, error=MountError.UNREACHABLE)
    handle = _handle(seen, sandbox, serve=_serve_recorded(seen, sandbox, down))

    assert not await handle.ready(WS_A)


@pytest.mark.asyncio
async def test_a_token_that_lapses_while_a_command_waits_on_links_is_replaced_first(
    monkeypatch,
):
    """Renewals that kept failing leave a token near its end, and the wait
    for the links can carry it past; the command must not take it then."""
    sandbox = _Sandbox()
    seen = _serving(sandbox, minutes_left=1.1)
    serve = _serve_recorded(seen, sandbox, _up())

    async def link():
        # Ten seconds pass while the links go in.
        later = datetime.now(UTC) + timedelta(seconds=10)
        monkeypatch.setattr(
            mount.tokens, "datetime", SimpleNamespace(now=lambda tz=None: later)
        )
        return _links(WS_A)

    handle = _handle(seen, sandbox, serve=serve, link=AsyncMock(side_effect=link))

    assert await handle.ready(WS_A)
    serve.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_command_never_waits_on_a_token_running_low():
    """Parallel commands start one renewal between them and run on the
    current token; the renewed one lands when the renewal does."""
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=5)
    renewed = asyncio.Event()

    async def renewing():
        await renewed.wait()
        return MountState(True, _in(60))

    serve, renew = AsyncMock(), AsyncMock(side_effect=renewing)
    handle = _handle(seen, sandbox, serve=serve, renew=renew)

    assert await asyncio.gather(handle.ready(WS_A), handle.ready(WS_A)) == [True, True]
    await asyncio.sleep(0)

    serve.assert_not_awaited()
    renew.assert_awaited_once()
    assert not seen.renewal_owed()
    renewed.set()
    for _ in range(3):
        await asyncio.sleep(0)
    await handle.ready(WS_A)
    renew.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_renewal_that_renewed_nothing_is_tried_again_later(clock):
    sandbox = _Sandbox()
    renew = AsyncMock(return_value=None)
    handle = _handle(_serving(sandbox, WS_A, minutes_left=5), sandbox, renew=renew)

    await handle.ready(WS_A)
    await asyncio.sleep(0)
    await handle.ready(WS_A)
    assert renew.await_count == 1

    clock.t += tracker._RENEW_RETRY_S
    await handle.ready(WS_A)
    await asyncio.sleep(0)
    assert renew.await_count == 2


@pytest.mark.asyncio
async def test_a_mount_a_renewal_found_down_is_served_by_the_next_command(clock):
    """The renewed token has an hour in it, so only the mount being down
    sends the next command to serve, which hands the file tools the store
    while it stays down."""
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=5)
    down = MountState(False, _in(60), error=MountError.UNREACHABLE)
    serve = _serve_recorded(seen, sandbox, down)
    handle = _handle(seen, sandbox, serve=serve, renew=AsyncMock(return_value=down))

    await handle.ready(WS_A)
    for _ in range(3):
        await asyncio.sleep(0)
    assert not await handle.ready(WS_A)
    await handle.ready(WS_A)

    serve.assert_awaited_once()


@pytest.mark.asyncio
async def test_parallel_commands_share_one_serve_for_a_token_gone():
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=0.5)
    serve = _serve_recorded(seen, sandbox, _up(60))
    handle = _handle(seen, sandbox, serve=serve)

    assert await asyncio.gather(handle.ready(WS_A), handle.ready(WS_A)) == [True, True]

    serve.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_serve_a_settle_held_off_is_not_waited_on_by_every_command():
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=0.5)
    busy = MountState(False, reason="folders are moving", error=MountError.BUSY)
    serve = _serve_recorded(seen, sandbox, busy)
    handle = _handle(seen, sandbox, serve=serve)

    await handle.ready(WS_A)
    await handle.ready(WS_A)

    serve.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_command_still_running_on_a_forgotten_machine_mints_nothing():
    """A stop revokes the token; a command it did not wait for must not bring
    one back, nor lay links."""
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A, minutes_left=0.5)
    serve, renew = AsyncMock(), AsyncMock()
    handle = _handle(seen, sandbox, serve=serve, renew=renew)

    seen.forget()
    assert not await handle.ready(WS_A)
    await asyncio.sleep(0)

    serve.assert_not_awaited()
    renew.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_command_files_who_it_runs_for_only_with_both_its_id_and_context(
    monkeypatch,
):
    open_call = AsyncMock()
    monkeypatch.setattr(mount.outcomes, "open_call", open_call)
    sandbox = _Sandbox()
    handle = _handle(_serving(sandbox, WS_A), sandbox)
    context = CallContext(workspace_id=WS_A, thread_id="thread-test-1", timezone="Asia/Tokyo")

    await handle.prepare("c0ffee00c0ffee00", context)
    await handle.prepare("c0ffee00c0ffee01")
    await handle.prepare(None, context)

    open_call.assert_awaited_once_with(COMPUTER, "c0ffee00c0ffee00", context)


@pytest.mark.asyncio
async def test_a_report_collects_the_late_saves_of_the_thread_it_ran_for(monkeypatch):
    # The tool names the thread, so collecting reads no filing back.
    collect = AsyncMock(return_value=[])
    monkeypatch.setattr(mount.outcomes, "collect", collect)
    sandbox = _Sandbox()
    handle = _handle(_serving(sandbox, WS_A), sandbox)

    await handle.report("c0ffee00c0ffee00", "ok\n", CallContext(thread_id="thread-test-1"))
    await handle.report("c0ffee00c0ffee01", "ok\n")

    assert [c.args for c in collect.await_args_list] == [
        (COMPUTER, "c0ffee00c0ffee00", "thread-test-1"),
        (COMPUTER, "c0ffee00c0ffee01", None),
    ]


@pytest.mark.asyncio
async def test_an_ordinary_command_never_touches_the_daemon(monkeypatch):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    sandbox = _Sandbox()
    serve = AsyncMock()
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=serve)

    assert await handle.report("c0ffee00c0ffee00", "ok\n") == ""
    serve.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_command_that_met_a_dead_mount_restarts_it_and_says_so(monkeypatch):
    """Only the daemon is replaced: the links point through the mount's own
    link, which the start swaps."""
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    sandbox = _Sandbox()
    seen = _serving(sandbox, WS_A)
    serve, link = AsyncMock(return_value=_up(started=True)), AsyncMock()
    handle = _handle(seen, sandbox, serve=serve, link=link)

    text = await handle.report("c0ffee00c0ffee00", DEAD)

    # The rows still say it serves; only the sandbox can say otherwise.
    serve.assert_awaited_once_with(adopt=False)
    link.assert_not_awaited()
    assert seen.serves(sandbox, WS_A)
    assert "serving again; rerun the command" in text


@pytest.mark.parametrize(
    "output",
    [
        "OSError: [Errno 107] Transport endpoint is not connected: '/mnt/livefs/user'",
        "ls: cannot access '/mnt/.livefs/18f2a/memory': Transport endpoint is not connected",
    ],
)
@pytest.mark.asyncio
async def test_the_mount_and_its_generations_count_as_mounted_paths(monkeypatch, output):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    sandbox = _Sandbox()
    serve = AsyncMock(return_value=_up(started=True))
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=serve)

    await handle.report("c0ffee00c0ffee00", output)

    serve.assert_awaited_once()


@pytest.mark.parametrize(
    "output",
    [
        "OSError: [Errno 107] Transport endpoint is not connected",
        # The path and the error sit on different lines: two unrelated facts.
        "wrote .agents/user/memory/notes.md\n"
        "socket.send: Transport endpoint is not connected",
    ],
)
@pytest.mark.asyncio
async def test_a_socket_that_is_not_connected_leaves_the_mount_alone(monkeypatch, output):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    sandbox = _Sandbox()
    serve = AsyncMock(return_value=_up(started=True))
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=serve)

    assert await handle.report("c0ffee00c0ffee00", output) == ""
    serve.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_dead_mount_already_restarted_elsewhere_adds_no_note(monkeypatch):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    sandbox = _Sandbox()
    serve = AsyncMock(return_value=_up(started=False))
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=serve)

    assert await handle.report("c0ffee00c0ffee00", DEAD) == ""


MISSING = "ls: cannot access '.agents/user/automations': No such file or directory"


@pytest.mark.parametrize(
    "answers, restarted", [(False, True), (True, False), (None, False)], ids=["gone", "there", "unasked"]
)
@pytest.mark.asyncio
async def test_a_missing_file_under_the_mount_asks_whether_the_mount_is_gone(
    monkeypatch, clock, answers, restarted
):
    """A restart no reconnect saw leaves the links dangling, answering
    ENOENT, so only the sandbox can tell a gone mount from a missing file."""
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    probe = AsyncMock(return_value=answers)
    monkeypatch.setattr(mount.livefs_mount, "answers", probe)
    sandbox = _Sandbox()
    serve = AsyncMock(return_value=_up(started=True))
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=serve)

    text = await handle.report("c0ffee00c0ffee00", MISSING)

    probe.assert_awaited_once_with(sandbox)
    assert serve.await_count == int(restarted)
    assert ("serving again; rerun the command" in text) is restarted


@pytest.mark.asyncio
async def test_missing_files_ask_the_sandbox_at_most_once_a_minute(monkeypatch, clock):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    probe = AsyncMock(return_value=True)
    monkeypatch.setattr(mount.livefs_mount, "answers", probe)
    sandbox = _Sandbox()
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=AsyncMock())

    await handle.report("c0ffee00c0ffee00", MISSING)
    clock.t += 59
    await handle.report("c0ffee00c0ffee01", MISSING)
    assert probe.await_count == 1
    clock.t += 2
    await handle.report("c0ffee00c0ffee02", MISSING)
    assert probe.await_count == 2


@pytest.mark.asyncio
async def test_a_missing_file_outside_the_mount_asks_nothing(monkeypatch, clock):
    monkeypatch.setattr(mount.outcomes, "collect", AsyncMock(return_value=[]))
    probe = AsyncMock(return_value=False)
    monkeypatch.setattr(mount.livefs_mount, "answers", probe)
    sandbox = _Sandbox()
    handle = _handle(_serving(sandbox, WS_A), sandbox, serve=AsyncMock())

    assert await handle.report("c0ffee00c0ffee00", "cat: data/prices.csv: No such file or directory") == ""
    probe.assert_not_awaited()
