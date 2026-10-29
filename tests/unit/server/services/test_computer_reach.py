"""The user's other computers, as the Chief of Staff's tool calls reach them."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from ptc_agent.agent.tools.computer_reach import ComputerTarget, ComputerUnreachable
from ptc_agent.core.project_context import ProjectContext, current_project, project_bound
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.services import computer_reach
from src.server.services.computer_reach import (
    TurnComputers,
    _why_not,
    pick_computer,
    turn_computers,
)

HOME_ID = "11111111-1111-1111-1111-111111111111"
MAC_ID = "22222222-2222-2222-2222-222222222222"
ROWS = [
    {"computer_id": HOME_ID, "name": "Cloud computer"},
    {"computer_id": MAC_ID, "name": "Studio Mac", "root_dir": "/Users/studio/langalpha"},
]
USER = "user-1"
#: The turn's own session, which no test here reads files through.
OWN = SimpleNamespace(sandbox=None)


@pytest.mark.parametrize("wanted", [MAC_ID, "studio mac", "STUDIO MAC", '"Studio Mac"', "`Studio Mac`", MAC_ID[:8], MAC_ID[:12]])
def test_a_computer_is_picked_by_id_name_or_a_long_enough_id_prefix(wanted):
    assert pick_computer(ROWS, wanted)["computer_id"] == MAC_ID


def test_a_name_with_a_run_of_spaces_is_picked_as_activity_prints_it():
    rows = [*ROWS, {"computer_id": "33333333-3333-3333-3333-333333333333", "name": "Lab  box"}]
    assert pick_computer(rows, "lab box")["computer_id"] == "33333333-3333-3333-3333-333333333333"


def test_a_name_is_picked_as_activity_escapes_it_and_as_typed():
    escaped = {"computer_id": "33333333-3333-3333-3333-333333333333", "name": "A<B"}
    literal = {"computer_id": "44444444-4444-4444-4444-444444444444", "name": "A&lt;B"}
    assert pick_computer([*ROWS, escaped], "A&lt;B") is escaped
    assert pick_computer([*ROWS, literal], "A&lt;B") is literal
    assert pick_computer([*ROWS, escaped, literal], "A<B") is escaped


def test_a_spelling_two_computers_print_alike_asks_for_the_id():
    escaped = {"computer_id": "33333333-3333-3333-3333-333333333333", "name": "A<B"}
    literal = {"computer_id": "44444444-4444-4444-4444-444444444444", "name": "A&lt;B"}
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer([*ROWS, escaped, literal], "A&lt;B")
    assert escaped["computer_id"] in str(e.value)
    assert literal["computer_id"] in str(e.value)


def test_a_name_in_quotes_is_picked_as_typed_and_unquoted():
    quoted = {"computer_id": "33333333-3333-3333-3333-333333333333", "name": '"Lab"'}
    plain = {"computer_id": "44444444-4444-4444-4444-444444444444", "name": "Lab"}
    assert pick_computer([*ROWS, quoted], '"Lab"') is quoted
    assert pick_computer([*ROWS, quoted, plain], "Lab") is plain
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer([*ROWS, quoted, plain], '"Lab"')
    assert quoted["computer_id"] in str(e.value)
    assert plain["computer_id"] in str(e.value)


def test_a_name_that_is_another_computers_id_asks_for_the_id():
    named_like_mac = {"computer_id": "33333333-3333-3333-3333-333333333333", "name": MAC_ID}
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer([*ROWS, named_like_mac], MAC_ID)
    assert named_like_mac["computer_id"] in str(e.value)
    assert pick_computer([*ROWS, named_like_mac], named_like_mac["computer_id"]) is named_like_mac


def test_a_name_that_is_another_computers_id_prefix_asks_for_the_id():
    named_like_mac = {"computer_id": "33333333-3333-3333-3333-333333333333", "name": MAC_ID[:8]}
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer([*ROWS, named_like_mac], MAC_ID[:8])
    assert MAC_ID in str(e.value)
    assert named_like_mac["computer_id"] in str(e.value)


def test_names_fold_as_the_index_folds_them():
    """casefold() read "STRASSE" as "Straße", which the index keeps apart."""
    rows = [*ROWS, {"computer_id": "33333333-3333-3333-3333-333333333333", "name": "Straße"},
            {"computer_id": "44444444-4444-4444-4444-444444444444", "name": "STRASSE"}]
    assert pick_computer(rows, "strasse")["computer_id"] == "44444444-4444-4444-4444-444444444444"
    assert pick_computer(rows, "straße")["computer_id"] == "33333333-3333-3333-3333-333333333333"


def test_a_seven_character_id_prefix_is_not_enough():
    with pytest.raises(ComputerUnreachable):
        pick_computer(ROWS, MAC_ID[:7])


def test_two_computers_with_one_name_ask_for_the_id():
    rows = [*ROWS, {"computer_id": "33333333-3333-3333-3333-333333333333", "name": "studio mac"}]
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer(rows, "Studio Mac")
    assert MAC_ID in str(e.value)
    assert "33333333-3333-3333-3333-333333333333" in str(e.value)
    assert '2 computers answer to "Studio Mac"' in str(e.value)


def test_an_unknown_name_lists_every_computer():
    with pytest.raises(ComputerUnreachable) as e:
        pick_computer(ROWS, "Basement")
    text = str(e.value)
    assert 'no computer named "Basement"' in text
    for row in ROWS:
        assert row["name"] in text
        assert row["computer_id"] in text


def test_a_limit_refusal_uses_the_platforms_own_message():
    error = HTTPException(status_code=429, detail={"message": "Computer limit reached."})
    assert _why_not("Studio Mac", error) == 'Computer "Studio Mac" could not start: Computer limit reached.'


def test_an_ssh_failure_is_worded_for_the_owner():
    error = SSHConnectError("timeout", "The machine did not answer.")
    assert error.for_owner in _why_not("Studio Mac", error)
    assert _why_not("Studio Mac", error).startswith('Computer "Studio Mac" is not reachable:')


def test_a_transient_failure_says_the_machine_may_be_asleep():
    text = _why_not("Studio Mac", SandboxTransientError("blip"))
    assert "asleep or offline" in text


def test_any_other_failure_points_the_user_at_computers():
    text = _why_not("Studio Mac", RuntimeError("boom"))
    assert text == 'Computer "Studio Mac" could not be started. The user can check it in Computers.'
    assert "boom" not in text


def _manager(session=None):
    session = session or SimpleNamespace(sandbox=None)
    return SimpleNamespace(
        cached_session_for_computer=lambda row: None,
        reach_computer=AsyncMock(return_value=session),
        keep_computer_alive=AsyncMock(),
    )


def _patched(manager, *, status="running", folders=None):
    folders = {"semis": "ws-semis", "nvda": "ws-nvda"} if folders is None else folders
    return (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=ROWS)),
        patch.object(computer_reach, "get_computer", AsyncMock(return_value={"status": status})),
        patch.object(computer_reach, "get_computer_folders", AsyncMock(return_value=folders)),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=manager,
        ),
    )


async def _reach(manager, wanted, own=HOME_ID, turn=None, **kw):
    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in _patched(manager, **kw):
            stack.enter_context(p)
        return await (turn or TurnComputers(USER, own, OWN)).reach(wanted)


@pytest.mark.asyncio
async def test_the_turns_own_computer_is_answered_without_waking_anything():
    manager = _manager()
    assert await _reach(manager, "Cloud computer") is None
    manager.reach_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_computer_in_error_is_refused_without_a_start():
    manager = _manager()
    with pytest.raises(ComputerUnreachable) as e:
        await _reach(manager, "Studio Mac", status="error")
    assert "Studio Mac" in str(e.value)
    manager.reach_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_another_computer_is_reached_at_its_root_with_its_folders_as_siblings():
    session = SimpleNamespace(sandbox=None)
    manager = _manager(session)
    target = await _reach(manager, "Studio Mac")
    assert isinstance(target, ComputerTarget)
    assert target.computer_id == MAC_ID
    assert target.name == "Studio Mac"
    assert target.session is session
    assert target.project.workspace_id == ""
    assert target.project.dir_name is None
    assert tuple(target.project.sibling_dir_names) == ("semis", "nvda")
    # Home's agent knows that computer only by the machine's own paths.
    assert target.project.machine is not None
    assert target.root == "/Users/studio/langalpha"
    manager.reach_computer.assert_awaited_once_with(MAC_ID, user_id=USER)


@pytest.mark.asyncio
async def test_the_turn_collects_the_other_computers_its_calls_ran_on():
    """The turn's end backs them up: no turn of their own sweeps what a call wrote."""
    touched: set[str] = set()
    turn = TurnComputers(USER, HOME_ID, OWN, touched)
    assert await _reach(_manager(), "Cloud computer", turn=turn) is None
    assert touched == set()
    await _reach(_manager(), "Studio Mac", turn=turn)
    assert touched == {MAC_ID}


@pytest.mark.asyncio
async def test_a_failed_wake_becomes_the_agents_sentence():
    manager = _manager()
    manager.reach_computer = AsyncMock(side_effect=SandboxTransientError("blip"))
    with pytest.raises(ComputerUnreachable) as e:
        await _reach(manager, "Studio Mac")
    assert "asleep or offline" in str(e.value)


def test_a_user_with_one_computer_gets_no_reach():
    assert turn_computers(USER, HOME_ID, OWN, ROWS[:1]) is None


def test_a_user_with_several_computers_gets_a_reach():
    assert isinstance(turn_computers(USER, HOME_ID, OWN, ROWS), TurnComputers)


@pytest.mark.asyncio
async def test_a_wake_runs_outside_the_turns_project():
    """A start lays out the bound project's folder on the machine it starts,
    so a wake from inside a Home turn once left a Home folder on the computer
    it reached."""
    seen = []

    async def reach_computer(computer_id, *, user_id):
        seen.append(current_project())
        return SimpleNamespace(sandbox=None)

    manager = _manager()
    manager.reach_computer = reach_computer
    with project_bound(ProjectContext("home-ws", "Home")):
        await _reach(manager, "Studio Mac")
        assert current_project() == ProjectContext("home-ws", "Home")
    assert seen == [None]


@pytest.mark.asyncio
async def test_elsewhere_names_the_other_computers_that_hold_workspaces():
    empty_id = "33333333-3333-3333-3333-333333333333"
    rows = [*ROWS, {"computer_id": empty_id, "name": "Spare"}]
    held = AsyncMock(return_value={HOME_ID, MAC_ID})
    with (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=rows)),
        patch.object(computer_reach, "get_computers_with_folders", held),
    ):
        found = await TurnComputers(USER, HOME_ID, OWN).elsewhere()
    assert found == (("Studio Mac", "/Users/studio/langalpha"),)
    # One read for every other computer, whatever their number.
    held.assert_awaited_once_with([MAC_ID, empty_id])


@pytest.mark.asyncio
async def test_a_call_that_stops_waiting_leaves_the_wake_to_the_next_call():
    """A turn cancelled mid-wake once left the start running unrecorded, so
    the next call began a second one alongside it."""
    started = asyncio.Event()
    release = asyncio.Event()
    session = SimpleNamespace(sandbox=None)

    async def reach_computer(computer_id, *, user_id):
        started.set()
        await release.wait()
        return session

    manager = _manager()
    manager.reach_computer = AsyncMock(side_effect=reach_computer)
    turn = TurnComputers(USER, HOME_ID, OWN)
    first = asyncio.create_task(_reach(manager, "Studio Mac", turn=turn))
    await started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    retry = asyncio.create_task(_reach(manager, "Studio Mac", turn=turn))
    await asyncio.sleep(0)
    release.set()
    assert (await retry).session is session
    manager.reach_computer.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_computer_still_up_is_not_woken_again():
    session = SimpleNamespace(sandbox=None)
    manager = _manager(session)
    turn = TurnComputers(USER, HOME_ID, OWN)
    await _reach(manager, "Studio Mac", turn=turn)
    manager.cached_session_for_computer = lambda row: session
    await _reach(manager, "Studio Mac", turn=turn)
    manager.reach_computer.assert_awaited_once()

    # A restart under the turn hands back another session: woken again.
    manager.cached_session_for_computer = lambda row: SimpleNamespace(sandbox=None)
    await _reach(manager, "Studio Mac", turn=turn)
    assert manager.reach_computer.await_count == 2


@pytest.mark.asyncio
async def test_a_computer_another_worker_stopped_is_woken_again():
    """This worker's handle outlives a stop another worker made, so a call
    trusting it brought the machine back up past admission, the row stopped."""
    session = SimpleNamespace(sandbox=None)
    manager = _manager(session)
    manager.cached_session_for_computer = lambda row: session
    turn = TurnComputers(USER, HOME_ID, OWN)
    await _reach(manager, "Studio Mac", turn=turn)
    await _reach(manager, "Studio Mac", turn=turn, status="stopped")
    assert manager.reach_computer.await_count == 2


@pytest.mark.asyncio
async def test_a_failed_wake_is_logged_once_and_the_next_call_tries_again(caplog):
    manager = _manager()
    manager.reach_computer = AsyncMock(side_effect=SandboxTransientError("blip"))
    turn = TurnComputers(USER, HOME_ID, OWN)
    with caplog.at_level(logging.INFO, logger=computer_reach.__name__):
        with pytest.raises(ComputerUnreachable):
            await _reach(manager, "Studio Mac", turn=turn)
        await asyncio.sleep(0)
    assert [r.levelno for r in caplog.records] == [logging.WARNING]

    manager.reach_computer = AsyncMock(return_value=SimpleNamespace(sandbox=None))
    assert await _reach(manager, "Studio Mac", turn=turn) is not None


# -- the login home and files a send attaches -----------------------------------

from ptc_agent.core.paths import SandboxLayout  # noqa: E402
from ptc_agent.core.sandbox import path_resolution as _paths  # noqa: E402
from ptc_agent.core.sandbox.files import FileOverCap  # noqa: E402
from ptc_agent.core.sandbox.providers import provider_class  # noqa: E402

MAC_ROOT = "/Users/studio/langalpha"
SSH_ROWS = [
    {"computer_id": HOME_ID, "name": "Cloud computer", "kind": "daytona", "root_dir": "/home/workspace"},
    {"computer_id": MAC_ID, "name": "Studio Mac", "kind": "ssh", "root_dir": MAC_ROOT},
]


def _machine(root, files=None, home=None, *, files_as_command_user=True):
    """A computer at ``root`` with the real path rules, holding ``files``."""
    layout = SandboxLayout(root)
    files = files if files is not None else {}
    sandbox = SimpleNamespace(
        _work_dir=root,
        working_dir=root,
        layout=layout,
        uploaded={},
        files_as_command_user=files_as_command_user,
        sandbox_id=None,
    )
    sandbox.config = SimpleNamespace(
        filesystem=SimpleNamespace(
            allowed_directories=layout.allowed_directories,
            denied_directories=layout.denied_directories,
            enable_path_validation=True,
        )
    )
    sandbox.workspace = lambda project=None: layout.for_workspace(
        (project or current_project() or ProjectContext("home", "Home")).dir_name
    )
    sandbox.normalize_path = lambda p, project=None: _paths.normalize_path(sandbox, p, project)
    sandbox.validate_path = lambda p, project=None: _paths.validate_path(sandbox, p, project)

    async def download(path, cap):
        data = files.get(path)
        if data is not None and len(data) > cap(data):
            raise FileOverCap(cap(data))
        return data

    async def upload(path, data):
        sandbox.uploaded[path] = data
        Path(sandbox.folder.name, path).write_bytes(data)
        return True

    # Home's folder, where a server command runs: the claim script runs for
    # real, against the files a test puts there.
    sandbox.folder = tempfile.TemporaryDirectory()
    for path, data in files.items():
        if not path.startswith("/"):
            held = Path(sandbox.folder.name, path)
            held.parent.mkdir(parents=True, exist_ok=True)
            held.write_bytes(data)

    async def execute_bash_command(command, working_dir=None, timeout=60):
        done = subprocess.run(
            ["sh", "-c", command], cwd=sandbox.folder.name, capture_output=True, text=True
        )
        return {"success": done.returncode == 0, "stdout": done.stdout, "stderr": done.stderr}

    sandbox.adownload_file_bytes_capped = download
    sandbox.aupload_file_bytes = upload
    sandbox.execute_bash_command = execute_bash_command
    runtime = SimpleNamespace(login_home=AsyncMock(return_value=home)) if home else None
    sandbox.runtime = runtime
    return SimpleNamespace(sandbox=sandbox)


@contextmanager
def _sending_from_home(mac_files=None):
    """A Home turn on the cloud computer, the Studio Mac holding ``mac_files``.
    Entered once around sends made at once: patches entered per send unwind
    out of order and leave a fake behind."""
    mac = _machine(MAC_ROOT, mac_files, home="/Users/studio")
    folders = {MAC_ID: {"lithium": "ws-lithium"}, HOME_ID: {"Home": "ws-home", "Semis": "ws-semis"}}
    with (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=SSH_ROWS)),
        patch.object(computer_reach, "get_computer", AsyncMock(return_value={"status": "running"})),
        patch.object(computer_reach, "get_computer_folders", AsyncMock(side_effect=lambda cid: folders[cid])),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=_manager(mac),
        ),
        project_bound(ProjectContext("ws-home", "Home", ("Semis",))),
    ):
        yield


async def _attach_all(files, *, mac_files=None, home_files=None, home=None):
    home = home or _machine("/home/workspace", home_files)
    with _sending_from_home(mac_files):
        return await TurnComputers(USER, HOME_ID, home).attach(files), home.sandbox.uploaded


async def _attach(path, *, computer="Studio Mac", **kw):
    got, uploaded = await _attach_all([(computer, path)], **kw)
    return got[0], uploaded


@pytest.mark.asyncio
async def test_a_machine_of_the_users_own_is_reached_with_its_login_home():
    mac = _machine(MAC_ROOT, home="/Users/studio")
    with (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=SSH_ROWS)),
        patch.object(computer_reach, "get_computer", AsyncMock(return_value={"status": "running"})),
        patch.object(computer_reach, "get_computer_folders", AsyncMock(return_value={})),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=_manager(mac),
        ),
    ):
        target = await TurnComputers(USER, HOME_ID, OWN).reach("Studio Mac")
    assert target.project.machine.home == "/Users/studio"


async def _reach_kind(kind):
    rows = [SSH_ROWS[0], {**SSH_ROWS[1], "kind": kind}]
    mac = _machine(
        MAC_ROOT,
        home="/root",
        files_as_command_user=provider_class(kind).files_as_command_user,
    )
    with (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=rows)),
        patch.object(computer_reach, "get_computer", AsyncMock(return_value={"status": "running"})),
        patch.object(computer_reach, "get_computer_folders", AsyncMock(return_value={})),
        patch(
            "src.server.services.workspace_manager.WorkspaceManager.get_instance",
            return_value=_manager(mac),
        ),
    ):
        return await TurnComputers(USER, HOME_ID, OWN).reach("Studio Mac")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["daytona", "e2b"])
async def test_a_hosted_computer_whose_files_run_as_its_commands_is_reached_whole(kind):
    target = await _reach_kind(kind)
    assert target.project.machine.home == "/root"


@pytest.mark.asyncio
async def test_a_docker_computer_keeps_its_file_calls_to_its_root():
    target = await _reach_kind("docker")
    assert target.project.machine.home is None


@pytest.mark.asyncio
async def test_a_file_outside_the_root_of_the_turns_own_computer_is_sent():
    home_files = {"/root/exports/q3.csv": b"a,b\n"}
    home = _machine("/home/workspace", home_files, home="/root")
    folders = {HOME_ID: {"Home": "ws-home"}}
    with (
        patch.object(computer_reach, "get_computer_roster", AsyncMock(return_value=SSH_ROWS)),
        patch.object(computer_reach, "get_computer_folders", AsyncMock(side_effect=lambda cid: folders[cid])),
        project_bound(ProjectContext("ws-home", "Home")),
    ):
        (got,) = await TurnComputers(USER, HOME_ID, home).attach(
            [("Cloud computer", "/root/exports/q3.csv")]
        )
    assert (got.path, got.workspace_id, got.copied) == ("sent/q3.csv", None, 4)
    assert home.sandbox.uploaded == {"sent/q3.csv": b"a,b\n"}


@pytest.mark.asyncio
async def test_a_file_in_a_workspace_folder_goes_from_that_workspace():
    got, uploaded = await _attach(f"{MAC_ROOT}/lithium/reports/alb.md")
    assert (got.path, got.workspace_id, got.copied) == ("reports/alb.md", "ws-lithium", 0)
    assert uploaded == {}


@pytest.mark.asyncio
async def test_a_file_outside_every_folder_is_copied_into_home_without_overwriting():
    got, uploaded = await _attach(
        "~/Documents/budget.pdf",
        mac_files={"/Users/studio/Documents/budget.pdf": b"%PDF\xe2\xe3"},
        home_files={"sent/budget.pdf": b"older"},
    )
    assert (got.path, got.workspace_id, got.copied) == ("sent/budget-2.pdf", None, 6)
    assert uploaded == {"sent/budget-2.pdf": b"%PDF\xe2\xe3"}


@pytest.mark.asyncio
async def test_a_file_on_the_turns_own_computer_goes_from_its_folder():
    got, _ = await _attach("/home/workspace/Semis/notes.md", computer="Cloud computer")
    assert (got.path, got.workspace_id) == ("notes.md", "ws-semis")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kw", "says"),
    [
        ({"mac_files": {}}, 'There is no file ~/Documents/budget.pdf on computer "Studio Mac".'),
    ],
)
async def test_a_file_that_cannot_be_copied_says_why(kw, says):
    with pytest.raises(ComputerUnreachable) as e:
        await _attach("~/Documents/budget.pdf", **kw)
    assert says in str(e.value)


_MB = 1024 * 1024


def _sized(n):
    """File contents that say they are ``n`` bytes long."""

    class _Sized(bytes):
        def __len__(self):
            return n

    return _Sized()


@pytest.mark.asyncio
async def test_a_file_over_the_per_file_limit_names_it():
    with pytest.raises(ComputerUnreachable) as e:
        await _attach("~/a.pdf", mac_files={"/Users/studio/a.pdf": _sized(20 * _MB + 1)})
    assert str(e.value) == '~/a.pdf on computer "Studio Mac" is over 20 MB; nothing was sent.'


@pytest.mark.asyncio
async def test_copies_past_what_a_send_holds_name_that_limit_and_copy_nothing():
    files = {f"/Users/studio/{n}.pdf": _sized(20 * _MB) for n in "abc"}
    home = _machine("/home/workspace")
    with pytest.raises(ComputerUnreachable) as e:
        await _attach_all([("Studio Mac", f"~/{n}.pdf") for n in "abc"], mac_files=files, home=home)
    assert str(e.value) == (
        '~/c.pdf on computer "Studio Mac" is over the 50 MB a send\'s files hold; nothing was sent.'
    )
    assert home.sandbox.uploaded == {}


@pytest.mark.asyncio
async def test_a_send_that_cannot_take_one_file_copies_none():
    """Each file was copied as it was reached, so a later file's refusal
    answered "Nothing was sent." over copies already made in Home."""
    home = _machine("/home/workspace")
    with pytest.raises(ComputerUnreachable) as e:
        await _attach_all(
            [("Studio Mac", "~/Documents/budget.pdf"), ("Studio Mac", "~/Documents/missing.pdf")],
            mac_files={"/Users/studio/Documents/budget.pdf": b"%PDF"},
            home=home,
        )
    assert "There is no file ~/Documents/missing.pdf" in str(e.value)
    assert home.sandbox.uploaded == {}


@pytest.mark.asyncio
async def test_two_files_of_one_name_in_a_send_get_two_copies():
    got, uploaded = await _attach_all(
        [("Studio Mac", "~/a/report.pdf"), ("Studio Mac", "~/b/report.pdf"), ("Studio Mac", f"{MAC_ROOT}/lithium/x.md")],
        mac_files={"/Users/studio/a/report.pdf": b"one", "/Users/studio/b/report.pdf": b"two"},
    )
    assert [(a.path, a.workspace_id, a.copied) for a in got] == [
        ("sent/report.pdf", None, 3),
        ("sent/report-2.pdf", None, 3),
        ("x.md", "ws-lithium", 0),
    ]
    assert uploaded == {"sent/report.pdf": b"one", "sent/report-2.pdf": b"two"}


@pytest.mark.asyncio
async def test_two_sends_at_once_never_copy_onto_one_name():
    """Each send listed `sent` and picked the first free name before either
    upload landed, so two at once took one name and one copy replaced the
    other under a link already sent."""
    home = _machine("/home/workspace")
    upload = home.sandbox.aupload_file_bytes

    async def slow_upload(path, data):
        await asyncio.sleep(0)
        return await upload(path, data)

    home.sandbox.aupload_file_bytes = slow_upload
    mac_files = {"/Users/studio/a/report.pdf": b"one", "/Users/studio/b/report.pdf": b"two"}
    with _sending_from_home(mac_files):
        sent = await asyncio.gather(
            TurnComputers(USER, HOME_ID, home).attach([("Studio Mac", "~/a/report.pdf")]),
            TurnComputers(USER, HOME_ID, home).attach([("Studio Mac", "~/b/report.pdf")]),
        )
    assert sorted(got.path for (got,) in sent) == ["sent/report-2.pdf", "sent/report.pdf"]
    assert sorted(home.sandbox.uploaded.values()) == [b"one", b"two"]


@pytest.mark.asyncio
async def test_a_copy_home_cannot_take_says_so():
    with pytest.raises(ComputerUnreachable) as e:
        await _attach(
            "~/Documents/budget.pdf",
            mac_files={"/Users/studio/Documents/budget.pdf": b"%PDF"},
            home_files={"sent": b"a file where the folder goes"},
        )
    assert str(e.value) == "~/Documents/budget.pdf could not be copied into Home to send."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "upload",
    [
        AsyncMock(return_value=False),
        AsyncMock(side_effect=OSError("toolbox down")),
        AsyncMock(side_effect=asyncio.CancelledError()),
    ],
    ids=["refused", "raised", "cancelled"],
)
async def test_a_copy_that_fails_takes_its_claim_with_it(upload):
    """Left behind, the claim was a file in Home's ``sent`` named like the
    copy and holding only the mark, and the next send of it counted past it."""
    home = _machine("/home/workspace")
    mac_files = {"/Users/studio/Documents/budget.pdf": b"%PDF-1.7"}
    send = [("Studio Mac", "~/Documents/budget.pdf")]
    working, home.sandbox.aupload_file_bytes = home.sandbox.aupload_file_bytes, upload
    with pytest.raises((ComputerUnreachable, OSError, asyncio.CancelledError)):
        await _attach_all(send, mac_files=mac_files, home=home)
    assert not Path(home.sandbox.folder.name, "sent/budget.pdf").exists()

    home.sandbox.aupload_file_bytes = working
    (got,), _ = await _attach_all(send, mac_files=mac_files, home=home)
    assert got.path == "sent/budget.pdf"


@pytest.mark.asyncio
async def test_a_send_run_again_names_the_copy_its_first_attempt_made():
    """The messaging service answers a repeat of a call by its file paths, so
    a re-run that copied the file again under a new name went out twice."""
    home = _machine("/home/workspace")
    mac_files = {"/Users/studio/Documents/budget.pdf": b"%PDF-1.7"}
    send = [("Studio Mac", "~/Documents/budget.pdf")]
    first, _ = await _attach_all(send, mac_files=mac_files, home=home)
    home.sandbox.uploaded.clear()
    again, _ = await _attach_all(send, mac_files=mac_files, home=home)
    assert first[0].path == again[0].path == "sent/budget.pdf"
    assert home.sandbox.uploaded == {}

    # Changed since, it is another file, and the first copy stays as sent.
    changed = {"/Users/studio/Documents/budget.pdf": b"%PDF-1.7 v2"}
    (later,), _ = await _attach_all(send, mac_files=changed, home=home)
    assert later.path == "sent/budget-2.pdf"


@pytest.mark.asyncio
async def test_an_empty_file_sent_again_names_its_first_copy():
    home = _machine("/home/workspace")
    send = [("Studio Mac", "~/notes.txt")]
    mac_files = {"/Users/studio/notes.txt": b""}
    first, _ = await _attach_all(send, mac_files=mac_files, home=home)
    home.sandbox.uploaded.clear()
    again, _ = await _attach_all(send, mac_files=mac_files, home=home)
    assert first[0].path == again[0].path == "sent/notes.txt"
    assert home.sandbox.uploaded == {}


@pytest.mark.asyncio
async def test_a_name_another_send_is_still_copying_to_is_not_reused():
    got, uploaded = await _attach(
        "~/notes.txt",
        mac_files={"/Users/studio/notes.txt": b""},
        home_files={"sent/notes.txt": computer_reach._CLAIM_MARK.encode()},
    )
    assert got.path == "sent/notes-2.txt"
    assert uploaded == {"sent/notes-2.txt": b""}
