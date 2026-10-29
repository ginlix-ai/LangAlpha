"""Repairing an ssh computer that will not start: a reinstall or a reconnect.

What is pinned is who may be cleared from a machine and through which login:
never the computer itself, never another of the owner's live computers, and
only a computer the owner named once they were shown it.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers.ssh import SSHProbe
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, SSHFolderMarked
from src.server.models.computer import (
    ComputerReinstallRequest,
    ComputerSSHReconnectTarget,
)
from src.server.services import ssh_computer_repair as repair
from src.server.services.computer_errors import ComputerRepairRefused
from src.server.services.computer_manager import _repair as manager_repair

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
SIBLING_ID = "33333333-3333-4333-8333-333333333333"
USER = "user-1"
OWN_REF = "a" * 32
SIBLING_REF = "b" * 32
FOREIGN = "3cd2aabcfce04ecf83f69a2e2952866c"
TUNNEL = "5b0f3c1e-8d2a-4c6b-9e7f-1a2b3c4d5e6f"
NEW_TUNNEL = "6c1f4d2f-9e3b-4d7c-8f80-2b3c4d5e6f70"
KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
NEW_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINtrK5WFrGlR2uf038jBnBgAvICChwc8DD1Mw46Bixsq"
SETTINGS = SSHConfig()


def _computer(**overrides):
    row = {
        "computer_id": COMPUTER_ID,
        "user_id": USER,
        "kind": "ssh",
        "name": "mbp-16",
        "status": "stopped",
        "provider_ref": OWN_REF,
        "root_dir": "/Users/dev/langalpha",
        "provider_config": {
            "host": "macbook-pro",
            "port": 22,
            "username": "dev",
            "host_key": KEY,
            "key_owner": USER,
            "tunnel": TUNNEL,
            "helper": True,
            "os": "Darwin",
        },
        "config": {},
        "spec_change": None,
    }
    row.update(overrides)
    return row


def _sibling():
    return _computer(
        computer_id=SIBLING_ID,
        name="other",
        provider_ref=SIBLING_REF,
        root_dir="/Users/dev/langalpha-2",
    )


def _found(**overrides):
    fields = {
        "host_key": NEW_KEY,
        "fingerprint": "SHA256:x",
        "os": "Darwin",
        "arch": "arm64",
        "distro": "macOS 15.4",
        "home": "/Users/dev",
        "shell": "/bin/zsh",
        "is_root": False,
        "has_sudo": False,
        "disk_free_bytes": None,
        "supported": True,
        "hostname": "macbook-pro",
        "helper": True,
    }
    return SSHProbe(**{**fields, **overrides})


def _marked(owner: str) -> SSHFolderMarked:
    return SSHFolderMarked("/Users/dev/langalpha on macbook-pro is taken.", owner)


@pytest.fixture
def machine(monkeypatch):
    """The dials and reads the repairs make, each answerable per test."""
    # The machine a reconnect reaches holds the computer unless a test says not.
    probe = AsyncMock(return_value=_found(knows_computer=True))
    take_over = AsyncMock()
    computers = AsyncMock(return_value=[_computer(), _sibling()])
    monkeypatch.setattr(repair, "probe", probe)
    monkeypatch.setattr(repair.ssh_provider, "take_over_root", take_over)
    monkeypatch.setattr(repair, "load_client_key", AsyncMock(return_value="own-key"))
    monkeypatch.setattr(repair, "client_key", AsyncMock(return_value="caller-key"))
    monkeypatch.setattr(repair, "_removed_ssh_computers", AsyncMock(return_value=[]))
    monkeypatch.setattr(repair, "get_computers_for_user", computers)
    monkeypatch.setattr(repair, "spend_dials", AsyncMock())
    monkeypatch.setattr(repair, "tunnels_enabled", lambda: True)
    monkeypatch.setattr(repair, "get_user_tunnel", AsyncMock(return_value={"live": True}))
    return MagicMock(probe=probe, take_over=take_over, computers=computers)


# ---------------------------------------------------------------------------
# Take back
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_take_back_finds_the_computer_whose_mark_is_on_the_folder(machine):
    machine.probe.side_effect = _marked(FOREIGN)

    assert await repair.folder_owner(_computer(), settings=SETTINGS) == FOREIGN

    kwargs = machine.probe.await_args.kwargs
    # Its own login, and its own mark passes.
    assert machine.probe.await_args.args == ("own-key",)
    assert (kwargs["host"], kwargs["host_key"], kwargs["tunnel_id"]) == (
        "macbook-pro",
        KEY,
        TUNNEL,
    )
    assert kwargs["root_dir"] == "/Users/dev/langalpha"
    assert kwargs["taking_over"] == OWN_REF


@pytest.mark.asyncio
async def test_take_back_never_clears_another_of_the_owners_live_computers(machine):
    machine.probe.side_effect = _marked(SIBLING_REF)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.folder_owner(_computer(), settings=SETTINGS)

    assert refused.value.code == "folder_owned"
    assert refused.value.status == 409
    assert '"other"' in str(refused.value)


@pytest.mark.asyncio
async def test_any_other_refusal_of_the_folder_is_left_to_the_start(machine):
    """A check reads an unmarked folder with files as taken, which a start
    keeps when the computer's own files are on the machine."""
    machine.probe.side_effect = SSHConnectError("root_dir", "holds files")

    assert await repair.folder_owner(_computer(), settings=SETTINGS) is None


@pytest.mark.asyncio
async def test_take_back_clears_the_owner_through_the_computers_own_login(machine):
    await repair.take_back_folder(_computer(), FOREIGN, settings=SETTINGS)

    kwargs = machine.take_over.await_args.kwargs
    assert kwargs["owner"] == FOREIGN
    assert kwargs["private_key"] == "own-key"
    assert (kwargs["host"], kwargs["host_key"], kwargs["tunnel"]) == (
        "macbook-pro",
        KEY,
        TUNNEL,
    )
    assert kwargs["root_dir"] == "/Users/dev/langalpha"
    # Its own from then, so the cleared computer's uninstall cannot empty it.
    assert (kwargs["claim_as"], tuple(kwargs["forget"])) == (OWN_REF, ())


@pytest.mark.asyncio
async def test_a_folder_this_computer_never_worked_in_forgets_its_restores(machine):
    await repair.take_back_folder(
        _computer(), FOREIGN, settings=SETTINGS, forget_restores=True
    )

    assert set(machine.take_over.await_args.kwargs["forget"]) == {
        repair.SYNC_MARKER_NAME,
        repair.DEFERRED_MARKER,
    }


@pytest.mark.asyncio
async def test_take_back_never_clears_the_computer_itself(machine):
    await repair.take_back_folder(_computer(), OWN_REF, settings=SETTINGS)

    machine.take_over.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_folder_marked_again_meanwhile_is_refused(machine):
    machine.take_over.side_effect = _marked(SIBLING_REF)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.take_back_folder(_computer(), FOREIGN, settings=SETTINGS)

    assert refused.value.detail()["owner"] == SIBLING_REF
    assert refused.value.code == "folder_marked"


# ---------------------------------------------------------------------------
# A new folder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_new_folder_under_home_is_read_on_the_machine(machine):
    checked = await repair.check_new_root(
        _computer(), "~/langalpha-new", settings=SETTINGS
    )

    assert checked.root_dir == "/Users/dev/langalpha-new"
    assert checked.take_over is None
    assert machine.probe.await_args.kwargs["root_dir"] == "/Users/dev/langalpha-new"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "requested", ["/Users/dev/langalpha/inner", "/Users/dev", "/Users/dev/langalpha"]
)
async def test_a_new_folder_inside_or_around_the_old_one_is_refused(machine, requested):
    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(_computer(), requested, settings=SETTINGS)

    assert (refused.value.code, refused.value.status) == ("root_dir", 422)
    machine.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_new_folder_another_of_the_owners_computers_works_in_is_refused(
    machine,
):
    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(
            _computer(), "/Users/dev/langalpha-2/sub", settings=SETTINGS
        )

    assert refused.value.code == "folder_owned"
    assert refused.value.detail()["computer_id"] == SIBLING_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("requested", ["~/.langalpha/la", "/Users/dev/.langalpha"])
async def test_a_new_folder_among_langalphas_own_files_is_refused(machine, requested):
    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(_computer(), requested, settings=SETTINGS)

    assert (refused.value.code, refused.value.status) == ("root_dir", 422)


@pytest.mark.asyncio
async def test_a_path_longer_than_the_column_is_refused(machine):
    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(
            _computer(), "/srv/" + "x" * 300, settings=SETTINGS
        )

    assert refused.value.code == "root_dir"


@pytest.mark.asyncio
async def test_a_new_folder_another_computer_marked_names_it(machine):
    machine.probe.side_effect = _marked(FOREIGN)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(_computer(), "/srv/la", settings=SETTINGS)

    assert refused.value.code == "folder_marked"
    assert refused.value.detail()["owner"] == FOREIGN


@pytest.mark.asyncio
async def test_a_new_folder_named_for_take_over_is_answered_for_clearing(machine):
    machine.probe.side_effect = [_marked(FOREIGN), _found()]

    checked = await repair.check_new_root(
        _computer(), "/srv/la", settings=SETTINGS, take_over=FOREIGN
    )

    assert (checked.root_dir, checked.take_over) == ("/srv/la", FOREIGN)
    assert machine.probe.await_args.kwargs["taking_over"] == FOREIGN


@pytest.mark.asyncio
async def test_a_new_folder_that_holds_files_is_the_owners_to_fix(machine):
    machine.probe.side_effect = SSHConnectError("root_dir", "/srv/la holds files.")

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.check_new_root(_computer(), "/srv/la", settings=SETTINGS)

    assert (refused.value.code, refused.value.status) == ("root_dir", 422)
    assert str(refused.value) == "/srv/la holds files."


def test_a_request_for_a_new_folder_names_one():
    with pytest.raises(ValueError):
        ComputerReinstallRequest(folder="new")
    with pytest.raises(ValueError):
        ComputerReinstallRequest(folder="new", root_dir="relative/path")
    assert ComputerReinstallRequest(folder="new", root_dir="/srv//la/").root_dir == "/srv/la"
    assert ComputerReinstallRequest(folder="new", root_dir="~/la").root_dir == "~/la"
    taken = ComputerReinstallRequest(folder="take_back", root_dir="/x", take_over=FOREIGN)
    assert (taken.root_dir, taken.take_over) == (None, FOREIGN)


# ---------------------------------------------------------------------------
# Reconnect
# ---------------------------------------------------------------------------


def _target(**overrides):
    fields = {
        "host": "macbook-pro",
        "port": 22,
        "username": "dev",
        "host_key": NEW_KEY,
        "tunnel_id": NEW_TUNNEL,
    }
    return ComputerSSHReconnectTarget(**{**fields, **overrides})


@pytest.mark.asyncio
async def test_a_reconnect_points_the_computer_at_the_new_pairing(machine):
    plan = await repair.plan_reconnect(_computer(), _target(), settings=SETTINGS)

    assert plan.provider_config == {
        "host": "macbook-pro",
        "port": 22,
        "username": "dev",
        "host_key": NEW_KEY,
        "key_owner": USER,
        "tunnel": NEW_TUNNEL,
        "helper": True,
        "os": "Darwin",
    }
    assert plan.tunnel_id == NEW_TUNNEL and plan.take_over is None
    kwargs = machine.probe.await_args.kwargs
    # The caller's key, the new target, the computer's own folder and mark.
    assert machine.probe.await_args.args == ("caller-key",)
    assert (kwargs["host_key"], kwargs["tunnel_id"]) == (NEW_KEY, NEW_TUNNEL)
    assert kwargs["root_dir"] == "/Users/dev/langalpha"
    assert kwargs["taking_over"] == OWN_REF
    repair.spend_dials.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_machine_that_holds_nothing_of_the_computer_is_refused(machine):
    machine.probe.return_value = _found(knows_computer=False)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.plan_reconnect(_computer(), _target(), settings=SETTINGS)

    assert (refused.value.code, refused.value.status) == ("different_machine", 409)
    # Asked of the machine by the computer's own id.
    assert machine.probe.await_args.kwargs["known_as"] == OWN_REF


@pytest.mark.asyncio
async def test_the_pinned_host_key_is_the_computers_machine(machine):
    machine.probe.return_value = _found(knows_computer=False)

    plan = await repair.plan_reconnect(
        _computer(), _target(host_key=KEY), settings=SETTINGS
    )

    assert plan.moved is False


@pytest.mark.asyncio
async def test_a_move_its_owner_chose_is_planned_as_one(machine):
    machine.probe.return_value = _found(knows_computer=False)

    plan = await repair.plan_reconnect(
        _computer(), _target(move=True), settings=SETTINGS
    )

    assert plan.moved is True


@pytest.mark.asyncio
async def test_a_reconnect_through_someone_elses_tunnel_is_refused(machine):
    repair.get_user_tunnel.return_value = None

    with pytest.raises(SSHConnectError) as refused:
        await repair.plan_reconnect(_computer(), _target(), settings=SETTINGS)

    assert refused.value.code == "not_connected"
    machine.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_direct_reconnect_spends_a_dial(machine):
    await repair.plan_reconnect(
        _computer(), _target(tunnel_id=None, host="203.0.113.7"), settings=SETTINGS
    )

    repair.spend_dials.assert_awaited_once_with(USER, 1, settings=SETTINGS)


@pytest.mark.asyncio
async def test_a_reconnect_onto_another_computers_folder_tree_is_refused(machine):
    # The sibling works in /Users/dev/langalpha-2 on the machine paired.
    computer = _computer(root_dir="/Users/dev/langalpha-2/inner")

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.plan_reconnect(
            computer, _target(host_key=KEY, tunnel_id=TUNNEL), settings=SETTINGS
        )

    assert refused.value.code == "folder_owned"
    assert refused.value.detail()["computer_id"] == SIBLING_ID
    machine.probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_direct_reconnect_taking_a_folder_over_spends_each_dial(machine):
    machine.probe.side_effect = [_marked(FOREIGN), _found(knows_computer=True)]

    await repair.plan_reconnect(
        _computer(),
        _target(tunnel_id=None, host="203.0.113.7", take_over=FOREIGN),
        settings=SETTINGS,
        take_over=FOREIGN,
    )

    spent = [c.args[1] for c in repair.spend_dials.await_args_list]
    assert sum(spent) == 3


@pytest.mark.asyncio
async def test_a_reconnect_to_a_folder_another_computer_marked_is_refused(machine):
    machine.probe.side_effect = _marked(FOREIGN)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.plan_reconnect(_computer(), _target(), settings=SETTINGS)

    assert refused.value.code == "folder_marked"
    assert refused.value.detail()["owner"] == FOREIGN


@pytest.mark.asyncio
async def test_a_reconnect_naming_the_mark_takes_the_folder_over(machine):
    machine.probe.side_effect = [_marked(FOREIGN), _found(knows_computer=True)]

    plan = await repair.plan_reconnect(
        _computer(), _target(take_over=FOREIGN), settings=SETTINGS, take_over=FOREIGN
    )

    assert plan.take_over == FOREIGN


@pytest.mark.asyncio
async def test_a_reconnect_never_takes_over_the_owners_own_computer(machine):
    machine.probe.side_effect = _marked(SIBLING_REF)

    with pytest.raises(ComputerRepairRefused) as refused:
        await repair.plan_reconnect(
            _computer(), _target(), settings=SETTINGS, take_over=SIBLING_REF
        )

    assert refused.value.code == "folder_owned"


# ---------------------------------------------------------------------------
# The manager: locks, fences and what each repair writes
# ---------------------------------------------------------------------------


class _Manager(manager_repair.ComputerRepairMixin):
    def __init__(self, locked=True):
        self.locked = locked
        self.retired = []
        self.revoked = []

    @asynccontextmanager
    async def _machine_decision_lock(self, computer_id):
        yield self.locked

    async def retire_computer_session_if_present(self, computer_id, *, reason):
        self.retired.append(reason)
        return True

    async def _revoke_livefs(self, computer_id):
        self.revoked.append(computer_id)


@pytest.fixture
def rows(monkeypatch):
    """The row the repair reads, and what it writes."""
    current = {"row": _computer()}
    monkeypatch.setattr(
        manager_repair, "get_computer", AsyncMock(side_effect=lambda _id: current["row"])
    )
    monkeypatch.setattr(manager_repair, "clear_computer_start_failure", AsyncMock())

    @asynccontextmanager
    async def folders(_id):
        yield object()

    monkeypatch.setattr(manager_repair, "workspace_folders_lock", folders)
    return current


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "code", "status"),
    [
        ({"kind": "daytona"}, "not_ssh", 400),
        ({"status": "running"}, "not_stopped", 409),
        ({"status": "starting"}, "not_stopped", 409),
        (
            {
                "spec_change": {
                    "target_tier": "max",
                    "from_tier": "standard",
                    "state": "in_progress",
                    "started_at": "2026-10-25T16:00:00+00:00",
                }
            },
            "spec_in_progress",
            409,
        ),
    ],
)
async def test_a_repair_is_refused_unless_the_ssh_computer_is_down(
    rows, overrides, code, status
):
    rows["row"] = _computer(**overrides)

    with pytest.raises(ComputerRepairRefused) as refused:
        await _Manager().reinstall_computer(
            COMPUTER_ID, folder="take_back", root_dir=None, settings=SETTINGS
        )

    assert (refused.value.code, refused.value.status) == (code, status)


@pytest.mark.asyncio
async def test_take_back_clears_the_owner_under_the_lock_and_forgets_the_failure(
    rows, monkeypatch
):
    monkeypatch.setattr(manager_repair, "folder_owner", AsyncMock(return_value=FOREIGN))
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)
    manager = _Manager()

    await manager.reinstall_computer(
        COMPUTER_ID,
        folder="take_back",
        root_dir=None,
        settings=SETTINGS,
        take_over=FOREIGN,
    )

    assert take_back.await_args.args[1] == FOREIGN
    manager_repair.clear_computer_start_failure.assert_awaited_once_with(COMPUTER_ID)
    assert manager.retired == ["reinstalled"]


@pytest.mark.asyncio
@pytest.mark.parametrize("shown", [None, "a" * 32])
async def test_take_back_is_refused_for_a_computer_its_owner_was_not_shown(
    rows, monkeypatch, shown
):
    monkeypatch.setattr(manager_repair, "folder_owner", AsyncMock(return_value=FOREIGN))
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)

    with pytest.raises(ComputerRepairRefused) as refused:
        await _Manager().reinstall_computer(
            COMPUTER_ID,
            folder="take_back",
            root_dir=None,
            settings=SETTINGS,
            take_over=shown,
        )

    assert (refused.value.code, refused.value.extra.get("owner")) == (
        "folder_marked",
        FOREIGN,
    )
    take_back.assert_not_awaited()


@pytest.mark.asyncio
async def test_take_back_with_no_other_mark_only_starts(rows, monkeypatch):
    monkeypatch.setattr(manager_repair, "folder_owner", AsyncMock(return_value=None))
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)

    await _Manager().reinstall_computer(
        COMPUTER_ID, folder="take_back", root_dir=None, settings=SETTINGS
    )

    take_back.assert_not_awaited()
    manager_repair.clear_computer_start_failure.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_row_that_moved_since_the_check_is_busy(rows, monkeypatch):
    async def moved(*_a, **_k):
        rows["row"] = _computer(root_dir="/elsewhere")
        return FOREIGN

    monkeypatch.setattr(manager_repair, "folder_owner", moved)
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)

    with pytest.raises(ComputerRepairRefused) as refused:
        await _Manager().reinstall_computer(
            COMPUTER_ID,
            folder="take_back",
            root_dir=None,
            settings=SETTINGS,
            take_over=FOREIGN,
        )

    assert refused.value.code == "busy"
    take_back.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_new_folder_moves_the_root_fenced_on_the_old_one(rows, monkeypatch):
    monkeypatch.setattr(
        manager_repair,
        "check_new_root",
        AsyncMock(return_value=repair.NewRoot(root_dir="/srv/la", take_over=FOREIGN)),
    )
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)
    move = AsyncMock(return_value=_computer(root_dir="/srv/la"))
    monkeypatch.setattr(manager_repair, "move_computer_root", move)
    manager = _Manager()

    row = await manager.reinstall_computer(
        COMPUTER_ID, folder="new", root_dir="/srv/la", settings=SETTINGS, take_over=FOREIGN
    )

    assert row["root_dir"] == "/srv/la"
    move.assert_awaited_once_with(
        COMPUTER_ID, old_root="/Users/dev/langalpha", new_root="/srv/la"
    )
    # Cleared from the folder the row now names, with what vouched for its
    # files there: this computer never worked in it.
    assert take_back.await_args.args[:2] == (move.return_value, FOREIGN)
    assert take_back.await_args.kwargs["forget_restores"] is True
    assert manager.retired == ["moved to a new folder"]
    assert manager.revoked == [COMPUTER_ID]


@pytest.mark.asyncio
async def test_a_root_move_the_fence_refused_is_busy(rows, monkeypatch):
    monkeypatch.setattr(
        manager_repair,
        "check_new_root",
        AsyncMock(return_value=repair.NewRoot(root_dir="/srv/la")),
    )
    monkeypatch.setattr(manager_repair, "move_computer_root", AsyncMock(return_value=None))

    with pytest.raises(ComputerRepairRefused) as refused:
        await _Manager().reinstall_computer(
            COMPUTER_ID, folder="new", root_dir="/srv/la", settings=SETTINGS
        )

    assert refused.value.code == "busy"


@pytest.mark.asyncio
async def test_a_reconnect_saves_the_target_then_takes_the_folder_over(
    rows, monkeypatch
):
    config = {**_computer()["provider_config"], "host_key": NEW_KEY, "tunnel": NEW_TUNNEL}
    monkeypatch.setattr(
        manager_repair,
        "plan_reconnect",
        AsyncMock(
            return_value=repair.ReconnectPlan(
                provider_config=config, tunnel_id=NEW_TUNNEL, take_over=FOREIGN
            )
        ),
    )
    renew = AsyncMock()
    monkeypatch.setattr(manager_repair, "renew_reconnect_tunnel", renew)
    saved = _computer(provider_config=config)
    repoint = AsyncMock(return_value=saved)
    monkeypatch.setattr(manager_repair, "repoint_computer_machine", repoint)
    take_back = AsyncMock(side_effect=SSHConnectError("timeout", "slow"))
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)
    manager = _Manager()

    # Saved by then, so the take-over that failed is left to the start.
    updated, _before = await manager.reconnect_computer(
        COMPUTER_ID, _target(take_over=FOREIGN), settings=SETTINGS
    )

    assert updated is saved
    renew.assert_awaited_once_with(USER, NEW_TUNNEL)
    assert repoint.await_args.kwargs == {
        "provider_config": config,
        "expected_provider_config": _computer()["provider_config"],
        "moved": False,
    }
    # Through the new target: the old one may not reach the machine.
    assert take_back.await_args.args == (saved, FOREIGN)
    # The same machine: the folder's restore markers are this computer's own.
    assert take_back.await_args.kwargs["forget_restores"] is False
    # No worker keeps the old target.
    assert manager.retired == ["reconnected"]


@pytest.mark.asyncio
async def test_a_reconnect_moved_onto_a_taken_folder_forgets_its_restores(
    rows, monkeypatch
):
    config = {**_computer()["provider_config"], "tunnel": NEW_TUNNEL}
    monkeypatch.setattr(
        manager_repair,
        "plan_reconnect",
        AsyncMock(
            return_value=repair.ReconnectPlan(
                provider_config=config,
                tunnel_id=NEW_TUNNEL,
                take_over=FOREIGN,
                moved=True,
            )
        ),
    )
    monkeypatch.setattr(manager_repair, "renew_reconnect_tunnel", AsyncMock())
    saved = _computer(provider_config=config)
    monkeypatch.setattr(
        manager_repair, "repoint_computer_machine", AsyncMock(return_value=saved)
    )
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)

    await _Manager().reconnect_computer(
        COMPUTER_ID, _target(take_over=FOREIGN, move=True), settings=SETTINGS
    )

    # A folder it never worked in: the markers there vouch for the other
    # computer's files, not for its own backup's.
    assert take_back.await_args.kwargs["forget_restores"] is True


@pytest.mark.asyncio
async def test_a_reconnect_answers_the_rows_before_and_after(rows, monkeypatch):
    config = {**_computer()["provider_config"], "tunnel": NEW_TUNNEL}
    monkeypatch.setattr(
        manager_repair,
        "plan_reconnect",
        AsyncMock(return_value=repair.ReconnectPlan(provider_config=config, tunnel_id=NEW_TUNNEL)),
    )
    monkeypatch.setattr(manager_repair, "renew_reconnect_tunnel", AsyncMock())
    saved = _computer(provider_config=config)
    monkeypatch.setattr(manager_repair, "repoint_computer_machine", AsyncMock(return_value=saved))
    take_back = AsyncMock()
    monkeypatch.setattr(manager_repair, "take_back_folder", take_back)

    after, before = await _Manager().reconnect_computer(
        COMPUTER_ID, _target(), settings=SETTINGS
    )

    assert after["provider_config"]["tunnel"] == NEW_TUNNEL
    assert before["provider_config"]["tunnel"] == TUNNEL
    take_back.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_tunnel_gone_since_the_probe_saves_nothing(rows, monkeypatch):
    monkeypatch.setattr(
        manager_repair,
        "plan_reconnect",
        AsyncMock(return_value=repair.ReconnectPlan(provider_config={}, tunnel_id=NEW_TUNNEL)),
    )
    monkeypatch.setattr(
        manager_repair,
        "renew_reconnect_tunnel",
        AsyncMock(side_effect=SSHConnectError("not_connected", "gone")),
    )
    repoint = AsyncMock()
    monkeypatch.setattr(manager_repair, "repoint_computer_machine", repoint)
    manager = _Manager()

    with pytest.raises(SSHConnectError):
        await manager.reconnect_computer(COMPUTER_ID, _target(), settings=SETTINGS)

    repoint.assert_not_awaited()
    assert manager.retired == []


# ---------------------------------------------------------------------------
# Another worker's handle on the old folder or target
# ---------------------------------------------------------------------------


def _session(root: str, **ssh):
    config = MagicMock()
    config.filesystem.working_directory = root
    config.sandbox.ssh = SSHConfig.model_validate(
        {**_computer()["provider_config"], **ssh}
    )
    session = MagicMock()
    session.config = config
    return session


def _binding(**overrides):
    from src.server.services.computer_manager._types import ComputerBinding

    row = _computer()
    fields = {
        "workspace_id": "w",
        "computer_id": COMPUTER_ID,
        "kind": "ssh",
        "root_dir": row["root_dir"],
        "provider_ref": OWN_REF,
        "provider_config": row["provider_config"],
    }
    return ComputerBinding(**{**fields, **overrides})


def test_a_handle_on_the_old_folder_or_target_is_stale():
    from src.server.services.computer_manager._sessions import SessionCacheMixin

    stale = SessionCacheMixin._machine_target_is_stale
    current = _session("/Users/dev/langalpha")

    assert stale(_binding(), current) is None
    assert "root moved" in stale(_binding(root_dir="/srv/la"), current)
    moved = {**_computer()["provider_config"], "tunnel": NEW_TUNNEL}
    assert "target moved" in stale(_binding(provider_config=moved), current)
    # Only an ssh computer's root and target ever change under its id.
    assert stale(_binding(kind="daytona", root_dir="/srv/la"), current) is None


def test_the_manager_assembles_the_repairs():
    from src.server.services.computer_manager.manager import ComputerManager

    assert issubclass(ComputerManager, manager_repair.ComputerRepairMixin)
