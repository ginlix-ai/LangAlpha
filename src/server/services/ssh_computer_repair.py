"""The machine side of repairing an ssh computer that will not start.

Two failures need their owner and recur on every start until they act: the
computer's folder is another computer's (a reinstall takes it back or moves
to a new one), or the machine no longer answers as it was paired (a
reconnect points the computer at it as a new pairing reached it). Each
function here only dials and decides; the manager holds the computer's
locks and writes the row.
"""

from __future__ import annotations

import asyncio
import posixpath
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from ptc_agent.core.sandbox.providers._ssh_bootstrap import STATE_DIR
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError, SSHFolderMarked
from src.server.database.computer import get_computers_for_user
from src.server.database.ssh_tunnels import get_user_tunnel, renew_tunnel
from src.server.models.computer import (
    CLAIMABLE_FOR_START,
    ROOT_DIR_MAX,
    ComputerSSHPinnedTarget,
    normal_root_dir,
)
from src.server.services.computer_errors import ComputerRepairRefused
from src.server.services.persistence.transfer import DEFERRED_MARKER, SYNC_MARKER_NAME
from src.server.services.spec_change import SPEC_CHANGE_IN_PROGRESS, spec_change_from_row
from src.server.services.ssh_computers import (
    PROBE_TIMEOUT_S,
    SSH_KIND,
    _nested,
    _removed_ssh_computers,
    _ssh_computer_by_machine_id,
    client_key,
    computer_sharing_folder,
    is_public_key,
    load_client_key,
    probe,
    provider_config_for,
)
from src.server.services.ssh_dial_budget import spend_dials
from src.server.services.ssh_tunnels import tunnels_enabled


@dataclass(frozen=True)
class ReconnectPlan:
    """The target a reconnect writes, its machine already logged in to, and
    the computer whose mark on the folder its owner chose to take over."""

    provider_config: Dict[str, Any]
    tunnel_id: Optional[str]
    take_over: Optional[str] = None
    # The machine holds nothing of the computer, and its owner chose to move
    # it there: its workspaces' folders start empty.
    moved: bool = False


@dataclass(frozen=True)
class NewRoot:
    """The folder a reinstall moves to, and the computer whose mark on it its
    owner chose to take over."""

    root_dir: str
    take_over: Optional[str] = None


def _target(computer: Dict[str, Any]) -> Dict[str, Any]:
    """How the computer's own row logs in to its machine, as :func:`probe`
    takes it, the pinned host key included."""
    config = computer.get("provider_config") or {}
    return {
        "host": config.get("host") or "",
        "port": int(config.get("port") or 22),
        "username": config.get("username") or "",
        "host_key": config.get("host_key") or None,
        "tunnel_id": config.get("tunnel") or None,
    }


async def _own_key(computer: Dict[str, Any]) -> str:
    config = computer.get("provider_config") or {}
    return await load_client_key(config.get("key_owner") or str(computer["user_id"]))


def _own_ref(computer: Dict[str, Any]) -> Optional[str]:
    """The computer's id on its machine, which its own mark names."""
    ref = computer.get("provider_ref")
    return str(ref) if ref else None


async def _refuse_own_computer(computer: Dict[str, Any], owner: str) -> None:
    """A mark naming another of the owner's live computers is that computer's
    folder: removing it is how the folder frees, never a take-back."""
    mine = _ssh_computer_by_machine_id(
        await get_computers_for_user(str(computer["user_id"])), owner
    )
    if mine is not None and str(mine["computer_id"]) != str(computer["computer_id"]):
        raise ComputerRepairRefused(
            "folder_owned",
            f'This folder belongs to your computer "{mine.get("name")}". Remove '
            "that computer to free it, or choose a new folder.",
            computer_id=str(mine["computer_id"]),
        )


def _marked_elsewhere(e: SSHFolderMarked) -> ComputerRepairRefused:
    return ComputerRepairRefused("folder_marked", str(e), owner=e.owner)


async def _probe_folder(
    computer: Dict[str, Any],
    key: str,
    target: Dict[str, Any],
    root_dir: str,
    *,
    settings: SSHConfig,
    take_over: Optional[str],
    known_as: Optional[str] = None,
) -> Tuple[ssh_provider.SSHProbe, Optional[str]]:
    """Log in and check *root_dir* as this computer's folder.

    Its own mark passes. Another computer's refuses (``folder_marked``) unless
    it is *take_over*, the owner the caller chose to take the folder from,
    which is answered for the caller to clear once nothing else can refuse;
    one of the owner's own live computers is never taken over. *known_as*
    asks the probe whether the machine holds that computer.
    """
    try:
        found = await probe(
            key,
            settings=settings,
            **target,
            root_dir=root_dir,
            reclaim=await _removed_ssh_computers(str(computer["user_id"])),
            taking_over=_own_ref(computer),
            known_as=known_as,
        )
    except SSHFolderMarked as e:
        await _refuse_own_computer(computer, e.owner)
        if take_over is None or e.owner != take_over:
            raise _marked_elsewhere(e) from None
    else:
        return found, None
    try:
        found = await probe(
            key,
            settings=settings,
            **target,
            root_dir=root_dir,
            taking_over=take_over,
            known_as=known_as,
        )
    except SSHFolderMarked as e:
        raise _marked_elsewhere(e) from None
    return found, take_over


async def folder_owner(computer: Dict[str, Any], *, settings: SSHConfig) -> Optional[str]:
    """The computer whose mark keeps this one out of its folder, None when none does.

    Checked as a probe checks a folder, which never makes or marks it. Its
    own mark passes, and a removed computer of the owner's is cleared as the
    probe clears one. Any other refusal of the folder is left to the start,
    which keeps a folder whose mark was lost when the computer's own files
    are on the machine, where a check refuses it as holding files.

    Raises:
        ComputerRepairRefused: ``folder_owned``, the mark is another of the
            owner's live computers.
        SSHConnectError: The machine refused or did not answer.
    """
    try:
        await probe(
            await _own_key(computer),
            settings=settings,
            **_target(computer),
            root_dir=computer.get("root_dir") or None,
            reclaim=await _removed_ssh_computers(str(computer["user_id"])),
            taking_over=_own_ref(computer),
        )
    except SSHFolderMarked as e:
        await _refuse_own_computer(computer, e.owner)
        return e.owner
    except SSHConnectError as e:
        if e.code == "root_dir":
            return None
        raise
    return None


async def take_back_folder(
    computer: Dict[str, Any],
    owner: str,
    *,
    settings: SSHConfig,
    forget_restores: bool = False,
) -> None:
    """Stop *owner* and clear it from the computer's folder as its removal
    would have, then mark the folder as this computer's. Logs in as the
    row's target and key, which is the new one after a reconnect.

    *forget_restores* is for a folder this computer never worked in: a
    restore's marker in a workspace folder there vouches for the cleared
    computer's files, and left in place it would keep this computer's own
    backup from being restored over them.

    Never with the computer's own id: that would delete its own files from
    the machine.

    Raises:
        ComputerRepairRefused: ``folder_marked``, another computer marked the
            folder since it was checked.
        SSHConnectError: As :func:`ssh_provider.take_over_root` raises it.
    """
    if owner == _own_ref(computer):
        return
    target = _target(computer)
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_S):
            await ssh_provider.take_over_root(
                host=target["host"],
                port=target["port"],
                username=target["username"],
                private_key=await _own_key(computer),
                host_key=target["host_key"] or "",
                root_dir=computer["root_dir"],
                owner=owner,
                connect_timeout=settings.connect_timeout,
                allow_private_addresses=settings.allow_private_addresses,
                tunnel=target["tunnel_id"],
                claim_as=_own_ref(computer),
                forget=(SYNC_MARKER_NAME, DEFERRED_MARKER) if forget_restores else (),
            )
    except TimeoutError:
        raise SSHConnectError(
            "timeout",
            f"{target['host']} did not finish answering within "
            f"{int(PROBE_TIMEOUT_S)} seconds.",
        ) from None
    except SSHFolderMarked as e:
        raise _marked_elsewhere(e) from None


async def check_new_root(
    computer: Dict[str, Any],
    requested: str,
    *,
    settings: SSHConfig,
    take_over: Optional[str] = None,
) -> NewRoot:
    """The folder *requested* names on the computer's machine, checked as a
    new computer's folder is: missing or empty, the login's, and nobody else's
    but *take_over*'s, a computer the owner chose to take it from.

    ``~`` is the login's home, which only the machine knows. The folder's
    check never makes it; the start does, by the same rule.

    Raises:
        ComputerRepairRefused: ``root_dir`` (no usable folder, or this
            computer's own), ``folder_owned`` (another of the owner's
            computers works there) or ``folder_marked`` (a computer the
            owner does not have marked it).
        SSHConnectError: The machine refused or did not answer.
    """
    target = _target(computer)
    key = await _own_key(computer)
    if requested == "~" or requested.startswith("~/"):
        found = await probe(key, settings=settings, **target)
        requested = posixpath.join(found.home or "/", requested[2:])
    try:
        root = normal_root_dir(requested)
    except ValueError as e:
        raise ComputerRepairRefused("root_dir", str(e), status=422) from None
    if len(root) > ROOT_DIR_MAX:
        raise ComputerRepairRefused(
            "root_dir",
            f"Choose a folder whose path is at most {ROOT_DIR_MAX} characters.",
            status=422,
        )
    current = computer.get("root_dir") or ""
    if current and _nested(current, root):
        raise ComputerRepairRefused(
            "root_dir",
            f"{root} is inside or around this computer's folder, {current}. "
            "Choose a folder outside it.",
            status=422,
        )
    computers = await get_computers_for_user(str(computer["user_id"]))
    sibling = computer_sharing_folder(
        [c for c in computers if str(c["computer_id"]) != str(computer["computer_id"])],
        host=target["host"],
        port=target["port"],
        host_key=target["host_key"] or "",
        root_dir=root,
        tunnel_id=target["tunnel_id"],
    )
    if sibling is not None:
        raise ComputerRepairRefused(
            "folder_owned",
            f'Your computer "{sibling.get("name")}" already works in '
            f"{sibling.get('root_dir')} on this machine. Choose a folder outside it.",
            computer_id=str(sibling["computer_id"]),
        )
    try:
        found, owner = await _probe_folder(
            computer, key, target, root, settings=settings, take_over=take_over
        )
    except SSHConnectError as e:
        if e.code != "root_dir":
            raise
        raise ComputerRepairRefused("root_dir", str(e), status=422) from None
    if not found.supported:
        raise SSHConnectError(
            "unsupported",
            " ".join(found.problems) or f"{target['host']} cannot host a computer.",
        )
    state = posixpath.join(found.home or "/", STATE_DIR)
    if found.home and _nested(state, root):
        # Every computer's own folder is in there, and its removal deletes it.
        raise ComputerRepairRefused(
            "root_dir",
            f"{root} is inside or around {state}, where LangAlpha keeps each "
            "computer's own files. Choose a folder outside it.",
            status=422,
        )
    return NewRoot(root_dir=root, take_over=owner)


async def plan_reconnect(
    computer: Dict[str, Any],
    machine: ComputerSSHPinnedTarget,
    *,
    settings: SSHConfig,
    take_over: Optional[str] = None,
) -> ReconnectPlan:
    """Log in to the machine as a pairing reached it again, and compose the
    target the computer is pointed at.

    The folder is checked as the computer's own: its own mark passes, and a
    mark of any other computer refuses the reconnect unless it is
    *take_over*, which the caller clears through the new target once it is
    saved: the old one may be dead, as a Mac's helper paired again is a new
    host key and tunnel. Any other refusal of the folder is left to the
    start, as for a take-back.

    A machine is the computer's when it presents the pinned host key or holds
    the computer (its own folder in the login's home, or its mark on its
    folder). Any other machine is refused with ``different_machine`` unless
    ``machine.move`` says its owner chose to move the computer there, which
    the plan carries as ``moved``.

    Raises:
        ComputerRepairRefused: ``folder_marked``, ``folder_owned`` or
            ``different_machine``.
        SSHConnectError: The tunnel is not the caller's, the host key does
            not parse, or the machine refused, did not answer or cannot host
            a computer.
        SSHDialBudgetExceeded: The address is the caller's to name, so a
            direct dial is counted as a probe's is.
    """
    user_id = str(computer["user_id"])
    if machine.tunnel_id is not None:
        if not tunnels_enabled():
            raise SSHConnectError("unsupported", "This server does not offer tunnels.")
        if await get_user_tunnel(user_id, machine.tunnel_id) is None:
            raise SSHConnectError(
                "not_connected",
                "That machine's tunnel is no longer yours or no longer exists. "
                "Run the command on the machine again.",
            )
    if not is_public_key(machine.host_key):
        raise SSHConnectError(
            "host_key_invalid",
            "The host key given is not a valid SSH public key. Pair the "
            "machine again.",
        )
    target = {
        "host": machine.host,
        "port": machine.port,
        "username": machine.username,
        "host_key": machine.host_key,
        "tunnel_id": machine.tunnel_id,
    }
    sibling = computer_sharing_folder(
        [
            c
            for c in await get_computers_for_user(user_id)
            if str(c["computer_id"]) != str(computer["computer_id"])
        ],
        host=machine.host,
        port=machine.port,
        host_key=machine.host_key,
        root_dir=computer["root_dir"],
        tunnel_id=machine.tunnel_id,
    )
    if sibling is not None:
        raise ComputerRepairRefused(
            "folder_owned",
            f'Your computer "{sibling.get("name")}" already works in '
            f"{sibling.get('root_dir')} on that machine, around or inside this "
            f"computer's folder, {computer['root_dir']}. Reconnect to another "
            "machine, or reinstall this computer in a new folder there.",
            computer_id=str(sibling["computer_id"]),
        )
    if machine.tunnel_id is None:
        await spend_dials(user_id, 1, settings=settings)
    key = await client_key(user_id)
    owner: Optional[str] = None
    try:
        found, owner = await _probe_folder(
            computer,
            key,
            target,
            computer["root_dir"],
            settings=settings,
            take_over=take_over,
            known_as=_own_ref(computer),
        )
    except SSHConnectError as e:
        if e.code != "root_dir":
            raise
        # The folder is the start's to judge; the machine's facts are read
        # again without it.
        if machine.tunnel_id is None:
            await spend_dials(user_id, 1, settings=settings)
        found = await probe(key, settings=settings, **target, known_as=_own_ref(computer))
    if owner is not None and machine.tunnel_id is None:
        # The take-over's probe dialed again, and its clearing will.
        await spend_dials(user_id, 2, settings=settings)
    if not found.supported:
        raise SSHConnectError(
            "unsupported",
            " ".join(found.problems) or f"{machine.host} cannot host a computer.",
        )
    config = computer.get("provider_config") or {}
    pinned = {line.strip() for line in (config.get("host_key") or "").splitlines()}
    same = machine.host_key.strip() in pinned or found.knows_computer
    if not same and not machine.move:
        raise ComputerRepairRefused(
            "different_machine",
            f"{machine.host} holds nothing of this computer: its folder and "
            "files are not there, so it may not be the machine the computer "
            "was on. Move the computer there to go on; workspace files come "
            "back from their backup.",
        )
    return ReconnectPlan(
        provider_config=provider_config_for(
            config.get("key_owner") or user_id,
            host=machine.host,
            port=machine.port,
            username=machine.username,
            host_key=machine.host_key,
            tunnel_id=machine.tunnel_id,
            helper=found.helper,
            os=found.os or config.get("os") or "",
        ),
        tunnel_id=machine.tunnel_id,
        take_over=owner,
        moved=not same,
    )


async def renew_reconnect_tunnel(user_id: str, tunnel_id: Optional[str]) -> None:
    """Renew the tunnel a reconnect names, before the row names it, so the
    sweep cannot take it off the machine meanwhile.

    Raises:
        SSHConnectError: ``not_connected``, the tunnel is gone or being
            taken off its machine.
    """
    if tunnel_id is None:
        return
    renewed = await renew_tunnel(user_id, tunnel_id)
    if renewed is not True:
        raise SSHConnectError(
            "not_connected",
            "That machine's tunnel no longer exists. Run the command on the "
            "machine again."
            if renewed is None
            else "That machine's tunnel is being taken off it. Run the command "
            "on the machine again in a minute.",
        )


def require_ssh(computer: Dict[str, Any]) -> None:
    if computer.get("kind") != SSH_KIND:
        raise ComputerRepairRefused(
            "not_ssh",
            "Only a computer on a machine you brought can be reinstalled or reconnected.",
            status=400,
        )


def check_repairable(computer: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Refuse a repair of a computer that is not an ssh one, not down, or mid
    spec change. ValueError when it is gone, which the routes answer 404."""
    if computer is None or computer.get("status") == "deleted":
        raise ValueError("Computer not found")
    require_ssh(computer)
    running = spec_change_from_row(computer)
    if running is not None and running.state == "in_progress":
        raise ComputerRepairRefused("spec_in_progress", SPEC_CHANGE_IN_PROGRESS)
    if (
        computer.get("status") not in CLAIMABLE_FOR_START
        or (computer.get("config") or {}).get("retire_when_empty")
    ):
        raise ComputerRepairRefused(
            "not_stopped", "Stop the computer before you repair it."
        )
    return computer
