"""What a removed ssh computer leaves on its machine: the folders its owner
asked to have deleted, LangAlpha's own files when they asked for those too,
the key's line and the tunnel.

All three go after the computer's delete, on the connection the delete ran on
(``ssh.removal_link``): a machine the delete could not reach is not dialed
again. The folders go first, since the tunnel's uninstall may take this
connection with it, then LangAlpha's files for the same reason: each leaves
its name by a rename and is then deleted
detached, as the uninstall is, so the removal answers its owner before a large
folder is gone. With a tunnel to take off, the key's line goes
in the uninstall's own command, just ahead of it, since the key is the login a
later try at the uninstall would need, and it stays when the uninstall never
went out. The removal has happened by then, so nothing here raises.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.paths import LEFTOVERS_TRASH_PREFIX, workspace_root
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from ptc_agent.core.sandbox.providers._ssh_authorized_key import remove_script
from ptc_agent.core.sandbox.providers._ssh_shell import (
    home_shared_script,
    uninstall_script,
)
from ptc_agent.core.sandbox.providers._ssh_transport import TRANSPORT_ERRORS, Link
from src.server.database.computer import get_computers_for_user
from src.server.database.ssh_keys import get_user_ssh_public_key
from src.server.database.workspace import (
    complete_workspace_folder_cleanup,
    get_folder_cleanups_owed_by,
)
from src.server.database.workspace_folders import (
    LEFTOVERS_DIR,
    is_top_level,
    leftovers_path,
)
from src.server.models.computer import (
    ComputerFilesRemoval,
    ComputerKeyRemoval,
    KeyRemovalOutcome,
)
from src.server.services.ssh_computers import (
    computer_sharing_login,
    key_line_logins,
    tunnel_of,
)
from src.server.services.ssh_tunnel_sweep import Ahead, release_tunnel

logger = logging.getLogger(__name__)

# One short command on a connection already open.
_KEY_TIMEOUT_S = 20.0
# Renames and the small folders the trash could not take, on one connection.
_FILES_TIMEOUT_S = 60.0


@dataclass(frozen=True)
class MachineLeft:
    key: Optional[ComputerKeyRemoval]
    # False leaves the tunnel to a release that logs in anew, as one with no
    # connection open must, and one after a release here failed or while
    # another release or the sweep is taking it off.
    tunnel_released: bool
    files: Optional[ComputerFilesRemoval] = None


@dataclass(frozen=True)
class _KeyLine:
    """The key line's removal as decided before anything runs on the machine:
    the ``script`` that takes it away, or what to say instead; ``homes`` names
    the computers whose folder in the login's home keeps it."""

    computer_id: Any
    fingerprint: Optional[str]
    comment: Optional[str]
    script: Optional[str] = None
    decided: KeyRemovalOutcome = "skipped"
    used_by: Optional[str] = None
    homes: Mapping[str, Optional[str]] = field(default_factory=dict)

    def outcome(
        self, outcome: KeyRemovalOutcome, used_by: Optional[str] = None
    ) -> ComputerKeyRemoval:
        return ComputerKeyRemoval(
            outcome=outcome,
            used_by=used_by,
            fingerprint=self.fingerprint,
            comment=self.comment,
        )


async def leave_machine(
    user_id: str,
    computer: Dict[str, Any],
    *,
    settings: SSHConfig,
    remove_key: bool,
    delete_files: bool = False,
) -> MachineLeft:
    """Take what a removed ssh computer owes off its machine: the folders its
    owner asked to have deleted, the key's line, and its tunnel.

    ``computer`` is the row as it was before the removal; ``settings`` the
    deployment's, which its ``provider_config`` is laid over as for any of its
    calls, so the connection found is the one its delete used. A machine
    LangAlpha's helper answers for has no line of ours in ``~/.ssh`` to take,
    so ``remove_key`` is not asked of it: its key goes with the helper's folder.
    ``delete_files`` also takes LangAlpha's own files off (``uninstall_script``).
    """
    remove_key = remove_key and not (computer.get("provider_config") or {}).get(
        "helper"
    )
    try:
        config = SSHConfig.model_validate(
            {**settings.model_dump(), **(computer.get("provider_config") or {})}
        )
        async with ssh_provider.removal_link(config) as link:
            if link is not None:
                try:
                    await _remove_owed_folders(computer, link)
                except Exception:  # noqa: BLE001 - the key and the tunnel go regardless
                    logger.exception(
                        "Could not delete the folders of removed computer %s",
                        computer.get("computer_id"),
                    )
            files = None
            if delete_files:
                files = await _remove_files(computer, link)
            line = await _key_line(user_id, computer, link) if remove_key else None
            ahead = Ahead(line.script) if line is not None and line.script else None
            released = False
            if link is not None and tunnel_of(computer):
                released = await release_tunnel(
                    user_id, computer, settings=settings, link=link, ahead=ahead
                )
            tunnel_left = bool(tunnel_of(computer)) and not released
            key = None
            if line is not None:
                key = await _take_key_line(line, link, ahead, tunnel_left=tunnel_left)
            return MachineLeft(key=key, files=files, tunnel_released=released)
    except Exception:  # noqa: BLE001 - the removal it follows already succeeded
        logger.exception(
            "Could not finish leaving the machine of removed computer %s",
            computer.get("computer_id"),
        )
        return MachineLeft(
            key=ComputerKeyRemoval(outcome="skipped") if remove_key else None,
            files=ComputerFilesRemoval(outcome="failed") if delete_files else None,
            tunnel_released=False,
        )


async def _remove_files(
    computer: Dict[str, Any], link: Optional[Link]
) -> ComputerFilesRemoval:
    """LangAlpha's files off the machine, after the computer's own folder went
    with its delete; a machine that delete could not reach keeps them all."""
    computer_id = computer.get("computer_id")
    root = computer.get("root_dir") or ""
    sandbox_id = computer.get("provider_ref") or ""
    if link is None:
        return ComputerFilesRemoval(outcome="unreached")
    if not root or not sandbox_id:
        logger.warning("No root to delete files from for removed computer %s", computer_id)
        return ComputerFilesRemoval(outcome="failed")
    # Link.run, split where the script goes out: a failure before is a
    # machine that deleted nothing, one after may have deleted part.
    sent = False
    try:
        async with link.in_use(), asyncio.timeout(_FILES_TIMEOUT_S):
            run = await link.start(
                uninstall_script(root, sandbox_id), slot=link.control_slot
            )
            sent = True
            try:
                result = await run.finish()
            except BaseException:
                run.abandon()
                raise
    except (TimeoutError, *TRANSPORT_ERRORS) as e:
        logger.info(
            "Lost removed computer %s's machine deleting its files (%s): %r",
            computer_id,
            "after sending" if sent else "before sending",
            e,
        )
        return ComputerFilesRemoval(outcome="failed" if sent else "unreached")
    said = result.stdout.decode(errors="replace").splitlines()
    if result.exit_code != 0:
        logger.warning(
            "Could not delete the files of removed computer %s: %s",
            computer_id,
            (result.stderr or b"").decode(errors="replace").strip()[:200],
        )
        return ComputerFilesRemoval(outcome="failed")
    return ComputerFilesRemoval(
        outcome="deleted",
        root_kept="root kept" in said,
        runtime_kept="runtime kept" in said,
    )


async def _remove_owed_folders(computer: Dict[str, Any], link: Link) -> None:
    """Delete the folders of workspaces deleted, folder and all, while the
    computer was stopped; the reaper that would have runs only on a running one.

    Each leaves its name by a rename first, as the reaper's do, and only what
    was moved is deleted in the background: the machine added again may give
    a new workspace that name while a large folder is still going. No start
    comes after a removal to try again, so a claim clears once its folder is
    out of its name.
    """
    computer_id = str(computer["computer_id"])
    root = (computer.get("root_dir") or "").rstrip("/")
    if not root:
        return
    owed = []
    for workspace in await get_folder_cleanups_owed_by(computer_id):
        workspace_id = str(workspace["workspace_id"])
        dir_name = (workspace.get("dir_name") or "").strip("/")
        # As the reaper reads it: never a path that would name the root.
        if dir_name in (".", "..") or not (
            is_top_level(dir_name) or dir_name == leftovers_path(workspace_id)
        ):
            continue
        owed.append((workspace_id, dir_name))
    if not owed:
        return
    leftovers = shlex.quote(f"{root}/{LEFTOVERS_DIR}")
    trash = shlex.quote(f"{root}/{LEFTOVERS_DIR}/{LEFTOVERS_TRASH_PREFIX}")
    script = [f"t=$(mkdir -p {leftovers} && mktemp -d {trash}XXXXXX) || exit 1"]
    for i, (_, dir_name) in enumerate(owed):
        folder = shlex.quote(workspace_root(root, dir_name))
        # Says which are out of their name: gone already, or moved now.
        script.append(
            f'if [ ! -e {folder} ] && [ ! -L {folder} ] || mv -- {folder} "$t/{i}"; '
            f"then echo {i}; fi"
        )
    script.append('nohup rm -rf -- "$t" >/dev/null 2>&1 &')
    try:
        result = await link.run("\n".join(script) + "\n", timeout=_KEY_TIMEOUT_S)
    except (TimeoutError, *TRANSPORT_ERRORS) as e:
        logger.info(
            "Left the folders of removed computer %s on its machine: %r", computer_id, e
        )
        return
    out = set(result.stdout.decode(errors="replace").split())
    left = [dir_name for i, (_, dir_name) in enumerate(owed) if str(i) not in out]
    if result.exit_code != 0 or left:
        logger.warning(
            "Could not delete the folders %s of removed computer %s: %s",
            left,
            computer_id,
            (result.stderr or b"").decode(errors="replace").strip()[:200],
        )
    for i, (workspace_id, dir_name) in enumerate(owed):
        if str(i) in out:
            await complete_workspace_folder_cleanup(
                workspace_id, computer_id=computer_id, dir_name=dir_name
            )


async def _key_line(
    user_id: str, computer: Dict[str, Any], link: Optional[Link]
) -> _KeyLine:
    config = computer.get("provider_config") or {}
    row = await get_user_ssh_public_key(config.get("key_owner") or user_id) or {}
    public_key = row.get("public_key") or ""
    parts = public_key.split()
    known = {
        "computer_id": computer.get("computer_id"),
        "fingerprint": row.get("fingerprint"),
        "comment": parts[2] if len(parts) > 2 else None,
    }
    computers = await get_computers_for_user(user_id)
    sibling = computer_sharing_login(computers, computer)
    if sibling is not None:
        return _KeyLine(**known, decided="kept", used_by=sibling.get("name"))
    if link is None:
        return _KeyLine(**known, decided="unreached")
    # Hosts that mount one home read one authorized_keys, so a computer on
    # another host can log in through the line too; only its folder there says so.
    homes = {
        str(c["provider_ref"]): c.get("name")
        for c in key_line_logins(computers, computer)
        if c.get("provider_ref")
    }
    try:
        script = home_shared_script(homes) + remove_script(public_key)
        return _KeyLine(**known, script=script, homes=homes)
    except ValueError:
        logger.warning("No key to remove for removed computer %s", known["computer_id"])
        return _KeyLine(**known)


async def _take_key_line(
    line: _KeyLine, link: Optional[Link], ahead: Optional[Ahead], *, tunnel_left: bool
) -> ComputerKeyRemoval:
    if line.script is None or link is None:
        return line.outcome(line.decided, line.used_by)
    if ahead is not None and ahead.sent:
        said, failed = ahead.said, ""
    elif tunnel_left:
        # The uninstall never went out, and the release that tries it again
        # logs in with this key.
        logger.info(
            "Left the key line of removed computer %s for its tunnel's release",
            line.computer_id,
        )
        return line.outcome("skipped")
    else:
        try:
            result = await link.run(line.script, timeout=_KEY_TIMEOUT_S)
        except (TimeoutError, *TRANSPORT_ERRORS) as e:
            said, failed = None, repr(e)
        else:
            said = (
                result.stdout.decode(errors="replace") if result.exit_code == 0 else ""
            )
            failed = (result.stderr or b"").decode(errors="replace").strip()[:200]
    if said is None:
        # Sent with the uninstall, which then lost the machine, or on its own.
        logger.info(
            "Lost removed computer %s's machine before its key line went: %s",
            line.computer_id,
            failed or "with the tunnel's uninstall",
        )
        return line.outcome("unreached")
    if said.strip() == "removed":
        return line.outcome("removed")
    word, _, ref = said.strip().partition(" ")
    if word == "shared":
        return line.outcome("kept", line.homes.get(ref))
    # absent too: the login came by a file sshd reads that is not ours to edit.
    logger.info(
        "Left the key line of removed computer %s on its machine: %s",
        line.computer_id,
        said.strip() or failed,
    )
    return line.outcome("skipped")
