"""Repairing an ssh computer that will not start: a reinstall or a reconnect.

Both change what a start reads (the machine's folder, the computer's root, or
its target), so each runs only on a computer that is down, under its decision
lock, re-read under that lock and fenced on what was decided. The route then
starts it as a lazy start does.
"""

import logging
from typing import Any, Dict, Optional, Tuple

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.database.computer import clear_computer_start_failure, get_computer
from src.server.database.computer_root import move_computer_root, repoint_computer_machine
from src.server.database.workspace_folders import workspace_folders_lock
from src.server.models.computer import ComputerSSHReconnectTarget
from src.server.services.computer_errors import ComputerRepairRefused
from src.server.services.ssh_computer_repair import (
    check_new_root,
    check_repairable,
    folder_owner,
    plan_reconnect,
    renew_reconnect_tunnel,
    take_back_folder,
)

logger = logging.getLogger(__name__)

_BUSY = "This computer changed while it was being repaired. Try again."


def _same_machine_row(decided: Dict[str, Any], fresh: Dict[str, Any]) -> bool:
    return decided.get("root_dir") == fresh.get("root_dir") and (
        decided.get("provider_config") or {}
    ) == (fresh.get("provider_config") or {})


class ComputerRepairMixin:
    async def _repair_row(self, computer_id: str, decided: Dict[str, Any]) -> Dict[str, Any]:
        """The row again under the decision lock, refused if it moved since
        the dial was decided on it."""
        fresh = check_repairable(await get_computer(computer_id))
        if not _same_machine_row(decided, fresh):
            raise ComputerRepairRefused("busy", _BUSY)
        return fresh

    async def _forget_machine_handles(self, computer_id: str, reason: str) -> None:
        """Drop what this worker holds of the computer's old folder or target.

        Another worker's handle is caught by the identity check of its next
        acquisition, which compares the session's root and target with the
        row's. A new target is a new pool key, so no connection made to the
        old one is reused.
        """
        await self.retire_computer_session_if_present(computer_id, reason=reason)

    async def _take_back_after_repair(
        self,
        computer: Dict[str, Any],
        owner: str,
        *,
        settings: SSHConfig,
        forget_restores: bool = False,
    ) -> None:
        """Clear *owner* from a folder the computer was just moved or pointed
        to. The repair is saved by then, so a refusal is left to the start it
        schedules, which records it with the reinstall that takes the folder
        back."""
        try:
            await take_back_folder(
                computer, owner, settings=settings, forget_restores=forget_restores
            )
        except (ComputerRepairRefused, SSHConnectError) as e:
            logger.warning(
                "Could not take the folder of computer %s from computer %s: %s",
                computer["computer_id"],
                owner,
                e,
            )

    async def reinstall_computer(
        self,
        computer_id: str,
        *,
        folder: str,
        root_dir: Optional[str],
        settings: SSHConfig,
        take_over: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Give a computer a folder it may have, and answer its row.

        ``take_back`` clears the computer whose mark is on its folder from the
        machine, which must be *take_over*, the one its owner was shown; a
        folder with no other mark is left as it is, for the start to claim.
        ``new`` moves it to *root_dir*, where each workspace's files are
        restored from its backup, then clears *take_over* from that folder
        when its mark is there.

        Raises:
            ComputerRepairRefused: The computer cannot be repaired now, or
                the folder cannot be its.
            SSHConnectError: The machine refused or did not answer.
        """
        decided = check_repairable(await get_computer(computer_id))
        if folder == "take_back":
            owner = await folder_owner(decided, settings=settings)
            if owner is not None and owner != take_over:
                # Marked anew since its owner was shown the failure: clearing
                # a computer they never saw is theirs to choose.
                raise ComputerRepairRefused(
                    "folder_marked",
                    "Another computer marked this folder since the start "
                    "failed. Check which one before you take the folder back.",
                    owner=owner,
                )
            async with self._machine_decision_lock(computer_id) as locked:
                if not locked:
                    raise ComputerRepairRefused("busy", _BUSY)
                fresh = await self._repair_row(computer_id, decided)
                if owner is not None:
                    await take_back_folder(fresh, owner, settings=settings)
                    logger.info(
                        "Took the folder of computer %s back from computer %s",
                        computer_id,
                        owner,
                    )
                await clear_computer_start_failure(computer_id)
            await self._forget_machine_handles(computer_id, "reinstalled")
            return await get_computer(computer_id) or fresh

        assert root_dir is not None
        checked = await check_new_root(
            decided, root_dir, settings=settings, take_over=take_over
        )
        new_root = checked.root_dir
        async with self._machine_decision_lock(computer_id) as locked:
            if not locked:
                raise ComputerRepairRefused("busy", _BUSY)
            fresh = await self._repair_row(computer_id, decided)
            async with workspace_folders_lock(computer_id) as folders:
                if folders is None:
                    raise ComputerRepairRefused("busy", _BUSY)
                moved = await move_computer_root(
                    computer_id, old_root=fresh["root_dir"], new_root=new_root
                )
            if moved is None:
                raise ComputerRepairRefused("busy", _BUSY)
            if checked.take_over is not None:
                # After the move, so a move refused leaves the other computer
                # as it was; a take-over refused here is the start's failure.
                await self._take_back_after_repair(
                    moved, checked.take_over, settings=settings, forget_restores=True
                )
        logger.info(
            "Moved computer %s from %s to %s",
            computer_id,
            fresh["root_dir"],
            new_root,
        )
        await self._forget_machine_handles(computer_id, "moved to a new folder")
        # The mount names the old root; a stopped computer holds no token, but
        # one a failed start left behind would serve files from there.
        await self._revoke_livefs(computer_id)
        return moved

    async def reconnect_computer(
        self,
        computer_id: str,
        machine: ComputerSSHReconnectTarget,
        *,
        settings: SSHConfig,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Point a computer at its machine as a pairing reached it again.

        With ``machine.take_over`` naming the computer whose mark is on the
        folder, that computer is cleared through the new target once it is
        saved: the old target may no longer reach the machine, and the new
        pairing would be left with no computer naming it. Answers the row
        after and the row before, whose tunnel the route releases when
        nothing names it any more.

        Raises:
            ComputerRepairRefused: The computer cannot be repaired now, or
                its folder there is another computer's.
            SSHConnectError: The machine refused, did not answer, or its
                tunnel is not the caller's.
            SSHDialBudgetExceeded: As for a probe.
        """
        decided = check_repairable(await get_computer(computer_id))
        plan = await plan_reconnect(
            decided, machine, settings=settings, take_over=machine.take_over
        )
        updated: Optional[Dict[str, Any]] = None
        try:
            async with self._machine_decision_lock(computer_id) as locked:
                if not locked:
                    raise ComputerRepairRefused("busy", _BUSY)
                fresh = await self._repair_row(computer_id, decided)
                await renew_reconnect_tunnel(str(fresh["user_id"]), plan.tunnel_id)
                updated = await repoint_computer_machine(
                    computer_id,
                    provider_config=plan.provider_config,
                    expected_provider_config=fresh.get("provider_config") or {},
                    moved=plan.moved,
                )
                if updated is None:
                    raise ComputerRepairRefused("busy", _BUSY)
                logger.info(
                    "Reconnected computer %s to %s@%s%s%s",
                    computer_id,
                    machine.username,
                    machine.host,
                    f" through tunnel {machine.tunnel_id[:8]}"
                    if machine.tunnel_id
                    else "",
                    ", a machine it was not on" if plan.moved else "",
                )
                if plan.moved:
                    # The mount and the folders it named are on the old machine.
                    await self._revoke_livefs(computer_id)
                if plan.take_over is not None:
                    # A machine it was not on is a folder it never worked in,
                    # whose restore markers vouch for the other computer's files.
                    await self._take_back_after_repair(
                        updated,
                        plan.take_over,
                        settings=settings,
                        forget_restores=plan.moved,
                    )
        finally:
            if updated is not None:
                await self._forget_machine_handles(computer_id, "reconnected")
        return updated, fresh
