"""Seam: retiring a computer, when its last workspace leaves or its owner asks.

One file of the ComputerManager split; see the package __init__."""

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.providers import provider_class

from src.server.database.computer import (
    RETIRE_BY_OWNER,
    RETIRE_TORN_DOWN,
    clear_computer_retirement_pending,
    defer_computer_retirement,
    get_computer,
    get_retirement_pending_computers,
    mark_computer_retirement_pending,
    update_computer_status,
)
from src.server.database.home_away import stand_in_held
from src.server.database.home_stand_in import (
    STAND_IN_HOST,
    StandIn,
    stand_in_needed,
    stand_in_on,
)
from src.server.database.workspace import get_live_workspace_ids_for_computer
from src.server.database.workspace_move import primary_candidate
from src.server.models.computer import ComputerStatus
from src.server.services.computer_errors import ComputerRemovalRefused, Retirement
from src.server.services.spec_change import spec_change_from_row

logger = logging.getLogger(__name__)

# A computer leaves service at its owner's request only from a settled state.
# Any other belongs to the start, stop or first build moving it, which a claim
# from here would pull the sandbox out from under.
_REMOVABLE_FROM = (
    ComputerStatus.STOPPED,
    ComputerStatus.RUNNING,
    ComputerStatus.ERROR,
)


def _mid_transition(computer: Dict[str, Any]) -> bool:
    """A spec change owns the machine from its claim, before its status moves."""
    if computer.get("status") not in _REMOVABLE_FROM:
        return True
    change = spec_change_from_row(computer)
    return change is not None and change.state == "in_progress"


def _retirement_pending(computer: Dict[str, Any]) -> bool:
    return bool((computer.get("config") or {}).get("retire_when_empty"))


def _owners_machine(computer: Dict[str, Any]) -> bool:
    """A machine the deployment cannot make again leaves only at its owner's request.

    Retiring an empty computer stops paying for a hosted machine. A machine the
    user brings costs nothing here, and retiring it would drop its pinned host
    key, so its owner would have to add it again for their next workspace.
    """
    return not provider_class(computer["kind"]).recreatable


class RetirementMixin:
    async def _retire_machine_if_empty(
        self,
        computer_id: str,
        workspace_id: str,
        workspace: Dict[str, Any],
        *,
        keep_folder: bool = False,
        owner_removing: bool = False,
    ) -> bool:
        """Check emptiness under the machine advisory key after project tombstoning.

        Otherwise concurrent last-project deletes could both see a sibling and
        leave the machine alive. Keep empty primaries for future projects to reuse
        their sandbox and disk; only final retirement tombstones the machine."""
        # The project is already tombstoned. Record the remaining cleanup before
        # attempting the contended lock so a process exit or timeout cannot turn
        # this into an indefinitely billed empty machine. Explicitly created bare
        # computers carry no marker and are not swept.
        try:
            await mark_computer_retirement_pending(computer_id, by_owner=owner_removing)
            async with self._retirement_decision(
                computer_id, workspace_id=workspace_id
            ) as (outcome, computer):
                if outcome is Retirement.DEFERRED:
                    computer = await get_computer(computer_id)
                if computer is None or not (
                    outcome.keeps_machine or outcome is Retirement.DEFERRED
                ):
                    return outcome is Retirement.RETIRED
                # The shared machine survives; remove only the departing
                # project's folder, and none its owner kept on their machine.
                if not keep_folder:
                    await self._remove_workspace_folder(workspace_id, workspace, computer)
                return False
        except Exception as e:
            # The project is already unlinked; do not fail its completed delete.
            # Start paths can reconcile a machine left running behind its live row.
            logger.warning(
                f"Could not settle computer {computer_id} after deleting "
                f"workspace {workspace_id}: {e}"
            )
            return False

    @staticmethod
    def removal_held(computer: Dict[str, Any]) -> Optional[Retirement]:
        """What refuses the owner's remove whatever is on the computer, read
        off the row: the primary, then a transition another owner holds,
        which only a retirement owed to a delete may claim from under it.

        Asked before emptying one for its removal, which would otherwise
        delete its workspaces only to be refused, and again under the key by
        ``_removal_refusal``, where it is decisive."""
        if computer.get("is_primary"):
            return Retirement.PRIMARY
        return Retirement.BUSY if _mid_transition(computer) else None

    @staticmethod
    async def removal_refused(computer: Dict[str, Any]) -> Optional[Retirement]:
        """``removal_held``, with the primary told apart from the user's only
        computer: the first is removed once another is made primary, which
        moves Home; the second only once another that could be is added.
        A computer that could not take Home now (failed, being retired, still
        on its first build) is no other to make primary."""
        held = RetirementMixin.removal_held(computer)
        if held is not Retirement.PRIMARY:
            return held
        other = await primary_candidate(
            str(computer["user_id"]), excluding=str(computer["computer_id"])
        )
        return Retirement.PRIMARY if other is not None else Retirement.ONLY_COMPUTER

    async def remove_computer(self, computer_id: str) -> None:
        """Retire an empty computer its owner no longer wants.

        The refusals are read off the row first, so a start this worker is
        building is refused rather than waited out behind its machine lock.
        The decision is the one every retirement makes, under the key, which
        is where a race with a create or a start settles.

        Raises:
            ComputerRemovalRefused: the computer stays, usable; ``code`` says why.
            ValueError: there is no such computer, or it is already gone.
        """
        computer = await get_computer(computer_id)
        if computer is None:
            raise ValueError(f"Computer {computer_id} not found")
        refusal = await self._removal_refusal(
            computer_id, computer, await stand_in_on(computer)
        )
        if refusal is not None:
            raise ComputerRemovalRefused(refusal)
        async with self._observed_lock(computer_id, "computer.remove"):
            async with self._retirement_decision(
                computer_id, owner_request=True
            ) as (outcome, _):
                pass
        if outcome is Retirement.GONE:
            raise ValueError(f"Computer {computer_id} not found")
        if outcome is not Retirement.RETIRED:
            # A key held past the wait is a transition someone else owns too.
            raise ComputerRemovalRefused(
                Retirement.BUSY if outcome is Retirement.DEFERRED else outcome
            )
        # Once the machine lock is released, since the record holds it, as a
        # workspace delete does.
        self._forget_machine(computer_id)

    @asynccontextmanager
    async def _retirement_decision(
        self,
        computer_id: str,
        *,
        workspace_id: str = "",
        owner_request: bool = False,
        sweep: bool = False,
    ):
        """Decide under the computer's key whether it goes, and retire it if so.

        Every retirement comes through here, so no two can disagree on what an
        empty computer is. The marker is on before the emptiness read: a create
        or start reading the row afterwards refuses it, and one that landed
        before is in the read. A workspace delete marks it before asking, so a
        key it cannot take leaves the retirement to the ``sweep``, which
        retires only what is still marked once it holds the key. An
        ``owner_request`` marks it here instead and takes back what it marked
        on any refusal, so a refused remove leaves the computer as it was, and
        it refuses a machine mid transition rather than claim it from its
        owner. The marker says when a retirement is its owner's remove
        (``RETIRE_BY_OWNER``), so whoever finishes it keeps the stand-in mark
        as the owner's own remove does, and when that remove has already torn
        a cloud computer's sandbox down (``RETIRE_TORN_DOWN``), so a sweep
        finishing it tombstones it rather than keep a computer with no
        sandbox left. Yields
        the outcome and the row it was read from, with the key still held.
        """
        async with self._machine_decision_lock(computer_id) as locked, AsyncExitStack() as held:
            if not locked:
                yield Retirement.DEFERRED, None
                return
            marked_here = owner_request and await mark_computer_retirement_pending(
                computer_id
            )
            computer = await get_computer(computer_id)
            if computer is None:
                logger.info(
                    f"Computer {computer_id} is already retired; nothing "
                    "left to tear down"
                )
                yield Retirement.GONE, None
                return
            if sweep and not _retirement_pending(computer):
                logger.info(
                    f"Computer {computer_id} is no longer pending retirement; "
                    "whoever took the marker off settled it"
                )
                yield Retirement.WITHDRAWN, computer
                return
            # Home's stand-in on it: no Home turn is admitted onto it from the
            # read of what Home is owed to the teardown, or its files would go
            # with the computer. One placed later meets the marker. Read again
            # under the hold, which a turn admitted before has marked.
            stand_in = await stand_in_on(computer)
            if stand_in is not None:
                if not await held.enter_async_context(stand_in_held(str(computer["user_id"]))):
                    if marked_here:
                        await clear_computer_retirement_pending(computer_id)
                    yield Retirement.BUSY, computer
                    return
                stand_in = await stand_in_on(computer)
            config = computer.get("config") or {}
            # A delete or sweep finishing what its owner asked for decides as
            # that remove would, rather than keep the computer and take the
            # marker off. One of the user's own still keeps its machine here,
            # its folders going as each delete asked; its owner's remove
            # follows.
            owners_remove = owner_request or (
                bool(config.get(RETIRE_BY_OWNER)) and not _owners_machine(computer)
            )
            if sweep and config.get(RETIRE_TORN_DOWN):
                # Its owner's remove tore its sandbox down; only the
                # tombstone is left to write.
                kept = None
            elif owners_remove:
                kept = await self._removal_refusal(computer_id, computer, stand_in)
            elif _owners_machine(computer):
                kept = Retirement.OWNERS_MACHINE
            else:
                kept = await self._why_machine_is_kept(computer_id, computer, stand_in)
                if kept is None and stand_in is not None and stand_in.left:
                    # Files a hand-back left there are not in Home; only
                    # their owner's remove, told so, takes them.
                    logger.info(
                        f"Computer {computer_id} holds Home stand-in files a "
                        "hand-back left; leaving it in place"
                    )
                    kept = Retirement.HANDBACK_LEFT
                elif (
                    kept is None
                    and (stand_in is not None or config.get(STAND_IN_HOST))
                    and await stand_in_needed(str(computer["user_id"]))
                ):
                    # Where Home's turns run while the user's own machine is
                    # away, or the computer kept for them to run on before
                    # any has; its owner's remove still takes it.
                    logger.info(
                        f"Computer {computer_id} holds Home's stand-in for a "
                        "primary of the user's own; leaving it in place"
                    )
                    kept = Retirement.STAND_IN
            if kept is not None:
                if not kept.stays_pending or marked_here:
                    await clear_computer_retirement_pending(computer_id)
                yield kept, computer
                return
            # In the claim itself: a remove whose tombstone write never lands,
            # its worker gone at any point after the claim or the write
            # missed, is the sweep's to finish as the owner's, and a claim
            # that misses marks nothing.
            owners = (
                {"merge_config": {"retire_when_empty": True, RETIRE_BY_OWNER: True}}
                if owner_request
                else {}
            )
            claimed = await update_computer_status(
                computer_id,
                ComputerStatus.STOPPING,
                expected=computer["status"],
                **owners,
            )
            if claimed is None:
                if marked_here:
                    await clear_computer_retirement_pending(computer_id)
                yield Retirement.BUSY, computer
                return
            binding = self._binding_from_computer(workspace_id, claimed)
            await self._teardown_machine(binding)
            if owners_remove and provider_class(claimed["kind"]).recreatable:
                # Only once its sandbox is gone, which is all a cloud computer
                # owes: what one of the user's own owes its machine comes after
                # the tombstone, so that one is left for its owner to remove
                # again.
                await mark_computer_retirement_pending(
                    computer_id, by_owner=True, torn_down=True
                )
            deleted = await update_computer_status(
                computer_id,
                ComputerStatus.DELETED,
                expected=ComputerStatus.STOPPING,
                # Retired for being empty: a mark left on the tombstone would
                # read as its owner's remove, which refuses Home's stand-in
                # another. Taken off in the tombstone write itself, unless the
                # row says by then that this is its owner's remove, so a mark
                # adopted or a remove begun since this decision read the row
                # is read where it lands.
                drop_config_key=None if owner_request else STAND_IN_HOST,
                unless_config_key=RETIRE_BY_OWNER,
            )
            # Torn down already, so the marker stays for the sweep to finish.
            outcome = Retirement.BUSY if deleted is None else Retirement.RETIRED
            yield outcome, computer

    async def reap_retirement_pending_computers(self) -> int:
        """Settle every delete-time retirement decision that lost its lock."""
        retired = 0
        for candidate in await get_retirement_pending_computers(limit=1000):
            computer_id = str(candidate["computer_id"])
            settled = False
            try:
                async with self._retirement_decision(computer_id, sweep=True) as (
                    outcome,
                    _,
                ):
                    settled = not outcome.stays_pending
                    if outcome is Retirement.RETIRED:
                        retired += 1
            except Exception as e:
                logger.warning(
                    "Could not retire empty computer %s during cleanup: %s",
                    computer_id,
                    e,
                )
            finally:
                if not settled:
                    # Move a contended or failed candidate behind newer work.
                    # Otherwise the same 1,000 rows can monopolize every sweep.
                    try:
                        await defer_computer_retirement(computer_id)
                    except Exception as e:
                        logger.warning(
                            "Could not defer empty computer %s after a failed "
                            "retirement attempt: %s",
                            computer_id,
                            e,
                        )
        return retired

    async def _removal_refusal(
        self,
        computer_id: str,
        computer: Dict[str, Any],
        stand_in: Optional[StandIn],
    ) -> Optional[Retirement]:
        """None when the owner's remove may go ahead; decisive only under the key.

        ``removal_refused``, then what is still on it."""
        held = await self.removal_refused(computer)
        if held is None:
            return await self._why_machine_is_kept(computer_id, computer, stand_in)
        if held is Retirement.BUSY:
            logger.info(
                f"Computer {computer_id} is {computer['status']!r} or "
                "changing spec; leaving it to that transition"
            )
        return held

    @staticmethod
    async def _why_machine_is_kept(
        computer_id: str, computer: Dict[str, Any], stand_in: Optional[StandIn]
    ) -> Optional[Retirement]:
        """None when nothing keeps the machine; decisive only under its key.

        The primary is asked first: it stays however empty it gets, so a user
        told to empty it first would empty it for nothing. A bound Home counts
        as a workspace, since it takes its computer's status and the
        tombstone's shadow write would end it too. Home's stand-in
        (``stand_in_on``) does not: the shadow write ends it with the
        computer, and it is placed again on the next need, unless Home is
        owed files it holds."""
        if computer.get("is_primary"):
            logger.info(
                f"Computer {computer_id} is the user's primary and is kept; "
                "the next workspace joins this machine"
            )
            return Retirement.PRIMARY
        # Home's stand-in alone keeps no computer: it is placed again on the
        # next need.
        remaining = await get_live_workspace_ids_for_computer(
            computer_id, exclude_stand_in=True
        )
        if remaining:
            logger.info(
                f"Computer {computer_id} still carries {len(remaining)} "
                "live workspace(s); leaving it in place"
            )
            return Retirement.HAS_WORKSPACES
        if stand_in is not None and stand_in.owed:
            logger.info(
                f"Computer {computer_id} holds Home stand-in files not yet "
                "handed back; leaving it in place"
            )
            return Retirement.HANDBACK_OWED
        return None
