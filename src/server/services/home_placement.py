"""Which computer a Home turn runs on, decided at its admission from Postgres alone.

Home lives on the user's primary computer, which may be a machine of their
own. While that machine can't be reached (its tunnel is down, the signal the
"can't be reached" card reads), a Home turn runs in Home's stand-in instead: a
hidden workspace on a cloud computer the user already has, placed again on
another only when its own fails or is retired with nothing owed to Home. None
is ever built for it: a user with no cloud computer waits for their machine,
the turn failing as any turn on it does. Two workers admitting at once read
the same rows and converge on the stand-in's fixed id; nothing here is kept in
process memory.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from ptc_agent.core.sandbox.providers import RECREATABLE_KINDS
from src.server.database.computer import (
    get_computer_for_workspace,
    holds_stand_in,
    pick_stand_in_computer,
)
from src.server.database.home_workspace import (
    get_home_stand_in_id,
    get_home_workspace_id,
    is_home_stand_in_id,
)
from src.server.services.ssh_computers import computer_unreachable

logger = logging.getLogger(__name__)

@contextlib.asynccontextmanager
async def stand_in_held(user_id: str) -> AsyncIterator[bool]:
    """Hold the user's stand-in against new Home turns, or yield False while
    one is being admitted. Never waits.

    A turn placed in the stand-in takes the shared side in its START
    transaction, where it marks that Home is owed files
    (``home_away.mark_away``). So a removal or retirement that reads nothing
    owed under this hold deletes the stand-in before any turn's mark, and that
    turn then finds it gone and is refused rather than writing into it."""
    from src.server.database.home_away import stand_in_lock_key
    from src.server.database.session_lock import try_own_session_lock

    async with try_own_session_lock(stand_in_lock_key(user_id)) as held:
        yield held


@dataclass(frozen=True, slots=True)
class HomeAway:
    """A Home turn running in the stand-in rather than on Home's own computer."""

    #: The computer Home lives on.
    computer_id: str
    computer_name: str
    #: The cloud computer the stand-in is on, where the turn runs.
    cloud_computer_id: str
    cloud_name: str
    #: When the turn was admitted, ISO 8601: a file card of the turn finds the
    #: hand-back that carried its file by it (the first one after).
    at: str = ""
    #: ``unreachable`` (Home's computer can't be reached) or ``command`` (the
    #: turn reports on a background command that ran in the stand-in, whose log
    #: and files are there, though Home's computer is back).
    reason: str = "unreachable"

    def as_metadata(self) -> Dict[str, str]:
        return {
            "computer_id": self.computer_id,
            "computer_name": self.computer_name,
            "cloud_computer_id": self.cloud_computer_id,
            "cloud_name": self.cloud_name,
            "at": self.at,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class HomePlacement:
    #: The workspace whose computer and folder the turn runs in.
    runs_in: str
    away: Optional[HomeAway] = None


async def stand_in_computer(user_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    """The cloud computer Home's stand-in is on, else the one it would be
    placed on now (``pick_stand_in_computer``); None when the user has no
    cloud computer. A read: nothing is placed, built or started. A stand-in
    already placed stays on its computer while that computer can keep it
    (``holds_stand_in``), so its files are where the next turn on the cloud
    looks, and while files Home is owed are there, since they are nowhere
    else; a turn on a computer that failed then fails as it would anyway."""
    from src.server.database.home_away import handback_owed

    placed = await get_computer_for_workspace(get_home_stand_in_id(user_id), conn=conn)
    if placed is not None and (
        holds_stand_in(placed) or await handback_owed(user_id, conn=conn)
    ):
        return placed
    return await pick_stand_in_computer(user_id, RECREATABLE_KINDS, conn=conn)


def _away(
    home_computer: Mapping[str, Any], cloud: Mapping[str, Any], reason: str
) -> HomeAway:
    return HomeAway(
        computer_id=str(home_computer["computer_id"]),
        computer_name=str(home_computer.get("name") or ""),
        cloud_computer_id=str(cloud["computer_id"]),
        cloud_name=str(cloud.get("name") or ""),
        at=datetime.now(timezone.utc).isoformat(),
        reason=reason,
    )


async def place_home_turn(
    user_id: str,
    *,
    make: bool = True,
    follows: Optional[str] = None,
) -> HomePlacement:
    """Where a Home turn runs, Home already bound (``turn_runtime.ensure_home``).

    The stand-in while Home's computer can't be reached and the user has a
    cloud computer; otherwise Home's own computer, where a turn on a machine
    that can't be reached fails as a workspace turn does. ``make`` places the
    stand-in row if it is not placed yet (cheap, and never builds or starts a
    computer); off, only one already placed is used. Nothing is marked here:
    a turn placed in the stand-in marks the user away in its START
    transaction (``home_away.mark_away``), and work between turns (a
    compaction, opening Home) writes nothing the hand-back would carry.

    ``follows`` is the workspace a background command ran in when the turn
    reports on it: one that ran in the stand-in keeps its turn there even once
    Home's computer is back, since its log and the files it wrote are there
    and reach Home only with the next hand-back.
    """
    from src.server.database.workspace import get_workspace, place_home_stand_in

    home_id = get_home_workspace_id(user_id)
    stand_in_id = get_home_stand_in_id(user_id)
    home_computer = await get_computer_for_workspace(home_id)
    try:
        reachable = not await computer_unreachable(home_computer)
        if reachable and (follows is None or not is_home_stand_in_id(user_id, follows)):
            return HomePlacement(home_id)
        if not reachable and make:
            cloud = await stand_in_computer(user_id)
            if cloud is None:
                return HomePlacement(home_id)
            if await place_home_stand_in(user_id, str(cloud["computer_id"])) is None:
                return HomePlacement(home_id)
        # Another worker may have placed it first, on another computer,
        # and a removal of its computer deletes its row.
        cloud = await get_computer_for_workspace(stand_in_id)
        row = await get_workspace(stand_in_id) if cloud is not None else None
        if row is None or row.get("status") == "deleted" or home_computer is None:
            return HomePlacement(home_id)
        reason = "command" if reachable else "unreachable"
    except Exception:  # noqa: BLE001 - no stand-in is the turn as it was
        logger.warning(
            "Could not place user %s's Home turn on its stand-in", user_id, exc_info=True
        )
        return HomePlacement(home_id)
    away = _away(home_computer, cloud, reason)
    logger.info(
        "Home work of user %s runs in its stand-in on computer %s (%s)",
        user_id,
        away.cloud_computer_id,
        reason,
    )
    return HomePlacement(stand_in_id, away)


async def home_back_notice(user_id: str) -> Optional[Dict[str, Any]]:
    """What a Home turn on Home's own computer may say came back from the cloud.

    None for a user whose Home never ran in its stand-in. The turn row states
    it only on the first turn after one that said Home was away, so this is
    read on each Home turn but rarely rendered. Never fails the turn.
    """
    from src.server.database.home_away import get_home_away, is_owed

    try:
        row = await get_home_away(user_id)
        if row is None:
            return None
        home_computer = await get_computer_for_workspace(get_home_workspace_id(user_id))
        notice: Dict[str, Any] = {
            "computer_name": str((home_computer or {}).get("name") or ""),
        }
        last = row.get("last_handback") or {}
        if is_owed(row):
            notice["pending"] = True
        elif last.get("folder"):
            notice["folder"] = last["folder"]
            notice["files"] = int(last.get("files") or 0)
        return notice
    except Exception:  # noqa: BLE001 - a line of context is not worth the turn
        logger.warning("Could not read where %s's Home ran", user_id, exc_info=True)
        return None


async def home_placement_view(user_id: str) -> Dict[str, Any]:
    """Where Home is and where its next turn would run, for the client.

    A read: nothing is placed or marked, so a page asking every few seconds
    never places a stand-in or starts a computer.
    """
    from src.server.database.home_away import get_home_away, is_owed
    from src.server.database.workspace_move import get_latest_workspace_move
    from src.server.services.workspace_move import move_from_row

    home_id = get_home_workspace_id(user_id)
    home_computer = await get_computer_for_workspace(home_id)
    cloud = await stand_in_computer(user_id)
    row = await get_home_away(user_id)
    last = (row or {}).get("last_handback")
    return {
        "computer_id": str(home_computer["computer_id"]) if home_computer else None,
        "computer_name": (home_computer or {}).get("name"),
        "computer_kind": (home_computer or {}).get("kind"),
        "unreachable": await computer_unreachable(home_computer),
        "stand_in": (
            {
                "computer_id": str(cloud["computer_id"]),
                "name": cloud.get("name") or "",
                "status": cloud.get("status") or "",
            }
            if cloud is not None
            else None
        ),
        "handback_owed": is_owed(row),
        "handback": last if last and row.get("handback_seen_at") is None else None,
        "move": move_from_row(await get_latest_workspace_move(home_id)),
    }
