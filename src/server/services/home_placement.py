"""Which computer a Home turn runs on, decided at its admission from Postgres alone.

Home lives on the user's primary computer, which may be a machine of their
own. While that machine can't be reached (its tunnel is down, the signal the
"can't be reached" card reads), a Home turn runs in Home's stand-in instead: a
hidden workspace on a cloud computer the user already has, placed again on
another only when its own fails or is retired with nothing owed to Home. A
turn builds none for it (one the plan grants is made ahead, see
``ComputerManager.ensure_stand_in_host``): a user with no cloud computer waits for their machine,
as does one whose stand-in keeps owed files on a computer that failed, the
turn failing as any turn on it does (``home_stand_in``). Two workers admitting
at once read the same rows and converge on the stand-in's fixed id; nothing
here is kept in process memory.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional, Tuple

from ptc_agent.agent.middleware.runtime_context import HomeAway
from src.server.database.computer import get_computer_for_workspace, get_home_computer
from src.server.database.home_stand_in import (
    place_home_stand_in,
    read_stand_in,
    stand_in_computer,
)
from src.server.database.home_workspace import (
    STAND_IN_COMPUTER,
    get_home_stand_in_id,
    get_home_workspace_id,
    is_home_id,
    is_home_stand_in_id,
)
from src.server.services.ssh_computers import computer_unreachable

if TYPE_CHECKING:
    from src.server.services.turn_runtime import TurnRoute

logger = logging.getLogger(__name__)


async def home_turn_computer(
    user_id: str, *, conn=None
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """The computer a Home turn would run on now, and whether it is the
    stand-in's: that one while Home's computer can't be reached and the user
    has a cloud computer to stand on, else Home's (``get_home_computer``),
    None for a user with no computer. A read for a gate that judges a turn
    before it runs: nothing is placed, built or started."""
    computer = await get_home_computer(user_id, conn=conn)
    if await computer_unreachable(computer, conn=conn):
        cloud = await stand_in_computer(user_id, conn=conn)
        if cloud is not None:
            return cloud, True
    return computer, False


async def place_home_turn(
    user_id: str,
    *,
    make: bool = True,
    follows: Optional[str] = None,
) -> Optional[HomeAway]:
    """Where a Home turn runs, Home already bound (``turn_runtime.ensure_home``):
    the stand-in while Home's computer can't be reached and the user has a
    cloud computer, else None for Home's own computer, where a turn on a
    machine that can't be reached fails as a workspace turn does. ``make``
    places the stand-in row if it is not placed yet (cheap, and never builds
    or starts a computer); off, only one already placed is used. Nothing is
    marked here: a turn placed in the stand-in marks the user away in its
    START transaction (``home_away.mark_away``), and work between turns (a
    compaction, opening Home) writes nothing the hand-back would carry.

    ``follows`` is the workspace a background command ran in when the turn
    reports on it: one that ran in the stand-in keeps its turn there even once
    Home's computer is back, since its log and the files it wrote are there
    and reach Home only with the next hand-back.
    """
    home_computer = await get_home_computer(user_id)
    reachable = not await computer_unreachable(home_computer)
    if reachable and not is_home_stand_in_id(user_id, follows):
        return None
    place = place_home_stand_in if make and not reachable else read_stand_in
    stand_in = await place(user_id)
    # None stands, or the one standing keeps owed files on a computer that
    # takes no turn: the turn goes where Home is, as it was.
    if stand_in is None or not stand_in.runs or home_computer is None:
        return None
    away = HomeAway(
        workspace_id=get_home_stand_in_id(user_id),
        computer_id=str(home_computer["computer_id"]),
        computer_name=str(home_computer.get("name") or ""),
        cloud_computer_id=str(stand_in.computer["computer_id"]),
        cloud_name=str(stand_in.computer.get("name") or ""),
        at=datetime.now(timezone.utc).isoformat(),
        reason="command" if reachable else "unreachable",
    )
    logger.info(
        "Home work of user %s runs in its stand-in on computer %s (%s)",
        user_id,
        away.cloud_computer_id,
        away.reason,
    )
    return away


async def runs_in(user_id: Optional[str], workspace_id: str, *, make: bool) -> str:
    """The workspace whose computer and folder a turn in ``workspace_id``,
    or work between its turns, runs in: Home's stand-in for Home while
    ``place_home_turn`` puts it there, else ``workspace_id`` itself. ``make``
    as there: work that only attaches to a running computer places nothing."""
    if not user_id or not is_home_id(user_id, workspace_id):
        return workspace_id
    away = await place_home_turn(user_id, make=make)
    return away.workspace_id if away is not None else workspace_id


@dataclass(frozen=True, slots=True)
class HomeTurnStart:
    """What where a Home turn runs adds to its start, read once so the run
    states it alike in its context, its rows, its START and its stream."""

    #: The thread's workspace, which the stream names.
    workspace_id: str
    #: Set while the turn runs in Home's stand-in.
    away: Optional[HomeAway] = None
    #: On a Home turn back on Home's own computer, what came back from the
    #: stand-in (``home_back_notice``).
    back: Optional[Dict[str, Any]] = None
    #: A retry of a Home turn, which writes no query row of its own
    #: (``begin_run``), so its START corrects where the turn's row says it ran.
    retry: bool = False
    #: What START learned: the turn's placement time (``mark_away``), which
    #: its row and its stream then carry. Per request, written once by START.
    stamped: Dict[str, str] = field(default_factory=dict, compare=False)

    @classmethod
    async def read(
        cls, user_id: str, route: TurnRoute, *, retry: bool = False
    ) -> HomeTurnStart:
        home = route.role == "chief_of_staff"
        home_on_its_own = home and route.away is None
        return cls(
            workspace_id=route.workspace_id,
            away=route.away,
            back=await home_back_notice(user_id) if home_on_its_own and user_id else None,
            retry=retry and home,
        )

    def query_metadata(self) -> Dict[str, Any]:
        """Read back with the turn, so a reload still says where it ran."""
        return {"home_fallback": self.away.as_metadata()} if self.away else {}

    def run_metadata(self) -> Dict[str, Any]:
        """Where it runs, for the idle sweep to count it there."""
        return {STAND_IN_COMPUTER: self.away.cloud_computer_id} if self.away else {}

    @property
    def moved_off(self) -> Optional[str]:
        """Where it runs, which START holds a turn checked for a computer's
        token to (``admission.refuse_moved_off``); None for its thread's own."""
        return self.away.workspace_id if self.away else None

    def _placement(self) -> Optional[Dict[str, Any]]:
        return {**self.away.as_metadata(), **self.stamped} if self.away else None

    def frame(self) -> Optional[str]:
        """The stream's word on where the turn runs, said before the computer
        it runs on starts. Id 0: it comes before any cursor counts. A retry
        back on Home says so too, clearing what its first attempt showed."""
        if self.away is None and not self.retry:
            return None
        data = {"home_fallback": self._placement(), "workspace_id": self.workspace_id}
        return f"id: 0\nevent: turn_placement\ndata: {json.dumps(data)}\n\n"

    def in_start(
        self,
        user_id: str,
        run_id: str,
        then: Optional[Callable[[Any], Awaitable[None]]],
    ) -> Optional[Callable[[Any], Awaitable[None]]]:
        """What the START transaction also writes. A turn in the stand-in
        marks its user away there, so the mark commits with its run row and
        after Home's row lock, which the hand-back reads under, and stamps its
        row with that time: which hand-back carried its files is decided by
        it. A retry restates on its turn's query row where this attempt runs."""
        if self.away is None and not self.retry:
            return then

        async def run(conn: Any) -> None:
            from src.server.database.conversation.queries import (
                set_run_query_metadata,
            )
            from src.server.database.home_away import StandInGone, mark_away
            from src.server.database.workspace import WorkspaceMovingError

            if self.away is not None:
                try:
                    at = await mark_away(
                        user_id, computer_id=self.away.cloud_computer_id, conn=conn
                    )
                except StandInGone:
                    # Its folder is laid again by the next turn, so it is
                    # refused as a move is, which chat and automations both
                    # retry.
                    raise WorkspaceMovingError(self.away.workspace_id) from None
                self.stamped["at"] = at.isoformat()
            if self.away is not None or self.retry:
                # A retry back on Home stores a null rather than dropping the
                # key, so a reconnect can still clear its first attempt's badge.
                await set_run_query_metadata(
                    conn, run_id, "home_fallback", self._placement(), keep_null=True
                )
            if then is not None:
                await then(conn)

        return run


async def placement_frame(run_id: str) -> Optional[str]:
    """The ``turn_placement`` frame of a turn that ran in Home's stand-in, or
    of a retry of one back on Home, which clears it, for a client attaching
    after the live one went by: a send that dropped silently during setup is
    adopted with no frame of its own. No id, so the client's cursor stays
    where the run's own frames left it. Never fails the reconnect."""
    from src.server.database.conversation.queries import get_run_query_metadata

    try:
        stated, placement = await get_run_query_metadata(run_id, "home_fallback")
    except Exception:  # noqa: BLE001 - the badge is not worth the stream
        logger.warning("Could not read where run %s ran", run_id, exc_info=True)
        return None
    if not stated:
        return None
    return f"event: turn_placement\ndata: {json.dumps({'home_fallback': placement})}\n\n"


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
        last = row["last_handback"]
        if is_owed(row):
            notice["pending"] = True
        elif last is not None and last["folder"]:
            notice["folder"] = last["folder"]
            notice["files"] = last["files"]
        return notice
    except Exception:  # noqa: BLE001 - a line of context is not worth the turn
        logger.warning("Could not read where %s's Home ran", user_id, exc_info=True)
        return None


async def home_placement_view(user_id: str) -> Dict[str, Any]:
    """Where Home is and where its next turn would run, for the client.

    A read: nothing is placed or marked, so a page asking every few seconds
    never places a stand-in or starts a computer.
    """
    from src.server.database.home_away import (
        get_home_away,
        is_owed,
        left_on_stand_in,
    )
    from src.server.database.workspace_move import get_latest_workspace_move
    from src.server.services.workspace_move import move_from_row

    home_id = get_home_workspace_id(user_id)
    home_computer = await get_computer_for_workspace(home_id)
    cloud = await stand_in_computer(user_id)
    stand_in = await read_stand_in(user_id)
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
        # Kept after the notice is dismissed: removing the stand-in's computer
        # takes these files with it.
        "left_on_stand_in": left_on_stand_in(
            last, str(stand_in.computer["computer_id"]) if stand_in else None
        ),
        "move": move_from_row(await get_latest_workspace_move(home_id)),
    }
