"""What Home's turns wrote in its stand-in, copied into Home once Home's computer is back.

While the machine Home lives on can't be reached, Home's turns run in its
stand-in on a cloud computer (``home_placement``), and the files they write
stay there. Once Home's computer answers again and nothing of Home's is
running, one worker copies every file there that differs from what earlier
hand-backs carried into a folder of their own in Home, ``<folder>/<date>[ n]/``
named by the account's language: never over a file already in Home and never
merged into one (``home_handback_transfer``). The stand-in's copies stay where
they are. The user's notice reads ``home_away.last_handback``.

A failed hand-back waits, longer each time, and gives up after
``MAX_ATTEMPTS`` in a row: the files are then no longer owed, so their
computer can be removed, and the notice says they are still on the cloud.

Every worker sweeps (``HomeHandbackSweeper``) and reads only Postgres to
decide; an advisory lock per user, held for the whole copy, keeps two from
handing back at once, and the watermark moves with a guarded update.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, cast

from src.server.database.computer import get_computer_for_workspace
from src.server.database.home_away import HandbackNotice
from src.server.database.home_workspace import (
    get_home_stand_in_id,
    get_home_workspace_id,
)
from src.server.database.sql_fences import advisory_key
from src.server.services.commands.mirror import mirror_computer
from src.server.services.ssh_computers import computer_unreachable

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 30.0
STOP_GRACE = 10.0

# Failures in a row before a hand-back gives up; the delays between them
# double from a minute to an hour, so this is about four hours of trying.
MAX_ATTEMPTS = 10
# How long a hand-back that found Home busy steps aside for the others.
BUSY_DEFER_S = 60.0

# The folder in Home the stand-in's files arrive in, by the account's language.
HANDBACK_FOLDER_EN = "From the cloud"
HANDBACK_FOLDER_ZH = "来自云端"


def handback_key(user_id: str) -> int:
    return advisory_key("home_handback", user_id)


def handback_folder(locale: Optional[str]) -> str:
    """The folder's name for an account in ``locale``; English unless Chinese."""
    return (
        HANDBACK_FOLDER_ZH
        if (locale or "").lower().startswith("zh")
        else HANDBACK_FOLDER_EN
    )


async def _home_busy(user_id: str) -> bool:
    """A command still running in the stand-in, or Home moving: what the
    first writes would land after this hand-back's mark, and the second would
    move the folder the copy lands in. A live run of Home's is read under
    Home's row lock (``begin_handback``)."""
    from src.server.database.runs import command_runs
    from src.server.database.workspace import workspace_is_moving

    if await workspace_is_moving(get_home_workspace_id(user_id)):
        return True
    return bool(
        await command_runs.list_running(workspace_id=get_home_stand_in_id(user_id))
    )


async def _user_locale(user_id: str) -> Optional[str]:
    try:
        from src.server.database.user import get_user

        return (await get_user(user_id) or {}).get("locale")
    except Exception:  # noqa: BLE001 - the folder's name falls back to English
        return None


async def _names(user_id: str) -> Dict[str, str]:
    home_computer = await get_computer_for_workspace(get_home_workspace_id(user_id))
    cloud_computer = await get_computer_for_workspace(get_home_stand_in_id(user_id))
    return {
        "computer_name": str((home_computer or {}).get("name") or ""),
        "cloud_name": str((cloud_computer or {}).get("name") or ""),
        "cloud_computer_id": str((cloud_computer or {}).get("computer_id") or ""),
    }


def _notice(**fields: Any) -> HandbackNotice:
    return cast(
        HandbackNotice,
        {
            "handback_id": str(uuid.uuid4()),
            "folder": None,
            "files": 0,
            "left_on_cloud": [],
            "unreadable": [],
            **fields,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )


async def _record(
    user_id: str, moved: Any, last: Optional[HandbackNotice]
) -> Optional[HandbackNotice]:
    """The user's notice of a hand-back: what arrived and where, and what
    stays on the cloud, too large or unreadable. None when nothing arrived
    and what stays is what the last notice already named."""
    from src.server.services.home_handback_transfer import is_note

    left, unreadable = sorted(moved.too_large), sorted(moved.unreadable)
    if moved.folder is None and not left and not unreadable:
        return None
    names = await _names(user_id)
    if (
        moved.folder is None
        and last is not None
        and last["left_on_cloud"] == left
        and last["unreadable"] == unreadable
        and last.get("cloud_computer_id") == names["cloud_computer_id"]
    ):
        return None
    return _notice(
        folder=moved.folder,
        # The threads' notes go back where they were, not into the folder.
        files=sum(not is_note(path) for path in moved.carried),
        left_on_cloud=left,
        unreadable=unreadable,
        **names,
    )


async def hand_back_owed(user_id: str) -> bool:
    """One user's hand-back, when Home's computer answers and nothing of
    Home's runs. True when this worker finished it."""
    from src.server.database.home_away import (
        begin_handback,
        defer_handback,
        fail_handback,
        finish_handback,
        handed_back,
        is_owed,
    )
    from src.server.database.session_lock import try_own_session_lock
    from src.server.services.home_handback_transfer import move_into_home

    home_computer = await get_computer_for_workspace(get_home_workspace_id(user_id))
    if home_computer is not None and await computer_unreachable(home_computer):
        return False
    async with try_own_session_lock(handback_key(user_id)) as held:
        if not held:
            return False
        # Read under the lock, after Home's live runs: another worker may have
        # finished it, and a turn admitted from here on owes another.
        begun = await begin_handback(user_id)
        if begun is None or await _home_busy(user_id):
            await defer_handback(user_id, BUSY_DEFER_S)
            return False
        row, through = begun
        if not is_owed(row):
            return False
        previous = row.get("handed_back_through")
        if await get_computer_for_workspace(get_home_stand_in_id(user_id)) is None:
            # Nowhere left to read from: the stand-in's computer is gone. Its
            # removal is refused while owed, so this is a retirement's
            # leftover; the watermark moves so the user is not owed it
            # forever. Read after the watermark: a turn that placed the
            # stand-in again since marks the user away past it, still owed.
            await finish_handback(
                user_id, previous=previous, through=through, record=None
            )
            return False
        try:
            if home_computer is None:
                raise RuntimeError("Home is on no computer")
            moved = await move_into_home(
                user_id,
                await handed_back(user_id),
                base=handback_folder(await _user_locale(user_id)),
                day=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            )
        except Exception:
            gave_up = await fail_handback(
                user_id,
                max_attempts=MAX_ATTEMPTS,
                given_up=_notice(failed=True, **await _names(user_id)),
            )
            logger.warning(
                "Handing back user %s's Home failed; %s",
                user_id,
                "given up, the files stay on the cloud" if gave_up else "it stays owed",
                exc_info=True,
            )
            return False
        record = await _record(user_id, moved, row["last_handback"])
        finished = await finish_handback(
            user_id,
            previous=previous,
            through=through,
            record=record,
            carried=moved.carried,
            kept=moved.kept,
            done=not moved.deferred,
        )
        # Recorded first: a cleanup that fails never repeats the copy.
        try:
            await moved.clean_up()
        finally:
            if finished and moved.carried and moved.computer_id:
                # No turn of Home's backs up what landed, and a Home moved
                # from its backup before one runs would come back without it.
                # The computer the files went to, which a move since the
                # first read may have changed.
                await mirror_computer(moved.computer_id)
        logger.info(
            "Handed %d file(s) of user %s's Home back from its stand-in into %s"
            " (%d too large, %d unreadable, %d next time)",
            len(moved.carried),
            user_id,
            moved.folder,
            len(moved.too_large),
            len(moved.unreadable),
            len(moved.deferred),
        )
        return finished


async def sweep_once() -> int:
    from src.server.database.home_away import list_owed

    done = 0
    for row in await list_owed():
        try:
            if await hand_back_owed(str(row["user_id"])):
                done += 1
        except Exception:
            logger.warning(
                "Home hand-back for %s failed", row.get("user_id"), exc_info=True
            )
    return done


class HomeHandbackSweeper:
    """Per worker: hands back Home's stand-in files once Home's computer answers.

    Process memory holds only the loop; what is owed and who holds it are in
    Postgres (``home_away``, the per-user advisory lock).
    """

    _instance: Optional["HomeHandbackSweeper"] = None

    def __init__(self, *, interval: float = SWEEP_INTERVAL_S) -> None:
        self._interval = interval
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "HomeHandbackSweeper":
        if cls._instance is None:
            cls._instance = HomeHandbackSweeper()
        return cls._instance

    def start(self) -> None:
        if self._loop_task is not None and not self._loop_task.done():
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(
            self._loop(), name="home-handback-sweeper"
        )

    async def stop(self) -> None:
        if self._loop_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._loop_task, timeout=STOP_GRACE)
        except (TimeoutError, asyncio.TimeoutError):
            self._loop_task.cancel()
        except Exception:
            logger.warning(
                "[HomeHandback] loop ended with an error at shutdown", exc_info=True
            )
        self._loop_task = None

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            jitter = self._interval * (0.8 + 0.4 * random.random())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=jitter)
                return
            except (TimeoutError, asyncio.TimeoutError):
                pass
            try:
                await sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("[HomeHandback] sweep failed", exc_info=True)
