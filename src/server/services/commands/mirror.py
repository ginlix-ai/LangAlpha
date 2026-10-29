"""Mirroring the computer a command ran on, when its thread's turns do not.

A command run on its thread's own computer reports to a turn there, and that
turn's end mirrors the computer. One the Chief of Staff ran on another of the
user's computers reports to Home, whose turns mirror Home: nothing of that
computer's own sweeps what the command wrote, and an always-on computer may
never stop. So the command's end does, as does the end of a turn whose Chief
of Staff reached the computer (``mirror_computer``). In the background, since
a watcher pass or a stop must not wait on a mirror, and one at a time per
computer, a burst of ends coalescing into one more pass.

Only a worker holding the computer's live session mirrors it
(``backup_computer_changes``), and the end lands on any worker. So the end
first owes the computer in Redis, where the debt outlives the worker, and the
command watcher's pass on every worker takes the owed computers it holds. A
mirror runs under a lease on its computer and pays the debt only once it
succeeds, and only if no end owed the computer again meanwhile. One that fails
keeps the lease to its end, which spaces the retries, and a worker lost
mid-mirror leaves its lease to expire. A debt nobody pays within the hour of
its last end, or a lost set, leaves the changes to the computer's next sweep
or its stop.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Mapping, Optional

from src.server.database.computer import get_computer
from src.server.database.workspace import get_live_workspace_ids_for_computer
from src.server.models.computer import ComputerStatus

logger = logging.getLogger(__name__)

_LOG = "[COMMANDS]"

#: The computers owed a mirror, each scored by when an end last owed it.
_OWED_KEY = "commands:mirror:owed"
#: The lease a mirror holds on its computer, by computer id.
_LEASE_KEY = "commands:mirror:lease:"
#: How long a debt waits for a worker holding the computer's session.
_OWED_FOR_S = 3600
#: How long a mirror holds its computer before another may take it, and so
#: how long a failed one waits to be tried again.
_LEASE_S = 300

# Takes the lease and reads the debt it covers: nil when another mirror holds
# the computer, else the debt's score as Redis spells it, or '' for none.
_TAKE = """
if not redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', ARGV[3]) then return false end
return redis.call('ZSCORE', KEYS[1], ARGV[1]) or ''
"""

# Pays the debt a mirror covered, unless an end owed the computer again since
# it was taken, and gives up the lease if it is still this mirror's.
_PAY = """
local owed = redis.call('ZSCORE', KEYS[1], ARGV[1])
if owed and owed == ARGV[2] then redis.call('ZREM', KEYS[1], ARGV[1]) end
if redis.call('GET', KEYS[2]) == ARGV[3] then redis.call('DEL', KEYS[2]) end
return 0
"""

_mirrors: dict[str, asyncio.Task] = {}
_again: set[str] = set()


async def mirror_after_end(ended: Mapping[str, Any]) -> None:
    """Mirror the computer the ``ended`` command ran on, unless its thread's
    workspace is there. Never raises, since the end is already recorded, and
    an end not yet placed mirrors anyway: it does not come round again, and an
    extra pass costs one sweep."""
    computer_id = str(ended["computer_id"])
    try:
        here = {str(w) for w in await get_live_workspace_ids_for_computer(computer_id)}
    except Exception as e:  # noqa: BLE001
        logger.warning(
            f"{_LOG} could not tell whether {ended['command_id']} ran on its "
            f"thread's computer ({type(e).__name__}: {e}); mirroring {computer_id} anyway"
        )
        here = set()
    if str(ended["workspace_id"]) not in here:
        await mirror_computer(computer_id)


async def mirror_computer(computer_id: str) -> None:
    """Owe ``computer_id`` a mirror and start one on this worker. Never raises."""
    redis = _redis()
    if redis is not None:
        try:
            await redis.zadd(_OWED_KEY, {computer_id: time.time()})
            await redis.expire(_OWED_KEY, _OWED_FOR_S)
        except Exception as e:  # noqa: BLE001 - mirrored here if this worker holds it
            logger.warning(f"{_LOG} mirror of {computer_id} not owed ({type(e).__name__}: {e})")
    _schedule(computer_id)


async def take_owed() -> None:
    """Mirror the owed computers whose live session this worker holds; from
    the command watcher's pass. Never raises: the next pass reads them again."""
    from src.server.services.workspace_manager import WorkspaceManager

    manager = WorkspaceManager.current()
    redis = _redis()
    if manager is None or redis is None:
        return
    try:
        await redis.zremrangebyscore(_OWED_KEY, "-inf", time.time() - _OWED_FOR_S)
        for member in await redis.zrange(_OWED_KEY, 0, -1):
            computer_id = member.decode() if isinstance(member, bytes) else str(member)
            computer = await get_computer(computer_id)
            if (
                computer is not None
                and computer.get("status") == ComputerStatus.RUNNING
                and manager.cached_session_for_computer(computer) is not None
            ):
                _schedule(computer_id)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"{_LOG} owed mirrors not read ({type(e).__name__}: {e})")


def _redis() -> Optional[Any]:
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    return cache.client if cache.enabled and cache.client else None


def _schedule(computer_id: str) -> None:
    running = _mirrors.get(computer_id)
    if running is not None and not running.done():
        _again.add(computer_id)
        return

    async def _run() -> None:
        try:
            while True:
                _again.discard(computer_id)
                if not await _mirror(computer_id) or computer_id not in _again:
                    break
        finally:
            _mirrors.pop(computer_id, None)

    _mirrors[computer_id] = asyncio.create_task(_run())


async def _mirror(computer_id: str) -> bool:
    """One mirror of ``computer_id`` under its lease; whether it paid."""
    from src.server.services.workspace_manager import WorkspaceManager

    manager = WorkspaceManager.current()
    if manager is None:
        return False
    redis = _redis()
    keys = (_OWED_KEY, f"{_LEASE_KEY}{computer_id}")
    token = uuid.uuid4().hex
    owed: Optional[str] = None
    if redis is not None:
        try:
            taken = await redis.eval(_TAKE, 2, *keys, computer_id, token, _LEASE_S)
        except Exception as e:  # noqa: BLE001 - the mirror still runs, unleased
            logger.warning(f"{_LOG} lease on {computer_id} not taken ({type(e).__name__}: {e})")
            redis, taken = None, b""
        if taken is None:
            # Another worker is mirroring it, or a failed mirror's lease runs on.
            return False
        owed = taken.decode() if isinstance(taken, bytes) else str(taken)
    try:
        paid = await manager.backup_computer_changes(computer_id)
    except Exception as e:  # noqa: BLE001 - owed still, tried again once the lease ends
        logger.warning(f"{_LOG} mirror of computer {computer_id} failed ({type(e).__name__}: {e})")
        return False
    if redis is not None:
        # Unpaid, the lease goes now: the worker holding the session may take it.
        try:
            await redis.eval(_PAY, 2, *keys, computer_id, owed if paid else "", token)
        except Exception as e:  # noqa: BLE001 - paid again on the next take
            logger.warning(f"{_LOG} mirror of {computer_id} not paid ({type(e).__name__}: {e})")
    return paid
