"""Opening and closing a computer's run at its status boundaries, and the
status event that says why it started or stopped (``events.py``).

``note_computer_status`` is called by every write that moves a computer's
status (``database/computer.py``: ``create_computer``,
``update_computer_status``, ``try_claim_computer_for_start``,
``try_bind_computer_provider_ref``), and ``note_mirrored_status`` by the
workspace write mirrored to it (``database/workspace.py``:
``update_workspace_status``), after the write, on the same connection when
the write was given one. The ledger is telemetry, so this never raises and
never waits long: a failure is logged and left to the sweep's reconcile.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Mapping, Optional

from src.server.database import computer_runs as runs_db
from src.server.database.computer_runs import hook_cursor
from src.server.services.computer_usage.causes import current_cause
from src.server.services.computer_usage.events import note_status

logger = logging.getLogger(__name__)

# What a hook may cost a status write on its own connection, above the
# statement and lock timeouts ``hook_cursor`` sets: it also covers waiting
# for a pool slot, which no statement timeout does.
_HOOK_TIMEOUT_S = 3.0

# Why a run ends, by the status that ends it. 'starting' is not here: the
# old sandbox still runs through a replacement claim (``RUN_LIVE_STATUSES``),
# and the run ends as 'replaced' when the new one binds.
_CLOSING_REASONS = {
    "stopped": "stopped",
    "error": "error",
    "deleted": "deleted",
    "creating": "replaced",
}

# Kinds whose stop is a pause: an E2B sandbox keeps its memory and processes.
PAUSING_KINDS = ("e2b",)


def metered_kinds() -> tuple[str, ...]:
    from ptc_agent.core.sandbox.providers import METERED_KINDS

    return METERED_KINDS


def metric_kinds() -> tuple[str, ...]:
    from ptc_agent.core.sandbox.providers import METRIC_KINDS

    return METRIC_KINDS


def allocation_kinds() -> tuple[str, ...]:
    from ptc_agent.core.sandbox.providers import ALLOCATION_KINDS

    return ALLOCATION_KINDS


def resolve_spec(
    computer: Mapping[str, Any],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """The cpu, memory GB and disk GB the computer's tier names, as configured now.

    Only a stand-in for a run's size until the sweep asks the provider: a
    sandbox keeps the size it was created at, which a preset changed since
    no longer names. Read from this process's manager, whose settings carry
    the computer's own provider overrides; (None, None, None) without one,
    or for a tier no longer configured.
    """
    from ptc_agent.core.sandbox.providers._tiers import resolve_tier
    from src.server.services.computer_manager import ComputerManager

    manager = ComputerManager.current()
    if manager is None:
        return None, None, None
    try:
        settings = manager._provider_settings(
            computer["kind"], computer.get("provider_config") or None
        )
        tiers = getattr(settings, "resource_tiers", None)
        if not tiers:
            return None, None, None
        choice = resolve_tier(
            computer.get("resource_tier"),
            default_tier=getattr(settings, "default_tier", "standard"),
            resource_tiers=tiers,
        )
    except Exception as e:
        logger.debug(f"No tier spec for computer {computer.get('computer_id')}: {e}")
        return None, None, None
    if choice.preset is None:
        return None, None, None
    preset = choice.preset
    return float(preset.cpu), float(preset.memory), float(preset.disk)


async def _open(cur, computer_id: str) -> None:
    computer = await runs_db.get_computer_for_run(cur, computer_id)
    if computer is None or computer["status"] != "running":
        return
    metered = metered_kinds()
    if computer["kind"] not in metered:
        return
    cpu, memory_gb, disk_gb = resolve_spec(computer)
    cause = current_cause()
    await runs_db.open_computer_run(
        cur,
        computer_id,
        metered_kinds=metered,
        cpu=cpu,
        memory_gb=memory_gb,
        disk_gb=disk_gb,
        metrics_final=computer["kind"] not in metric_kinds(),
        provider_sized=computer["kind"] in allocation_kinds(),
        start_cause=cause.name if cause else None,
        start_client=cause.client if cause else None,
    )


async def _apply(
    computer_id: str, status: str, *, conn, reason: Optional[str]
) -> None:
    if status == "running":
        async with hook_cursor(conn) as cur:
            await _open(cur, computer_id)
    elif status == "stopping":
        if reason:
            async with hook_cursor(conn) as cur:
                await runs_db.stamp_run_end_reason(cur, computer_id, reason)
    elif status in _CLOSING_REASONS:
        async with hook_cursor(conn) as cur:
            await runs_db.close_computer_run(
                cur,
                computer_id,
                reason=reason or _CLOSING_REASONS[status],
                pausing_kinds=PAUSING_KINDS,
            )


async def _quietly(what: str, work: Awaitable[Any], *, conn=None) -> Any:
    """Await a telemetry write the caller must never wait on long or see fail.

    Bounded only on a connection of its own: one the caller holds is already
    checked out, and a cancellation mid-statement would abort the caller's
    transaction. A cancellation of the caller still reaches it.
    """
    try:
        return await asyncio.wait_for(
            work, timeout=_HOOK_TIMEOUT_S if conn is None else None
        )
    except Exception as e:
        logger.warning(f"{what}: {type(e).__name__}: {e}")
        return None


async def _replaced(computer_id: str, sandbox_id: str) -> None:
    async with hook_cursor() as cur:
        await runs_db.close_replaced_run(cur, computer_id, sandbox_id)


async def note_sandbox_replaced(computer_id: str, sandbox_id: str) -> None:
    """Record that a replacement deleted *sandbox_id*; never raises.

    Called once the provider confirms the delete, so the old sandbox's run
    ends there rather than when the new one binds after its restore.
    """
    await _quietly(
        f"Usage ledger not updated for computer {computer_id} -> replaced",
        _replaced(computer_id, sandbox_id),
    )


async def note_computer_status(
    computer: Optional[Mapping[str, Any]],
    status: str,
    *,
    conn=None,
    reason: Optional[str] = None,
) -> None:
    """Record that *computer*, the row a computer write returned, is now *status*.

    *reason* is why a stop is happening, stamped on the open run when the
    computer goes to 'stopping' and kept when the stop closes it, and the
    stop's recorded cause.
    """
    computer_id = computer.get("computer_id") if computer else None
    if not computer_id:
        return
    computer_id = str(computer_id)
    await _quietly(
        f"Status not recorded for computer {computer_id} -> {status}",
        _record_status(computer_id, status, conn=conn, reason=reason, written=computer),
        conn=conn,
    )


async def note_mirrored_status(computer_id: str, status: str, *, conn=None) -> None:
    """Record a workspace status write mirrored to its computer.

    The mirror may not have reached the machine (it guards on the computer's
    own status), so the computer is read back and nothing is recorded unless
    it is at *status*.
    """
    await _quietly(
        f"Status not recorded for computer {computer_id} -> {status}",
        _record_status(computer_id, status, conn=conn, reason=None, written=None),
        conn=conn,
    )


async def _record_status(
    computer_id: str,
    status: str,
    *,
    conn,
    reason: Optional[str],
    written: Optional[Mapping[str, Any]],
) -> None:
    """Both records of a status write, under the one bound ``_quietly`` gives.

    The run ledger goes first and on its own: an event that cannot be
    written must not cost the ledger its write. *written* is the computer as
    its own write returned it; without it, it is read back.
    """
    try:
        await _apply(computer_id, status, conn=conn, reason=reason)
    except Exception as e:
        logger.warning(
            f"Usage ledger not updated for computer {computer_id} -> {status}: "
            f"{type(e).__name__}: {e}"
        )
    computer = written
    if computer is None:
        async with hook_cursor(conn) as cur:
            computer = await runs_db.get_computer_for_run(cur, computer_id)
        if computer is None or computer["status"] != status:
            return
    note_status(computer, status, reason=reason)
