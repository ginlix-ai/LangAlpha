"""Opening and closing a computer's run at its status boundaries.

``note_computer_status`` is called by every write that moves a computer's
status (``database/computer.py``: ``create_computer``,
``update_computer_status``, ``try_claim_computer_for_start``,
``try_bind_computer_provider_ref``; ``database/workspace.py``: the mirrored
``update_workspace_status``), after the write, on the same connection when
the write was given one. The ledger is telemetry, so this never raises and
never waits long: a failure is logged and left to the sweep's reconcile.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Mapping, Optional

from src.server.database import computer_runs as runs_db
from src.server.database.computer_runs import hook_cursor

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
    from ptc_agent.core.sandbox.providers import PROVIDER_KINDS, provider_class

    return tuple(k for k in PROVIDER_KINDS if provider_class(k).metered)


def metric_kinds() -> tuple[str, ...]:
    from ptc_agent.core.sandbox.providers import PROVIDER_KINDS, provider_class

    return tuple(
        k
        for k in PROVIDER_KINDS
        if provider_class(k).metered and provider_class(k).reports_metrics
    )


def resolve_spec(
    computer: Mapping[str, Any],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """The cpu, memory GB and disk GB the computer's tier names, as configured now.

    Read from this process's manager, whose settings carry the computer's own
    provider overrides; (None, None, None) without one, or for a tier no
    longer configured.
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
    await runs_db.open_computer_run(
        cur,
        computer_id,
        metered_kinds=metered,
        cpu=cpu,
        memory_gb=memory_gb,
        disk_gb=disk_gb,
        metrics_final=computer["kind"] not in metric_kinds(),
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


async def note_sandbox_replaced(computer_id: str, sandbox_id: str) -> None:
    """Record that a replacement deleted *sandbox_id*; never raises.

    Called once the provider confirms the delete, so the old sandbox's run
    ends there rather than when the new one binds after its restore.
    """

    async def close() -> None:
        async with hook_cursor() as cur:
            await runs_db.close_replaced_run(cur, computer_id, sandbox_id)

    try:
        await asyncio.wait_for(close(), timeout=_HOOK_TIMEOUT_S)
    except Exception as e:
        logger.warning(
            f"Usage ledger not updated for computer {computer_id} -> replaced: "
            f"{type(e).__name__}: {e}"
        )


async def note_computer_status(
    row: Optional[Mapping[str, Any]],
    status: str,
    *,
    conn=None,
    reason: Optional[str] = None,
) -> None:
    """Record that the computer in *row* was written to *status*.

    *row* is whatever the write returned, a computer or a workspace row, and
    only its ``computer_id`` is read. *reason* is why a stop is happening,
    stamped on the open run when the row goes to 'stopping' and kept when
    the stop closes it.
    """
    if row is None:
        return
    computer_id = row.get("computer_id")
    if not computer_id:
        return
    computer_id = str(computer_id)
    try:
        # A connection the caller holds is already checked out, and a
        # cancellation mid-statement would abort the caller's transaction.
        await asyncio.wait_for(
            _apply(computer_id, status, conn=conn, reason=reason),
            timeout=_HOOK_TIMEOUT_S if conn is None else None,
        )
    except Exception as e:
        logger.warning(
            f"Usage ledger not updated for computer {computer_id} -> {status}: "
            f"{type(e).__name__}: {e}"
        )
