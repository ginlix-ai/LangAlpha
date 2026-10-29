"""The spec change record on a computer row: one claim, one heartbeat, one settle.

``computers.spec_change`` holds a :class:`ComputerSpecChange`. The claim, the
heartbeat and the settle are each a single conditional UPDATE, so the row
itself is the mutex between workers: a claim wins only over no change, a
finished one, or one gone stale, and a heartbeat or a settle lands only on
the claim that ran it.
"""

from typing import Any, Dict, Optional
from uuid import uuid4

from psycopg.types.json import Json

from src.server.database.computer import _computer_cursor
from src.server.database.sql_fences import (
    COMPUTER_COLS,
    shadowed_write,
    spec_change_stale,
)


# The keys the SQL below reads. The rest of the record is written from the
# dicts built here; a unit test holds both to ComputerSpecChange's fields.
_OWNED_BY_CLAIM = "spec_change->>'claim_id' = %(claim_id)s"
_IN_PROGRESS = "spec_change->>'state' = 'in_progress'"


# A claim that takes over a stale record is the settle its dead worker never
# ran, so it carries the same tier revert: back to where that change started,
# while the row still reads its target. Without it the row says target over a
# machine still at the old size, and a retry to that tier is a same-tier
# no-op that never rebuilds. The row cannot tell whether the dead worker got
# as far as the recreate, so the claim also records ``took_over`` and the run
# rebuilds even when the reverted tier matches the request: a retry back to
# the old tier over a machine left at the target size must not be a no-op.
_REVERT_TIER = (
    "CASE WHEN c.resource_tier = c.spec_change->>'target_tier'"
    " THEN COALESCE(c.spec_change->>'from_tier', c.resource_tier)"
    " ELSE c.resource_tier END"
)
_TAKEOVER_TIER = (
    f"CASE WHEN {spec_change_stale('c.')} THEN {_REVERT_TIER}"
    " ELSE c.resource_tier END"
)


def claim_record(target_tier: str, claim_id: str) -> Dict[str, Any]:
    """The claim's fields that Python decides; the row adds from_tier and the clocks."""
    return {
        "target_tier": target_tier,
        "state": "in_progress",
        "error": None,
        "finished_at": None,
        "claim_id": claim_id,
    }


def outcome_record(state: str, error: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The settle's fields that Python decides; the row adds finished_at."""
    return {"state": state, "error": error}


async def claim_computer_spec_change(
    computer_id: str,
    *,
    target_tier: str,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Record a spec change as in progress unless a fresh one already is.

    ``from_tier`` is read off the row inside the UPDATE, so it is the tier the
    claim actually displaced. A stale claim (its worker died without settling)
    is taken over with its tier reverted in the same statement, workspace
    shadows included, so ``from_tier`` names the reverted tier, and marked
    ``took_over`` so the run rebuilds whatever the tiers say. Returns None when a fresh change holds the row or
    the computer is gone; the claim's id is in ``spec_change.claim_id``.
    """
    record = claim_record(target_tier, str(uuid4()))
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            shadowed_write(
                authority="computer",
                computer_set=(
                    f"resource_tier = {_TAKEOVER_TIER},\n"
                    "                    spec_change = %(record)s::jsonb"
                    " || jsonb_build_object("
                    f"'from_tier', COALESCE({_TAKEOVER_TIER}, 'standard'),"
                    " 'started_at', to_jsonb(NOW()),"
                    " 'heartbeat_at', to_jsonb(NOW()),"
                    f" 'took_over', to_jsonb({spec_change_stale('c.')}))"
                ),
                workspace_set="resource_tier = comp.resource_tier",
                computer_guard=(
                    "\n                  AND (c.spec_change IS NULL"
                    "\n                       OR c.spec_change->>'state'"
                    " IS DISTINCT FROM 'in_progress'"
                    f"\n                       OR {spec_change_stale('c.')})"
                ),
                computer_returning=COMPUTER_COLS,
                workspace_returning="w.workspace_id",
                select=f"SELECT {COMPUTER_COLS} FROM comp",
            ),
            {"record": Json(record), "computer_id": computer_id},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def heartbeat_computer_spec_change(
    computer_id: str,
    *,
    claim_id: str,
    conn=None,
) -> bool:
    """Stamp the claim's heartbeat; False once the claim no longer holds the row.

    The one fenced write a runner makes to learn it still owns the change: a
    takeover replaced the claim id and a settle ended the record, and either
    leaves nothing here to match. ``updated_at`` is left alone, as for a disk
    reading: the change observing itself is not an edit of the row.
    """
    async with _computer_cursor(conn) as cur:
        await cur.execute(
            f"""
            UPDATE computers
            SET spec_change = spec_change
                || jsonb_build_object('heartbeat_at', to_jsonb(NOW()))
            WHERE computer_id = %(computer_id)s
              AND {_IN_PROGRESS}
              AND {_OWNED_BY_CLAIM}
            """,
            {"computer_id": computer_id, "claim_id": claim_id},
        )
        return cur.rowcount > 0


async def settle_computer_spec_change(
    computer_id: str,
    *,
    claim_id: str,
    error: Optional[Dict[str, Any]] = None,
    conn=None,
) -> Optional[Dict[str, Any]]:
    """Write the outcome of the claim ``claim_id``; a failure also reverts the tier.

    One statement, so no reader sees a failed outcome beside the tier the
    change never reached. The revert is safe because the claim excludes a
    concurrent change, and it only applies while the row still reads the
    target tier: a failure before the change persisted anything leaves the
    tier alone, whoever else wrote it. Workspace shadows follow the tier, as
    every tier write keeps them. A settle for a claim that was taken over
    matches nothing and returns None.
    """
    params = {
        "computer_id": computer_id,
        "claim_id": claim_id,
        "outcome": Json(outcome_record("failed" if error else "succeeded", error)),
    }
    new_record = (
        "spec_change = c.spec_change || %(outcome)s::jsonb"
        " || jsonb_build_object('finished_at', to_jsonb(NOW()))"
    )
    async with _computer_cursor(conn) as cur:
        if error is None:
            await cur.execute(
                f"""
                UPDATE computers c
                SET {new_record}
                WHERE c.computer_id = %(computer_id)s
                  AND c.{_IN_PROGRESS}
                  AND c.{_OWNED_BY_CLAIM}
                RETURNING {COMPUTER_COLS}
                """,
                params,
            )
        else:
            await cur.execute(
                shadowed_write(
                    authority="computer",
                    computer_set=(
                        f"{new_record},\n"
                        f"                    resource_tier = {_REVERT_TIER}"
                    ),
                    workspace_set="resource_tier = comp.resource_tier",
                    computer_fence="",
                    computer_guard=(
                        f"\n                  AND c.{_IN_PROGRESS}"
                        f"\n                  AND c.{_OWNED_BY_CLAIM}"
                    ),
                    computer_returning=COMPUTER_COLS,
                    workspace_returning="w.workspace_id",
                    select=(
                        f"SELECT {COMPUTER_COLS},"
                        " (SELECT count(*) FROM shadow) AS shadowed_workspaces"
                        " FROM comp"
                    ),
                ),
                params,
            )
        row = await cur.fetchone()
    return dict(row) if row else None
