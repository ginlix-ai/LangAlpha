"""Whole-set grant sync that survives being superseded mid-flight."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from src.server.database.egress_grants import GrantRef, sync_egress_grants
from src.server.services.mcp_config import resolve_mcp_config

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3


class GrantSyncSuperseded(Exception):
    """A revocation could not be written because newer configs kept winning."""



async def sync_grants_until_current(
    base_config: Any,
    *,
    user_id: str,
    workspace_id: str,
    refs: Callable[[Any], Awaitable[Sequence[GrantRef]]],
) -> bool:
    """Retire this workspace's out-of-scope grants, re-resolving if superseded.

    ``sync_egress_grants`` touches no row and returns None when the config
    version moved under it, on the reading that a newer sync owns the set. That
    holds only for a bumper that then syncs the whole set itself, and not every
    bumper does: a binding PATCH bumps every workspace of the user and rewrites
    policy on the grants that exist, without ever resolving scope or revoking
    one. A revocation losing that race would be dropped, and both requests would
    answer 200 while the grant stayed active for an in-flight turn.

    Re-resolving is what makes the retry correct rather than hopeful: the winner
    has committed by the time this loses, so the next resolve carries both the
    winner's version and the change this call is here to apply. The resolve
    reads the database and dials nobody, and the bumpers competing for one
    workspace are finite, so a small attempt cap is enough.

    Returns whether the grants were actually written.
    """
    for attempt in range(_MAX_ATTEMPTS):
        resolved = await resolve_mcp_config(base_config, user_id, workspace_id)
        synced = await sync_egress_grants(
            user_id=user_id,
            workspace_id=workspace_id,
            refs=await refs(resolved),
            config_version=resolved.version,
        )
        if synced is not None:
            return True
        logger.info(
            "[EGRESS] grant sync superseded, re-resolving workspace=%s attempt=%d",
            workspace_id,
            attempt + 1,
        )
    logger.warning(
        "[EGRESS] grant sync still superseded after %d attempts workspace=%s; "
        "an out-of-scope grant may stay active until the next acquire",
        _MAX_ATTEMPTS,
        workspace_id,
    )
    return False
