"""The user's trading permission: read it, and change it.

Its own endpoint rather than a preference key: preferences are written by the
agent too (its profile files and its preference tool), and a level that lets
orders skip approval must only ever come from the user accepting the agreement.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.server.database.trading_permission import (
    get_trading_permission_row,
    set_trading_permission,
)
from src.server.services import activity
from src.server.services.activity import Action
from src.server.services.trading_permission import (
    DEFAULT_TRADING_PERMISSION,
    TRADING_AGREEMENT_VERSION,
    TradingPermission,
    effective_permission,
)
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Users"])


class TradingPermissionResponse(BaseModel):
    level: TradingPermission
    #: The agreement a client must send back to choose a level that needs one.
    agreement_version: int = TRADING_AGREEMENT_VERSION
    agreed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TradingPermissionUpdate(BaseModel):
    level: TradingPermission
    #: Required, and must be the current version, for a level that skips
    #: approval; ignored for one that asks.
    agreement_version: Optional[int] = None


def _response(row: dict | None) -> TradingPermissionResponse:
    if row is None:
        return TradingPermissionResponse(level=DEFAULT_TRADING_PERMISSION)
    level = effective_permission(row["level"], row["agreement_version"])
    return TradingPermissionResponse(
        level=level,
        # A stale acceptance granted nothing, so it is not shown as agreed.
        agreed_at=row["agreed_at"] if level.needs_agreement else None,
        updated_at=row["updated_at"],
    )


@router.get(
    "/users/me/trading-permission", response_model=TradingPermissionResponse
)
@handle_api_exceptions("get trading permission", logger)
async def get_trading_permission(user_id: CurrentUserId):
    return _response(await get_trading_permission_row(user_id))


@router.put(
    "/users/me/trading-permission", response_model=TradingPermissionResponse
)
@handle_api_exceptions("update trading permission", logger)
async def update_trading_permission(
    body: TradingPermissionUpdate, user_id: CurrentUserId
):
    if (
        body.level.needs_agreement
        and body.agreement_version != TRADING_AGREEMENT_VERSION
    ):
        raise HTTPException(
            status_code=422,
            detail=(
                "this level lets orders skip your approval, so it needs the "
                f"current agreement (version {TRADING_AGREEMENT_VERSION}) accepted"
            ),
        )
    await _refuse_raise_under_policy(user_id, body.level)
    row = await set_trading_permission(user_id, body.level, body.agreement_version)
    # A user who never chose was at the default, so choosing it changes nothing.
    previous = row.get("previous_level") or DEFAULT_TRADING_PERMISSION.value
    if previous != body.level.value:
        activity.record(
            user_id, Action.TRADING_PERMISSION_CHANGED,
            detail={"from": previous, "to": body.level.value},
        )
    return _response(row)


# Least to most the agent may do with orders on its own.
_RANK = {
    TradingPermission.NO_TRADING: 0,
    TradingPermission.APPROVE_EACH: 1,
    TradingPermission.PLAN_FIRST: 2,
    TradingPermission.AUTONOMOUS: 3,
}


async def _refuse_raise_under_policy(user_id: str, level: TradingPermission) -> None:
    """Refuse raising the level while the policy withholds trading; keeping
    or lowering it always works. The row is read only when the policy asks."""
    from src.server.services.policy import TRADING
    from src.server.services.account import require_policy

    if level is TradingPermission.NO_TRADING:
        return
    policy = await require_policy(user_id)
    if policy.allows(TRADING):
        return
    current = _response(await get_trading_permission_row(user_id)).level
    if _RANK[level] > _RANK[current]:
        policy.require(TRADING, "Raising the trading permission")
