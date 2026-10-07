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
    return _response(
        await set_trading_permission(user_id, body.level, body.agreement_version)
    )
