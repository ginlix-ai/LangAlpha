"""The caller's phones for push notifications: register, list, switch, remove.

A device registers even while this server sends nothing (``available`` says
so), so configuring APNs later reaches the phones already signed in. The token
is never returned: it addresses the phone, and the app already has it.
"""

import logging
from datetime import datetime
from typing import Dict, List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from src.config import env
from src.server.database import push_devices as push_db
from src.server.services.push.config import push_available
from src.server.services.push.notify import PUSH_KINDS, PushKind, device_wants
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/users/me/push-devices", tags=["Users"])


class PushDeviceRegister(BaseModel):
    token: str = Field(min_length=16, max_length=200, pattern=r"^[0-9a-fA-F]+$")
    platform: Literal["ios"]
    bundle_id: str = Field(min_length=1, max_length=255)
    environment: Literal["sandbox", "production"]
    device_name: Optional[str] = Field(default=None, max_length=255)
    app_version: Optional[str] = Field(default=None, max_length=64)


class PushDeviceUpdate(BaseModel):
    #: Per-kind switches; a kind left out keeps its current value, and one this
    #: server does not know (a newer app's) is ignored.
    preferences: Dict[str, bool]


class PushDevice(BaseModel):
    push_device_id: UUID
    platform: str
    bundle_id: str
    environment: str
    device_name: Optional[str] = None
    app_version: Optional[str] = None
    #: Every kind, on unless switched off.
    preferences: Dict[PushKind, bool]
    created_at: datetime
    updated_at: datetime
    last_sent_at: Optional[datetime] = None


class PushDevicesResponse(BaseModel):
    #: Whether this server sends pushes at all.
    available: bool
    devices: List[PushDevice]


def _device(row: dict) -> PushDevice:
    return PushDevice(
        push_device_id=row["push_device_id"],
        platform=row["platform"],
        bundle_id=row["bundle_id"],
        environment=row["environment"],
        device_name=row.get("device_name"),
        app_version=row.get("app_version"),
        preferences={kind: device_wants(row, kind) for kind in PUSH_KINDS},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_sent_at=row.get("last_sent_at"),
    )


@router.get("", response_model=PushDevicesResponse)
@handle_api_exceptions("list push devices", logger)
async def list_push_devices(user_id: CurrentUserId):
    rows = await push_db.list_push_devices(user_id)
    return PushDevicesResponse(
        available=push_available(), devices=[_device(r) for r in rows]
    )


@router.post("", response_model=PushDevice)
@handle_api_exceptions("register push device", logger)
async def register_push_device(body: PushDeviceRegister, user_id: CurrentUserId):
    # With none configured yet, any app may register, so that configuring
    # push later reaches it; a send under a bundle the key can't push to
    # prunes the row.
    if env.APNS_BUNDLE_IDS and body.bundle_id not in env.APNS_BUNDLE_IDS:
        raise HTTPException(
            status_code=400,
            detail=f"bundle id {body.bundle_id!r} is not one this server pushes to",
        )
    row = await push_db.upsert_push_device(
        user_id=user_id,
        token=body.token.lower(),
        platform=body.platform,
        bundle_id=body.bundle_id,
        environment=body.environment,
        device_name=body.device_name,
        app_version=body.app_version,
    )
    return _device(row)


@router.patch("/{push_device_id}", response_model=PushDevice)
@handle_api_exceptions("update push device", logger)
async def update_push_device(
    push_device_id: UUID, body: PushDeviceUpdate, user_id: CurrentUserId
):
    changes = {k: v for k, v in body.preferences.items() if k in PUSH_KINDS}
    if changes:
        row = await push_db.update_push_device_preferences(
            user_id, str(push_device_id), changes
        )
    else:
        row = await push_db.get_push_device(user_id, str(push_device_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Push device not found")
    return _device(row)


@router.delete("/{push_device_id}", status_code=204)
@handle_api_exceptions("remove push device", logger)
async def delete_push_device(push_device_id: UUID, user_id: CurrentUserId):
    if not await push_db.delete_push_device(user_id, str(push_device_id)):
        raise HTTPException(status_code=404, detail="Push device not found")
    return Response(status_code=204)
