"""The setup step of a self-hosted deployment.

Mounted only on an open-source server (``setup_settings.self_hosted``): a hosted
server's keys and sandbox provider are its operator's, set in its environment,
and no user of it may change them.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from src.server.auth import owner_requests
from src.server.database.encryption import encryption_configured
from src.server.models.setup_settings import (
    SaveKeysRequest,
    SaveSandboxRequest,
    KeyTestRequest,
)
from src.server.services.setup_settings import step
from src.server.services.setup_settings.catalog import key_names
from src.server.services.setup_settings.key_test import try_key


def _owner_only(request: Request) -> None:
    """These routes have no caller to check, being the deployment's own, and a
    container on the machine must not set the keys every computer gets."""
    owner_requests.refuse_unless_owner(request.scope)


router = APIRouter(
    prefix="/api/v1/setup", tags=["Setup"], dependencies=[Depends(_owner_only)]
)


def _can_encrypt() -> None:
    """The key that protects these rows stays out of the table, so without it
    nothing can be saved. The status and the key test need no such thing."""
    if not encryption_configured():
        raise HTTPException(
            status_code=503, detail="BYOK_ENCRYPTION_KEY must be set to save settings"
        )


@router.get("/status")
async def setup_status() -> dict[str, Any]:
    return await step.read_status()


@router.put("/keys", dependencies=[Depends(_can_encrypt)])
async def save_keys(body: SaveKeysRequest) -> dict[str, Any]:
    try:
        return await step.save_keys(body.keys)
    except step.SetupRefused as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None


@router.post("/keys/test")
async def try_setup_key(body: KeyTestRequest) -> dict[str, Any]:
    if body.name not in key_names():
        raise HTTPException(status_code=422, detail=f"{body.name} is not a key this step holds")
    return await try_key(body.name, body.value)


@router.put("/sandbox", dependencies=[Depends(_can_encrypt)])
async def save_sandbox(body: SaveSandboxRequest) -> dict[str, Any]:
    try:
        return await step.save_sandbox(body.provider, body.install_groups)
    except step.SetupRefused as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None


@router.post("/complete", dependencies=[Depends(_can_encrypt)])
async def complete_setup() -> dict[str, Any]:
    return await step.complete()
