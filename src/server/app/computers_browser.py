"""What a computer's owner chooses for its browser, and bringing the sign-ins of
their everyday browser into it on a machine of their own.

Endpoints:
- PATCH /api/v1/computers/{computer_id}/browser
- POST  /api/v1/computers/{computer_id}/browser-quality (the quality alone, answering the computer)
- GET   /api/v1/computers/{computer_id}/browser-signins/sources
- POST  /api/v1/computers/{computer_id}/browser-signins/bring
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException

from src.server.app import computers
from src.server.models.computer import (
    BrowserChoicesSaved,
    BrowserChoicesUpdate,
    BrowserSigninBringRequest,
    BrowserSigninBringResponse,
    BrowserSigninSourcesResponse,
    ComputerBrowserQualityRequest,
    ComputerResponse,
)
from src.server.services import browser_everyday, browser_settings
from src.server.services.ssh_computers import is_mac_under_remote_control
from src.server.utils.api import CurrentUserId

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/computers", tags=["Computers"])


async def _save(
    computer_id: str, user_id: str, update: BrowserChoicesUpdate
) -> tuple[dict[str, Any], bool]:
    """Save *update* on the user's computer and put it on its machine if that
    is running: the row as saved, and whether the machine has it."""
    async with computers._computer_action_errors("set the browser for", computer_id):
        computer = await computers._owned_computer(computer_id, user_id)
        if "window" in update.model_fields_set and not is_mac_under_remote_control(computer):
            raise HTTPException(
                status_code=409,
                detail="Only a Mac under remote control can show the browser as a window.",
            )
        saved = await browser_settings.save(computer_id, user_id, update)
        if saved is None:
            raise HTTPException(status_code=404, detail="Computer not found")
        applied = await browser_settings.apply_now(saved)
    logger.info(
        f"Set computer {computer_id} browser {update.model_dump(include=update.model_fields_set)} "
        f"for user {user_id}"
    )
    return saved, applied


@router.patch("/{computer_id}/browser", response_model=BrowserChoicesSaved)
async def set_computer_browser(
    computer_id: str, update: BrowserChoicesUpdate, x_user_id: CurrentUserId
):
    """Change what the computer's browser is set to: only the fields sent.

    The open browser keeps the way it started, since changing it restarts the
    browser and loses its pages, so a change reaches the next one it opens.
    Every save of ``signins`` signs the browser out of every site, the same
    value again included, which is how a person signs it out.
    """
    saved, applied = await _save(computer_id, x_user_id, update)
    return BrowserChoicesSaved(browser=browser_settings.shown(saved), applied=applied)


@router.post("/{computer_id}/browser-quality", response_model=ComputerResponse)
async def set_computer_browser_quality(
    computer_id: str, request: ComputerBrowserQualityRequest, x_user_id: CurrentUserId
):
    """The live view's quality alone, answering the whole computer: the route a
    web built before ``PATCH /browser`` calls, as one already open does while
    the backend it talks to is deployed first."""
    saved, _ = await _save(computer_id, x_user_id, BrowserChoicesUpdate(quality=request.quality))
    return await computers._counted_response(saved)


@router.get("/{computer_id}/browser-signins/sources", response_model=BrowserSigninSourcesResponse)
async def list_browser_signin_sources(computer_id: str, x_user_id: CurrentUserId):
    """The everyday browsers on the computer's machine whose sign-ins can be
    brought in, each profile on its own. Only for its owner; a computer not
    running is not started to ask."""
    computer = await computers._owned_computer(computer_id, x_user_id)
    return await browser_everyday.sources(computer)


@router.post("/{computer_id}/browser-signins/bring", response_model=BrowserSigninBringResponse)
async def bring_browser_signins(
    computer_id: str, body: BrowserSigninBringRequest, x_user_id: CurrentUserId
):
    """Bring one everyday browser profile's sign-ins into the computer's browser.

    Its cookies are read and set on the machine, into the profile kept between
    the browser's starts, which opens for it; nothing of them reaches the server.
    On a Mac the person is asked there to let the browser's key be read, so this
    waits on them. ``keychain`` is their no, or a login with no desktop to ask on.
    """
    computer = await computers._owned_computer(computer_id, x_user_id)
    result = await browser_everyday.bring(computer, body.browser, body.profile)
    if result.done:
        logger.info(
            f"Brought {result.count} cookies from {body.browser} into computer {computer_id} for user {x_user_id}"
        )
    return result
