"""The vendor terms the user is asked to accept, and their answer.

A person's surface: only a signed-in session (or the server's own call)
accepts terms, never a token, since a computer's agent accepting a vendor's
terms for the user would defeat asking them. Accepting creates the user's
account at the vendor, so it answers once the account exists, or says why it
could not be created.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Response

from src.server.dependencies.principal import CurrentPrincipal, Principal, forbidden
from src.server.services import vendor_terms
from src.server.services.egress import AccountUnavailable, RelayRejection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/vendor-terms", tags=["Vendor terms"])


def _person(principal: Principal) -> str:
    if not principal.unlimited:
        raise forbidden("Only you, signed in to the app, accept a service's terms")
    return principal.user_id


def _unknown() -> HTTPException:
    return HTTPException(status_code=404, detail="No such service")


@router.get("/pending")
async def list_pending(principal: CurrentPrincipal) -> dict[str, Any]:
    """The terms the user is being asked to accept; none, and logged, while
    Redis cannot be read."""
    user_id = _person(principal)
    try:
        waiting = await vendor_terms.pending(user_id)
    except vendor_terms.Unavailable:
        logger.warning("vendor terms not listed (user=%s): Redis unavailable", user_id)
        waiting = []
    return {"pending": waiting}


@router.post("/{vendor}/accept")
async def accept_terms(vendor: str, principal: CurrentPrincipal) -> dict[str, Any]:
    """Create the user's account at the vendor; the ask stays pending if it fails.

    503 when the vendor may let it through later, 409 when it will not, and a
    credits refusal keeps its own status.
    """
    user_id = _person(principal)
    try:
        return await vendor_terms.accept(user_id, vendor)
    except vendor_terms.Unknown:
        raise _unknown() from None
    except AccountUnavailable as e:
        logger.warning("vendor account not created (user=%s %s): %s", user_id, vendor, e.code)
        name = vendor_terms.terms_for(vendor).name
        raise HTTPException(
            status_code=503 if e.retryable else 409,
            detail=f"Your {name} account could not be set up",
        ) from None
    except RelayRejection as e:
        raise HTTPException(status_code=e.status, detail=e.detail) from None


@router.post("/{vendor}/dismiss", status_code=204)
async def dismiss_terms(vendor: str, principal: CurrentPrincipal) -> Response:
    try:
        await vendor_terms.dismiss(_person(principal), vendor)
    except vendor_terms.Unknown:
        raise _unknown() from None
    return Response(status_code=204)
