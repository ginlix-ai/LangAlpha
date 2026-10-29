"""Caller identity for the routes that also serve outside apps.

Opt-in on purpose. ``get_current_user_id`` refuses an outside app's token, so
every route that does not depend on ``get_principal`` keeps refusing one until
routes declare the scopes they need.
"""

from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.server.auth import oauth_resource
from src.server.utils.api import get_current_user_id

_bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    user_id: str
    auth: Literal["session", "oauth"]
    client_id: str | None = None
    # What an outside app was granted. Empty for a first-party session, which
    # no scope limits.
    scopes: tuple[str, ...] = ()


async def get_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Principal:
    """A first-party session, or an outside app's token chosen by its issuer."""
    if credentials is not None and oauth_resource.from_resource_issuer(
        credentials.credentials
    ):
        app = await oauth_resource.verify_access_token(credentials.credentials)
        return Principal(
            user_id=app.user_id, auth="oauth", client_id=app.client_id, scopes=app.scopes
        )
    return Principal(
        user_id=await get_current_user_id(request, credentials), auth="session"
    )


CurrentPrincipal = Annotated[Principal, Depends(get_principal)]
