"""User-level MCP OAuth API — connect, callback, disconnect, schema refresh.

Endpoints:
- POST   /api/v1/mcp/servers/{name}/oauth/start    → {authorize_url}
- GET    /api/v1/mcp/oauth/callback                → 302 back into the app
- DELETE /api/v1/mcp/servers/{name}/oauth          → disconnect
- POST   /api/v1/mcp/servers/{name}/oauth/refresh-schemas → host-side re-discovery

The callback carries NO session auth by design: it is the AS redirecting the
user's browser, on any worker — identity comes exclusively from the
single-use ``state`` record minted in phase 1.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from src.config.env import SERVER_BASE_URL
from src.server.services.mcp_oauth import (
    McpOAuthError,
    McpServerMoved,
    McpServerNotFound,
    TokenUnavailable,
    complete_callback,
    disconnect_server,
    start_connect,
)
from src.server.services.mcp_oauth.connect import STATE_TTL_SECONDS
from src.server.services.mcp_oauth.redirects import DEFAULT_RETURN_TO, CallbackError
from src.server.utils.api import CurrentUserId, handle_api_exceptions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/mcp", tags=["MCP OAuth"])

# CSRF binding cookie for the OAuth connect flow. Path-scoped to the callback,
# HttpOnly, SameSite=Lax (must survive the AS's top-level cross-site redirect —
# Strict would not send it). Secure only on an HTTPS deployment so http dev
# still works.
#
# The name is per-state (``mcp_oauth_cb_<state>``): a fixed name would let two
# concurrent connects from the same browser overwrite each other's nonce cookie
# (same name, same path), so the first flow's callback would read the second
# flow's nonce and fail state_mismatch. Keying on the single-use state isolates
# concurrent flows and lets each callback delete exactly its own cookie.
_OAUTH_COOKIE_PREFIX = "mcp_oauth_cb_"
_OAUTH_COOKIE_PATH = "/api/v1/mcp/oauth"
_OAUTH_COOKIE_SECURE = SERVER_BASE_URL.lower().startswith("https")


def _oauth_cookie_name(state: str) -> str:
    return f"{_OAUTH_COOKIE_PREFIX}{state}"


@router.post("/servers/{name}/oauth/start")
@handle_api_exceptions("start MCP OAuth connect", logger)
async def oauth_start(
    name: str,
    user_id: CurrentUserId,
    request: Request,
    response: Response,
    body: dict | None = Body(default=None),
) -> dict:
    return_to = (body or {}).get("return_to")
    # Coerced here rather than in the service: the body is an unvalidated dict,
    # and anything that is not a list of keys is not a consent decision. A
    # non-list becomes "no selection named", which the service refuses outright
    # for a brokerage rather than reading as either extreme -- nobody was asked,
    # so it is neither declining everything nor granting it.
    raw_capabilities = (body or {}).get("granted_capabilities")
    granted_capabilities = (
        [str(key) for key in raw_capabilities]
        if isinstance(raw_capabilities, list)
        else None
    )
    try:
        started = await start_connect(
            user_id,
            name,
            return_to=return_to,
            # The browser's Origin is where the UI lives; the callback later
            # redirects there, since its own origin is the API on split ports.
            web_origin=request.headers.get("origin"),
            # A desktop shell offering its own listener, for an AS that refuses
            # a hosted callback. Bounded to loopback by the service and ignored
            # when it is anything else, so an unrecognised value degrades to the
            # ordinary flow rather than failing the request.
            loopback_redirect=(body or {}).get("redirect_uri"),
            # The address the page drew this row from. A connect can carry a
            # question that was asked of a particular server, and the row is the
            # user's to edit from another tab in between; naming it here is what
            # lets that question be a gate. Absent from an older page, which then
            # behaves as it always did.
            expected_url=(body or {}).get("expected_url"),
            # The capability groups agreed to in the dialog that preceded this
            # call. Intersected with the vendor's real groups by the service, so
            # a page that names none grants none.
            granted_capabilities=granted_capabilities,
        )
    except McpServerNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
    except McpServerMoved as e:
        # 409 rather than 422: the request is well formed and was right when the
        # page was drawn. What it conflicts with is the row's current state, and
        # the client tells the two apart to say "reload" rather than "invalid".
        raise HTTPException(status_code=409, detail=str(e))
    except McpOAuthError as e:
        raise HTTPException(status_code=422, detail=str(e))
    # Bind the callback to THIS browser: the nonce goes only into an HttpOnly
    # cookie, never the JSON body. The callback requires it back, so a stolen
    # (state, code) replayed in another browser has no matching cookie. Empty
    # on a loopback callback, where the cookie provably cannot come back — see
    # redirects.callback_is_loopback.
    if started.browser_nonce:
        response.set_cookie(
            _oauth_cookie_name(started.state),
            started.browser_nonce,
            # The state record it binds to is what expires the flow; a cookie
            # outliving it would only ever be sent at a state that is gone.
            max_age=STATE_TTL_SECONDS,
            path=_OAUTH_COOKIE_PATH,
            httponly=True,
            secure=_OAUTH_COOKIE_SECURE,
            samesite="lax",
        )
    return {
        "authorize_url": started.authorize_url,
        # Both of these exist for a desktop shell that armed a loopback listener
        # before this request, and neither is a secret: `state` is already in the
        # authorize URL the caller is about to be sent to, and `redirect_uri` is
        # the value it just offered. The shell needs `state` to tell its own
        # callback from anything else that reaches the port, and `redirect_uri`
        # to notice that a build which does not read the field left it on the
        # hosted callback -- the one failure that otherwise looks like success
        # right up until nothing arrives. The nonce stays cookie-only.
        "state": started.state,
        "redirect_uri": started.redirect_uri,
    }


@router.get("/oauth/callback")
async def oauth_callback(
    request: Request,
    state: str | None = None,
    code: str | None = None,
    iss: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    """AS redirect target. Always answers a redirect — never an error page."""
    # The nonce cookie is named for this flow's state; with no state there is no
    # cookie to read and complete_callback will reject on the missing state.
    cookie_name = _oauth_cookie_name(state) if state else None
    try:
        target = await complete_callback(
            state=state,
            code=code,
            iss=iss,
            error=error,
            error_description=error_description,
            browser_nonce=(
                request.cookies.get(cookie_name) if cookie_name else None
            ),
        )
    except Exception:
        logger.exception("[mcp_oauth] callback crashed")
        target = f"{DEFAULT_RETURN_TO}?mcp_error={CallbackError.INTERNAL}"
    # 303: the browser must GET the app route regardless of how it got here.
    resp = RedirectResponse(url=target, status_code=303)
    # The nonce is single-use — clear this flow's cookie so a later navigation
    # can't resend it.
    if cookie_name:
        resp.delete_cookie(cookie_name, path=_OAUTH_COOKIE_PATH)
    return resp


@router.delete("/servers/{name}/oauth")
@handle_api_exceptions("disconnect MCP OAuth", logger)
async def oauth_disconnect(name: str, user_id: CurrentUserId) -> dict:
    found = await disconnect_server(user_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="No OAuth connection found")
    return {"ok": True}


@router.post("/servers/{name}/oauth/refresh-schemas")
@handle_api_exceptions("refresh MCP OAuth schemas", logger)
async def oauth_refresh_schemas(name: str, user_id: CurrentUserId) -> dict:
    from src.server.services.mcp_oauth.discovery import refresh_user_tool_schemas

    try:
        row = await refresh_user_tool_schemas(user_id, name)
    except TokenUnavailable as e:
        raise HTTPException(
            status_code=409, detail=f"Connection unusable: {e.reason}"
        )
    return {
        "server_name": row["server_name"],
        "status": row["status"],
        "error": row.get("error") or "",
        "tool_count": len(row.get("tools") or []),
        "discovered_at": row.get("discovered_at"),
    }
