"""User-level MCP OAuth API — connect, callback, disconnect, schema refresh.

Endpoints:
- POST   /api/v1/mcp/servers/{name}/oauth/start    → {authorize_url}
- POST   /api/v1/mcp/oauth/start-pending           → {authorize_url}, saving at callback
- POST   /api/v1/mcp/oauth/start-pending/upload    → the same, for an uploaded plugin
- GET    /api/v1/mcp/oauth/callback                → 302 back into the app
- DELETE /api/v1/mcp/servers/{name}/oauth          → disconnect
- POST   /api/v1/mcp/servers/{name}/oauth/refresh-schemas → host-side re-discovery

The callback carries NO session auth by design: it is the AS redirecting the
user's browser, on any worker — identity comes exclusively from the
single-use ``state`` record minted in phase 1.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Literal

from fastapi import (
    APIRouter,
    Body,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from src.config.env import SERVER_BASE_URL
from src.server.services.mcp_catalog import apply_catalog_enabled
from src.server.services.mcp_oauth import TokenUnavailable, disconnect_server
from src.server.services.mcp_oauth.connect import (
    STATE_TTL_SECONDS,
    McpOAuthError,
    McpServerMoved,
    McpServerNotFound,
    StartedConnect,
    complete_callback,
    start_connect,
    start_pending_connect,
)
from src.server.services.mcp_oauth.finish import PreparedSignIn
from src.server.services.mcp_oauth.redirects import DEFAULT_RETURN_TO, CallbackError
from src.server.services.mcp_saves import (
    BrokerageAdd,
    CatalogAdd,
    PendingKind,
    ServerEdit,
    WorkspaceAdd,
    parse_server,
    start_add,
    start_brokerage,
    start_edit,
    start_plugin,
)
from src.server.services.plugins import (
    MAX_PACKAGE_BYTES,
    prepare_pending_install,
    refusals_as_http,
    resolve_upload_source,
    resolve_url_source,
)
from src.server.services.plugins.source import SECRETS_FORM, SECRETS_REFUSAL
from src.server.utils.api import CurrentUserId, handle_api_exceptions
from src.server.utils.uploads import json_form_field, read_capped

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
    return _started_response(started, response)


def _started_response(started: StartedConnect, response: Response) -> dict:
    """What a started sign-in answers, and the cookie that binds its callback."""
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
        # How long the vendor has to answer. Past it no callback can finish this
        # flow, which is the only way a page that never unloaded (the shell
        # keeps it while the consent screen is in the system browser) can tell
        # a sign-in still underway from one that was given up.
        "expires_in": STATE_TTL_SECONDS,
    }


class _StartFields(BaseModel):
    """The reconnect start's fields, which every pending sign-in takes too."""

    model_config = ConfigDict(extra="forbid")

    return_to: str | None = None
    redirect_uri: str | None = None
    granted_capabilities: list[str] | None = None


# Each kind carries the fields its ordinary route takes. ``server`` stays a
# plain object until ``parse_server`` reads it, so a bad server is refused in
# the words its ordinary route uses.


class ServerAddStart(_StartFields):
    """``POST /api/v1/mcp/servers``."""

    kind: Literal[PendingKind.SERVER]
    server: dict[str, Any]

    async def prepare(self, user_id: str) -> PreparedSignIn:
        return await start_add(CatalogAdd(user_id, parse_server(self.server)))


class WorkspaceServerAddStart(_StartFields):
    """``POST /api/v1/workspaces/{workspace_id}/mcp/servers``."""

    kind: Literal[PendingKind.WORKSPACE_SERVER]
    workspace_id: str = Field(min_length=1)
    server: dict[str, Any]

    async def prepare(self, user_id: str) -> PreparedSignIn:
        return await start_add(
            WorkspaceAdd(user_id, parse_server(self.server), self.workspace_id)
        )


class ServerEditStart(_StartFields):
    """An edit of the row ``name``, from Plugins or from ``workspace_id``.

    ``expected_url`` is the address the page drew the row at.
    """

    kind: Literal[PendingKind.SERVER_EDIT]
    name: str
    server: dict[str, Any]
    workspace_id: str | None = None
    expected_url: str | None = None

    async def prepare(self, user_id: str) -> PreparedSignIn:
        edit = ServerEdit(
            user_id, self.name, parse_server(self.server), self.workspace_id
        )
        return await start_edit(edit, expected_url=self.expected_url)


class BrokerageStart(_StartFields):
    """A shipped brokerage's first connect."""

    kind: Literal[PendingKind.BROKERAGE]
    name: str

    async def prepare(self, user_id: str) -> PreparedSignIn:
        return await start_brokerage(BrokerageAdd(user_id, self.name))


class PluginStart(_StartFields):
    """``POST /api/v1/plugins``."""

    kind: Literal[PendingKind.PLUGIN]
    source_url: str = Field(min_length=1)
    subdir: str | None = None
    secrets: dict[str, str] | None = None

    async def prepare(self, user_id: str) -> PreparedSignIn:
        with refusals_as_http():
            source = await resolve_url_source(self.source_url, self.subdir)
            pending = await prepare_pending_install(user_id, source, self.secrets)
        return start_plugin(pending)


PendingSignInInput = Annotated[
    ServerAddStart
    | WorkspaceServerAddStart
    | ServerEditStart
    | BrokerageStart
    | PluginStart,
    Field(discriminator="kind"),
]


async def _start_pending(
    user_id: str,
    prepared: PreparedSignIn,
    request: Request,
    response: Response,
    *,
    return_to: str | None,
    redirect_uri: str | None,
    granted_capabilities: list[str] | None,
) -> dict:
    try:
        started = await start_pending_connect(
            user_id,
            prepared,
            return_to=return_to,
            web_origin=request.headers.get("origin"),
            loopback_redirect=redirect_uri,
            granted_capabilities=granted_capabilities,
        )
    except McpOAuthError as e:
        raise HTTPException(status_code=422, detail=str(e))
    answer = _started_response(started, response)
    # The name the callback will report in ``mcp_connected`` (for a plugin,
    # the row its install gives the sign-in server), so the page can tell
    # its own sign-in's return from another's.
    answer["server"] = prepared.server
    answer["kind"] = str(prepared.kind)
    if prepared.plugin:
        answer["plugin"] = prepared.plugin
    return answer


@router.post("/oauth/start-pending")
@handle_api_exceptions("start MCP sign-in", logger, conflict_on_value_error=True)
async def oauth_start_pending(
    user_id: CurrentUserId,
    request: Request,
    response: Response,
    body: PendingSignInInput,
) -> dict:
    """Start a sign-in that saves its server, rather than one for a saved row.

    Everything that can be refused without the user's token is refused here,
    in the shapes the ordinary route answers with, and nothing is written; the
    callback saves the server once the user has signed in and the server
    accepts the token. A server that does not ask for a sign-in is answered
    422 ``mcp_sign_in_not_needed`` (``plugin_sign_in_not_needed`` for a
    plugin): it saves through its ordinary route.
    """
    return await _start_pending(
        user_id,
        await body.prepare(user_id),
        request,
        response,
        return_to=body.return_to,
        redirect_uri=body.redirect_uri,
        granted_capabilities=body.granted_capabilities,
    )


# The ``granted_capabilities`` form field: a JSON list, or no selection.
_CAPABILITIES_FORM = TypeAdapter(list[str])


@router.post("/oauth/start-pending/upload")
@handle_api_exceptions("start MCP sign-in", logger, conflict_on_value_error=True)
async def oauth_start_pending_upload(
    user_id: CurrentUserId,
    request: Request,
    response: Response,
    file: UploadFile = File(...),
    subdir: str | None = Form(None),
    secrets: str | None = Form(None),
    return_to: str | None = Form(None),
    redirect_uri: str | None = Form(None),
    granted_capabilities: str | None = Form(None),
) -> dict:
    """``start-pending`` for a plugin installed from an uploaded package.

    The form fields are the upload install's, plus the sign-in's own;
    ``secrets`` and ``granted_capabilities`` hold JSON.
    """
    # Both read before the package is: a malformed field is refused without
    # validating and checking a plugin first.
    supplied = json_form_field(secrets, SECRETS_FORM, SECRETS_REFUSAL)
    capabilities = json_form_field(
        granted_capabilities,
        _CAPABILITIES_FORM,
        "granted_capabilities must be a JSON list",
    )
    raw = await read_capped(file, MAX_PACKAGE_BYTES)
    with refusals_as_http():
        pending = await prepare_pending_install(
            user_id, resolve_upload_source(raw, file.filename, subdir), supplied
        )
    return await _start_pending(
        user_id,
        start_plugin(pending),
        request,
        response,
        return_to=return_to,
        redirect_uri=redirect_uri,
        granted_capabilities=capabilities,
    )


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
    """Disconnect a server, and switch it off.

    A server that needs a sign-in is on only while its connection stands, so
    the user giving the connection up also takes the server out of every
    workspace. Only this door does: a token that expires or fails to refresh
    leaves the row on, showing reconnect. After the revoke rather than inside
    it, since ``disconnect_server`` is also the teardown under a row drop and
    a moved address; a switch that fails after it leaves the same row a dead
    refresh would, which the next enable refuses.
    """
    found = await disconnect_server(user_id, name)
    if not found:
        raise HTTPException(status_code=404, detail="No OAuth connection found")
    await apply_catalog_enabled(user_id, name, False)
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
