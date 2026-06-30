"""Robinhood Agentic Trading MCP connect flow (per-workspace, OAuth).

Connects a workspace to Robinhood's Agentic Trading MCP. The OAuth backend
(``services/robinhood_oauth.py``) does discovery + dynamic client registration +
PKCE; this router is the HTTP surface:

- POST   /api/v1/workspaces/{id}/robinhood/initiate  → authorize URL (opened in a popup)
- GET    /api/v1/workspaces/{id}/robinhood/callback   → browser redirect target; exchanges
         the code, stores tokens, writes the ROBINHOOD_TOKEN vault secret, and
         registers the gated Robinhood MCP server, then closes the popup
- GET    /api/v1/workspaces/{id}/robinhood/status     → connection status
- DELETE /api/v1/workspaces/{id}/robinhood            → disconnect (tokens + secret + server)

The trade tools are registered behind the ``tool_deny`` hard gate, so the agent
can read / quote / preview but never place or cancel an order on its own.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from src.config.env import SERVER_BASE_URL
from src.server.database import robinhood_oauth as ro_db
from src.server.database.mcp_servers import upsert_workspace_server
from src.server.database.vault_secrets import (
    create_secret as create_secret_db,
    delete_secret,
    update_secret,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.models.mcp_server import McpServerInput
from src.server.services.robinhood_oauth import (
    ROBINHOOD_MCP_URL,
    build_authorize_url,
    discover_metadata,
    exchange_code,
    generate_pkce_pair,
    register_client,
)
from src.server.utils.api import (
    CurrentUserId,
    handle_api_exceptions,
    require_workspace_owner,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Robinhood"])

ROBINHOOD_SERVER_NAME = "robinhood"
VAULT_SECRET_NAME = "ROBINHOOD_TOKEN"
# The vault secret holds the FULL header value ("Bearer <token>"), because the
# MCP server model only allows a header value that is exactly ``${vault:NAME}``
# (no embedded prefix). The in-sandbox resolver substitutes the whole value.
BEARER_PREFIX = "Bearer "
# Hard-gated trade tools — present on the server but never generated as callable
# wrappers (see MCPServerConfig.tool_deny). Enabling trading is a deliberate edit.
GATED_TRADE_TOOLS = ["place_equity_order", "cancel_equity_order"]

_SERVER_DESCRIPTION = (
    "Robinhood Agentic Trading — read your accounts, positions, portfolio, "
    "equity quotes, and order history, and preview orders. Trade execution "
    "(place/cancel) is disabled."
)
_SERVER_INSTRUCTION = (
    "Use for Robinhood account reads, quotes, and order previews "
    "(review_equity_order). Order placement and cancellation are intentionally "
    "unavailable; do not claim to have placed or cancelled any order."
)


async def _require_owned_workspace(workspace_id: str, user_id: str) -> dict:
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)
    return workspace


def _redirect_uri(workspace_id: str) -> str:
    base = SERVER_BASE_URL.rstrip("/")
    return f"{base}/api/v1/workspaces/{workspace_id}/robinhood/callback"


def _robinhood_server_blob() -> dict:
    """The gated Robinhood MCP server definition (validated through the model)."""
    return McpServerInput(
        name=ROBINHOOD_SERVER_NAME,
        transport="http",
        url=ROBINHOOD_MCP_URL,
        headers={"Authorization": f"${{vault:{VAULT_SECRET_NAME}}}"},
        tool_deny=list(GATED_TRADE_TOOLS),
        discovery_uses_secrets=True,
        description=_SERVER_DESCRIPTION,
        instruction=_SERVER_INSTRUCTION,
    ).to_config_blob()


# ---------------------------------------------------------------------------
# POST — initiate
# ---------------------------------------------------------------------------


@router.post("/{workspace_id}/robinhood/initiate")
@handle_api_exceptions("initiate Robinhood connect", logger)
async def initiate(workspace_id: str, user_id: CurrentUserId) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    try:
        meta = await discover_metadata(ROBINHOOD_MCP_URL)
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"Robinhood OAuth discovery failed: {e}"
        )

    redirect_uri = _redirect_uri(workspace_id)
    scope = " ".join(meta.get("scopes_supported") or [])
    reg_ep = meta.get("registration_endpoint")
    if not reg_ep:
        raise HTTPException(
            status_code=502,
            detail="Robinhood OAuth server does not advertise dynamic client "
            "registration; cannot connect automatically.",
        )
    try:
        client = await register_client(reg_ep, redirect_uri, scope=scope)
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"Robinhood client registration failed: {e}"
        )

    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(32)

    await ro_db.upsert_pending(
        user_id=user_id,
        workspace_id=workspace_id,
        resource=meta["resource"],
        authorization_endpoint=meta["authorization_endpoint"],
        token_endpoint=meta["token_endpoint"],
        registration_endpoint=reg_ep,
        client_id=client["client_id"],
        client_secret=client.get("client_secret"),
        redirect_uri=redirect_uri,
        scopes=scope,
        state=state,
        code_verifier=verifier,
    )

    authorize_url = build_authorize_url(
        authorization_endpoint=meta["authorization_endpoint"],
        client_id=client["client_id"],
        redirect_uri=redirect_uri,
        state=state,
        challenge=challenge,
        resource=meta["resource"],
        scope=scope,
    )
    return {"authorize_url": authorize_url}


# ---------------------------------------------------------------------------
# GET — callback (browser redirect target; bound by state, not auth header)
# ---------------------------------------------------------------------------


def _close_popup_html(message: str, *, ok: bool) -> HTMLResponse:
    """Return a tiny page that signals the opener and closes the popup."""
    # BroadcastChannel name + message must match web/src/lib/oauthPopup.ts.
    safe = message.replace("<", "&lt;").replace(">", "&gt;")
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Robinhood</title></head><body style="font:14px system-ui;padding:2rem">
<p>{safe}</p>
<script>
try {{ new BroadcastChannel('langalpha-oauth').postMessage({{type:'oauth-complete'}}); }} catch (e) {{}}
try {{ if (window.opener) window.opener.postMessage({{type:'oauth-complete'}}, '*'); }} catch (e) {{}}
setTimeout(function(){{ try {{ window.close(); }} catch (e) {{}} }}, {800 if ok else 2500});
</script></body></html>"""
    return HTMLResponse(content=html, status_code=200)


@router.get("/{workspace_id}/robinhood/callback")
@handle_api_exceptions("Robinhood OAuth callback", logger)
async def callback(
    workspace_id: str,
    state: str = "",
    code: str = "",
    error: str = "",
    error_description: str = "",
) -> HTMLResponse:
    if error:
        return _close_popup_html(
            f"Robinhood connection was not completed: {error_description or error}",
            ok=False,
        )
    if not state or not code:
        return _close_popup_html("Missing authorization response.", ok=False)

    row = await ro_db.get_pending_by_state(state)
    if row is None or row["workspace_id"] != workspace_id:
        return _close_popup_html("This connection request has expired.", ok=False)

    user_id = row["user_id"]
    try:
        tokens = await exchange_code(
            token_endpoint=row["token_endpoint"],
            client_id=row["client_id"],
            client_secret=row.get("client_secret") or None,
            code=code,
            redirect_uri=row["redirect_uri"],
            code_verifier=row["code_verifier"],
            resource=row["resource"],
        )
    except Exception as e:
        logger.error("[robinhood] token exchange failed: %s", e)
        return _close_popup_html(f"Token exchange failed: {e}", ok=False)

    expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=tokens.get("expires_in", 3600)
    )
    await ro_db.set_tokens(
        user_id=user_id,
        workspace_id=workspace_id,
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_at=expires_at,
    )

    # Write the live token into the workspace vault (create or update). The
    # secret value is the full "Bearer <token>" header value (see BEARER_PREFIX).
    header_value = f"{BEARER_PREFIX}{tokens['access_token']}"
    updated = await update_secret(
        workspace_id, VAULT_SECRET_NAME, value=header_value, description=None
    )
    if not updated:
        try:
            await create_secret_db(
                workspace_id,
                VAULT_SECRET_NAME,
                header_value,
                "Robinhood Agentic Trading MCP access token",
            )
        except ValueError as e:
            return _close_popup_html(f"Could not store token: {e}", ok=False)

    # Register the gated Robinhood MCP server (create-or-replace).
    await upsert_workspace_server(
        workspace_id,
        ROBINHOOD_SERVER_NAME,
        source="workspace",
        enabled=True,
        config=_robinhood_server_blob(),
    )

    # Push the new secret + bring the new server config live before the next turn.
    from src.server.app.mcp_servers import _push_vault_to_sandbox, _schedule_proactive_apply

    await _push_vault_to_sandbox(workspace_id)
    _schedule_proactive_apply(workspace_id, user_id)

    return _close_popup_html("Robinhood connected. You can close this window.", ok=True)


# ---------------------------------------------------------------------------
# GET — status
# ---------------------------------------------------------------------------


@router.get("/{workspace_id}/robinhood/status")
@handle_api_exceptions("Robinhood status", logger)
async def status(workspace_id: str, user_id: CurrentUserId) -> dict:
    await _require_owned_workspace(workspace_id, user_id)
    st = await ro_db.get_status(user_id, workspace_id)
    return {
        "connected": st["connected"],
        "expires_at": st["expires_at"].isoformat() if st.get("expires_at") else None,
        "trading_enabled": False,  # trade tools are always gated in this build
        "server_name": ROBINHOOD_SERVER_NAME,
    }


# ---------------------------------------------------------------------------
# DELETE — disconnect
# ---------------------------------------------------------------------------


@router.delete("/{workspace_id}/robinhood")
@handle_api_exceptions("disconnect Robinhood", logger)
async def disconnect(workspace_id: str, user_id: CurrentUserId) -> dict:
    await _require_owned_workspace(workspace_id, user_id)

    from src.server.database.mcp_servers import delete_workspace_server

    await ro_db.delete(user_id, workspace_id)
    await delete_secret(workspace_id, VAULT_SECRET_NAME)
    await delete_workspace_server(workspace_id, ROBINHOOD_SERVER_NAME)

    from src.server.app.mcp_servers import _schedule_proactive_apply

    _schedule_proactive_apply(workspace_id, user_id)
    return {"ok": True}
