"""Robinhood Agentic Trading MCP OAuth service.

Robinhood's MCP (``https://agent.robinhood.com/mcp/trading``) is protected by the
STANDARD MCP OAuth flow — there is no static, pre-registered client. So unlike
``claude_oauth.py`` (a fixed client_id + hosted paste page), this module adds the
two MCP-specific steps before the familiar PKCE auth-code dance:

  1. Discovery  — RFC 9728 protected-resource metadata → RFC 8414 authorization-
                  server metadata (authorization / token / registration endpoints).
  2. Registration — RFC 7591 Dynamic Client Registration to obtain a client_id.

then PKCE authorize → code exchange → refresh. The discovered endpoints + the
registered client + the refresh token are persisted per (user, workspace) in the
``robinhood_oauth`` table; the live access token is mirrored into the workspace
vault as ``ROBINHOOD_TOKEN`` so Phase 1's in-sandbox streamable-HTTP client can
send ``Authorization: Bearer ${vault:ROBINHOOD_TOKEN}``.

The ``mcp.shared.auth`` pydantic models are reused for safe parsing; the HTTP is
hand-rolled (httpx) for the same reasons ``claude_oauth.py`` is.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode, urlsplit

import httpx

logger = logging.getLogger(__name__)

# --- Constants ---

ROBINHOOD_MCP_URL = "https://agent.robinhood.com/mcp/trading"
CLIENT_NAME = "Ginlix LangAlpha"
# Spec-pinned (matches the codegen client in tool_generator.py).
MCP_PROTOCOL_VERSION = "2025-06-18"
# Refresh a little early so a turn never races expiry.
_REFRESH_BUFFER = timedelta(minutes=5)
_HTTP_TIMEOUT = 30.0

_RESOURCE_METADATA_RE = re.compile(r'resource_metadata="([^"]+)"')


# --- PKCE (same construction as claude_oauth.py) ---

def generate_pkce_pair() -> tuple[str, str]:
    """Return (verifier, S256 challenge)."""
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


# --- Discovery (RFC 9728 → RFC 8414) ---

def _wellknown_candidates(url: str, suffix: str) -> list[str]:
    """Build ``.well-known`` candidate URLs for ``url``, path-aware then root.

    RFC 8414/9728 insert the well-known segment after the host AND, for path-
    bearing resources, also try the path-appended and root forms. Order matters:
    the most specific (path-aware) first.
    """
    parts = urlsplit(url)
    origin = f"{parts.scheme}://{parts.netloc}"
    path = parts.path.rstrip("/")
    candidates = []
    if path:
        candidates.append(f"{origin}/.well-known/{suffix}{path}")
        candidates.append(f"{origin}/.well-known/{suffix}")
        candidates.append(f"{origin}{path}/.well-known/{suffix}")
    else:
        candidates.append(f"{origin}/.well-known/{suffix}")
    # De-dupe preserving order.
    seen: set[str] = set()
    return [c for c in candidates if not (c in seen or seen.add(c))]


async def _probe_resource_metadata_url(
    server_url: str, http: httpx.AsyncClient
) -> Optional[str]:
    """Read the ``resource_metadata`` hint from the MCP server's 401 challenge."""
    try:
        resp = await http.post(
            server_url,
            json={"jsonrpc": "2.0", "id": 0, "method": "ping"},
            headers={
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
            },
        )
    except httpx.HTTPError:
        return None
    if resp.status_code == 401:
        m = _RESOURCE_METADATA_RE.search(resp.headers.get("www-authenticate", ""))
        if m:
            return m.group(1)
    return None


async def _get_json(http: httpx.AsyncClient, url: str) -> Optional[dict[str, Any]]:
    try:
        resp = await http.get(url, headers={"Accept": "application/json"})
    except httpx.HTTPError:
        return None
    if resp.status_code == 200:
        try:
            return resp.json()
        except ValueError:
            return None
    return None


async def discover_metadata(
    server_url: str = ROBINHOOD_MCP_URL,
    *,
    http: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Discover the authorization server's endpoints for an MCP resource.

    Returns ``{authorization_endpoint, token_endpoint, registration_endpoint,
    scopes_supported, resource}``. Raises ``RuntimeError`` if discovery fails.
    """
    from mcp.shared.auth import OAuthMetadata, ProtectedResourceMetadata

    owns = http is None
    http = http or httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        # 1. Protected-resource metadata → authorization server URL(s).
        prm_url = await _probe_resource_metadata_url(server_url, http)
        prm_data = await _get_json(http, prm_url) if prm_url else None
        if prm_data is None:
            for cand in _wellknown_candidates(server_url, "oauth-protected-resource"):
                prm_data = await _get_json(http, cand)
                if prm_data:
                    break
        auth_servers: list[str] = []
        resource = server_url
        if prm_data:
            try:
                prm = ProtectedResourceMetadata.model_validate(prm_data)
                auth_servers = [str(a) for a in (prm.authorization_servers or [])]
                resource = str(prm.resource) if prm.resource else server_url
            except Exception:
                auth_servers = [
                    str(a) for a in (prm_data.get("authorization_servers") or [])
                ]
        # Fallback: treat the resource origin itself as the auth server.
        if not auth_servers:
            parts = urlsplit(server_url)
            auth_servers = [f"{parts.scheme}://{parts.netloc}"]

        # 2. Authorization-server metadata.
        for as_url in auth_servers:
            for suffix in ("oauth-authorization-server", "openid-configuration"):
                for cand in _wellknown_candidates(as_url, suffix):
                    data = await _get_json(http, cand)
                    if not data:
                        continue
                    try:
                        meta = OAuthMetadata.model_validate(data)
                        auth_ep = str(meta.authorization_endpoint)
                        token_ep = str(meta.token_endpoint)
                        reg_ep = (
                            str(meta.registration_endpoint)
                            if meta.registration_endpoint
                            else None
                        )
                        scopes = list(meta.scopes_supported or [])
                    except Exception:
                        auth_ep = data.get("authorization_endpoint")
                        token_ep = data.get("token_endpoint")
                        reg_ep = data.get("registration_endpoint")
                        scopes = list(data.get("scopes_supported") or [])
                    if auth_ep and token_ep:
                        return {
                            "authorization_endpoint": auth_ep,
                            "token_endpoint": token_ep,
                            "registration_endpoint": reg_ep,
                            "scopes_supported": scopes,
                            "resource": resource,
                        }
        raise RuntimeError(
            f"Could not discover OAuth metadata for {server_url!r} "
            f"(tried auth servers: {auth_servers})"
        )
    finally:
        if owns:
            await http.aclose()


# --- Dynamic Client Registration (RFC 7591) ---

async def register_client(
    registration_endpoint: str,
    redirect_uri: str,
    *,
    scope: str = "",
    http: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Register a public OAuth client via RFC 7591 DCR.

    Returns ``{client_id, client_secret}`` (client_secret may be ``None`` for a
    public client). Raises ``RuntimeError`` on failure.
    """
    owns = http is None
    http = http or httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        body: dict[str, Any] = {
            "client_name": CLIENT_NAME,
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",  # public client + PKCE
        }
        if scope:
            body["scope"] = scope
        resp = await http.post(
            registration_endpoint,
            json=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"DCR failed: status={resp.status_code} body={resp.text[:300]}"
            )
        data = resp.json()
        client_id = data.get("client_id")
        if not client_id:
            raise RuntimeError(f"DCR response missing client_id: {data}")
        return {"client_id": client_id, "client_secret": data.get("client_secret")}
    finally:
        if owns:
            await http.aclose()


# --- Authorize URL ---

def build_authorize_url(
    *,
    authorization_endpoint: str,
    client_id: str,
    redirect_uri: str,
    state: str,
    challenge: str,
    resource: str,
    scope: str = "",
) -> str:
    """Build the PKCE authorization-code URL."""
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "resource": resource,
    }
    if scope:
        params["scope"] = scope
    sep = "&" if "?" in authorization_endpoint else "?"
    return f"{authorization_endpoint}{sep}{urlencode(params)}"


# --- Token exchange / refresh ---

async def _token_request(
    token_endpoint: str, form: dict[str, str], *, http: httpx.AsyncClient
) -> dict[str, Any]:
    resp = await http.post(
        token_endpoint,
        data=form,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    if resp.status_code >= 400:
        logger.error(
            "[robinhood_oauth] token request failed: status=%s body=%s",
            resp.status_code, resp.text[:300],
        )
    resp.raise_for_status()
    data = resp.json()
    return {
        "access_token": data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_in": data.get("expires_in", 3600),
        "scope": data.get("scope", ""),
    }


async def exchange_code(
    *,
    token_endpoint: str,
    client_id: str,
    client_secret: Optional[str],
    code: str,
    redirect_uri: str,
    code_verifier: str,
    resource: str,
    http: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Exchange an authorization code for tokens."""
    owns = http is None
    http = http or httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": code_verifier,
            "resource": resource,
        }
        if client_secret:
            form["client_secret"] = client_secret
        return await _token_request(token_endpoint, form, http=http)
    finally:
        if owns:
            await http.aclose()


async def refresh_tokens(
    *,
    token_endpoint: str,
    client_id: str,
    client_secret: Optional[str],
    refresh_token: str,
    resource: str,
    http: Optional[httpx.AsyncClient] = None,
) -> dict[str, Any]:
    """Use a refresh token to get a new access token."""
    owns = http is None
    http = http or httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True)
    try:
        form = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "resource": resource,
        }
        if client_secret:
            form["client_secret"] = client_secret
        out = await _token_request(token_endpoint, form, http=http)
        # Some servers omit a rotated refresh token — keep the old one.
        if not out["refresh_token"]:
            out["refresh_token"] = refresh_token
        return out
    finally:
        if owns:
            await http.aclose()


# --- Orchestrator: valid token with refresh-lock + vault sync ---

async def get_valid_robinhood_token(
    user_id: str, workspace_id: str
) -> Optional[str]:
    """Return a valid Robinhood access token for this workspace, refreshing if
    needed and re-syncing the ``ROBINHOOD_TOKEN`` vault secret to the sandbox.

    Returns None when not connected or refresh fails. Uses a Redis SETNX lock so
    concurrent turns don't double-refresh (same pattern as claude_oauth).
    """
    from src.server.database import robinhood_oauth as db

    row = await db.get(user_id, workspace_id)
    if not row or row.get("status") != "connected":
        return None

    now = datetime.now(timezone.utc)
    expires_at = row.get("expires_at")
    if expires_at is not None and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if expires_at is not None and expires_at > now + _REFRESH_BUFFER:
        return row["access_token"] or None

    if not row.get("refresh_token"):
        # No way to refresh — surface the current token (may already be expired).
        return row["access_token"] or None

    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    lock_key = f"oauth:refresh:robinhood:{workspace_id}"
    if cache.enabled and cache.client:
        acquired = await cache.client.set(lock_key, "1", nx=True, ex=35)
        if not acquired:
            await asyncio.sleep(1)
            fresh = await db.get(user_id, workspace_id)
            return (fresh or {}).get("access_token") or None

    try:
        new = await refresh_tokens(
            token_endpoint=row["token_endpoint"],
            client_id=row["client_id"],
            client_secret=row.get("client_secret") or None,
            refresh_token=row["refresh_token"],
            resource=row["resource"],
        )
        new_expires = now + timedelta(seconds=new.get("expires_in", 3600))
        await db.set_tokens(
            user_id=user_id,
            workspace_id=workspace_id,
            access_token=new["access_token"],
            refresh_token=new["refresh_token"],
            expires_at=new_expires,
        )
        await _sync_vault_token(workspace_id, new["access_token"])
        logger.debug(
            "[robinhood_oauth] refreshed token for workspace_id=%s", workspace_id
        )
        return new["access_token"]
    except Exception as e:
        logger.error(
            "[robinhood_oauth] refresh failed for workspace_id=%s: %s",
            workspace_id, e,
        )
        return None
    finally:
        if cache.enabled and cache.client:
            try:
                await cache.client.delete(lock_key)
            except Exception:
                pass


async def _sync_vault_token(workspace_id: str, access_token: str) -> None:
    """Write the access token into the workspace vault and push to the sandbox."""
    from src.server.database.vault_secrets import (
        create_secret as create_secret_db,
        update_secret,
    )

    updated = await update_secret(
        workspace_id, "ROBINHOOD_TOKEN", value=access_token, description=None
    )
    if not updated:
        try:
            await create_secret_db(
                workspace_id,
                "ROBINHOOD_TOKEN",
                access_token,
                "Robinhood Agentic Trading MCP access token (auto-refreshed)",
            )
        except ValueError:
            pass  # raced create / cap — the update path will catch it next turn
    try:
        from src.server.services.workspace_manager import WorkspaceManager

        await WorkspaceManager.get_instance().push_vault_secrets(workspace_id)
    except Exception:
        logger.warning(
            "[robinhood_oauth] vault push failed for workspace_id=%s",
            workspace_id, exc_info=True,
        )
