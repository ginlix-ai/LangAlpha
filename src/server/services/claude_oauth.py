"""
Claude OAuth service: PKCE authorization code flow, token exchange and refresh.

Handles the OAuth 2.0 PKCE flow for connecting Claude models via users'
existing Anthropic subscriptions. Unlike Codex (device code flow), Anthropic
uses a hosted callback page where the user copies a code#state string.

Protocol details (discovered from @mariozechner/pi-ai 0.58.0):
- Authorize URL: https://claude.ai/oauth/authorize
- Token URL: https://platform.claude.com/v1/oauth/token
- Redirect URI: https://platform.claude.com/oauth/code/callback (hosted)
- PKCE: S256, with a random state: the callback page shows the state in the
  code the user pastes, so it must not be the verifier
- Token exchange: Content-Type: application/json (not form-urlencoded)
- Extra authorize param: code=true

A Claude Code setup token is the same grant asked for the way
``claude setup-token`` asks: inference scope only, on the CLI's authorize
host, for a year (``authorize_setup_token``, ``mint_setup_token``).
"""

import base64
import hashlib
import logging
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from src.server.services.oauth_protocol import TokenSet, upstream_error_code

logger = logging.getLogger(__name__)

# --- Constants ---

CLAUDE_PROVIDER = "claude-oauth"
CLAUDE_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
CLAUDE_AUTHORIZE_URL = "https://claude.ai/oauth/authorize"
CLAUDE_TOKEN_URL = "https://platform.claude.com/v1/oauth/token"
CLAUDE_REDIRECT_URI = "https://platform.claude.com/oauth/code/callback"
CLAUDE_SCOPES = "org:create_api_key user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
#: Where ``claude setup-token`` sends the user, and the one scope it asks for.
SETUP_TOKEN_AUTHORIZE_URL = "https://claude.com/cai/oauth/authorize"
SETUP_TOKEN_SCOPE = "user:inference"
#: The lifetime ``claude setup-token`` asks for: a year.
SETUP_TOKEN_LIFETIME_S = 31_536_000
#: Shorter than this and the grant is not the one asked for: the token would
#: lapse long before the user is told to sign in again.
SETUP_TOKEN_MIN_LIFETIME_S = 360 * 86_400


# --- PKCE ---

def generate_pkce_pair() -> tuple[str, str]:
    """Generate a PKCE verifier and S256 challenge.

    Returns:
        (verifier, challenge)
    """
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def generate_authorize_url(
    authorize_url: str = CLAUDE_AUTHORIZE_URL, scope: str = CLAUDE_SCOPES
) -> tuple[str, str, str]:
    """A sign-in: its authorize URL, and the verifier and state to keep
    server-side. The params and their order are the CLI's.
    """
    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(32)
    params = {
        "code": "true",
        "client_id": CLAUDE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": CLAUDE_REDIRECT_URI,
        "scope": scope,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    return f"{authorize_url}?{urlencode(params)}", verifier, state


def authorize_setup_token() -> tuple[str, str, str]:
    """A sign-in for a Claude Code setup token, as ``claude setup-token`` starts one."""
    return generate_authorize_url(SETUP_TOKEN_AUTHORIZE_URL, SETUP_TOKEN_SCOPE)


# --- Callback input parsing ---

def parse_callback_input(raw: str) -> tuple[str, str]:
    """Parse various callback input formats into (code, state).

    Supported formats:
    - Full URL: https://platform.claude.com/oauth/code/callback?code=X&state=Y
    - code#state
    - Just the code (state derived from context)

    Returns:
        (code, state)

    Raises:
        ValueError: If input cannot be parsed.
    """
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty callback input")

    # Try as full URL with query params
    if raw.startswith("http"):
        parsed = urlparse(raw)
        qs = parse_qs(parsed.query)
        code = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]
        if code and state:
            return code, state
        # Try fragment
        frag_qs = parse_qs(parsed.fragment)
        code = code or frag_qs.get("code", [None])[0]
        state = state or frag_qs.get("state", [None])[0]
        if code and state:
            return code, state

    # Try code#state format
    if "#" in raw:
        parts = raw.split("#", 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts[0].strip(), parts[1].strip()

    # Try code=X&state=Y query string format
    if "code=" in raw and "state=" in raw:
        qs = parse_qs(raw)
        code = qs.get("code", [None])[0]
        state = qs.get("state", [None])[0]
        if code and state:
            return code, state

    raise ValueError(
        "Could not parse callback input. "
        "Expected full URL, code#state, or code=X&state=Y format."
    )


# --- Token exchange ---

async def _token_request(body: dict) -> dict:
    # Anthropic takes a JSON body, not form-urlencoded.
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(CLAUDE_TOKEN_URL, json={**body, "client_id": CLAUDE_CLIENT_ID})
    if resp.status_code >= 400:
        # The code only: the body is provider text that may quote the grant.
        logger.error(
            f"[claude_oauth] {body['grant_type']} failed: status={resp.status_code} "
            f"code={upstream_error_code(resp)}"
        )
    resp.raise_for_status()
    return resp.json()


async def _exchange(code: str, state: str, code_verifier: str, **extra) -> dict:
    return await _token_request({
        "grant_type": "authorization_code",
        "code": code,
        "state": state,
        "redirect_uri": CLAUDE_REDIRECT_URI,
        "code_verifier": code_verifier,
        **extra,
    })


async def exchange_code(code: str, state: str, code_verifier: str) -> TokenSet:
    return token_set(await _exchange(code, state, code_verifier))


async def refresh_tokens(refresh_token: str) -> TokenSet:
    return token_set(await _token_request({"grant_type": "refresh_token", "refresh_token": refresh_token}))


class UnexpectedGrant(ValueError):
    """The token endpoint answered a setup-token exchange with some other grant."""


@dataclass(frozen=True)
class SetupToken:
    """A minted setup token and what the UI and logs know it by: its uuid,
    never the token. The grant's refresh token is dropped, as
    ``claude setup-token`` drops it: the user signs in again after a year."""

    access_token: str = field(repr=False)
    expires_at: datetime
    token_uuid: str
    email: str | None


async def mint_setup_token(code: str, state: str, code_verifier: str) -> SetupToken:
    """Exchange a setup-token sign-in's code, as the CLI does: a year asked
    for, and no scope, which the authorize step fixed."""
    data = await _exchange(code, state, code_verifier, expires_in=SETUP_TOKEN_LIFETIME_S)
    lifetime, token, account = data.get("expires_in"), data.get("access_token"), data.get("account") or {}
    if (
        data.get("scope") != SETUP_TOKEN_SCOPE or not isinstance(lifetime, int) or lifetime < SETUP_TOKEN_MIN_LIFETIME_S
        or not isinstance(token, str) or not token or not isinstance(account, dict)
    ):
        raise UnexpectedGrant(
            f"scope={data.get('scope')!r} expires_in={lifetime!r} "
            f"access_token={type(token).__name__} account={type(account).__name__}"
        )
    email = account.get("email_address")
    return SetupToken(
        access_token=token,
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=lifetime),
        token_uuid=str(data.get("token_uuid") or ""),
        email=email if isinstance(email, str) else None,
    )


def token_set(data: dict) -> TokenSet:
    """A token endpoint answer as stored. Anthropic's tokens name no account
    or plan, so the identity stays empty."""
    return TokenSet(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600)),
    )
