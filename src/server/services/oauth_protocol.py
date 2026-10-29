"""What every subscription sign-in (Codex, Claude, Grok) shares: the token set
a token endpoint answers with, how its refusals read, and the device code flow
(RFC 8628) Codex and Grok both sign in with.

Each provider keeps only what is its own: endpoints, how its answers map onto
these types, and its table of device-poll codes.
"""

import base64
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Literal, Optional

import httpx

from src.server.services.oauth_refresh_wait import token_expiry


@dataclass(frozen=True)
class TokenSet:
    """A connection's tokens, their expiry and the identity they belong to."""

    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at: datetime
    account_id: str = ""
    email: Optional[str] = None
    plan_type: Optional[str] = None

    @classmethod
    def from_row(cls, row: dict) -> "TokenSet":
        return cls(
            access_token=row["access_token"],
            refresh_token=row["refresh_token"],
            expires_at=token_expiry(row),
            account_id=row.get("account_id") or "",
            email=row.get("email"),
            plan_type=row.get("plan_type"),
        )


def jwt_payload(token: str) -> dict:
    """The payload of a JWT, unverified: tokens are only read straight from the
    provider's token endpoint over TLS, and only identity and lifetime are
    taken from them."""
    parts = (token or "").split(".")
    if len(parts) < 2:
        return {}
    try:
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


# --- Token endpoint refusals ---------------------------------------------------

class OAuthPlanNoApiAccess(Exception):
    """The provider refused the connection because the account's plan has no
    API access. Signing in again cannot change that, so nothing is flagged
    and the user is never sent to reconnect."""

    def __init__(self, provider: str, code: Optional[str] = None):
        super().__init__(f"{provider} plan has no API access ({code})")
        self.provider = provider
        self.code = code


# Token-endpoint codes meaning this refresh token is dead for good. The three
# ``refresh_token_*`` codes are the ones the official Codex CLI treats as
# permanent (codex-rs/login/src/auth/manager.rs); ``invalid_refresh_token`` is
# what auth.openai.com returns for an unknown token (401) and
# ``invalid_grant`` what platform.claude.com returns (400), as probed.
_DEAD_TOKEN_CODES = frozenset({
    "invalid_grant",
    "invalid_refresh_token",
    "refresh_token_expired",
    "refresh_token_reused",
    "refresh_token_invalidated",
})
# 401 codes about our client registration, not the user's token: flagging
# every user for them would send them all to a reconnect that cannot help.
_CLIENT_FAULT_CODES = frozenset({
    "invalid_client",
    "unauthorized_client",
    "unsupported_grant_type",
    "invalid_scope",
})
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")


def upstream_error_code(response: httpx.Response) -> Optional[str]:
    """The machine code in an OAuth error body, if it has a safe one.

    Handles OpenAI's ``{"error": {"code": ...}}`` and RFC 6749's
    ``{"error": "invalid_grant"}``. Only the code is ever kept: the message
    and description are provider text that may quote what we sent.
    """
    try:
        body = response.json()
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    error = body.get("error")
    code = None
    if isinstance(error, dict):
        code = error.get("code") or error.get("type")
    elif isinstance(error, str):
        code = error
    if isinstance(code, str) and _SAFE_CODE_RE.match(code):
        return code.lower()
    return None


def client_fault_code(code: Optional[str]) -> bool:
    """Whether a token-endpoint code blames our client rather than the user's token."""
    return code in _CLIENT_FAULT_CODES


def permanent_refresh_failure(exc: BaseException) -> Optional[str]:
    """The code to flag the connection with, or None for a transient failure.

    A 401 is permanent unless its code blames our client, as in the official
    Codex CLI; every other status is permanent only with a dead-token code.
    """
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    status = exc.response.status_code
    code = upstream_error_code(exc.response)
    if code in _DEAD_TOKEN_CODES:
        return code
    if status == 401 and code not in _CLIENT_FAULT_CODES:
        return code or "unauthorized"
    return None


# --- Device code flow (RFC 8628) ------------------------------------------------

DevicePollStatus = Literal["pending", "denied", "expired", "success"]

# RFC 8628 3.5: each slow_down adds five seconds to the interval.
SLOW_DOWN_STEP = 5


class DeviceFlowUnavailable(Exception):
    """The provider does not offer device sign-in to this client right now."""


class DeviceUpstreamUnavailable(Exception):
    """The provider could not answer right now (network, 5xx, 429, a
    challenge); asking again can succeed."""


class DevicePollFailed(Exception):
    """The provider refused the poll with an error that no later poll will change."""

    def __init__(self, status_code: int, code: Optional[str]):
        super().__init__(f"device poll refused: status={status_code} code={code}")
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class DevicePoll:
    status: DevicePollStatus
    code: Optional[str] = None
    tokens: Optional[TokenSet] = None


@dataclass(frozen=True)
class DeviceCode:
    """A device code as issued. ``state`` is what the provider's ``poll``
    reads back; ``url_has_code`` says the page arrives with the code filled
    in, so the user only confirms it there."""

    state: dict = field(repr=False)
    user_code: str
    verification_url: str
    url_has_code: bool
    interval: int
    expires_at: datetime


@dataclass(frozen=True)
class DeviceFlow:
    """One provider's device sign-in.

    ``poll(state, expires_at)`` asks once and answers with the token set on
    success, having made whatever exchange the provider needs for it.
    ``upstream`` is how refusals name the provider to the user; ``redis_key``
    prefixes the pending flow's key and its poll lock.
    """

    provider: str
    upstream: str
    redis_key: str
    request: Callable[[], Awaitable[DeviceCode]]
    poll: Callable[[dict, datetime], Awaitable[DevicePoll]]


def positive_number(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        return float(value)
    return None
