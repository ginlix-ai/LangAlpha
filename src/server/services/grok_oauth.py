"""
Grok OAuth service: device code flow and token refresh against auth.x.ai.

Signs a user in to xAI with the device code flow (RFC 8628) and uses the
access token as a bearer on https://api.x.ai/v1, exactly as an xAI API key is
used. Only xAI's documented OAuth endpoints are called and no client identity
is claimed beyond the public client id.

References: auth.x.ai/.well-known/openid-configuration; Hermes Agent
(hermes_cli/auth_xai.py) and OpenCode (plugin/provider/xai.ts), which sign in
the same way.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx

from src.server.services.oauth_protocol import (
    DeviceCode,
    DeviceFlow,
    DevicePoll,
    DevicePollFailed,
    DeviceUpstreamUnavailable,
    OAuthPlanNoApiAccess,
    TokenSet,
    client_fault_code,
    jwt_payload,
    permanent_refresh_failure,
    positive_number,
    upstream_error_code,
)
from src.server.services.oauth_refresh_wait import REFRESH_BUFFER

GROK_PROVIDER = "grok-oauth"
# The Grok CLI's public client, which other tools sign in with too; public
# clients carry no secret.
GROK_CLIENT_ID = "b1a00492-073a-47ea-816f-4c329264a828"
GROK_SCOPE = "openid profile email offline_access grok-cli:access api:access"
GROK_DEVICE_CODE_URL = "https://auth.x.ai/oauth2/device/code"
GROK_TOKEN_URL = "https://auth.x.ai/oauth2/token"
_DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"

# How long a device code lives when the response does not say (xAI said 30
# minutes when probed), and the most a pending flow is held whatever it says.
DEVICE_CODE_TTL = timedelta(minutes=15)
DEVICE_CODE_MAX_TTL = timedelta(hours=1)
_DEFAULT_INTERVAL = 5
_DEFAULT_TOKEN_TTL = timedelta(hours=1)


def is_xai_endpoint(url: str) -> bool:
    """Whether ``url`` is https on x.ai or a subdomain of it.

    Tokens and sign-in links only ever go to xAI's own origin, whatever a
    response names (Hermes pins the same way).
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    return parts.scheme == "https" and (host == "x.ai" or host.endswith(".x.ai"))


async def _post_form(url: str, data: dict) -> httpx.Response:
    if not is_xai_endpoint(url):
        raise ValueError("refusing to send an xAI credential off x.ai")
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        return await client.post(
            url, data=data, headers={"Accept": "application/json"}
        )


def _challenged(resp: httpx.Response) -> bool:
    """A Cloudflare challenge in front of auth.x.ai, not an answer from it."""
    return resp.headers.get("cf-mitigated", "").lower() == "challenge"


# --- Device code flow ---

def _device_expires_at(raw: Any, now: datetime) -> datetime:
    seconds = positive_number(raw)
    if seconds:
        return now + min(timedelta(seconds=int(seconds)), DEVICE_CODE_MAX_TTL)
    return now + DEVICE_CODE_TTL


async def request_device_code() -> DeviceCode:
    """Ask xAI for a device code."""
    try:
        resp = await _post_form(
            GROK_DEVICE_CODE_URL, {"client_id": GROK_CLIENT_ID, "scope": GROK_SCOPE}
        )
    except httpx.TransportError as e:
        raise DeviceUpstreamUnavailable(type(e).__name__) from e
    if resp.status_code == 429 or resp.status_code >= 500 or _challenged(resp):
        raise DeviceUpstreamUnavailable(f"status={resp.status_code}")
    resp.raise_for_status()
    data = resp.json()

    # The complete URL carries the code, so the user only confirms it. Either
    # is shown to the user to open, so neither is taken off x.ai.
    complete = data.get("verification_uri_complete")
    plain = data.get("verification_uri")
    if isinstance(complete, str) and is_xai_endpoint(complete):
        url, has_code = complete, True
    elif isinstance(plain, str) and is_xai_endpoint(plain):
        url, has_code = plain, False
    else:
        raise ValueError("xAI named no sign-in page on x.ai")

    interval = data.get("interval")
    return DeviceCode(
        state={"device_code": data["device_code"]},
        user_code=data["user_code"],
        verification_url=url,
        url_has_code=has_code,
        interval=interval if isinstance(interval, int) and interval > 0 else _DEFAULT_INTERVAL,
        expires_at=_device_expires_at(data.get("expires_in"), datetime.now(timezone.utc)),
    )


def classify_device_poll(
    status_code: int, code: Optional[str], *, expired_locally: bool
) -> DevicePoll:
    """Read one non-200 answer of the token endpoint to a device grant.

    RFC 8628 3.5's codes: ``authorization_pending`` and ``slow_down`` mean
    wait (the caller widens the interval on ``slow_down``), ``access_denied``
    a refusal (xAI's own spelling ``authorization_denied`` too) and
    ``expired_token`` an expiry. Any other refusal ends the flow, as an expiry
    once the code's lifetime is up, since some servers answer a spent code
    with ``invalid_grant``.
    """
    if code in ("authorization_pending", "slow_down"):
        return DevicePoll("pending", code=code)
    if code in ("access_denied", "authorization_denied"):
        return DevicePoll("denied", code=code)
    if code == "expired_token":
        return DevicePoll("expired", code=code)
    if status_code == 429 or status_code >= 500:
        raise DeviceUpstreamUnavailable(f"status={status_code} code={code}")
    if expired_locally:
        return DevicePoll("expired", code=code or "device_code_expired")
    raise DevicePollFailed(status_code, code)


async def poll_device(state: dict, expires_at: datetime) -> DevicePoll:
    """One poll of the token endpoint for the device grant; a success carries
    the token set."""
    try:
        resp = await _post_form(
            GROK_TOKEN_URL,
            {
                "grant_type": _DEVICE_GRANT,
                "client_id": GROK_CLIENT_ID,
                "device_code": state["device_code"],
            },
        )
    except httpx.TransportError as e:
        raise DeviceUpstreamUnavailable(type(e).__name__) from e
    if resp.status_code == 200:
        return DevicePoll("success", tokens=token_set(resp.json()))
    if _challenged(resp):
        raise DeviceUpstreamUnavailable(f"status={resp.status_code} challenge")
    return classify_device_poll(
        resp.status_code,
        upstream_error_code(resp),
        expired_locally=datetime.now(timezone.utc) >= expires_at,
    )


# --- Tokens ---

def access_token_expiry(data: dict, now: datetime) -> datetime:
    """``expires_in``, else the access token's own ``exp``, else an hour.

    Never the id_token's ``exp``: that is the OIDC session's, which may
    outlast the access token (OpenCode reads it the same way).
    """
    expires_in = positive_number(data.get("expires_in"))
    if expires_in:
        return now + timedelta(seconds=expires_in)
    exp = positive_number(jwt_payload(data.get("access_token", "")).get("exp"))
    if exp:
        return datetime.fromtimestamp(exp, tz=timezone.utc)
    return now + _DEFAULT_TOKEN_TTL


def token_set(data: dict, *, previous_refresh_token: str = "") -> TokenSet:
    """A token endpoint answer as stored.

    xAI may leave ``refresh_token`` out of a refresh answer, and the one held
    then stays good. No plan is read: xAI's tokens carry no claim naming it,
    so ``plan_type`` stays None rather than a guess.
    """
    refresh_token = data.get("refresh_token") or previous_refresh_token
    if not data.get("access_token") or not refresh_token:
        raise ValueError("xAI token answer is missing a token")
    claims = jwt_payload(data.get("id_token") or "") or jwt_payload(data["access_token"])
    return TokenSet(
        access_token=data["access_token"],
        refresh_token=refresh_token,
        expires_at=access_token_expiry(data, datetime.now(timezone.utc)),
        account_id=str(claims.get("sub") or ""),
        email=claims.get("email") if isinstance(claims.get("email"), str) else None,
    )


# Hermes' skew: refresh with an hour left, so a turn that starts on a token
# has an hour of it, but only two minutes before expiry for the short tokens a
# device-code sign-in can hand out, which an hour's skew would refresh on
# every use. Between the two, the skew is at most a quarter of the token's
# life: one no larger than the life refreshes a fresh token on every use.
_LONG_SKEW = timedelta(hours=1)
_SHORT_SKEW = timedelta(minutes=2)
_SHORT_LIFETIME = timedelta(minutes=45)
_MAX_SKEW_SHARE = 4


def refresh_buffer(tokens: TokenSet) -> timedelta:
    """How close to expiry the stored access token is refreshed rather than used.

    Judged by the token's issued lifetime (``exp - iat``), not the time left,
    so a long token is not used down to its last two minutes once it nears
    the end. Without both claims, the buffer every subscription gets.
    """
    claims = jwt_payload(tokens.access_token)
    exp, iat = positive_number(claims.get("exp")), positive_number(claims.get("iat"))
    if not (exp and iat):
        return REFRESH_BUFFER
    lifetime = timedelta(seconds=exp - iat)
    if lifetime <= _SHORT_LIFETIME:
        return _SHORT_SKEW
    return min(_LONG_SKEW, lifetime / _MAX_SKEW_SHARE)


async def refresh_tokens(refresh_token: str) -> TokenSet:
    """Trade the refresh token for a new token set.

    A 403 is xAI saying the account's plan does not include API access,
    which a new sign-in cannot change (Hermes reads it so), unless it is a
    Cloudflare challenge in front of the endpoint.
    """
    resp = await _post_form(
        GROK_TOKEN_URL,
        {
            "grant_type": "refresh_token",
            "client_id": GROK_CLIENT_ID,
            "refresh_token": refresh_token,
        },
    )
    if resp.status_code == 403 and not _challenged(resp):
        raise OAuthPlanNoApiAccess(GROK_PROVIDER, upstream_error_code(resp))
    resp.raise_for_status()
    return token_set(resp.json(), previous_refresh_token=refresh_token)


def permanent_grok_refresh_failure(exc: BaseException) -> Optional[str]:
    """``permanent_refresh_failure``, plus any 400 that does not
    blame our client: xAI refuses a dead refresh token with a 400 or a 401,
    and Hermes asks for a new sign-in on either."""
    code = permanent_refresh_failure(exc)
    if code is not None:
        return code
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 400:
        code = upstream_error_code(exc.response)
        if not client_fault_code(code):
            return code or "bad_request"
    return None


# Bodies of a 403 that refuse the token itself, not the plan: xAI answers a
# stale or just-rotated token so (Hermes issue #108122), and a refresh fixes it.
_STALE_TOKEN_MARKERS = ("unauthenticated", "bad-credentials", "could not be validated")


def stale_token_refusal(status_code: Any, error_text: str) -> bool:
    """Whether an inference refusal on a Grok token is of the token itself.

    A 401 always is. A 403 is the plan's lack of API access, unless its body
    says the token could not be authenticated.
    """
    if status_code == 401:
        return True
    if status_code != 403:
        return False
    lowered = (error_text or "").lower()
    return any(marker in lowered for marker in _STALE_TOKEN_MARKERS)


def plan_refusal(status_code: Any, error_text: str) -> bool:
    """Whether a 403 on a Grok token is the plan's lack of API access."""
    return status_code == 403 and not stale_token_refusal(status_code, error_text)


DEVICE_FLOW = DeviceFlow(GROK_PROVIDER, "SpaceXSI", "oauth:grok:device", request_device_code, poll_device)
