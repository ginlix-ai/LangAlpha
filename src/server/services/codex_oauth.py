"""
Codex OAuth service: device code flow and token refresh against auth.openai.com.

Handles the OAuth 2.0 device code flow (RFC 8628) for connecting ChatGPT Codex
models via users' existing ChatGPT Plus/Pro/Team subscriptions.

References:
- Official Codex CLI: github.com/openai/codex (codex-rs/login/src/device_code_auth.rs)
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from src.llms.extension.codex import CODEX_PROVIDER
from src.server.services.oauth_protocol import (
    DeviceCode,
    DeviceFlow,
    DeviceFlowUnavailable,
    DevicePoll,
    DevicePollFailed,
    DeviceUpstreamUnavailable,
    TokenSet,
    jwt_payload,
    upstream_error_code,
)

# --- Constants (matching official Codex CLI) ---

CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_TOKEN_URL = "https://auth.openai.com/oauth/token"

# Device code flow endpoints (codex-rs/login/src/device_code_auth.rs)
CODEX_DEVICE_USERCODE_URL = "https://auth.openai.com/api/accounts/deviceauth/usercode"
CODEX_DEVICE_TOKEN_URL = "https://auth.openai.com/api/accounts/deviceauth/token"
CODEX_DEVICE_CALLBACK = "https://auth.openai.com/deviceauth/callback"
CODEX_DEVICE_VERIFY_URL = "https://auth.openai.com/codex/device"


# --- Device code flow ---

# How long a device code lives when the response does not say; the official
# CLI waits 15 minutes (device_code_auth.rs).
DEVICE_CODE_TTL = timedelta(minutes=15)


def _expires_at(raw: Any) -> datetime:
    now = datetime.now(timezone.utc)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            # Never trust a lifetime longer than the CLI's own wait.
            return min(parsed, now + DEVICE_CODE_TTL)
        except ValueError:
            pass
    return now + DEVICE_CODE_TTL


async def request_device_code() -> DeviceCode:
    """Request a device code from OpenAI; ``DeviceFlowUnavailable`` on a 404,
    which is device-code login not being enabled for this client (the
    official CLI words it that way)."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                CODEX_DEVICE_USERCODE_URL,
                json={"client_id": CODEX_CLIENT_ID},
            )
    except httpx.TransportError as e:
        raise DeviceUpstreamUnavailable(type(e).__name__) from e
    if resp.status_code == 404:
        raise DeviceFlowUnavailable()
    if resp.status_code == 429 or resp.status_code >= 500:
        raise DeviceUpstreamUnavailable(f"status={resp.status_code}")
    resp.raise_for_status()
    data = resp.json()
    return DeviceCode(
        state={"device_auth_id": data["device_auth_id"], "user_code": data["user_code"]},
        user_code=data["user_code"],
        verification_url=CODEX_DEVICE_VERIFY_URL,
        url_has_code=False,
        interval=int(data.get("interval", "5")),
        expires_at=_expires_at(data.get("expires_at")),
    )


def classify_device_poll(
    status_code: int, code: Optional[str], *, expired_locally: bool
) -> DevicePoll:
    """Read one non-200 answer of the device token endpoint.

    Probed live: 403 ``deviceauth_authorization_pending`` while the user has
    not approved, 403 ``deviceauth_user_code_expired`` once the code's 15
    minutes are up, 404 ``deviceauth_not_found`` for an id OpenAI does not
    know, 400 ``deviceauth_user_code_mismatch`` for a wrong code. The official CLI
    keeps polling on any 403 or 404 until its 15 minutes are up, so a 404
    before the code's expiry is read as pending too (a code just issued may
    not be visible yet), and as expired after it. A code naming a denial or an
    expiry is taken at its word.
    """
    lowered = (code or "").lower()
    if any(word in lowered for word in ("denied", "declined", "rejected")):
        return DevicePoll("denied", code=code)
    if "expired" in lowered:
        return DevicePoll("expired", code=code)
    if status_code == 403:
        return DevicePoll("pending", code=code)
    if status_code == 404:
        if expired_locally:
            return DevicePoll("expired", code=code or "device_code_expired")
        return DevicePoll("pending", code=code)
    if status_code == 429 or status_code >= 500:
        raise DeviceUpstreamUnavailable(f"status={status_code} code={code}")
    raise DevicePollFailed(status_code, code)


async def poll_device(state: dict, expires_at: datetime) -> DevicePoll:
    """One poll; once approved, the authorization code it hands back is
    exchanged here, so a success carries the token set."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                CODEX_DEVICE_TOKEN_URL,
                json={"device_auth_id": state["device_auth_id"], "user_code": state["user_code"]},
            )
    except httpx.TransportError as e:
        raise DeviceUpstreamUnavailable(type(e).__name__) from e
    if resp.status_code == 200:
        data = resp.json()
        tokens = await exchange_device_code(data["authorization_code"], data["code_verifier"])
        return DevicePoll("success", tokens=tokens)
    return classify_device_poll(
        resp.status_code,
        upstream_error_code(resp),
        expired_locally=datetime.now(timezone.utc) >= expires_at,
    )


async def _token_request(form: dict) -> TokenSet:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            CODEX_TOKEN_URL,
            data={**form, "client_id": CODEX_CLIENT_ID},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    resp.raise_for_status()
    return token_set(resp.json())


async def exchange_device_code(authorization_code: str, code_verifier: str) -> TokenSet:
    return await _token_request({
        "grant_type": "authorization_code",
        "code": authorization_code,
        "redirect_uri": CODEX_DEVICE_CALLBACK,
        "code_verifier": code_verifier,
    })


async def refresh_tokens(refresh_token: str) -> TokenSet:
    """Trade the single-use refresh token for a new token set."""
    return await _token_request({"grant_type": "refresh_token", "refresh_token": refresh_token})


DEVICE_FLOW = DeviceFlow(CODEX_PROVIDER, "OpenAI", "oauth:device", request_device_code, poll_device)


# --- Tokens ---

def parse_jwt_claims(token: str) -> dict:
    """The identity and lifetime claims of an OpenAI token.

    Extracts chatgpt_account_id with the same priority as OpenCode:
    1. Root claim: chatgpt_account_id
    2. Namespaced: https://api.openai.com/auth → chatgpt_account_id
    3. Fallback: organizations[0].id

    Returns:
        {account_id, email, plan_type, exp}
    """
    payload = jwt_payload(token)
    auth_info = payload.get("https://api.openai.com/auth", {})

    account_id = (
        payload.get("chatgpt_account_id")
        or auth_info.get("chatgpt_account_id")
    )
    if not account_id:
        orgs = payload.get("organizations") or payload.get(
            "https://api.openai.com/organizations", []
        )
        if orgs and isinstance(orgs, list):
            account_id = orgs[0].get("id", "")

    if not account_id:
        account_id = auth_info.get("user_id", payload.get("sub", ""))

    return {
        "account_id": account_id or "",
        "email": payload.get("email"),
        "plan_type": auth_info.get("chatgpt_plan_type") or auth_info.get("plan_type"),
        "exp": payload.get("exp"),
    }


def token_set(data: dict) -> TokenSet:
    """A token endpoint answer as stored. Identity and expiry come from the
    id_token; the account falls back to the access token's claim."""
    claims = parse_jwt_claims(data.get("id_token") or "")
    exp = claims["exp"]
    return TokenSet(
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
        expires_at=(
            datetime.fromtimestamp(exp, tz=timezone.utc)
            if exp
            else datetime.now(timezone.utc) + timedelta(hours=1)
        ),
        account_id=claims["account_id"] or parse_jwt_claims(data["access_token"])["account_id"],
        email=claims["email"],
        plan_type=claims["plan_type"],
    )


def request_headers(tokens: TokenSet) -> dict[str, str]:
    """The ChatGPT backend serves the account the token names in this header."""
    return {"ChatGPT-Account-Id": tokens.account_id} if tokens.account_id else {}
