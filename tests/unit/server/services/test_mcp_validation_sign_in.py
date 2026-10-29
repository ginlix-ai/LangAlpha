"""When a sign-in challenge means "sign in" and when it means "your key was refused".

A remote server that signs in with OAuth answers a probe with the same 401
whatever was sent. Sent a key, it turned the key down, and the user is told
so; sent only plain config (a region, a tenant), it was offered nothing in
place of a sign-in, and the save is answered with the sign-in that saves it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_probe import ProbeOutcome
from src.server.services.mcp_validation import CheckFailure, SignInNeeded, check_server

_URL = "https://mcp.example.com/mcp"


async def _check(
    headers: dict[str, str], *, bearer: str | None = None, sent_log: list | None = None
):
    async def challenge(url, sent, *, credential, **_kwargs):
        if sent_log is not None:
            sent_log.append(dict(sent))
        # The probe reports what it was told went out.
        return ProbeOutcome(
            ok=False, auth="oauth", http_status=401, sent_credential=credential,
            error="server answered HTTP 401: it wants an OAuth connection",
        )

    server = McpServerInput(name="srv", transport="http", url=_URL, headers=headers)
    with (
        patch("src.server.services.mcp_probe.bounded_probe", new=challenge),
        patch(
            "src.server.services.mcp_probe.get_user_secrets_decrypted",
            new=AsyncMock(return_value={"KEY": "stored-key-value"}),
        ),
    ):
        return await check_server("u-1", server, bearer=bearer)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-Region": "us"},
        {"Accept-Language": "en", "X-Tenant": "acme"},
        # Never sent: the protocol frames its own session.
        {"Mcp-Session-Id": "forged"},
    ],
)
async def test_plain_config_is_not_a_credential_so_the_answer_is_a_sign_in(headers):
    result = await _check(headers)

    assert result == SignInNeeded(server="srv", url=_URL)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"X-Region": "${vault:KEY}"},
        {"Authorization": "Bearer abc"},
        {"X-Api-Key": "abc"},
        {"X-Subscriptionkey": "abc"},
        {"X-Auth-Token": "abc"},
        {"X-Client-Secret": "abc"},
        {"Cookie": "sid=1"},
        {"X-Session": "s-1"},
        {"X-Password": "hunter2"},
        # A long opaque value reads as a key whatever it is called.
        {"X-Tenant": "a1b2c3d4e5f6g7h8i9j0k1l2"},
        {"X-Region": "us", "X-Api-Key": "abc"},
    ],
)
async def test_a_turned_down_credential_is_reported_not_signed_past(headers):
    result = await _check(headers)

    assert isinstance(result, CheckFailure)
    assert (result.server, result.verdict) == ("srv", "credential_rejected")
    assert "remove the key to sign in instead" in result.reason


@pytest.mark.asyncio
async def test_a_sign_in_the_server_turned_down_asks_for_a_new_one():
    result = await _check({"X-Region": "us"}, bearer="tok")

    assert isinstance(result, CheckFailure)
    assert result.verdict == "credential_rejected"
    assert result.reason == "the server did not accept the sign-in; connect it again"


@pytest.mark.asyncio
async def test_a_signed_in_check_sends_the_token_alone():
    # Discovery and the relay send nothing else once a server is signed in
    # to, so a configured key is neither sent nor required to resolve.
    sent: list[dict[str, str]] = []

    await _check(
        {"X-Api-Key": "${vault:MISSING}", "X-Region": "us"}, bearer="tok", sent_log=sent
    )

    assert sent == [{"Authorization": "Bearer tok"}]
