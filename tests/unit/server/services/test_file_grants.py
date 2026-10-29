"""The file grant: one workspace, one expiry, one signature over both.

A grant replaces the bare workspace UUID as what a file URL proves, so the
tests here are the ways it must stop opening a workspace: after its expiry,
when any of its three parts is moved, and when it is not a ``v1`` grant at
all. The signing key is read from the database once; it is mocked here and
the module cache is cleared around each test so no test signs with another's
key.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.server.services import file_grants
from src.server.services.file_grants import (
    DEFAULT_TTL_SECONDS,
    FileGrantError,
    grant_prefix,
    mint_file_grant,
    seconds_left,
    verify_file_grant,
)

KEY = b"unit-test-file-grant-key-00000000"
OTHER_KEY = b"unit-test-file-grant-key-11111111"
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
OTHER_WORKSPACE_ID = "33333333-3333-4333-8333-333333333333"
NOW = 1_800_000_000


@pytest.fixture(autouse=True)
def signing_key():
    file_grants._key = None
    with patch.object(
        file_grants, "get_server_key", AsyncMock(return_value=KEY)
    ) as loader:
        yield loader
    file_grants._key = None


@pytest.mark.asyncio
async def test_round_trip():
    grant = await mint_file_grant(WORKSPACE_ID, now=NOW)

    assert grant.workspace_id == WORKSPACE_ID
    assert grant.expires_at == NOW + DEFAULT_TTL_SECONDS
    assert grant.token.split(".")[:3] == ["v1", WORKSPACE_ID, str(grant.expires_at)]
    assert await verify_file_grant(grant.token, now=NOW) == WORKSPACE_ID


@pytest.mark.asyncio
async def test_the_key_is_read_once_per_process(signing_key):
    await mint_file_grant(WORKSPACE_ID, now=NOW)
    await mint_file_grant(WORKSPACE_ID, now=NOW)
    assert signing_key.await_count == 1


@pytest.mark.asyncio
async def test_it_is_live_up_to_its_expiry_and_not_past_it():
    grant = await mint_file_grant(WORKSPACE_ID, ttl=60, now=NOW)

    assert await verify_file_grant(grant.token, now=NOW + 60) == WORKSPACE_ID
    with pytest.raises(FileGrantError, match="expired"):
        await verify_file_grant(grant.token, now=NOW + 61)


@pytest.mark.asyncio
async def test_a_grant_cannot_be_moved_onto_another_workspace():
    _v, _ws, exp, sig = (await mint_file_grant(WORKSPACE_ID, now=NOW)).token.split(".")
    with pytest.raises(FileGrantError, match="signature"):
        await verify_file_grant(f"v1.{OTHER_WORKSPACE_ID}.{exp}.{sig}", now=NOW)


@pytest.mark.asyncio
async def test_a_stretched_expiry_is_refused():
    _v, ws, exp, sig = (await mint_file_grant(WORKSPACE_ID, now=NOW)).token.split(".")
    with pytest.raises(FileGrantError, match="signature"):
        await verify_file_grant(f"v1.{ws}.{int(exp) + 3600}.{sig}", now=NOW)


@pytest.mark.asyncio
async def test_a_tampered_signature_is_refused():
    _v, ws, exp, sig = (await mint_file_grant(WORKSPACE_ID, now=NOW)).token.split(".")
    flipped = ("B" if sig[0] != "B" else "C") + sig[1:]
    with pytest.raises(FileGrantError, match="signature"):
        await verify_file_grant(f"v1.{ws}.{exp}.{flipped}", now=NOW)


@pytest.mark.asyncio
async def test_a_non_ascii_signature_is_refused_not_raised():
    _v, ws, exp, _sig = (await mint_file_grant(WORKSPACE_ID, now=NOW)).token.split(".")
    with pytest.raises(FileGrantError, match="signature"):
        await verify_file_grant(f"v1.{ws}.{exp}.é", now=NOW)


@pytest.mark.asyncio
async def test_another_key_cannot_mint_one(signing_key):
    signing_key.return_value = OTHER_KEY
    foreign = await mint_file_grant(WORKSPACE_ID, now=NOW)

    file_grants._key = None
    signing_key.return_value = KEY
    with pytest.raises(FileGrantError):
        await verify_file_grant(foreign.token, now=NOW)


@pytest.mark.asyncio
async def test_a_wrong_version_prefix_is_refused():
    _v, rest = (await mint_file_grant(WORKSPACE_ID, now=NOW)).token.split(".", 1)
    with pytest.raises(FileGrantError, match="malformed"):
        await verify_file_grant(f"v2.{rest}", now=NOW)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token",
    [
        "",
        None,
        "garbage",
        f"v1.{WORKSPACE_ID}",
        f"v1.{WORKSPACE_ID}.{NOW}",
        f"v1.{WORKSPACE_ID}.{NOW}.",
        f"v1..{NOW}.sig",
        f"v1.{WORKSPACE_ID}.soon.sig",
        f"v1.{WORKSPACE_ID}.{NOW}.sig.extra",
    ],
)
async def test_garbage_is_refused_without_consulting_the_key(token, signing_key):
    with pytest.raises(FileGrantError, match="malformed"):
        await verify_file_grant(token, now=NOW)
    assert signing_key.await_count == 0


@pytest.mark.asyncio
async def test_the_prefix_is_the_grant_route_ready_for_a_path():
    grant = await mint_file_grant(WORKSPACE_ID, now=NOW)
    assert grant_prefix(grant) == f"/api/v1/wsfiles/g/{grant.token}/"


def test_a_lifetime_goes_out_as_seconds_left_and_never_below_zero():
    # The web adds it to its own clock on receipt, so it is never a timestamp.
    assert seconds_left(NOW + DEFAULT_TTL_SECONDS, now=NOW) == DEFAULT_TTL_SECONDS
    assert seconds_left(NOW - 5, now=NOW) == 0
