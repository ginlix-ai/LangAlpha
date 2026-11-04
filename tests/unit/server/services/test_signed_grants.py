"""The shared grant codec signs the same string the three hand-written codecs
did, so a grant minted before it still verifies.

``_legacy_token`` is the old algorithm, written out here rather than imported:
the point is that it stays fixed while the codec may change.
"""

from __future__ import annotations

import base64
import hmac
from hashlib import sha256

import pytest

from src.server.services import file_grants
from src.server.services.artifacts import grants as artifact_grants

NOW = 1_800_000_000
EXP = NOW + 3600
WORKSPACE_ID = "22222222-2222-4222-8222-222222222222"
ARTIFACT_ID = "33333333-3333-4333-8333-333333333333"
CODE = "Ab12Cd34Ef56"


def _legacy_token(key: bytes, version: str, *fields: str) -> str:
    message = "\x00".join((version, *fields)).encode("utf-8")
    raw = hmac.new(key, message, sha256).digest()
    signature = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return ".".join((version, *fields, signature))


@pytest.fixture(autouse=True)
def _keys(monkeypatch):
    monkeypatch.setattr(file_grants._CODEC, "_key", b"f" * 32)
    monkeypatch.setattr(artifact_grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(artifact_grants._VISITOR, "_key", b"s" * 32)


@pytest.mark.asyncio
async def test_a_file_grant_minted_by_the_old_codec_verifies():
    token = _legacy_token(b"f" * 32, "v1", WORKSPACE_ID, str(EXP))

    assert await file_grants.verify_file_grant(token, now=NOW) == WORKSPACE_ID
    minted = await file_grants.mint_file_grant(WORKSPACE_ID, ttl=3600, now=NOW)
    assert minted.token == token


@pytest.mark.asyncio
async def test_an_owner_artifact_grant_minted_by_the_old_codec_verifies():
    token = _legacy_token(b"o" * 32, "v1", ARTIFACT_ID, str(EXP))

    assert await artifact_grants.verify_artifact_grant(token, now=NOW) == ARTIFACT_ID
    minted = await artifact_grants.mint_artifact_grant(ARTIFACT_ID, ttl=3600, now=NOW)
    assert minted.token == token


@pytest.mark.asyncio
async def test_a_visitor_grant_minted_by_the_old_codec_verifies():
    token = _legacy_token(b"s" * 32, "s1", CODE, ARTIFACT_ID, "4", "7", str(EXP))

    grant = await artifact_grants.verify_artifact_share_grant(token, now=NOW)
    assert (grant.code, grant.artifact_id, grant.revision, grant.seq) == (
        CODE,
        ARTIFACT_ID,
        4,
        7,
    )
    assert grant.expires_at == EXP
    minted = await artifact_grants.mint_artifact_share_grant(
        CODE, ARTIFACT_ID, revision=4, seq=7, ttl=3600, now=NOW
    )
    assert minted.token == token


def test_a_token_from_a_request_path_names_the_same_prefix_as_its_grant():
    assert file_grants.grant_prefix("v1.x.1.sig") == "/api/v1/wsfiles/g/v1.x.1.sig/"
    assert (
        artifact_grants.grant_prefix("s1.x.1.sig") == "/api/v1/artifacts/g/s1.x.1.sig/"
    )
