"""What a published version may fetch, on its artifact's host and on the API origin.

On its own host the version's origin is its own, so ``'self'`` is the
artifact. On the API origin ``'self'`` would be every API route, so a
sandboxed version names the route prefix it was asked through on the
configured origins, and none of the API when no origin is configured.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.server.database.artifacts import StoredVersionFile
from src.server.preview_proxy import artifacts as artifact_proxy
from src.server.services.artifacts import grants
from src.server.services.artifacts.grants import (
    mint_artifact_grant,
    mint_artifact_share_grant,
)
from src.server.utils.html_cdns import SCRIPT_CDNS

ARTIFACT = "11111111-2222-4333-8444-555555555555"
_ORIGINS = "src.server.utils.served_origins"
_APP = "https://app.example.com"
_STORED = StoredVersionFile(
    user_id="owner-1",
    sha256="a" * 64,
    byte_len=2,
    content_type="application/json",
    content=b"{}",
)


@pytest.fixture(autouse=True)
def _stored(monkeypatch):
    monkeypatch.setattr(grants._OWNER, "_key", b"o" * 32)
    monkeypatch.setattr(grants._VISITOR, "_key", b"s" * 32)
    monkeypatch.setattr(
        artifact_proxy, "get_version_file", AsyncMock(return_value=_STORED)
    )
    monkeypatch.setattr(
        artifact_proxy, "get_shared_version_file", AsyncMock(return_value=_STORED)
    )


def _connect_src(headers: dict[str, str]) -> list[str]:
    csp = headers["content-security-policy"]
    return next(d for d in csp.split(";") if d.strip().startswith("connect-src")).split()[1:]


async def _headers(grant: str, **kwargs) -> dict[str, str]:
    served = await artifact_proxy.load_artifact_file(
        grant, "1", "data/prices.json", **kwargs
    )
    assert served is not None
    return dict(served.headers)


@pytest.mark.asyncio
async def test_on_the_api_origin_a_version_reaches_only_its_grant_prefix(monkeypatch):
    monkeypatch.setattr(f"{_ORIGINS}.SERVER_BASE_URL", _APP)
    monkeypatch.setattr(f"{_ORIGINS}.PUBLIC_APP_URL", f"{_APP}/")
    owner = (await mint_artifact_grant(ARTIFACT)).token
    visitor = (
        await mint_artifact_share_grant("k3Vq9ZtR2mXa", ARTIFACT, revision=1, seq=1)
    ).token
    for grant in (owner, visitor):
        headers = await _headers(grant, sandboxed=True)
        assert _connect_src(headers) == [
            f"{_APP}/api/v1/artifacts/g/{grant}/",
            *SCRIPT_CDNS.split(),
        ]
        assert headers["access-control-allow-origin"] == "*"


@pytest.mark.asyncio
async def test_through_a_share_route_a_version_reaches_that_route_alone(monkeypatch):
    monkeypatch.setattr(f"{_ORIGINS}.SERVER_BASE_URL", _APP)
    monkeypatch.setattr(f"{_ORIGINS}.PUBLIC_APP_URL", f"{_APP}/")
    prefix = "/api/v1/public/shared/k3Vq9ZtR2mXa/files/serve/"
    served = await artifact_proxy.serve_version_file(
        _STORED, visitor=True, prefix=prefix, sandboxed=True
    )
    assert _connect_src(dict(served.headers)) == [
        f"{_APP}{prefix}",
        *SCRIPT_CDNS.split(),
    ]


@pytest.mark.asyncio
async def test_the_pdf_renderer_reaches_the_route_it_was_asked_through(monkeypatch):
    # A visitor's PDF of a report that fetches its data renders from the
    # renderer's own origin; without it the fetch is blocked and the PDF blank.
    monkeypatch.setattr(f"{_ORIGINS}.SERVER_BASE_URL", _APP)
    monkeypatch.setattr(f"{_ORIGINS}.PUBLIC_APP_URL", f"{_APP}/")
    monkeypatch.setattr(f"{_ORIGINS}.PDF_RENDER_INTERNAL_BASE", "http://127.0.0.1:8000")
    prefix = "/api/v1/public/shared/k3Vq9ZtR2mXa/files/serve/"
    served = await artifact_proxy.serve_version_file(
        _STORED, visitor=True, prefix=prefix, sandboxed=True, host="127.0.0.1:8000"
    )
    assert _connect_src(dict(served.headers)) == [
        f"{_APP}{prefix}",
        f"http://127.0.0.1:8000{prefix}",
        *SCRIPT_CDNS.split(),
    ]


@pytest.mark.asyncio
async def test_with_no_origin_configured_a_sandboxed_version_reaches_no_api_route(
    monkeypatch,
):
    monkeypatch.setattr(f"{_ORIGINS}.SERVER_BASE_URL", "")
    monkeypatch.setattr(f"{_ORIGINS}.PUBLIC_APP_URL", "")
    headers = await _headers((await mint_artifact_grant(ARTIFACT)).token, sandboxed=True)
    assert _connect_src(headers) == SCRIPT_CDNS.split()


@pytest.mark.asyncio
async def test_on_its_own_host_self_is_the_artifact():
    headers = await _headers(
        (await mint_artifact_grant(ARTIFACT)).token, host_artifact_id=ARTIFACT
    )
    assert _connect_src(headers) == ["'self'", *SCRIPT_CDNS.split()]
    # The app reads a file it cannot frame from here, with no credential.
    assert headers["access-control-allow-origin"] == "*"
