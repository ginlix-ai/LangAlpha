"""The one policy agent-written HTML is served under, live and published.

Pinned whole, byte for byte, because a directive that drifts is a reach that
changed: the live file and a version differ only where ``html_document_csp``
says they do (images from any https host and fetches to the CDNs, a version's
alone).
"""

from __future__ import annotations

import pytest

from src.server.app.workspace_files import serve
from src.server.preview_proxy import artifacts as artifact_proxy

_ORIGINS = "src.server.utils.served_origins"
_APP = "https://app.example.com"
_CDNS = (
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com "
    "https://esm.sh"
)
_SANDBOX = "sandbox allow-scripts allow-popups allow-popups-to-escape-sandbox; "
_LOADS = (
    "default-src 'none'; "
    f"script-src 'self' 'unsafe-inline' {_CDNS}; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com "
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com; "
    "font-src 'self' data: https://fonts.gstatic.com https://cdnjs.cloudflare.com; "
)
_TAIL = (
    "worker-src 'self' blob:; frame-src 'none'; form-action 'none'; "
    "base-uri 'none'; object-src 'none'"
)


@pytest.fixture(autouse=True)
def _origin(monkeypatch):
    monkeypatch.setattr(f"{_ORIGINS}.SERVER_BASE_URL", _APP)
    monkeypatch.setattr(f"{_ORIGINS}.PUBLIC_APP_URL", f"{_APP}/")


def _version_csp(*, sandboxed: bool, frame_ancestors: str | None = None) -> str:
    headers = artifact_proxy._headers(
        "text/html; charset=utf-8",
        "etag",
        cache_control="private, no-cache",
        sandboxed=sandboxed,
        frame_ancestors=frame_ancestors,
        prefix="/api/v1/artifacts/g/G/",
        host="app.example.com",
    )
    return dict(headers)["content-security-policy"]


def test_a_live_file_reaches_its_prefix_and_no_other_host():
    assert serve._wsfiles_csp("/api/v1/wsfiles/g/G/", "app.example.com") == (
        f"{_SANDBOX}{_LOADS}"
        "img-src 'self' data: blob:; media-src 'self' data: blob:; "
        f"connect-src {_APP}/api/v1/wsfiles/g/G/; {_TAIL}"
    )
    assert serve._wsfiles_csp(None, None) == (
        f"{_SANDBOX}{_LOADS}"
        "img-src 'self' data: blob:; media-src 'self' data: blob:; "
        f"connect-src 'none'; {_TAIL}"
    )


def test_a_version_on_the_api_origin_is_sandboxed_to_its_grant():
    assert _version_csp(sandboxed=True, frame_ancestors="'self' " + _APP) == (
        f"{_SANDBOX}{_LOADS}"
        "img-src 'self' data: blob: https:; media-src 'self' data: blob:; "
        f"connect-src {_APP}/api/v1/artifacts/g/G/ {_CDNS}; {_TAIL}; "
        f"frame-ancestors 'self' {_APP}"
    )


def test_a_version_on_its_own_host_reaches_itself():
    assert _version_csp(sandboxed=False) == (
        f"{_LOADS}"
        "img-src 'self' data: blob: https:; media-src 'self' data: blob:; "
        f"connect-src 'self' {_CDNS}; {_TAIL}"
    )
