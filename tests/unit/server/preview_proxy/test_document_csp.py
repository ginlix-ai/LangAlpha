"""The one policy agent-written HTML is served under, live and published.

Pinned whole, byte for byte, because the promise is that a page never looks
broken for want of a directive: anything added here is a kind of load some page
stops doing. The sandbox is what stays: never allow-same-origin on the API
origin, where it would be the app's own.
"""

from __future__ import annotations

from src.server.app.workspace_files import serve
from src.server.preview_proxy import artifacts as artifact_proxy

_APP = "https://app.example.com"
_SANDBOX = (
    "sandbox allow-scripts allow-forms allow-modals allow-downloads "
    "allow-popups allow-popups-to-escape-sandbox; "
)
_LOADS = "default-src * data: blob: 'unsafe-inline' 'unsafe-eval'"


def _version_csp(*, sandboxed: bool, frame_ancestors: str | None = None) -> str:
    headers = artifact_proxy._headers(
        "text/html; charset=utf-8",
        "etag",
        cache_control="private, no-cache",
        sandboxed=sandboxed,
        frame_ancestors=frame_ancestors,
    )
    return dict(headers)["content-security-policy"]


def test_a_live_file_is_sandboxed_and_otherwise_unlimited():
    assert serve._WSFILES_CSP == f"{_SANDBOX}{_LOADS}"


def test_a_version_on_the_api_origin_is_sandboxed_the_same():
    assert _version_csp(sandboxed=True, frame_ancestors=f"'self' {_APP}") == (
        f"{_SANDBOX}{_LOADS}; frame-ancestors 'self' {_APP}"
    )


def test_a_versions_page_may_frame_the_page_a_missing_file_answers_with():
    csp = dict(artifact_proxy.error_page(404, _APP).headers)["content-security-policy"]
    assert csp.endswith(f"; frame-ancestors 'self' {_APP}")


def test_a_version_on_its_own_host_needs_no_sandbox():
    assert _version_csp(sandboxed=False) == _LOADS
