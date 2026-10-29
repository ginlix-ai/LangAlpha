"""Tracing never records a preview request.

The client instrumentations would record each upstream URL, query and all,
and each capability lookup; both honor the suppression key the proxy sets.
"""

from __future__ import annotations

import pytest
from opentelemetry import context as otel_context
from opentelemetry.context import _SUPPRESS_INSTRUMENTATION_KEY

from src.server.preview_proxy import app as proxy_app
from src.server.preview_proxy.app import PreviewProxy
from src.server.services.preview_base import PreviewBase


def _scope(host: str) -> dict:
    return {
        "type": "http",
        "method": "GET",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": [(b"host", host.encode())],
    }


@pytest.mark.asyncio
async def test_a_preview_request_runs_untraced_and_an_app_request_does_not(monkeypatch):
    suppressed: dict[str, object] = {}

    async def resolve_host(host, *, base):
        suppressed["preview"] = otel_context.get_value(_SUPPRESS_INSTRUMENTATION_KEY)
        return None

    async def app(scope, receive, send):
        suppressed["app"] = otel_context.get_value(_SUPPRESS_INSTRUMENTATION_KEY)

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        return None

    monkeypatch.setattr(proxy_app, "resolve_host", resolve_host)
    proxy = PreviewProxy(PreviewBase("https", "preview.example.com"), app=app)

    await proxy(_scope("8080-" + "a" * 32 + ".preview.example.com"), receive, send)
    await proxy(_scope("app.example.com"), receive, send)

    assert suppressed == {"preview": True, "app": None}
